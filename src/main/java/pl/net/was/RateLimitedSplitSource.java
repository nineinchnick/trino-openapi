/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package pl.net.was;

import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.RateLimiter;
import io.airlift.log.Logger;
import io.trino.spi.connector.ConnectorSplit;
import io.trino.spi.connector.ConnectorSplitSource;
import io.trino.spi.connector.DynamicFilterSnapshot;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.function.Supplier;

import static java.util.Objects.requireNonNull;

/**
 * Generating a group of splits can make requests, so the next group is generated
 * in the background while splits from the previous one are returned.
 */
public class RateLimitedSplitSource
        implements ConnectorSplitSource
{
    private static final Logger log = Logger.get(RateLimitedSplitSource.class);

    private final ExecutorService executor;
    private final Iterator<Supplier<List<ConnectorSplit>>> splitGroups;
    private final RateLimiter rateLimiter;
    private final Deque<ConnectorSplit> splits = new ArrayDeque<>();
    private Optional<CompletableFuture<List<ConnectorSplit>>> nextSplitGroup = Optional.empty();
    private boolean closed;

    public RateLimitedSplitSource(ExecutorService executor, Iterable<Supplier<List<ConnectorSplit>>> splitGroups, double maxSplitsPerSecond)
    {
        this.executor = requireNonNull(executor, "executor is null");
        this.splitGroups = requireNonNull(splitGroups, "splitGroups is null").iterator();
        this.rateLimiter = RateLimiter.create(maxSplitsPerSecond);
    }

    @Override
    public synchronized CompletableFuture<List<ConnectorSplit>> getNextBatch(int maxSize, DynamicFilterSnapshot dynamicFilterSnapshot)
    {
        if (!splits.isEmpty()) {
            return CompletableFuture.supplyAsync(() -> prepareNextBatch(maxSize), executor);
        }
        startNextSplitGroup();
        if (nextSplitGroup.isEmpty()) {
            return CompletableFuture.completedFuture(List.of());
        }
        return nextSplitGroup.get().thenApplyAsync(
                splitGroup -> {
                    synchronized (this) {
                        nextSplitGroup = Optional.empty();
                        splits.addAll(splitGroup);
                        startNextSplitGroup();
                    }
                    return prepareNextBatch(maxSize);
                },
                executor);
    }

    private synchronized void startNextSplitGroup()
    {
        if (closed || nextSplitGroup.isPresent() || !splitGroups.hasNext()) {
            return;
        }
        nextSplitGroup = Optional.of(CompletableFuture.supplyAsync(splitGroups.next(), executor));
    }

    private List<ConnectorSplit> prepareNextBatch(int maxSize)
    {
        ImmutableList.Builder<ConnectorSplit> builder = ImmutableList.builder();
        int size = 0;
        // block before getting the first split to avoid returning empty batches
        rateLimiter.acquire();
        synchronized (this) {
            while (!splits.isEmpty() && size < maxSize) {
                builder.add(splits.removeFirst());
                size += 1;
                if (!rateLimiter.tryAcquire()) {
                    List<ConnectorSplit> result = builder.build();
                    log.debug("Got %d splits after rate limit, maxSize: %d, remaining: %d", result.size(), maxSize, splits.size());
                    return result;
                }
            }
            List<ConnectorSplit> result = builder.build();
            log.debug("Got %d splits, maxSize: %d, remaining: %d", result.size(), maxSize, splits.size());
            return result;
        }
    }

    @Override
    public synchronized void close()
    {
        closed = true;
        splits.clear();
        nextSplitGroup.ifPresent(future -> future.cancel(true));
        nextSplitGroup = Optional.empty();
    }

    @Override
    public synchronized boolean isFinished()
    {
        return closed || (splits.isEmpty() && nextSplitGroup.isEmpty() && !splitGroups.hasNext());
    }
}
