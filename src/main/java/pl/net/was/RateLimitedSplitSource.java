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

import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;

import static java.util.Objects.requireNonNull;

public class RateLimitedSplitSource
        implements ConnectorSplitSource
{
    private static final Logger log = Logger.get(RateLimitedSplitSource.class);

    private final ExecutorService executor;
    private final Iterator<? extends ConnectorSplit> splits;
    private final RateLimiter rateLimiter;

    public RateLimitedSplitSource(ExecutorService executor, Iterable<? extends ConnectorSplit> splits, double maxSplitsPerSecond)
    {
        this.executor = requireNonNull(executor, "executor is null");
        this.splits = requireNonNull(splits, "splits is null").iterator();
        this.rateLimiter = RateLimiter.create(maxSplitsPerSecond);
    }

    @Override
    public CompletableFuture<ConnectorSplitBatch> getNextBatch(int maxSize)
    {
        return CompletableFuture.supplyAsync(() -> new ConnectorSplitBatch(prepareNextBatch(maxSize), isFinished()), executor);
    }

    private List<ConnectorSplit> prepareNextBatch(int maxSize)
    {
        ImmutableList.Builder<ConnectorSplit> builder = ImmutableList.builder();
        int size = 0;
        // block before getting the first split to avoid returning empty batches
        rateLimiter.acquire();
        while (splits.hasNext() && size < maxSize) {
            builder.add(splits.next());
            size += 1;
            if (!rateLimiter.tryAcquire()) {
                List<ConnectorSplit> result = builder.build();
                log.debug("Got %d splits after rate limit, maxSize: %d, hasNext: %b", result.size(), maxSize, splits.hasNext());
                return result;
            }
        }
        List<ConnectorSplit> result = builder.build();
        log.debug("Got %d splits, maxSize: %d, hasNext: %b", result.size(), maxSize, splits.hasNext());
        return result;
    }

    @Override
    public void close()
    {
        splits.forEachRemaining(split -> {});
    }

    @Override
    public boolean isFinished()
    {
        return !splits.hasNext();
    }
}
