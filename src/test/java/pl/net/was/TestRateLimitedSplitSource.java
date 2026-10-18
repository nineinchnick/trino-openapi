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

import io.trino.spi.connector.ConnectorSplit;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

import static io.trino.spi.connector.DynamicFilterSnapshot.EMPTY;
import static java.util.concurrent.Executors.newCachedThreadPool;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;

@TestInstance(PER_CLASS)
public class TestRateLimitedSplitSource
{
    private final ExecutorService executor = newCachedThreadPool();

    @AfterAll
    public void tearDown()
    {
        executor.shutdownNow();
    }

    @Test
    public void returnsSplitsOfAllGroups()
            throws Exception
    {
        RateLimitedSplitSource source = new RateLimitedSplitSource(
                executor,
                List.of(() -> List.of(new TestingSplit(1), new TestingSplit(2)), List::of, () -> List.of(new TestingSplit(3))),
                Double.MAX_VALUE);
        List<ConnectorSplit> splits = new ArrayList<>();
        while (!source.isFinished()) {
            splits.addAll(source.getNextBatch(1, EMPTY).get(10, SECONDS));
        }
        assertThat(splits).containsExactly(new TestingSplit(1), new TestingSplit(2), new TestingSplit(3));
    }

    @Test
    public void returnsSplitsWhileGeneratingNextGroup()
            throws Exception
    {
        CountDownLatch nextGroupStarted = new CountDownLatch(1);
        CountDownLatch nextGroupAllowed = new CountDownLatch(1);
        Supplier<List<ConnectorSplit>> slowGroup = () -> {
            nextGroupStarted.countDown();
            try {
                nextGroupAllowed.await();
            }
            catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException(e);
            }
            return List.of(new TestingSplit(2));
        };
        RateLimitedSplitSource source = new RateLimitedSplitSource(executor, List.of(() -> List.of(new TestingSplit(1)), slowGroup), Double.MAX_VALUE);

        assertThat(source.getNextBatch(10, EMPTY).get(10, SECONDS)).containsExactly(new TestingSplit(1));
        assertThat(nextGroupStarted.await(10, SECONDS)).isTrue();
        assertThat(source.isFinished()).isFalse();
        nextGroupAllowed.countDown();
        assertThat(source.getNextBatch(10, EMPTY).get(10, SECONDS)).containsExactly(new TestingSplit(2));
        assertThat(source.isFinished()).isTrue();
    }

    @Test
    public void doesNotGenerateGroupsAfterClose()
            throws Exception
    {
        AtomicInteger generatedGroups = new AtomicInteger();
        Supplier<List<ConnectorSplit>> group = () -> List.of(new TestingSplit(generatedGroups.incrementAndGet()));
        RateLimitedSplitSource source = new RateLimitedSplitSource(executor, List.of(group, group, group), Double.MAX_VALUE);

        assertThat(source.getNextBatch(10, EMPTY).get(10, SECONDS)).containsExactly(new TestingSplit(1));
        source.close();
        assertThat(source.isFinished()).isTrue();
        // the second group could have started before closing, but not the third one
        assertThat(generatedGroups.get()).isLessThanOrEqualTo(2);
    }

    private record TestingSplit(int id)
            implements ConnectorSplit {}
}
