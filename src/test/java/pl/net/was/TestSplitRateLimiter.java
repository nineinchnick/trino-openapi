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

import io.airlift.units.Duration;
import org.junit.jupiter.api.Test;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class TestSplitRateLimiter
{
    @Test
    void testDelaysOnlyFirstRequest()
    {
        SplitRateLimiter limiter = new SplitRateLimiter(new SplitRequestRate(10, new Duration(300, MILLISECONDS)));
        long start = System.nanoTime();
        limiter.acquire();
        long firstRequest = System.nanoTime();
        assertThat(firstRequest - start).isGreaterThanOrEqualTo(MILLISECONDS.toNanos(300));

        limiter.acquire();
        limiter.acquire();
        // allow for the timer resolution, since the rate limiter doesn't round up the time it waits
        assertThat(System.nanoTime() - firstRequest).isGreaterThanOrEqualTo(MILLISECONDS.toNanos(190));
    }

    @Test
    void testRateMustBePositive()
    {
        assertThatThrownBy(() -> new SplitRequestRate(0, new Duration(0, MILLISECONDS)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("requestsPerSecond must be positive");
    }
}
