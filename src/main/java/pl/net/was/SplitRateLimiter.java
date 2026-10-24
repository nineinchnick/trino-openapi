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

import com.google.common.util.concurrent.RateLimiter;

import static com.google.common.util.concurrent.Uninterruptibles.sleepUninterruptibly;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

public class SplitRateLimiter
{
    private final SplitRequestRate rate;
    private RateLimiter rateLimiter;

    public SplitRateLimiter(SplitRequestRate rate)
    {
        this.rate = rate;
    }

    public synchronized void acquire()
    {
        if (rateLimiter == null) {
            sleepUninterruptibly(rate.firstRequestDelay().toMillis(), MILLISECONDS);
            // created after the delay, because a rate limiter stores permits while it's not used, which would let the next requests through without waiting
            rateLimiter = RateLimiter.create(rate.requestsPerSecond());
        }
        rateLimiter.acquire();
    }
}
