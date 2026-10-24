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

import java.time.Duration;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

record RetryOptions(int maxRetries, Duration initialDelay, Duration maxDelay)
{
    RetryOptions
    {
        checkArgument(maxRetries >= 0, "maxRetries is negative");
        requireNonNull(initialDelay, "initialDelay is null");
        requireNonNull(maxDelay, "maxDelay is null");
        checkArgument(!initialDelay.isNegative(), "initialDelay is negative");
        checkArgument(maxDelay.compareTo(initialDelay) >= 0, "maxDelay is less than initialDelay");
    }
}
