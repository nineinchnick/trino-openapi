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
import java.time.Instant;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

/**
 * Response headers that tell when an exceeded rate limit resets, for APIs that don't send Retry-After, like GitHub.
 */
public record RateLimitHeaders(String resetHeader, ResetFormat resetFormat, Optional<String> remainingHeader)
{
    public RateLimitHeaders
    {
        requireNonNull(resetHeader, "resetHeader is null");
        requireNonNull(resetFormat, "resetFormat is null");
        requireNonNull(remainingHeader, "remainingHeader is null");
    }

    /**
     * Returns how long to wait until the rate limit resets, if the response says it's exhausted.
     */
    Optional<Duration> getResetDelay(HttpRequestFailure.ErrorResponse response, Instant now)
    {
        // a response with requests remaining failed for another reason, like missing permissions
        if (remainingHeader.isPresent() && !response.getHeader(remainingHeader.get()).map(value -> value.trim().equals("0")).orElse(false)) {
            return Optional.empty();
        }
        Optional<Long> reset = response.getHeader(resetHeader).flatMap(RateLimitHeaders::parseLong);
        if (reset.isEmpty() || reset.get() < 0) {
            return Optional.empty();
        }
        return Optional.of(switch (resetFormat) {
            case EPOCH_SECONDS -> {
                Instant resetAt = Instant.ofEpochSecond(reset.get());
                yield resetAt.isBefore(now) ? Duration.ZERO : Duration.between(now, resetAt);
            }
            case DELAY_SECONDS -> Duration.ofSeconds(reset.get());
        });
    }

    private static Optional<Long> parseLong(String value)
    {
        try {
            return Optional.of(Long.parseLong(value.trim()));
        }
        catch (NumberFormatException _) {
            return Optional.empty();
        }
    }

    public enum ResetFormat
    {
        // seconds since the Unix epoch, in UTC, like GitHub's x-ratelimit-reset
        EPOCH_SECONDS("epochSeconds"),
        // seconds from receiving the response, like Retry-After
        DELAY_SECONDS("delaySeconds");

        private final String specValue;

        ResetFormat(String specValue)
        {
            this.specValue = specValue;
        }

        public String getSpecValue()
        {
            return specValue;
        }
    }
}
