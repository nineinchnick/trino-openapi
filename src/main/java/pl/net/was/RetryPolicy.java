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

import com.google.common.base.Throwables;
import io.airlift.http.client.Request;
import io.airlift.log.Logger;

import javax.net.ssl.SSLHandshakeException;

import java.io.IOException;
import java.net.UnknownHostException;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeoutException;
import java.util.function.Supplier;

import static com.google.common.base.Strings.nullToEmpty;
import static io.airlift.units.Duration.succinctNanos;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

/**
 * Retries requests that failed with errors that are likely to be transient,
 * but only when repeating the request has the same effect as making it once,
 * or when the server didn't process it.
 */
final class RetryPolicy
{
    private static final Logger log = Logger.get(RetryPolicy.class);

    private static final Set<String> IDEMPOTENT_METHODS = Set.of("GET", "HEAD", "OPTIONS", "PUT", "DELETE");
    private static final Set<Integer> TRANSIENT_STATUS_CODES = Set.of(429, 502, 503, 504);
    private static final int FORBIDDEN = 403;
    private static final int TOO_MANY_REQUESTS = 429;
    // requests waiting for the same rate limit reset shouldn't all be sent at the same time
    private static final Duration RATE_LIMIT_RESET_JITTER = Duration.ofSeconds(1);

    private final int maxRetries;
    private final Duration initialDelay;
    private final Duration maxDelay;
    private final Optional<RateLimitHeaders> rateLimitHeaders;

    RetryPolicy(RetryOptions options, Optional<RateLimitHeaders> rateLimitHeaders)
    {
        requireNonNull(options, "options is null");
        this.maxRetries = options.maxRetries();
        this.initialDelay = options.initialDelay();
        this.maxDelay = options.maxDelay();
        this.rateLimitHeaders = requireNonNull(rateLimitHeaders, "rateLimitHeaders is null");
    }

    /**
     * Calls the request until it succeeds, it fails with an error that is not transient, or the retries are exhausted.
     * Failures are converted with the failure mapper, which also gets the number of attempts.
     * Reads, like searches using POST, are retried like idempotent requests.
     */
    <T> T execute(Request request, boolean read, Supplier<T> call, FailureMapper failureMapper)
    {
        boolean repeatable = IDEMPOTENT_METHODS.contains(request.getMethod()) || (read && request.getMethod().equals("POST"));
        int attempts = 0;
        while (true) {
            attempts++;
            try {
                return call.get();
            }
            catch (HttpRequestFailure failure) {
                int retry = attempts - 1;
                if ((!repeatable && !isTooManyRequests(failure)) || retry >= maxRetries || !isTransient(failure) || Thread.currentThread().isInterrupted()) {
                    throw failureMapper.map(failure, attempts, Optional.empty());
                }
                Optional<Duration> retryAfter = getRetryAfter(failure);
                // retrying before the server allows it can get the client banned, see GitHub's documentation about rate limits
                if (retryAfter.isPresent() && retryAfter.get().compareTo(maxDelay) > 0) {
                    throw failureMapper.map(failure, attempts, Optional.of(format(
                            "the server asked to wait %d seconds before retrying, which is longer than retry-max-delay (%s)",
                            retryAfter.get().toSeconds(),
                            succinctNanos(maxDelay.toNanos()))));
                }
                Optional<Duration> rateLimitReset = retryAfter.isPresent() ? Optional.empty() : getRateLimitReset(failure);
                if (rateLimitReset.isPresent() && rateLimitReset.get().compareTo(maxDelay) > 0) {
                    throw failureMapper.map(failure, attempts, Optional.of(format(
                            "the rate limit resets in %d seconds, which is longer than retry-max-delay (%s)",
                            rateLimitReset.get().toSeconds(),
                            succinctNanos(maxDelay.toNanos()))));
                }
                Duration delay = retryAfter
                        .or(() -> rateLimitReset.map(reset -> reset.plusNanos(ThreadLocalRandom.current().nextLong(RATE_LIMIT_RESET_JITTER.toNanos() + 1))))
                        .orElseGet(() -> getBackoffDelay(retry));
                log.info("Retrying %s %s after %s in %s ms, attempt %d of %d",
                        request.getMethod(),
                        OpenApiClient.withoutQuery(request.getUri()),
                        describe(failure),
                        delay.toMillis(),
                        attempts + 1,
                        maxRetries + 1);
                try {
                    Thread.sleep(delay);
                }
                catch (InterruptedException _) {
                    Thread.currentThread().interrupt();
                    throw failureMapper.map(failure, attempts, Optional.empty());
                }
            }
        }
    }

    @FunctionalInterface
    interface FailureMapper
    {
        RuntimeException map(HttpRequestFailure failure, int attempts, Optional<String> notRetriedReason);
    }

    private boolean isTransient(HttpRequestFailure failure)
    {
        return switch (failure) {
            case HttpRequestFailure.TransportError error -> isNetworkError(error.getCause());
            // some servers, like GitHub, respond to exceeded rate limits with 403, so only a server asking to retry explicitly,
            // or saying when the rate limit resets, as the spec declares, can tell it apart from missing permissions
            case HttpRequestFailure.ErrorResponse error -> TRANSIENT_STATUS_CODES.contains(error.getStatusCode())
                    || (error.getStatusCode() == FORBIDDEN && (error.getRetryAfter().isPresent() || getRateLimitReset(error).isPresent()));
        };
    }

    // the server rejects a request with 429 before processing it, so repeating it can't apply it twice
    private static boolean isTooManyRequests(HttpRequestFailure failure)
    {
        return failure instanceof HttpRequestFailure.ErrorResponse error && error.getStatusCode() == TOO_MANY_REQUESTS;
    }

    private static boolean isNetworkError(Throwable throwable)
    {
        List<Throwable> causes = Throwables.getCausalChain(throwable);
        // a response over the maximum length would be just as long when it's requested again
        if (causes.stream().anyMatch(RetryPolicy::isOverMaxLength)) {
            return false;
        }
        // an unknown host or an untrusted certificate is a configuration issue, and retrying it would only delay the failure
        return causes.stream()
                .anyMatch(cause -> cause instanceof TimeoutException || (cause instanceof IOException
                        && !(cause instanceof UnknownHostException)
                        && !(cause instanceof SSLHandshakeException)));
    }

    // the Airlift HTTP client fails a body over its own limit with a plain IOException, without a more specific type,
    // so it can only be told apart by its message
    private static boolean isOverMaxLength(Throwable throwable)
    {
        return throwable instanceof ResponseTooLargeException
                || (throwable instanceof IOException && nullToEmpty(throwable.getMessage()).contains("exceeded maximum length"));
    }

    private static Optional<Duration> getRetryAfter(HttpRequestFailure failure)
    {
        if (failure instanceof HttpRequestFailure.ErrorResponse error) {
            return error.getRetryAfter();
        }
        return Optional.empty();
    }

    private Optional<Duration> getRateLimitReset(HttpRequestFailure failure)
    {
        // other errors, like 503 Service Unavailable, are not caused by the rate limit, even if the response says when it resets
        if (rateLimitHeaders.isEmpty()
                || !(failure instanceof HttpRequestFailure.ErrorResponse error)
                || (error.getStatusCode() != FORBIDDEN && error.getStatusCode() != TOO_MANY_REQUESTS)) {
            return Optional.empty();
        }
        return rateLimitHeaders.get().getResetDelay(error, Instant.now());
    }

    /**
     * Uses "equal jitter": half of the exponential delay, plus a random part of the other half,
     * so concurrent requests that failed at the same time don't retry at the same time,
     * but every retry still waits at least half of the delay.
     */
    private Duration getBackoffDelay(int retry)
    {
        Duration delay = initialDelay;
        for (int i = 0; i < retry && delay.compareTo(maxDelay) < 0; i++) {
            delay = delay.multipliedBy(2);
        }
        long delayNanos = min(delay, maxDelay).toNanos();
        long halfNanos = delayNanos / 2;
        return Duration.ofNanos(halfNanos + ThreadLocalRandom.current().nextLong(delayNanos - halfNanos + 1));
    }

    private static String describe(HttpRequestFailure failure)
    {
        return switch (failure) {
            case HttpRequestFailure.ErrorResponse error -> "HTTP " + error.getStatusCode();
            case HttpRequestFailure.TransportError error -> error.getCause().getClass().getSimpleName();
        };
    }

    private static Duration min(Duration first, Duration second)
    {
        return first.compareTo(second) <= 0 ? first : second;
    }
}
