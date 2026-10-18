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

import com.google.common.collect.ImmutableListMultimap;
import com.google.common.collect.ListMultimap;
import com.google.common.io.ByteStreams;
import io.airlift.http.client.HeaderName;
import io.airlift.http.client.Response;

import java.io.IOException;
import java.io.InputStream;
import java.time.DateTimeException;
import java.time.Duration;
import java.time.Instant;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.Optional;

import static io.airlift.http.client.HeaderNames.RETRY_AFTER;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;

/**
 * Thrown by response handlers, so the caller can decide how to report a failed request,
 * without the handlers knowing about the operation they're handling.
 */
abstract sealed class HttpRequestFailure
        extends RuntimeException
        permits HttpRequestFailure.ErrorResponse, HttpRequestFailure.TransportError
{
    // error responses can be large HTML pages, and only a short message is useful in logs and for errorPath
    static final int MAX_ERROR_BODY_BYTES = 64 * 1024;

    private HttpRequestFailure(String message, Throwable cause)
    {
        super(message, cause);
    }

    static final class ErrorResponse
            extends HttpRequestFailure
    {
        private final int statusCode;
        private final String responseBody;
        private final boolean responseBodyTruncated;
        private final Optional<Duration> retryAfter;
        private final ListMultimap<HeaderName, String> headers;

        private ErrorResponse(int statusCode, String responseBody, boolean responseBodyTruncated, Optional<Duration> retryAfter, ListMultimap<HeaderName, String> headers)
        {
            super("Server responded with error " + statusCode, null);
            this.statusCode = statusCode;
            this.responseBody = requireNonNull(responseBody, "responseBody is null");
            this.responseBodyTruncated = responseBodyTruncated;
            this.retryAfter = requireNonNull(retryAfter, "retryAfter is null");
            this.headers = ImmutableListMultimap.copyOf(requireNonNull(headers, "headers is null"));
        }

        static ErrorResponse read(Response response)
        {
            byte[] body;
            try (InputStream input = response.getInputStream()) {
                body = ByteStreams.limit(input, MAX_ERROR_BODY_BYTES + 1).readAllBytes();
            }
            catch (IOException e) {
                throw new TransportError(e);
            }
            boolean truncated = body.length > MAX_ERROR_BODY_BYTES;
            if (truncated) {
                body = Arrays.copyOf(body, MAX_ERROR_BODY_BYTES);
            }
            return new ErrorResponse(
                    response.getStatusCode(),
                    new String(body, UTF_8),
                    truncated,
                    response.getHeader(RETRY_AFTER).flatMap(value -> parseRetryAfter(value, Instant.now())),
                    response.getHeaders());
        }

        int getStatusCode()
        {
            return statusCode;
        }

        String getResponseBody()
        {
            return responseBody;
        }

        boolean isResponseBodyTruncated()
        {
            return responseBodyTruncated;
        }

        Optional<Duration> getRetryAfter()
        {
            return retryAfter;
        }

        Optional<String> getHeader(String name)
        {
            return headers.get(HeaderName.of(name)).stream().findFirst();
        }

        // the value is either a number of seconds, or an HTTP date, see RFC 9110, section 10.2.3
        private static Optional<Duration> parseRetryAfter(String value, Instant now)
        {
            String trimmed = value.trim();
            try {
                long seconds = Long.parseLong(trimmed);
                if (seconds < 0) {
                    return Optional.empty();
                }
                return Optional.of(Duration.ofSeconds(seconds));
            }
            catch (NumberFormatException _) {
                // not a number of seconds, so try parsing it as a date
            }
            try {
                Instant at = ZonedDateTime.parse(trimmed, DateTimeFormatter.RFC_1123_DATE_TIME).toInstant();
                return Optional.of(at.isBefore(now) ? Duration.ZERO : Duration.between(now, at));
            }
            catch (DateTimeException _) {
                return Optional.empty();
            }
        }
    }

    static final class TransportError
            extends HttpRequestFailure
    {
        TransportError(Exception cause)
        {
            super(requireNonNull(cause, "cause is null").getMessage(), cause);
        }
    }
}
