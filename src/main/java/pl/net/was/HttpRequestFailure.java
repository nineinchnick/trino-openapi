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

import com.google.common.io.ByteStreams;
import io.airlift.http.client.Response;

import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;

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

        private ErrorResponse(int statusCode, String responseBody, boolean responseBodyTruncated)
        {
            super("Server responded with error " + statusCode, null);
            this.statusCode = statusCode;
            this.responseBody = requireNonNull(responseBody, "responseBody is null");
            this.responseBodyTruncated = responseBodyTruncated;
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
            return new ErrorResponse(response.getStatusCode(), new String(body, UTF_8), truncated);
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
