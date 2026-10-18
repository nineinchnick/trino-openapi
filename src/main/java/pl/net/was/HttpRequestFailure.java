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

import static java.util.Objects.requireNonNull;

/**
 * Thrown by response handlers, so the caller can decide how to report a failed request,
 * without the handlers knowing about the operation they're handling.
 */
abstract sealed class HttpRequestFailure
        extends RuntimeException
        permits HttpRequestFailure.ErrorResponse, HttpRequestFailure.TransportError
{
    private HttpRequestFailure(String message, Throwable cause)
    {
        super(message, cause);
    }

    static final class ErrorResponse
            extends HttpRequestFailure
    {
        private final int statusCode;
        private final String responseBody;

        ErrorResponse(int statusCode, String responseBody)
        {
            super("Server responded with error " + statusCode, null);
            this.statusCode = statusCode;
            this.responseBody = requireNonNull(responseBody, "responseBody is null");
        }

        int getStatusCode()
        {
            return statusCode;
        }

        String getResponseBody()
        {
            return responseBody;
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
