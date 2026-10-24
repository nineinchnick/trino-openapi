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
package pl.net.was.authentication;

import io.airlift.http.client.BearerTokenProvider;
import io.airlift.http.client.HttpStatus;
import io.airlift.http.client.UnexpectedResponseException;
import io.trino.spi.TrinoException;

import java.io.UncheckedIOException;
import java.net.URI;
import java.util.function.Supplier;

import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static java.util.Objects.requireNonNullElse;
import static pl.net.was.OpenApiClient.withoutQuery;
import static pl.net.was.OpenApiErrorCode.OPENAPI_CONNECTION_ERROR;
import static pl.net.was.OpenApiErrorCode.OPENAPI_HTTP_ERROR;

/**
 * Reports failed requests to the token endpoint like failed requests to the API, with a connector error code,
 * and without the response, because it can include the credentials sent in the token request.
 */
final class ErrorReportingTokenProvider
        implements BearerTokenProvider
{
    private final BearerTokenProvider delegate;
    private final URI tokenUrl;

    ErrorReportingTokenProvider(BearerTokenProvider delegate, URI tokenUrl)
    {
        this.delegate = requireNonNull(delegate, "delegate is null");
        this.tokenUrl = requireNonNull(tokenUrl, "tokenUrl is null");
    }

    @Override
    public String getToken()
    {
        return reportingErrors(delegate::getToken);
    }

    @Override
    public boolean refreshToken(String rejectedToken)
    {
        return reportingErrors(() -> delegate.refreshToken(rejectedToken));
    }

    // the delegate retries token requests itself, so only its last failure is reported
    private <T> T reportingErrors(Supplier<T> call)
    {
        try {
            return call.get();
        }
        catch (UnexpectedResponseException e) {
            // a successful response that is not a valid token response is not an error response
            if (HttpStatus.familyForStatusCode(e.getStatusCode()) == HttpStatus.Family.SUCCESSFUL) {
                throw e;
            }
            throw new TrinoException(OPENAPI_HTTP_ERROR, format("%s with HTTP %d", describeFailedRequest(), e.getStatusCode()));
        }
        catch (UncheckedIOException e) {
            Throwable cause = e.getCause();
            throw new TrinoException(
                    OPENAPI_CONNECTION_ERROR,
                    format("%s: %s", describeFailedRequest(), requireNonNullElse(cause.getMessage(), cause.getClass().getName())),
                    cause);
        }
    }

    private String describeFailedRequest()
    {
        // the query string can contain credentials
        return format("Request POST %s for an OAuth token failed", withoutQuery(tokenUrl));
    }
}
