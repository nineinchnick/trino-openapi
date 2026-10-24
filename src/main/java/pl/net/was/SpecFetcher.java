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

import io.airlift.http.client.HttpClient;
import io.airlift.http.client.HttpStatus;
import io.airlift.http.client.Request;
import io.airlift.http.client.Response;
import io.airlift.http.client.ResponseHandler;
import io.trino.spi.TrinoException;
import pl.net.was.authentication.Authentication;
import pl.net.was.authentication.SpecLocationAuthentication;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.util.Optional;

import static io.airlift.http.client.HeaderNames.ACCEPT;
import static io.airlift.http.client.HeaderNames.USER_AGENT;
import static io.airlift.http.client.Request.Builder.prepareGet;
import static java.lang.String.format;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;
import static java.util.Objects.requireNonNullElse;
import static pl.net.was.OpenApiClient.USER_AGENT_VALUE;
import static pl.net.was.OpenApiClient.describeRedirect;
import static pl.net.was.OpenApiClient.withoutQuery;
import static pl.net.was.OpenApiErrorCode.OPENAPI_CONNECTION_ERROR;
import static pl.net.was.OpenApiErrorCode.OPENAPI_HTTP_ERROR;

/**
 * Fetches the spec with the HTTP client of the catalog, instead of the URL loader of the parser,
 * so its timeouts, proxy, and TLS settings apply, and transient failures are retried.
 */
final class SpecFetcher
{
    // same as the parser sends when it loads a URL
    private static final String ACCEPT_VALUE = "application/json, application/yaml, */*";

    private SpecFetcher() {}

    static String fetch(OpenApiConfig config, URI location, HttpClient httpClient)
    {
        requireNonNull(config, "config is null");
        requireNonNull(location, "location is null");
        requireNonNull(httpClient, "httpClient is null");
        Request request = createRequest(config, location);
        RetryOptions retryOptions = new RetryOptions(config.getMaxRetries(), config.getRetryInitialDelay().toJavaTime(), config.getRetryMaxDelay().toJavaTime());
        return new RetryPolicy(retryOptions, Optional.empty()).execute(
                request,
                true,
                () -> httpClient.execute(request, new SpecResponseHandler()),
                (failure, attempts, notRetriedReason) -> toTrinoException(location, failure, attempts, notRetriedReason));
    }

    private static Request createRequest(OpenApiConfig config, URI location)
    {
        Request.Builder builder = prepareGet()
                .setUri(location)
                .addHeader(USER_AGENT, USER_AGENT_VALUE)
                .addHeader(ACCEPT, ACCEPT_VALUE);
        if (config.getSpecLocationAuthentication() != SpecLocationAuthentication.CATALOG) {
            return builder.build();
        }
        // the HTTP client would send the credentials to any host the server redirects to
        return Authentication.addDefaultCredentials(config, builder.setFollowRedirects(false).build(), "GET " + withoutQuery(location));
    }

    private static TrinoException toTrinoException(URI location, HttpRequestFailure failure, int attempts, Optional<String> notRetriedReason)
    {
        // spec-location is security sensitive, because the query string or the user info can contain credentials
        String context = format("Failed to fetch the OpenAPI spec from %s", withoutQuery(location));
        String attemptsSuffix = attempts > 1 ? format(" after %d attempts", attempts) : "";
        return switch (failure) {
            case HttpRequestFailure.TransportError error -> new TrinoException(
                    OPENAPI_CONNECTION_ERROR,
                    format("%s%s: %s", context, attemptsSuffix, requireNonNullElse(error.getCause().getMessage(), error.getCause().getClass().getName())),
                    error.getCause());
            case HttpRequestFailure.ErrorResponse error -> new TrinoException(
                    OPENAPI_HTTP_ERROR,
                    format("%s with HTTP %d%s%s%s",
                            context,
                            error.getStatusCode(),
                            attemptsSuffix,
                            describeRedirect(location, error),
                            notRetriedReason.map(reason -> "; " + reason).orElse("")));
        };
    }

    private static class SpecResponseHandler
            implements ResponseHandler<String, RuntimeException>
    {
        @Override
        public String handleException(Request request, Exception exception)
        {
            throw new HttpRequestFailure.TransportError(exception);
        }

        @Override
        public String handle(Request request, Response response)
        {
            if (HttpStatus.familyForStatusCode(response.getStatusCode()) != HttpStatus.Family.SUCCESSFUL) {
                throw HttpRequestFailure.ErrorResponse.read(response);
            }
            // the parser reads documents as UTF-8 too
            try (InputStream input = response.getInputStream()) {
                return new String(input.readAllBytes(), UTF_8);
            }
            catch (IOException e) {
                throw new HttpRequestFailure.TransportError(e);
            }
        }
    }
}
