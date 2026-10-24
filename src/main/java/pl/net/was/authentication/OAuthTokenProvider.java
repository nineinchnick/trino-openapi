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

import com.fasterxml.jackson.annotation.JsonProperty;
import io.airlift.http.client.BearerTokenProvider;
import io.airlift.http.client.HttpClient;
import io.airlift.http.client.Request;
import io.trino.spi.TrinoException;

import java.time.Instant;

import static io.airlift.http.client.JsonResponseHandler.createSuccessfulJsonResponseHandler;
import static io.airlift.json.JsonCodec.jsonCodec;
import static java.lang.Math.min;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static pl.net.was.OpenApiErrorCode.OPENAPI_INVALID_RESPONSE;

/**
 * Gets access tokens by sending the same request to a token endpoint, and reuses them until shortly before they expire.
 */
final class OAuthTokenProvider
        implements BearerTokenProvider
{
    // RFC 6749 only recommends that token responses include expires_in, so when it's missing,
    // assume one hour, which is a common default lifetime of access tokens
    private static final long DEFAULT_TOKEN_EXPIRES_IN_SECONDS = 3600;
    private static final long MAX_TOKEN_REFRESH_MARGIN_SECONDS = 60;

    private final HttpClient httpClient;
    private final Request tokenRequest;
    // concurrent requests that need a new token wait for the one that gets it, instead of all requesting their own
    private final Object refreshLock = new Object();
    private volatile TokenWithExpiry cachedToken;

    OAuthTokenProvider(HttpClient httpClient, Request tokenRequest)
    {
        this.httpClient = requireNonNull(httpClient, "httpClient is null");
        this.tokenRequest = requireNonNull(tokenRequest, "tokenRequest is null");
    }

    @Override
    public String getToken()
    {
        TokenWithExpiry token = cachedToken;
        if (token != null && !token.isExpiringSoon()) {
            return token.token();
        }
        synchronized (refreshLock) {
            token = cachedToken;
            if (token != null && !token.isExpiringSoon()) {
                return token.token();
            }
            token = fetchToken();
            cachedToken = token;
            return token.token();
        }
    }

    private TokenWithExpiry fetchToken()
    {
        TokenResponse response = httpClient.execute(
                tokenRequest,
                // the safe handler doesn't put the response, which holds the token, in exception messages
                createSuccessfulJsonResponseHandler(jsonCodec(TokenResponse.class)));
        if (response.accessToken() == null) {
            throw new TrinoException(OPENAPI_INVALID_RESPONSE, format("Response of the token endpoint %s doesn't include an access_token", tokenRequest.getUri()));
        }
        long expiresIn = response.expiresIn() != null ? response.expiresIn() : DEFAULT_TOKEN_EXPIRES_IN_SECONDS;
        // a fixed margin would make tokens that expire sooner than it be fetched again on every request
        long refreshMargin = min(MAX_TOKEN_REFRESH_MARGIN_SECONDS, expiresIn / 2);
        return new TokenWithExpiry(response.accessToken(), Instant.now().plusSeconds(expiresIn - refreshMargin));
    }

    private record TokenWithExpiry(String token, Instant refreshAt)
    {
        boolean isExpiringSoon()
        {
            return Instant.now().isAfter(refreshAt);
        }
    }

    public record TokenResponse(
            @JsonProperty("token_type") String tokenType,
            @JsonProperty("access_token") String accessToken,
            @JsonProperty("expires_in") Long expiresIn) {}
}
