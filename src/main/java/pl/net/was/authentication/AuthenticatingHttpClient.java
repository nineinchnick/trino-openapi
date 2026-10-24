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

import io.airlift.http.client.HttpClient;
import io.airlift.http.client.Request;
import io.airlift.http.client.RequestStats;
import io.airlift.http.client.Response;
import io.airlift.http.client.ResponseHandler;
import io.airlift.http.client.StreamingResponse;

import static io.airlift.http.client.HeaderNames.WWW_AUTHENTICATE;
import static java.util.Objects.requireNonNull;

// unlike a request filter of the HTTP client, a wrapper sees both the credentials it sent and the response
public class AuthenticatingHttpClient
        implements HttpClient
{
    private static final int UNAUTHORIZED = 401;
    private static final String BEARER = "Bearer";

    private final HttpClient delegate;
    private final Authentication authentication;
    private final UserCredentials userCredentials;

    public AuthenticatingHttpClient(HttpClient delegate, Authentication authentication, UserCredentials userCredentials)
    {
        this.delegate = requireNonNull(delegate, "delegate is null");
        this.authentication = requireNonNull(authentication, "authentication is null");
        this.userCredentials = requireNonNull(userCredentials, "userCredentials is null");
    }

    /**
     * Sends the request with credentials, and if the server rejects an OAuth token with a Bearer challenge,
     * sends it once more with a new token. That's not a retry of a transient error, so it's not counted as one.
     */
    @Override
    public <T, E extends Exception> T execute(Request request, ResponseHandler<T, E> responseHandler)
            throws E
    {
        Request authenticatedRequest = authentication.filterRequest(request, userCredentials);
        try {
            return delegate.execute(authenticatedRequest, new RejectedTokenHandler<>(request, authenticatedRequest, responseHandler));
        }
        catch (TokenRefreshedException _) {
            // the server rejects a request with 401 before processing it, so sending it again can't apply it twice
            return delegate.execute(authentication.filterRequest(request, userCredentials), responseHandler);
        }
    }

    @Override
    public <T, E extends Exception> HttpResponseFuture<T> executeAsync(Request request, ResponseHandler<T, E> responseHandler)
    {
        return delegate.executeAsync(authentication.filterRequest(request, userCredentials), responseHandler);
    }

    @Override
    public StreamingResponse executeStreaming(Request request)
    {
        return delegate.executeStreaming(authentication.filterRequest(request, userCredentials));
    }

    @Override
    public RequestStats getStats()
    {
        return delegate.getStats();
    }

    @Override
    public void close()
    {
        delegate.close();
    }

    @Override
    public boolean isClosed()
    {
        return delegate.isClosed();
    }

    private final class RejectedTokenHandler<T, E extends Exception>
            implements ResponseHandler<T, E>
    {
        private final Request request;
        private final Request authenticatedRequest;
        private final ResponseHandler<T, E> responseHandler;

        private RejectedTokenHandler(Request request, Request authenticatedRequest, ResponseHandler<T, E> responseHandler)
        {
            this.request = request;
            this.authenticatedRequest = authenticatedRequest;
            this.responseHandler = responseHandler;
        }

        @Override
        public T handleException(Request request, Exception exception)
                throws E
        {
            return responseHandler.handleException(request, exception);
        }

        @Override
        public T handle(Request request, Response response)
                throws E
        {
            if (response.getStatusCode() == UNAUTHORIZED
                    && response.getHeaders(WWW_AUTHENTICATE).stream().anyMatch(AuthenticatingHttpClient::containsBearerChallenge)
                    && authentication.refreshRejectedTokens(this.request, authenticatedRequest, userCredentials)) {
                throw new TokenRefreshedException();
            }
            return responseHandler.handle(request, response);
        }
    }

    // a header can hold several challenges separated by commas, which can also be in quoted parameter values, see RFC 9110, section 11.6.1
    private static boolean containsBearerChallenge(String header)
    {
        int challengeStart = 0;
        boolean quoted = false;
        boolean escaped = false;
        for (int index = 0; index <= header.length(); index++) {
            if (index == header.length() && (quoted || escaped)) {
                return false;
            }
            if (index == header.length() || (header.charAt(index) == ',' && !quoted)) {
                if (isBearerChallenge(header.substring(challengeStart, index).strip())) {
                    return true;
                }
                challengeStart = index + 1;
                continue;
            }
            char character = header.charAt(index);
            if (escaped) {
                escaped = false;
            }
            else if (quoted && character == '\\') {
                escaped = true;
            }
            else if (character == '"') {
                quoted = !quoted;
            }
        }
        return false;
    }

    private static boolean isBearerChallenge(String challenge)
    {
        if (challenge.equalsIgnoreCase(BEARER)) {
            return true;
        }
        // an auth-param named bearer, like bearer = x, is not a challenge
        return challenge.length() > BEARER.length()
                && challenge.regionMatches(true, 0, BEARER, 0, BEARER.length())
                && Character.isWhitespace(challenge.charAt(BEARER.length()))
                && !challenge.substring(BEARER.length()).stripLeading().startsWith("=");
    }

    private static final class TokenRefreshedException
            extends RuntimeException
    {
        private TokenRefreshedException()
        {
            super(null, null, false, false);
        }
    }
}
