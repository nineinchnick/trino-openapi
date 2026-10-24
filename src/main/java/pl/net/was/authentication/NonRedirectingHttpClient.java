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
import io.airlift.http.client.ResponseHandler;
import io.airlift.http.client.StreamingResponse;

import static io.airlift.http.client.Request.Builder.fromRequest;
import static java.util.Objects.requireNonNull;

// following a redirect of a token request would send the client secret and the password to another location,
// and token providers from Airlift don't disable redirects themselves
final class NonRedirectingHttpClient
        implements HttpClient
{
    private final HttpClient delegate;

    NonRedirectingHttpClient(HttpClient delegate)
    {
        this.delegate = requireNonNull(delegate, "delegate is null");
    }

    @Override
    public <T, E extends Exception> T execute(Request request, ResponseHandler<T, E> responseHandler)
            throws E
    {
        return delegate.execute(withoutRedirects(request), responseHandler);
    }

    @Override
    public <T, E extends Exception> HttpResponseFuture<T> executeAsync(Request request, ResponseHandler<T, E> responseHandler)
    {
        return delegate.executeAsync(withoutRedirects(request), responseHandler);
    }

    @Override
    public StreamingResponse executeStreaming(Request request)
    {
        return delegate.executeStreaming(withoutRedirects(request));
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

    private static Request withoutRedirects(Request request)
    {
        if (!request.isFollowRedirects()) {
            return request;
        }
        return fromRequest(request)
                .setFollowRedirects(false)
                .build();
    }
}
