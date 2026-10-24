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

import static java.util.Objects.requireNonNull;

// unlike a request filter of the HTTP client, a wrapper sees both the credentials it sent and the response
public class AuthenticatingHttpClient
        implements HttpClient
{
    private final HttpClient delegate;
    private final Authentication authentication;

    public AuthenticatingHttpClient(HttpClient delegate, Authentication authentication)
    {
        this.delegate = requireNonNull(delegate, "delegate is null");
        this.authentication = requireNonNull(authentication, "authentication is null");
    }

    @Override
    public <T, E extends Exception> T execute(Request request, ResponseHandler<T, E> responseHandler)
            throws E
    {
        return delegate.execute(authentication.filterRequest(request), responseHandler);
    }

    @Override
    public <T, E extends Exception> HttpResponseFuture<T> executeAsync(Request request, ResponseHandler<T, E> responseHandler)
    {
        return delegate.executeAsync(authentication.filterRequest(request), responseHandler);
    }

    @Override
    public StreamingResponse executeStreaming(Request request)
    {
        return delegate.executeStreaming(authentication.filterRequest(request));
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
}
