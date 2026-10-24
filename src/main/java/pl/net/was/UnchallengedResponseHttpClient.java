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

import com.google.common.collect.ListMultimap;
import io.airlift.http.client.HeaderName;
import io.airlift.http.client.HttpClient;
import io.airlift.http.client.HttpStatus;
import io.airlift.http.client.HttpVersion;
import io.airlift.http.client.Request;
import io.airlift.http.client.RequestStats;
import io.airlift.http.client.Response;
import io.airlift.http.client.ResponseHandler;
import io.airlift.http.client.StreamingResponse;
import org.eclipse.jetty.client.ContentResponse;
import org.eclipse.jetty.client.HttpResponseException;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;

import static java.util.Objects.requireNonNull;

// Jetty treats 401 and 407 responses without a WWW-Authenticate or Proxy-Authenticate header as protocol violations,
// and fails reading their body, but many APIs send them, and the connector authenticates requests itself.
// Airlift doesn't allow removing the authentication protocol handlers from the Jetty client, so this reads the body from the failure.
public class UnchallengedResponseHttpClient
        implements HttpClient
{
    private final HttpClient delegate;

    public UnchallengedResponseHttpClient(HttpClient delegate)
    {
        this.delegate = requireNonNull(delegate, "delegate is null");
    }

    @Override
    public <T, E extends Exception> T execute(Request request, ResponseHandler<T, E> responseHandler)
            throws E
    {
        return delegate.execute(request, new ResponseHandler<T, E>()
        {
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
                if (response.getStatusCode() != HttpStatus.UNAUTHORIZED.code() && response.getStatusCode() != HttpStatus.PROXY_AUTHENTIATION_REQUIRED.code()) {
                    return responseHandler.handle(request, response);
                }
                return responseHandler.handle(request, new UnchallengedResponse(response));
            }
        });
    }

    @Override
    public <T, E extends Exception> HttpResponseFuture<T> executeAsync(Request request, ResponseHandler<T, E> responseHandler)
    {
        return delegate.executeAsync(request, responseHandler);
    }

    @Override
    public StreamingResponse executeStreaming(Request request)
    {
        return delegate.executeStreaming(request);
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

    private static class UnchallengedResponse
            implements Response
    {
        private final Response response;

        private UnchallengedResponse(Response response)
        {
            this.response = response;
        }

        @Override
        public HttpVersion getHttpVersion()
        {
            return response.getHttpVersion();
        }

        @Override
        public int getStatusCode()
        {
            return response.getStatusCode();
        }

        @Override
        public ListMultimap<HeaderName, String> getHeaders()
        {
            return response.getHeaders();
        }

        @Override
        public Content getContent()
        {
            try {
                return new InputStreamContent(getInputStream());
            }
            catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }

        @Override
        public InputStream getInputStream()
                throws IOException
        {
            try (InputStream input = response.getInputStream()) {
                return new ByteArrayInputStream(input.readAllBytes());
            }
            catch (HttpResponseException e) {
                // the authentication protocol handler buffers the whole body before failing the response
                if (!(e.getResponse() instanceof ContentResponse contentResponse)) {
                    throw e;
                }
                return new ByteArrayInputStream(contentResponse.getContent());
            }
        }

        @Override
        public long getBytesRead()
        {
            return response.getBytesRead();
        }
    }
}
