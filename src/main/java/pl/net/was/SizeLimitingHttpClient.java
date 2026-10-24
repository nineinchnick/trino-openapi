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
import io.airlift.http.client.HttpVersion;
import io.airlift.http.client.Request;
import io.airlift.http.client.RequestStats;
import io.airlift.http.client.Response;
import io.airlift.http.client.ResponseHandler;
import io.airlift.http.client.StreamingResponse;
import io.airlift.units.DataSize;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;

import static java.util.Objects.requireNonNull;

// limits bodies as they're received, so responses that are not compressed are limited too,
// and the cache, which reads them into memory before storing them, doesn't have to know the limit
final class SizeLimitingHttpClient
        implements HttpClient
{
    private final HttpClient delegate;
    private final DataSize maxResponseSize;

    public SizeLimitingHttpClient(HttpClient delegate, DataSize maxResponseSize)
    {
        this.delegate = requireNonNull(delegate, "delegate is null");
        this.maxResponseSize = requireNonNull(maxResponseSize, "maxResponseSize is null");
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
                return responseHandler.handle(request, new SizeLimitedResponse(response, maxResponseSize));
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

    private static class SizeLimitedResponse
            implements Response
    {
        private final Response response;
        private final DataSize maxSize;

        private SizeLimitedResponse(Response response, DataSize maxSize)
        {
            this.response = response;
            this.maxSize = maxSize;
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
            return new MaxSizeInputStream(response.getInputStream(), maxSize);
        }

        @Override
        public long getBytesRead()
        {
            return response.getBytesRead();
        }
    }
}
