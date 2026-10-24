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

import com.google.common.collect.ImmutableListMultimap;
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
import java.util.Locale;
import java.util.zip.GZIPInputStream;

import static io.airlift.http.client.HeaderNames.ACCEPT_ENCODING;
import static io.airlift.http.client.HeaderNames.CONTENT_ENCODING;
import static io.airlift.http.client.HeaderNames.CONTENT_LENGTH;
import static java.util.Objects.requireNonNull;

// asks for gzip-compressed responses, which the Airlift HTTP client doesn't, and decompresses them for response handlers;
// it wraps the cache, so cached responses stay compressed
// the Airlift HTTP client limits only the compressed size, and a small response of repetitive JSON can expand to more than the heap,
// so the size of decompressed bodies is limited too
public class DecompressingHttpClient
        implements HttpClient
{
    private static final int BUFFER_SIZE = 64 * 1024;

    private final HttpClient delegate;
    private final DataSize maxResponseSize;

    public DecompressingHttpClient(HttpClient delegate, DataSize maxResponseSize)
    {
        this.delegate = requireNonNull(delegate, "delegate is null");
        this.maxResponseSize = requireNonNull(maxResponseSize, "maxResponseSize is null");
    }

    @Override
    public <T, E extends Exception> T execute(Request request, ResponseHandler<T, E> responseHandler)
            throws E
    {
        Request compressedRequest = request.getHeaders().containsKey(ACCEPT_ENCODING)
                ? request
                : Request.Builder.fromRequest(request).setHeader(ACCEPT_ENCODING, "gzip").build();
        return delegate.execute(compressedRequest, new ResponseHandler<T, E>()
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
                if (!isGzip(response)) {
                    return responseHandler.handle(request, response);
                }
                return responseHandler.handle(request, new DecompressedResponse(response, maxResponseSize));
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

    private static boolean isGzip(Response response)
    {
        return response.getHeader(CONTENT_ENCODING)
                .map(encoding -> encoding.trim().toLowerCase(Locale.ENGLISH))
                .filter(encoding -> encoding.equals("gzip") || encoding.equals("x-gzip"))
                .isPresent();
    }

    private static class DecompressedResponse
            implements Response
    {
        private final Response response;
        private final DataSize maxSize;
        private final ListMultimap<HeaderName, String> headers;

        private DecompressedResponse(Response response, DataSize maxSize)
        {
            this.response = response;
            this.maxSize = maxSize;
            // the headers describe the compressed body
            ImmutableListMultimap.Builder<HeaderName, String> headers = ImmutableListMultimap.builder();
            response.getHeaders().entries().stream()
                    .filter(entry -> !entry.getKey().equals(CONTENT_ENCODING) && !entry.getKey().equals(CONTENT_LENGTH))
                    .forEach(entry -> headers.put(entry.getKey(), entry.getValue()));
            this.headers = headers.build();
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
            return headers;
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
            return new MaxSizeInputStream(new GZIPInputStream(response.getInputStream(), BUFFER_SIZE), maxSize);
        }

        @Override
        public long getBytesRead()
        {
            return response.getBytesRead();
        }
    }
}
