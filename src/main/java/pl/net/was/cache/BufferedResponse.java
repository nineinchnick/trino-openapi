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

package pl.net.was.cache;

import com.google.common.collect.ImmutableListMultimap;
import com.google.common.collect.ListMultimap;
import io.airlift.http.client.HeaderName;
import io.airlift.http.client.HttpVersion;
import io.airlift.http.client.Request;
import io.airlift.http.client.Response;
import io.airlift.http.client.ResponseHandler;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;

import static java.util.Objects.requireNonNull;

// a response read into memory, so it can be stored, and then passed to the caller's response handler
// responses are never compared, so comparing the body by reference in equals() doesn't matter
@SuppressWarnings("ArrayRecordComponent")
record BufferedResponse(int statusCode, ListMultimap<HeaderName, String> headers, byte[] body)
        implements Response
{
    static final ResponseHandler<BufferedResponse, RuntimeException> HANDLER = new ResponseHandler<>()
    {
        @Override
        public BufferedResponse handleException(Request request, Exception exception)
        {
            throw new FailedRequestException(exception);
        }

        @Override
        public BufferedResponse handle(Request request, Response response)
        {
            try (InputStream input = response.getInputStream()) {
                return new BufferedResponse(response.getStatusCode(), ImmutableListMultimap.copyOf(response.getHeaders()), input.readAllBytes());
            }
            catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }
    };

    BufferedResponse
    {
        headers = ImmutableListMultimap.copyOf(requireNonNull(headers, "headers is null"));
        requireNonNull(body, "body is null");
    }

    @Override
    public HttpVersion getHttpVersion()
    {
        return HttpVersion.HTTP_1;
    }

    @Override
    public int getStatusCode()
    {
        return statusCode;
    }

    @Override
    public ListMultimap<HeaderName, String> getHeaders()
    {
        return headers;
    }

    @Override
    public Content getContent()
    {
        return new BytesContent(body);
    }

    @Override
    public InputStream getInputStream()
    {
        return new ByteArrayInputStream(body);
    }

    @Override
    public long getBytesRead()
    {
        return body.length;
    }

    // carries the exception of a failed request, so it can be passed to the caller's response handler
    static class FailedRequestException
            extends RuntimeException
    {
        FailedRequestException(Exception cause)
        {
            super(cause);
        }

        Exception getFailure()
        {
            return (Exception) getCause();
        }
    }
}
