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
import io.airlift.http.client.HeaderName;
import io.airlift.http.client.HttpStatus;
import io.airlift.http.client.Request;
import io.airlift.http.client.Response;
import io.airlift.http.client.ResponseHandler;
import io.airlift.http.client.testing.TestingHttpClient;
import io.airlift.http.client.testing.TestingResponse;
import io.airlift.units.DataSize;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import pl.net.was.cache.CachingHttpClient;
import pl.net.was.cache.HttpCache;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Clock;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;
import java.util.zip.GZIPOutputStream;

import static io.airlift.http.client.HeaderNames.ACCEPT_ENCODING;
import static io.airlift.http.client.HeaderNames.CACHE_CONTROL;
import static io.airlift.http.client.HeaderNames.CONTENT_ENCODING;
import static io.airlift.http.client.HeaderNames.CONTENT_LENGTH;
import static io.airlift.http.client.Request.Builder.prepareGet;
import static io.airlift.units.DataSize.Unit.MEGABYTE;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;

class TestDecompressingHttpClient
{
    private static final URI ITEMS = URI.create("http://example.com/items");
    private static final String BODY = "[" + "{\"name\":\"item\"},".repeat(1000) + "{}]";

    @TempDir
    Path cacheDirectory;

    private final List<Request> serverRequests = new ArrayList<>();

    @Test
    void testCompressedResponsesAreDecompressed()
    {
        DecompressingHttpClient client = new DecompressingHttpClient(server(true));

        assertThat(client.execute(get(), RESPONSE)).isEqualTo("200 headers=[] " + BODY);
        assertThat(serverRequests.getFirst().getHeaders().get(ACCEPT_ENCODING)).containsExactly("gzip");
    }

    @Test
    void testUncompressedResponsesArePassedThrough()
    {
        DecompressingHttpClient client = new DecompressingHttpClient(server(false));

        assertThat(client.execute(get(), RESPONSE)).isEqualTo("200 headers=[" + BODY.length() + "] " + BODY);
    }

    @Test
    void testRequestedEncodingIsKept()
    {
        DecompressingHttpClient client = new DecompressingHttpClient(server(false));

        client.execute(Request.Builder.fromRequest(get()).setHeader(ACCEPT_ENCODING, "identity").build(), RESPONSE);
        assertThat(serverRequests.getFirst().getHeaders().get(ACCEPT_ENCODING)).containsExactly("identity");
    }

    @Test
    void testCacheStoresCompressedResponses()
            throws IOException
    {
        HttpCache cache = new HttpCache(cacheDirectory, DataSize.of(1, MEGABYTE), DataSize.of(1, MEGABYTE));
        DecompressingHttpClient client = new DecompressingHttpClient(new CachingHttpClient(server(true), cache, Clock.systemUTC()));

        assertThat(client.execute(get(), RESPONSE)).isEqualTo("200 headers=[] " + BODY);
        assertThat(client.execute(get(), RESPONSE)).isEqualTo("200 headers=[] " + BODY);
        assertThat(serverRequests).hasSize(1);
        long storedSize;
        try (Stream<Path> files = Files.list(cacheDirectory)) {
            storedSize = files.mapToLong(file -> file.toFile().length()).sum();
        }
        assertThat(storedSize).isLessThan(BODY.length() / 10);
    }

    private TestingHttpClient server(boolean compress)
    {
        return new TestingHttpClient(request -> {
            serverRequests.add(request);
            byte[] body = compress ? gzip(BODY) : BODY.getBytes(UTF_8);
            ImmutableListMultimap.Builder<HeaderName, String> headers = ImmutableListMultimap.builder();
            headers.put(CACHE_CONTROL, "max-age=60");
            headers.put(CONTENT_LENGTH, String.valueOf(body.length));
            if (compress) {
                headers.put(CONTENT_ENCODING, "gzip");
            }
            return new TestingResponse(HttpStatus.OK, headers.build(), body);
        });
    }

    private static Request get()
    {
        return prepareGet().setUri(ITEMS).build();
    }

    private static byte[] gzip(String value)
    {
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        try (GZIPOutputStream gzip = new GZIPOutputStream(output)) {
            gzip.write(value.getBytes(UTF_8));
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        return output.toByteArray();
    }

    // reports the encoding headers seen by the handler, which must not describe the decompressed body
    private static final ResponseHandler<String, RuntimeException> RESPONSE = new ResponseHandler<>()
    {
        @Override
        public String handleException(Request request, Exception exception)
        {
            throw new AssertionError(exception);
        }

        @Override
        public String handle(Request request, Response response)
        {
            try (InputStream input = response.getInputStream()) {
                List<String> encodingHeaders = Stream.concat(response.getHeaders(CONTENT_ENCODING).stream(), response.getHeaders(CONTENT_LENGTH).stream()).toList();
                return response.getStatusCode() + " headers=" + encodingHeaders + " " + new String(input.readAllBytes(), UTF_8);
            }
            catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }
    };
}
