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

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.net.ConnectException;
import java.net.SocketException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermissions;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Stream;

import static io.airlift.http.client.HeaderNames.ACCEPT;
import static io.airlift.http.client.HeaderNames.AGE;
import static io.airlift.http.client.HeaderNames.CACHE_CONTROL;
import static io.airlift.http.client.HeaderNames.COOKIE;
import static io.airlift.http.client.HeaderNames.DATE;
import static io.airlift.http.client.HeaderNames.ETAG;
import static io.airlift.http.client.HeaderNames.EXPIRES;
import static io.airlift.http.client.HeaderNames.IF_MODIFIED_SINCE;
import static io.airlift.http.client.HeaderNames.IF_NONE_MATCH;
import static io.airlift.http.client.HeaderNames.LAST_MODIFIED;
import static io.airlift.http.client.HeaderNames.PRAGMA;
import static io.airlift.http.client.HeaderNames.SET_COOKIE;
import static io.airlift.http.client.HeaderNames.VARY;
import static io.airlift.http.client.Request.Builder.prepareGet;
import static io.airlift.http.client.Request.Builder.preparePost;
import static io.airlift.units.DataSize.Unit.KILOBYTE;
import static io.airlift.units.DataSize.Unit.MEGABYTE;
import static java.nio.charset.StandardCharsets.ISO_8859_1;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.time.format.DateTimeFormatter.RFC_1123_DATE_TIME;
import static java.util.Objects.requireNonNullElse;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assumptions.assumeThat;

class TestCachingHttpClient
{
    private static final URI ITEMS = URI.create("http://example.com/items");
    private static final URI OTHER_ITEMS = URI.create("http://example.com/other-items");
    private static final HeaderName ITEM_IDS = HeaderName.of("X-Item-Ids");
    private static final HeaderName AUTHENTICATION_INFO = HeaderName.of("Authentication-Info");

    @TempDir
    Path cacheDirectory;

    private final TestingClock clock = new TestingClock();
    private final List<Request> serverRequests = new ArrayList<>();

    @Test
    void testFreshResponseIsReused()
    {
        CachingHttpClient client = client(_ -> response(HttpStatus.OK, "first", CACHE_CONTROL, "max-age=60"));

        assertThat(client.execute(get(ITEMS), BODY)).isEqualTo("200 first");
        clock.advance(Duration.ofSeconds(30));
        assertThat(client.execute(get(ITEMS), RESPONSE_AGE)).isEqualTo("30");
        assertThat(serverRequests).hasSize(1);
    }

    @Test
    void testStaleResponseIsRevalidated()
    {
        CachingHttpClient client = client(request -> request.getHeaders().containsKey(IF_NONE_MATCH)
                ? response(HttpStatus.NOT_MODIFIED, "", CACHE_CONTROL, "max-age=60")
                : response(HttpStatus.OK, "first", CACHE_CONTROL, "max-age=60", ETAG, "\"v1\""));

        client.execute(get(ITEMS), BODY);
        clock.advance(Duration.ofSeconds(61));
        assertThat(client.execute(get(ITEMS), BODY)).isEqualTo("200 first");
        assertThat(serverRequests).hasSize(2);
        assertThat(serverRequests.getLast().getHeaders().get(IF_NONE_MATCH)).containsExactly("\"v1\"");

        // the 304 refreshed the stored response
        assertThat(client.execute(get(ITEMS), BODY)).isEqualTo("200 first");
        assertThat(serverRequests).hasSize(2);
    }

    @Test
    void testResponseWithValidatorsIsAlwaysRevalidated()
    {
        String lastModified = RFC_1123_DATE_TIME.format(clock.instant().atZone(ZoneOffset.UTC));
        CachingHttpClient client = client(request -> request.getHeaders().containsKey(IF_MODIFIED_SINCE)
                ? response(HttpStatus.NOT_MODIFIED, "")
                : response(HttpStatus.OK, "first", LAST_MODIFIED, lastModified));

        client.execute(get(ITEMS), BODY);
        assertThat(client.execute(get(ITEMS), BODY)).isEqualTo("200 first");
        assertThat(serverRequests).hasSize(2);
        assertThat(serverRequests.getLast().getHeaders().get(IF_MODIFIED_SINCE)).containsExactly(lastModified);
    }

    @Test
    void testChangedResponseReplacesStoredResponse()
    {
        List<String> bodies = new ArrayList<>(List.of("first", "second"));
        CachingHttpClient client = client(_ -> response(HttpStatus.OK, bodies.removeFirst(), ETAG, "\"v" + bodies.size() + "\""));

        assertThat(client.execute(get(ITEMS), BODY)).isEqualTo("200 first");
        assertThat(client.execute(get(ITEMS), BODY)).isEqualTo("200 second");
        assertThat(serverRequests.getLast().getHeaders().get(IF_NONE_MATCH)).containsExactly("\"v1\"");
    }

    @Test
    void testErrorResponsesKeepStoredResponse()
    {
        List<HttpStatus> statuses = new ArrayList<>(List.of(HttpStatus.OK, HttpStatus.TOO_MANY_REQUESTS, HttpStatus.SERVICE_UNAVAILABLE));
        CachingHttpClient client = client(request -> request.getHeaders().containsKey(IF_NONE_MATCH) && statuses.isEmpty()
                ? response(HttpStatus.NOT_MODIFIED, "")
                : response(statuses.removeFirst(), "first", ETAG, "\"v1\""));

        assertThat(client.execute(get(ITEMS), BODY)).isEqualTo("200 first");
        // an exceeded rate limit or a server failure says nothing about the resource
        assertThat(client.execute(get(ITEMS), BODY)).isEqualTo("429 first");
        assertThat(client.execute(get(ITEMS), BODY)).isEqualTo("503 first");
        // so the next request can be a revalidation, which some APIs don't count against rate limits
        assertThat(client.execute(get(ITEMS), BODY)).isEqualTo("200 first");
        assertThat(serverRequests).hasSize(4);
        assertThat(serverRequests.getLast().getHeaders().get(IF_NONE_MATCH)).containsExactly("\"v1\"");
    }

    @Test
    void testMissingResourceRemovesStoredResponse()
    {
        List<HttpStatus> statuses = new ArrayList<>(List.of(HttpStatus.OK, HttpStatus.NOT_FOUND, HttpStatus.OK));
        CachingHttpClient client = client(_ -> {
            HttpStatus status = statuses.removeFirst();
            // a response without freshness or validators can't be stored, so it can't replace the stored one
            return status == HttpStatus.OK ? response(status, "first", ETAG, "\"v1\"") : response(status, "missing");
        });

        client.execute(get(ITEMS), BODY);
        assertThat(client.execute(get(ITEMS), BODY)).isEqualTo("404 missing");
        client.execute(get(ITEMS), BODY);
        assertThat(serverRequests).hasSize(3);
        assertThat(serverRequests.getLast().getHeaders().containsKey(IF_NONE_MATCH)).isFalse();
    }

    @Test
    void testResponsesThatCantBeReusedAreNotStored()
    {
        CachingHttpClient client = client(request -> switch (request.getUri().getPath()) {
            case "/items" -> response(HttpStatus.OK, "no-store", CACHE_CONTROL, "max-age=60, no-store");
            case "/other-items" -> response(HttpStatus.OK, "no freshness or validators");
            default -> throw new IllegalArgumentException(request.getUri().toString());
        });

        client.execute(get(ITEMS), BODY);
        client.execute(get(ITEMS), BODY);
        client.execute(get(OTHER_ITEMS), BODY);
        client.execute(get(OTHER_ITEMS), BODY);
        assertThat(serverRequests).hasSize(4);
        assertThat(serverRequests).allSatisfy(request -> assertThat(request.getHeaders().containsKey(IF_NONE_MATCH)).isFalse());
    }

    @Test
    void testExpiresHeader()
    {
        String date = RFC_1123_DATE_TIME.format(clock.instant().atZone(ZoneOffset.UTC));
        CachingHttpClient client = client(request -> switch (request.getUri().getPath()) {
            case "/items" -> response(HttpStatus.OK, "expires", DATE, date, EXPIRES, RFC_1123_DATE_TIME.format(clock.instant().plusSeconds(60).atZone(ZoneOffset.UTC)));
            // an invalid date means the response is already expired
            case "/other-items" -> response(HttpStatus.OK, "expired", DATE, date, EXPIRES, "0", ETAG, "\"v1\"");
            default -> throw new IllegalArgumentException(request.getUri().toString());
        });

        client.execute(get(ITEMS), BODY);
        clock.advance(Duration.ofSeconds(59));
        client.execute(get(ITEMS), BODY);
        assertThat(serverRequests).hasSize(1);

        client.execute(get(OTHER_ITEMS), BODY);
        client.execute(get(OTHER_ITEMS), BODY);
        assertThat(serverRequests).hasSize(3);
    }

    @Test
    void testRequestCanForceRevalidation()
    {
        CachingHttpClient client = client(request -> request.getHeaders().containsKey(IF_NONE_MATCH)
                ? response(HttpStatus.NOT_MODIFIED, "")
                : response(HttpStatus.OK, "first", CACHE_CONTROL, "max-age=60", ETAG, "\"v1\""));

        client.execute(get(ITEMS), BODY);
        Request noCache = Request.Builder.fromRequest(get(ITEMS)).setHeader(CACHE_CONTROL, "no-cache").build();
        assertThat(client.execute(noCache, BODY)).isEqualTo("200 first");
        assertThat(serverRequests).hasSize(2);
        assertThat(serverRequests.getLast().getHeaders().get(IF_NONE_MATCH)).containsExactly("\"v1\"");
    }

    @Test
    void testVary()
    {
        CachingHttpClient client = client(request -> response(HttpStatus.OK, "for " + request.getHeader(ACCEPT), CACHE_CONTROL, "max-age=60", VARY, "Accept"));

        assertThat(client.execute(get(ITEMS, "application/json"), BODY)).isEqualTo("200 for application/json");
        assertThat(client.execute(get(ITEMS, "application/json"), BODY)).isEqualTo("200 for application/json");
        assertThat(client.execute(get(ITEMS, "text/plain"), BODY)).isEqualTo("200 for text/plain");
        assertThat(serverRequests).hasSize(2);
    }

    @Test
    void testHeaderParametersSelectStoredResponse()
    {
        // servers rarely list header parameters in the Vary header, even when they filter the response
        CachingHttpClient client = client(request -> response(HttpStatus.OK, "for " + requireNonNullElse(request.getHeader(ITEM_IDS), "all"), CACHE_CONTROL, "max-age=60"));

        assertThat(client.execute(get(ITEMS, ITEM_IDS, "1"), BODY)).isEqualTo("200 for 1");
        assertThat(client.execute(get(ITEMS, ITEM_IDS, "2"), BODY)).isEqualTo("200 for 2");
        assertThat(client.execute(get(ITEMS), BODY)).isEqualTo("200 for all");
        assertThat(client.execute(get(ITEMS, ITEM_IDS, "1"), BODY)).isEqualTo("200 for 1");
        assertThat(client.execute(get(ITEMS, ITEM_IDS, "2"), BODY)).isEqualTo("200 for 2");
        assertThat(serverRequests).hasSize(3);
    }

    @Test
    void testCookieParametersSelectStoredResponse()
    {
        CachingHttpClient client = client(request -> response(HttpStatus.OK, "for " + requireNonNullElse(request.getHeader(COOKIE), "all"), CACHE_CONTROL, "max-age=60"));

        assertThat(client.execute(get(ITEMS, COOKIE, "ids=1"), BODY)).isEqualTo("200 for ids=1");
        assertThat(client.execute(get(ITEMS, COOKIE, "ids=2"), BODY)).isEqualTo("200 for ids=2");
        assertThat(client.execute(get(ITEMS, COOKIE, "ids=1"), BODY)).isEqualTo("200 for ids=1");
        assertThat(serverRequests).hasSize(2);
    }

    @Test
    void testCacheDirectivesDontSelectStoredResponse()
    {
        CachingHttpClient client = client(request -> request.getHeaders().containsKey(IF_NONE_MATCH)
                ? response(HttpStatus.NOT_MODIFIED, "")
                : response(HttpStatus.OK, "first", CACHE_CONTROL, "max-age=60", ETAG, "\"v1\""));

        client.execute(get(ITEMS), BODY);
        assertThat(client.execute(get(ITEMS, CACHE_CONTROL, "no-cache"), BODY)).isEqualTo("200 first");
        assertThat(client.execute(get(ITEMS, PRAGMA, "no-cache"), BODY)).isEqualTo("200 first");
        assertThat(serverRequests).hasSize(2);
        assertThat(serverRequests.getLast().getHeaders().get(IF_NONE_MATCH)).containsExactly("\"v1\"");
    }

    @Test
    void testSuccessfulChangeInvalidatesStoredResponses()
    {
        CachingHttpClient client = client(request -> request.getMethod().equals("POST")
                ? response(HttpStatus.CREATED, "")
                : response(HttpStatus.OK, "first", CACHE_CONTROL, "max-age=60"));

        client.execute(get(ITEMS), BODY);
        client.execute(get(ITEMS, ITEM_IDS, "1"), BODY);
        client.execute(get(OTHER_ITEMS), BODY);
        client.execute(preparePost().setUri(ITEMS).build(), BODY);
        // responses requested with other headers are outdated too
        client.execute(get(ITEMS), BODY);
        client.execute(get(ITEMS, ITEM_IDS, "1"), BODY);
        client.execute(get(OTHER_ITEMS), BODY);
        assertThat(serverRequests).extracting(request -> request.getMethod() + " " + request.getUri().getPath())
                .containsExactly("GET /items", "GET /items", "GET /other-items", "POST /items", "GET /items", "GET /items");
    }

    @Test
    void testStoredResponsesSurviveRestarts()
    {
        Function<Request, Response> server = _ -> response(HttpStatus.OK, "first", CACHE_CONTROL, "max-age=60");
        client(server).execute(get(ITEMS), BODY);

        assertThat(client(server).execute(get(ITEMS), BODY)).isEqualTo("200 first");
        assertThat(serverRequests).hasSize(1);
    }

    @Test
    void testLeastRecentlyUsedResponsesAreEvicted()
            throws InterruptedException
    {
        String body = "x".repeat(600);
        HttpCache cache = new HttpCache(cacheDirectory, DataSize.of(2, KILOBYTE), DataSize.of(1, KILOBYTE));
        CachingHttpClient client = client(cache, _ -> response(HttpStatus.OK, body, CACHE_CONTROL, "max-age=60"));

        client.execute(get(ITEMS), BODY);
        // access times have a millisecond precision
        Thread.sleep(5);
        client.execute(get(OTHER_ITEMS), BODY);
        Thread.sleep(5);
        client.execute(get(URI.create("http://example.com/more-items")), BODY);

        assertThat(cache.sizeInBytes()).isLessThanOrEqualTo(DataSize.of(2, KILOBYTE).toBytes());
        client.execute(get(OTHER_ITEMS), BODY);
        client.execute(get(ITEMS), BODY);
        assertThat(serverRequests).extracting(request -> request.getUri().getPath())
                .containsExactly("/items", "/other-items", "/more-items", "/items");
    }

    @Test
    void testLargeResponsesAreNotStored()
    {
        HttpCache cache = new HttpCache(cacheDirectory, DataSize.of(10, KILOBYTE), DataSize.of(1, KILOBYTE));
        CachingHttpClient client = client(cache, _ -> response(HttpStatus.OK, "x".repeat(2000), CACHE_CONTROL, "max-age=60"));

        client.execute(get(ITEMS), BODY);
        client.execute(get(ITEMS), BODY);
        assertThat(serverRequests).hasSize(2);
        assertThat(cache.sizeInBytes()).isZero();
    }

    @Test
    void testUnreadableEntriesAreIgnored()
            throws IOException
    {
        Function<Request, Response> server = _ -> response(HttpStatus.OK, "first", CACHE_CONTROL, "max-age=60");
        client(server).execute(get(ITEMS), BODY);
        try (Stream<Path> files = Files.list(cacheDirectory)) {
            for (Path file : files.toList()) {
                Files.write(file, "not an entry".getBytes(UTF_8));
            }
        }

        assertThat(client(server).execute(get(ITEMS), BODY)).isEqualTo("200 first");
        assertThat(serverRequests).hasSize(2);
    }

    @Test
    void testFailuresArePassedToResponseHandler()
    {
        CachingHttpClient client = client(_ -> {
            throw new UncheckedIOException(new ConnectException("refused"));
        });

        assertThat(client.execute(get(ITEMS), BODY)).startsWith("failed:");
    }

    @Test
    void testSessionHeadersAreNotStored()
            throws IOException
    {
        CachingHttpClient client = client(request -> request.getHeaders().containsKey(IF_NONE_MATCH)
                ? response(HttpStatus.NOT_MODIFIED, "", CACHE_CONTROL, "max-age=60", SET_COOKIE, "session=revalidated")
                : response(HttpStatus.OK, "first", ETAG, "\"v1\"", SET_COOKIE, "session=s3cr3t", AUTHENTICATION_INFO, "nextnonce=\"n0nc3\""));

        assertThat(client.execute(get(ITEMS), HEADERS)).contains("set-cookie=[session=s3cr3t]", "authentication-info=[nextnonce=\"n0nc3\"]");
        assertThat(client.execute(get(ITEMS), HEADERS)).doesNotContain("set-cookie", "authentication-info");
        assertThat(client.execute(get(ITEMS), HEADERS)).doesNotContain("set-cookie", "authentication-info");
        assertThat(serverRequests).hasSize(2);
        try (Stream<Path> files = Files.list(cacheDirectory)) {
            for (Path file : files.toList()) {
                assertThat(Files.readString(file, ISO_8859_1)).doesNotContain("s3cr3t", "n0nc3", "revalidated");
            }
        }
    }

    @Test
    void testOnlyOwnerCanReadStoredResponses()
            throws IOException
    {
        assumeThat(cacheDirectory.getFileSystem().supportedFileAttributeViews()).contains("posix");
        Path catalogDirectory = cacheDirectory.resolve("catalog");
        Path existingCatalogDirectory = Files.createDirectory(cacheDirectory.resolve("existing"), PosixFilePermissions.asFileAttribute(PosixFilePermissions.fromString("rwxr-xr-x")));
        new HttpCache(existingCatalogDirectory, DataSize.of(1, MEGABYTE), DataSize.of(1, MEGABYTE));
        HttpCache cache = new HttpCache(catalogDirectory, DataSize.of(1, MEGABYTE), DataSize.of(1, MEGABYTE));
        client(cache, _ -> response(HttpStatus.OK, "first", CACHE_CONTROL, "max-age=60")).execute(get(ITEMS), BODY);

        assertThat(PosixFilePermissions.toString(Files.getPosixFilePermissions(catalogDirectory))).isEqualTo("rwx------");
        assertThat(PosixFilePermissions.toString(Files.getPosixFilePermissions(existingCatalogDirectory))).isEqualTo("rwx------");
        try (Stream<Path> files = Files.list(catalogDirectory)) {
            assertThat(files.toList())
                    .singleElement()
                    .satisfies(file -> assertThat(PosixFilePermissions.toString(Files.getPosixFilePermissions(file))).isEqualTo("rw-------"));
        }
    }

    @Test
    void testBodyReadFailuresArePassedToResponseHandler()
    {
        List<Response> responses = new ArrayList<>(List.of(
                new TestingResponse(HttpStatus.OK, ImmutableListMultimap.of(CACHE_CONTROL, "max-age=60"), new InputStream()
                {
                    @Override
                    public int read()
                            throws IOException
                    {
                        throw new SocketException("Connection reset");
                    }

                    @Override
                    public int read(byte[] buffer, int offset, int length)
                            throws IOException
                    {
                        throw new SocketException("Connection reset");
                    }
                }),
                response(HttpStatus.OK, "second", CACHE_CONTROL, "max-age=60")));
        CachingHttpClient client = client(_ -> responses.removeFirst());

        // without the cache, the caller's handler reads the body, and reports the failure, so it's retried like other transport errors
        assertThat(client.execute(get(ITEMS), BODY)).isEqualTo("failed: java.net.SocketException: Connection reset");
        assertThat(client.execute(get(ITEMS), BODY)).isEqualTo("200 second");
        assertThat(client.execute(get(ITEMS), BODY)).isEqualTo("200 second");
        assertThat(serverRequests).hasSize(2);
    }

    private CachingHttpClient client(Function<Request, Response> server)
    {
        return client(new HttpCache(cacheDirectory, DataSize.of(1, MEGABYTE), DataSize.of(1, MEGABYTE)), server);
    }

    private CachingHttpClient client(HttpCache cache, Function<Request, Response> server)
    {
        return new CachingHttpClient(
                new TestingHttpClient(request -> {
                    serverRequests.add(request);
                    return server.apply(request);
                }),
                cache,
                clock);
    }

    private static Request get(URI uri)
    {
        return prepareGet().setUri(uri).build();
    }

    private static Request get(URI uri, String accept)
    {
        return get(uri, ACCEPT, accept);
    }

    private static Request get(URI uri, HeaderName header, String value)
    {
        return prepareGet().setUri(uri).setHeader(header, value).build();
    }

    private static Response response(HttpStatus status, String body, Object... headers)
    {
        ImmutableListMultimap.Builder<HeaderName, String> headerMap = ImmutableListMultimap.builder();
        for (int i = 0; i < headers.length; i += 2) {
            headerMap.put((HeaderName) headers[i], (String) headers[i + 1]);
        }
        return new TestingResponse(status, headerMap.build(), body.getBytes(UTF_8));
    }

    private static final ResponseHandler<String, RuntimeException> BODY = new ResponseHandler<>()
    {
        @Override
        public String handleException(Request request, Exception exception)
        {
            return "failed: " + exception;
        }

        @Override
        public String handle(Request request, Response response)
        {
            try (InputStream input = response.getInputStream()) {
                return response.getStatusCode() + " " + new String(input.readAllBytes(), UTF_8);
            }
            catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }
    };

    private static final ResponseHandler<String, RuntimeException> HEADERS = new ResponseHandler<>()
    {
        @Override
        public String handleException(Request request, Exception exception)
        {
            throw new AssertionError(exception);
        }

        @Override
        public String handle(Request request, Response response)
        {
            return response.getHeaders().asMap().toString();
        }
    };

    private static final ResponseHandler<String, RuntimeException> RESPONSE_AGE = new ResponseHandler<>()
    {
        @Override
        public String handleException(Request request, Exception exception)
        {
            throw new AssertionError(exception);
        }

        @Override
        public String handle(Request request, Response response)
        {
            return response.getHeader(AGE).orElseThrow();
        }
    };

    private static class TestingClock
            extends Clock
    {
        private Instant instant = Instant.parse("2026-01-01T00:00:00Z");

        void advance(Duration duration)
        {
            instant = instant.plus(duration);
        }

        @Override
        public ZoneId getZone()
        {
            return ZoneOffset.UTC;
        }

        @Override
        public Clock withZone(ZoneId zone)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public Instant instant()
        {
            return instant;
        }
    }
}
