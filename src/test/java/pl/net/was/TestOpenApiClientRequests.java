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

import com.google.common.base.Stopwatch;
import com.google.common.collect.ImmutableList;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;
import io.airlift.http.client.HttpClient;
import io.airlift.http.client.jetty.JettyHttpClient;
import io.airlift.units.Duration;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.TupleDomain;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.io.TempDir;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.zip.GZIPOutputStream;

import static com.google.common.base.Throwables.getStackTraceAsString;
import static io.airlift.slice.Slices.utf8Slice;
import static io.trino.spi.StandardErrorCode.INVALID_ROW_FILTER;
import static io.trino.spi.connector.SchemaTableName.schemaTableName;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNullElse;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.MINUTES;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;
import static pl.net.was.OpenApiErrorCode.OPENAPI_CONNECTION_ERROR;
import static pl.net.was.OpenApiErrorCode.OPENAPI_HTTP_ERROR;
import static pl.net.was.OpenApiErrorCode.OPENAPI_INVALID_RESPONSE;
import static pl.net.was.OpenApiSpec.SCHEMA_NAME;

@TestInstance(PER_CLASS)
class TestOpenApiClientRequests
{
    private static final String OK_RESPONSES =
            """
                  responses:
                    '200':
                      description: OK
                      content:
                        application/json:
                          schema:
                            type: array
                            items:
                              type: object
                              properties:
                                id:
                                  type: integer
            """;
    private static final String SPEC =
            """
            openapi: 3.0.0
            info:
              title: test
              version: '1'
            paths:
              /items:
                get:
                  operationId: listItems
                  parameters:
                    - name: secret
                      in: query
                      schema:
                        type: string
                  x-trino:
                    errorPath: $response.body#/message
            %s
              /untitled:
                get:
            %s
              /search:
                post:
                  operationId: searchItems
                  x-trino:
                    sqlOperation: select
            %s
              /patched:
                patch:
                  operationId: patchItems
                  x-trino:
                    sqlOperation: select
            %s
            """.formatted(OK_RESPONSES, OK_RESPONSES, OK_RESPONSES, OK_RESPONSES);

    // like GitHub, which sends the reset time with every response, including ones that fail because of missing permissions
    private static final String RATE_LIMITED_SPEC = SPEC.replace("paths:\n",
            """
            x-trino:
              rateLimitResetHeader: x-ratelimit-reset
              rateLimitResetFormat: epochSeconds
              rateLimitRemainingHeader: x-ratelimit-remaining
            paths:
            """);

    private static final String DOCUMENT_ERROR_PATH_SPEC = SPEC.replace("paths:\n",
            """
            x-trino:
              errorPath: $response.body#/error
            paths:
            """);

    private static final String PATH_PARAMETER_SPEC =
            """
            openapi: 3.0.0
            info:
              title: test
              version: '1'
            paths:
              /{tenant}/items:
                get:
                  operationId: listTenantItems
                  parameters:
                    - name: tenant
                      in: path
                      required: true
                      schema:
                        type: string
            %s
            """.formatted(OK_RESPONSES);

    private final Queue<HttpHandler> responses = new ConcurrentLinkedQueue<>();
    private final Queue<URI> requestUris = new ConcurrentLinkedQueue<>();
    private final AtomicInteger requestCount = new AtomicInteger();
    private HttpServer server;
    private HttpClient httpClient;
    private URI baseUri;

    @TempDir
    Path tempDir;

    @BeforeAll
    void setUp()
            throws IOException
    {
        server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/", exchange -> {
            requestCount.incrementAndGet();
            requestUris.add(exchange.getRequestURI());
            HttpHandler handler = responses.poll();
            if (handler == null) {
                handler = respond(200, "[]");
            }
            handler.handle(exchange);
        });
        server.start();
        baseUri = URI.create("http://127.0.0.1:" + server.getAddress().getPort());
        httpClient = new JettyHttpClient();
    }

    @AfterAll
    void tearDown()
    {
        httpClient.close();
        server.stop(0);
    }

    @BeforeEach
    void resetServer()
    {
        responses.clear();
        requestUris.clear();
        requestCount.set(0);
    }

    @Test
    void testBaseUriWithTrailingSlash()
            throws IOException
    {
        assertThat(getRows(newConfig().setBaseUri(URI.create(baseUri + "/")), "items", "x")).isEmpty();
        assertThat(requestUris).containsExactly(URI.create("/items?secret=x"));
    }

    @Test
    void testBaseUriWithPath()
            throws IOException
    {
        assertThat(getRows(newConfig().setBaseUri(URI.create(baseUri + "/v1")), "items", "x")).isEmpty();
        assertThat(getRows(newConfig().setBaseUri(URI.create(baseUri + "/v1/")), "items", "x")).isEmpty();
        assertThat(requestUris).containsExactly(URI.create("/v1/items?secret=x"), URI.create("/v1/items?secret=x"));
    }

    @Test
    void testBaseUriWithQuery()
            throws IOException
    {
        assertThat(getRows(newConfig().setBaseUri(URI.create(baseUri + "/v1?api-version=2")), "items", "x")).isEmpty();
        assertThat(getRows(newConfig().setBaseUri(URI.create(baseUri + "/v1?api-version=2")), "items", null)).isEmpty();
        assertThat(requestUris).containsExactly(URI.create("/v1/items?api-version=2&secret=x"), URI.create("/v1/items?api-version=2"));
    }

    @Test
    void testLeadingPathParameter()
            throws IOException
    {
        assertThat(getRows(newConfig(PATH_PARAMETER_SPEC), "items", "tenant", "a")).isEmpty();
        assertThat(requestUris).containsExactly(URI.create("/a/items"));
    }

    @Test
    void testEmptyPathParameter()
            throws IOException
    {
        assertThatThrownBy(() -> getRows(newConfig(PATH_PARAMETER_SPEC), "items", "tenant", ""))
                .isInstanceOfSatisfying(TrinoException.class, e -> assertThat(e.getErrorCode()).isEqualTo(INVALID_ROW_FILTER.toErrorCode()))
                .hasMessage("Path parameter tenant can't be empty");
        assertThat(requestCount).hasValue(0);
    }

    @Test
    void testRetriesTransientErrorResponses()
            throws IOException
    {
        responses.add(respond(502, ""));
        responses.add(respond(503, ""));
        responses.add(respond(504, ""));
        responses.add(respond(429, ""));
        responses.add(respond(200, "[{\"id\":1}]"));

        assertThat(getRows(newConfig().setMaxRetries(4), "items", null)).hasSize(1);
        assertThat(requestCount).hasValue(5);
    }

    @Test
    void testRetriesNetworkErrors()
            throws IOException
    {
        responses.add(disconnect());
        responses.add(respond(200, "[{\"id\":1}]"));

        assertThat(getRows(newConfig(), "items", null)).hasSize(1);
        assertThat(requestCount).hasValue(2);
    }

    @Test
    void testFailsAfterMaxRetries()
            throws IOException
    {
        responses.add(respond(503, ""));
        responses.add(respond(503, ""));
        responses.add(respond(503, "{\"message\":\"Try again later\"}"));

        assertThatThrownBy(() -> getRows(newConfig().setMaxRetries(2), "items", null))
                .isInstanceOf(TrinoException.class)
                .hasMessage("Request GET %s/items for operation listItems failed with HTTP 503 after 3 attempts: \"Try again later\"", baseUri);
        assertThat(requestCount).hasValue(3);
    }

    @Test
    void testDoesNotRetryPermanentErrors()
            throws IOException
    {
        responses.add(respond(500, ""));

        assertThatThrownBy(() -> getRows(newConfig(), "items", null))
                .hasMessage("Request GET %s/items for operation listItems failed with HTTP 500", baseUri);
        assertThat(requestCount).hasValue(1);
    }

    @Test
    void testDoesNotRetryNonIdempotentRequests()
            throws IOException
    {
        responses.add(respond(502, ""));

        assertThatThrownBy(() -> getRows(newConfig(), "search", null))
                .hasMessage("Request POST %s/search for operation searchItems failed with HTTP 502", baseUri);
        assertThat(requestCount).hasValue(1);

        requestCount.set(0);
        responses.add(respond(502, ""));

        // a PATCH can apply relative changes, like appending to a list, so repeating it can change the result
        assertThatThrownBy(() -> getRows(newConfig(), "patched", null))
                .hasMessage("Request PATCH %s/patched for operation patchItems failed with HTTP 502", baseUri);
        assertThat(requestCount).hasValue(1);
    }

    @Test
    void testRetriesCanBeDisabled()
            throws IOException
    {
        responses.add(respond(502, ""));

        assertThatThrownBy(() -> getRows(newConfig().setMaxRetries(0), "items", null))
                .hasMessage("Request GET %s/items for operation listItems failed with HTTP 502", baseUri);
        assertThat(requestCount).hasValue(1);
    }

    @Test
    void testHonorsRetryAfter()
            throws IOException
    {
        responses.add(respond(429, "", "Retry-After", "1"));
        Stopwatch stopwatch = Stopwatch.createStarted();

        assertThat(getRows(newConfig().setRetryMaxDelay(new Duration(1, MINUTES)), "items", null)).isEmpty();
        assertThat(stopwatch.elapsed()).isGreaterThanOrEqualTo(java.time.Duration.ofSeconds(1));
        assertThat(requestCount).hasValue(2);
    }

    @Test
    void testFailsFastWhenRetryAfterExceedsMaxDelay()
            throws IOException
    {
        responses.add(respond(503, "", "Retry-After", "3600"));

        assertThatThrownBy(() -> getRows(newConfig(), "items", null))
                .isInstanceOf(TrinoException.class)
                .hasMessage("Request GET %s/items for operation listItems failed with HTTP 503; " +
                        "the server asked to wait 3600 seconds before retrying, which is longer than retry-max-delay (10.00ms)", baseUri);
        assertThat(requestCount).hasValue(1);
    }

    @Test
    void testBackoffDelayIsJitteredWithinBounds()
            throws IOException
    {
        responses.add(respond(502, ""));
        responses.add(respond(502, ""));
        Stopwatch stopwatch = Stopwatch.createStarted();

        // with equal jitter, every retry waits between half of the delay and the whole delay, so two retries wait between 400 and 800 ms
        assertThat(getRows(newConfig()
                .setRetryInitialDelay(new Duration(400, MILLISECONDS))
                .setRetryMaxDelay(new Duration(400, MILLISECONDS)), "items", null)).isEmpty();
        assertThat(stopwatch.elapsed()).isGreaterThanOrEqualTo(java.time.Duration.ofMillis(400));
        assertThat(stopwatch.elapsed()).isLessThan(java.time.Duration.ofSeconds(30));
        assertThat(requestCount).hasValue(3);
    }

    @Test
    void testRetriesForbiddenOnlyWithRetryAfter()
            throws IOException
    {
        responses.add(respond(403, "", "Retry-After", "0"));

        assertThat(getRows(newConfig(), "items", null)).isEmpty();
        assertThat(requestCount).hasValue(2);

        requestCount.set(0);
        responses.add(respond(403, "", "X-RateLimit-Remaining", "0"));

        assertThatThrownBy(() -> getRows(newConfig(), "items", null))
                .hasMessage("Request GET %s/items for operation listItems failed with HTTP 403", baseUri);
        assertThat(requestCount).hasValue(1);
    }

    @Test
    void testWaitsForRateLimitReset()
            throws IOException
    {
        // the reset time has a precision of seconds, so the next one is at least a second later
        String reset = String.valueOf(Instant.now().getEpochSecond() + 2);
        responses.add(respond(403, "", "X-RateLimit-Remaining", "0", "X-RateLimit-Reset", reset));
        responses.add(respond(429, "", "X-RateLimit-Remaining", "0", "X-RateLimit-Reset", reset));
        Stopwatch stopwatch = Stopwatch.createStarted();

        assertThat(getRows(newConfig(RATE_LIMITED_SPEC).setRetryMaxDelay(new Duration(1, MINUTES)), "items", null)).isEmpty();
        assertThat(stopwatch.elapsed()).isGreaterThanOrEqualTo(java.time.Duration.ofSeconds(1));
        assertThat(requestCount).hasValue(3);
    }

    @Test
    void testWaitsForRateLimitResetDelay()
            throws IOException
    {
        String spec = RATE_LIMITED_SPEC
                .replace("x-ratelimit-reset\n", "x-ratelimit-reset-after\n")
                .replace("epochSeconds", "delaySeconds")
                .replace("  rateLimitRemainingHeader: x-ratelimit-remaining\n", "");
        responses.add(respond(429, "", "X-RateLimit-Reset-After", "1"));
        Stopwatch stopwatch = Stopwatch.createStarted();

        assertThat(getRows(newConfig(spec).setRetryMaxDelay(new Duration(1, MINUTES)), "items", null)).isEmpty();
        assertThat(stopwatch.elapsed()).isGreaterThanOrEqualTo(java.time.Duration.ofSeconds(1));
        assertThat(requestCount).hasValue(2);
    }

    @Test
    void testDoesNotRetryForbiddenWithRemainingRateLimit()
            throws IOException
    {
        String reset = String.valueOf(Instant.now().getEpochSecond());
        responses.add(respond(403, "", "X-RateLimit-Remaining", "4999", "X-RateLimit-Reset", reset));

        assertThatThrownBy(() -> getRows(newConfig(RATE_LIMITED_SPEC), "items", null))
                .hasMessage("Request GET %s/items for operation listItems failed with HTTP 403", baseUri);
        assertThat(requestCount).hasValue(1);

        requestCount.set(0);
        responses.add(respond(403, ""));

        assertThatThrownBy(() -> getRows(newConfig(RATE_LIMITED_SPEC), "items", null))
                .hasMessage("Request GET %s/items for operation listItems failed with HTTP 403", baseUri);
        assertThat(requestCount).hasValue(1);
    }

    @Test
    void testFailsFastWhenRateLimitResetExceedsMaxDelay()
            throws IOException
    {
        String reset = String.valueOf(Instant.now().getEpochSecond() + 3600);
        responses.add(respond(403, "", "X-RateLimit-Remaining", "0", "X-RateLimit-Reset", reset));

        assertThatThrownBy(() -> getRows(newConfig(RATE_LIMITED_SPEC), "items", null))
                .isInstanceOf(TrinoException.class)
                .hasMessageStartingWith("Request GET %s/items for operation listItems failed with HTTP 403; the rate limit resets in 35", baseUri)
                .hasMessageEndingWith(" seconds, which is longer than retry-max-delay (10.00ms)");
        assertThat(requestCount).hasValue(1);
    }

    @Test
    void testRateLimitResetCountsAgainstMaxRetries()
            throws IOException
    {
        String reset = String.valueOf(Instant.now().getEpochSecond());
        responses.add(respond(403, "", "X-RateLimit-Remaining", "0", "X-RateLimit-Reset", reset));
        responses.add(respond(403, "", "X-RateLimit-Remaining", "0", "X-RateLimit-Reset", reset));

        assertThatThrownBy(() -> getRows(newConfig(RATE_LIMITED_SPEC).setMaxRetries(1).setRetryMaxDelay(new Duration(1, MINUTES)), "items", null))
                .hasMessage("Request GET %s/items for operation listItems failed with HTTP 403 after 2 attempts", baseUri);
        assertThat(requestCount).hasValue(2);
    }

    @Test
    void testWaitsForRateLimitResetWithHttpCache()
            throws IOException
    {
        OpenApiConfig config = newConfig(RATE_LIMITED_SPEC)
                .setRetryMaxDelay(new Duration(1, MINUTES))
                .setHttpCacheDirectory(Files.createTempDirectory(tempDir, "cache").toString());
        responses.add(respond(429, "", "X-RateLimit-Remaining", "0", "X-RateLimit-Reset", String.valueOf(Instant.now().getEpochSecond())));
        responses.add(respond(200, "[{\"id\": 1}]", "Cache-Control", "max-age=60"));

        assertThat(getRows(config, "items", null)).hasSize(1);
        assertThat(requestCount).hasValue(2);
        // only the successful response is cached, so it's reused without a request
        assertThat(getRows(config, "items", null)).hasSize(1);
        assertThat(requestCount).hasValue(2);
    }

    @Test
    void testRetryAfterRateLimitRevalidatesCachedResponse()
            throws IOException
    {
        OpenApiConfig config = newConfig(RATE_LIMITED_SPEC)
                .setRetryMaxDelay(new Duration(1, MINUTES))
                .setHttpCacheDirectory(Files.createTempDirectory(tempDir, "cache").toString());
        responses.add(respond(200, "[{\"id\": 1}]", "ETag", "\"v1\""));
        assertThat(getRows(config, "items", null)).hasSize(1);

        Queue<String> conditionalRequests = new ConcurrentLinkedQueue<>();
        responses.add(respond(429, "", "X-RateLimit-Remaining", "0", "X-RateLimit-Reset", String.valueOf(Instant.now().getEpochSecond())));
        responses.add(exchange -> {
            conditionalRequests.add(requireNonNullElse(exchange.getRequestHeaders().getFirst("If-None-Match"), "none"));
            exchange.sendResponseHeaders(304, -1);
            exchange.close();
        });

        // the rate limited response doesn't remove the cached one, so the retry only revalidates it, which GitHub doesn't count against the rate limit
        assertThat(getRows(config, "items", null)).hasSize(1);
        assertThat(conditionalRequests).containsExactly("\"v1\"");
        assertThat(requestCount).hasValue(3);
    }

    @Test
    void testDocumentErrorPathWithCompressedResponse()
            throws IOException
    {
        OpenApiConfig config = newConfig(DOCUMENT_ERROR_PATH_SPEC)
                .setHttpCacheDirectory(Files.createTempDirectory(tempDir, "cache").toString());
        responses.add(respondGzip(422, "{\"error\": \"Validation Failed\", \"message\": \"Invalid page\"}"));

        assertThatThrownBy(() -> getRows(config, "untitled", null))
                .hasMessage("Request GET %s/untitled for path /untitled failed with HTTP 422: \"Validation Failed\"", baseUri);

        // the operation's errorPath takes precedence
        responses.add(respondGzip(422, "{\"error\": \"Validation Failed\", \"message\": \"Invalid page\"}"));

        assertThatThrownBy(() -> getRows(config, "items", null))
                .hasMessage("Request GET %s/items for operation listItems failed with HTTP 422: \"Invalid page\"", baseUri);
    }

    @Test
    void testRetriesCountAgainstRateLimit()
            throws IOException
    {
        responses.add(respond(502, ""));
        responses.add(respond(502, ""));
        Stopwatch stopwatch = Stopwatch.createStarted();

        // the rate limiter lets the first request through immediately, and delays every next one by half a second
        assertThat(getRows(newConfig().setMaxRequestsPerSecond(2), "items", null)).isEmpty();
        assertThat(stopwatch.elapsed()).isGreaterThanOrEqualTo(java.time.Duration.ofMillis(900));
        assertThat(requestCount).hasValue(3);
    }

    @Test
    void testErrorIncludesRequestContextAndErrorPathMessage()
            throws IOException
    {
        responses.add(respond(500, "{\"message\":\"Something broke\",\"details\":\"internal-details\"}"));

        assertThatThrownBy(() -> getRows(newConfig(), "items", "s3cr3t"))
                .isInstanceOfSatisfying(TrinoException.class, e -> assertThat(e.getErrorCode()).isEqualTo(OPENAPI_HTTP_ERROR.toErrorCode()))
                .hasMessage("Request GET %s/items for operation listItems failed with HTTP 500: \"Something broke\"", baseUri)
                .hasNoCause()
                .message()
                .doesNotContain("s3cr3t", "internal-details");
    }

    @Test
    void testErrorWithoutOperationIdOrErrorPath()
            throws IOException
    {
        responses.add(respond(400, "<html>internal-details</html>"));

        assertThatThrownBy(() -> getRows(newConfig(), "untitled", null))
                .isInstanceOf(TrinoException.class)
                .hasMessage("Request GET %s/untitled for path /untitled failed with HTTP 400", baseUri)
                .hasNoCause();
    }

    @Test
    void testTruncatedErrorBodyIsNotParsed()
            throws IOException
    {
        String padding = "x".repeat(HttpRequestFailure.MAX_ERROR_BODY_BYTES);
        responses.add(respond(500, "{\"message\":\"Something broke\",\"padding\":\"%s\"}".formatted(padding)));

        assertThatThrownBy(() -> getRows(newConfig(), "items", null))
                .isInstanceOf(TrinoException.class)
                .hasMessage("Request GET %s/items for operation listItems failed with HTTP 500", baseUri);
    }

    @Test
    void testInvalidJsonIsExternalError()
            throws IOException
    {
        responses.add(respond(200, "not JSON, with internal-details"));

        assertThatThrownBy(() -> getRows(newConfig(), "items", "s3cr3t"))
                .isInstanceOfSatisfying(TrinoException.class, e -> assertThat(e.getErrorCode()).isEqualTo(OPENAPI_INVALID_RESPONSE.toErrorCode()))
                .hasMessage("Response to GET %s/items is not valid JSON", baseUri)
                .hasNoCause();
    }

    @Test
    void testUnexpectedResponseShapeIsExternalError()
            throws IOException
    {
        responses.add(respond(200, "[\"internal-details\"]"));

        assertThatThrownBy(() -> getRows(newConfig(), "items", null))
                .isInstanceOfSatisfying(TrinoException.class, e -> assertThat(e.getErrorCode()).isEqualTo(OPENAPI_INVALID_RESPONSE.toErrorCode()))
                .hasMessage("Response to GET %s/items contains a JSON STRING where an object was expected", baseUri);
    }

    @Test
    void testValueNotMatchingSpecIsExternalError()
            throws IOException
    {
        responses.add(respond(200, "[{\"id\":\"internal-details\"}]"));

        assertThatThrownBy(() -> getRows(newConfig(), "items", "s3cr3t"))
                .isInstanceOfSatisfying(TrinoException.class, e -> assertThat(e.getErrorCode()).isEqualTo(OPENAPI_INVALID_RESPONSE.toErrorCode()))
                .hasMessage("Invalid value of column id in the response to GET %s/items: Cannot read a JSON string as bigint", baseUri)
                .satisfies(e -> assertThat(getStackTraceAsString(e)).doesNotContain("internal-details", "s3cr3t"));
    }

    @Test
    void testTransportErrorIncludesRequestContext()
            throws IOException
    {
        OpenApiConfig config = newConfig();
        URI unreachableUri;
        try (ServerSocket closedSocket = new ServerSocket(0, 0, InetAddress.getLoopbackAddress())) {
            unreachableUri = URI.create("http://127.0.0.1:" + closedSocket.getLocalPort());
        }
        config.setBaseUri(unreachableUri);

        assertThatThrownBy(() -> getRows(config, "items", "s3cr3t"))
                .isInstanceOfSatisfying(TrinoException.class, e -> assertThat(e.getErrorCode()).isEqualTo(OPENAPI_CONNECTION_ERROR.toErrorCode()))
                .hasMessageStartingWith("Request GET %s/items for operation listItems failed after 4 attempts: ", unreachableUri)
                .hasCauseInstanceOf(IOException.class)
                .message()
                .doesNotContain("s3cr3t");
    }

    private OpenApiConfig newConfig()
            throws IOException
    {
        return newConfig(SPEC);
    }

    private OpenApiConfig newConfig(String spec)
            throws IOException
    {
        Path specFile = Files.writeString(tempDir.resolve("spec.yaml"), spec);
        return new OpenApiConfig()
                .setSpecLocation(specFile.toString())
                .setBaseUri(baseUri)
                .setRetryInitialDelay(new Duration(1, MILLISECONDS))
                .setRetryMaxDelay(new Duration(10, MILLISECONDS));
    }

    private List<List<?>> getRows(OpenApiConfig config, String tableName, String secret)
    {
        return getRows(config, tableName, "secret", secret);
    }

    private List<List<?>> getRows(OpenApiConfig config, String tableName, String columnName, String value)
    {
        OpenApiSpec spec = new OpenApiSpec(config);
        OpenApiClient client = new OpenApiClient(config, new CatalogName("test"), httpClient, spec);
        OpenApiTableHandle table = spec.getTableHandle(schemaTableName(SCHEMA_NAME, tableName));
        if (value != null) {
            OpenApiColumn column = spec.getTables().get(tableName).stream()
                    .filter(candidate -> candidate.getName().equals(columnName))
                    .findFirst()
                    .orElseThrow();
            table = table.cloneWithConstraint(TupleDomain.withColumnDomains(Map.of((ColumnHandle) column.getHandle(), Domain.singleValue(VARCHAR, utf8Slice(value)))));
        }
        return ImmutableList.copyOf(client.getRows(table, 1, 1, false));
    }

    private static HttpHandler disconnect()
    {
        return _ -> {
            throw new IOException("Closing the connection without a response");
        };
    }

    private static HttpHandler respondGzip(int statusCode, String body)
    {
        return exchange -> {
            ByteArrayOutputStream compressed = new ByteArrayOutputStream();
            try (GZIPOutputStream output = new GZIPOutputStream(compressed)) {
                output.write(body.getBytes(UTF_8));
            }
            exchange.getResponseHeaders().add("Content-Type", "application/json");
            exchange.getResponseHeaders().add("Content-Encoding", "gzip");
            exchange.sendResponseHeaders(statusCode, compressed.size());
            try (OutputStream output = exchange.getResponseBody()) {
                compressed.writeTo(output);
            }
        };
    }

    private static HttpHandler respond(int statusCode, String body, String... headers)
    {
        return exchange -> {
            for (int i = 0; i < headers.length; i += 2) {
                exchange.getResponseHeaders().add(headers[i], headers[i + 1]);
            }
            exchange.getResponseHeaders().add("Content-Type", "application/json");
            byte[] bytes = body.getBytes(UTF_8);
            exchange.sendResponseHeaders(statusCode, bytes.length);
            try (OutputStream output = exchange.getResponseBody()) {
                output.write(bytes);
            }
        };
    }
}
