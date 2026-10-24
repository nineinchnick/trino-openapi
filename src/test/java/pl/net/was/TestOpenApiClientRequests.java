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
import com.sun.net.httpserver.Headers;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;
import io.airlift.http.client.HttpClient;
import io.airlift.http.client.jetty.JettyHttpClient;
import io.airlift.log.Level;
import io.airlift.log.Logging;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;
import io.trino.spi.TrinoException;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.catalog.CatalogName;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.type.ArrayType;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.io.TempDir;
import pl.net.was.authentication.Authentication;
import pl.net.was.authentication.AuthenticationScheme;
import pl.net.was.authentication.AuthenticationType;
import pl.net.was.authentication.ExtraCredentialProvider;

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
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;
import java.util.zip.GZIPOutputStream;

import static com.google.common.base.Throwables.getStackTraceAsString;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static io.airlift.slice.Slices.utf8Slice;
import static io.airlift.units.DataSize.Unit.GIGABYTE;
import static io.airlift.units.DataSize.Unit.KILOBYTE;
import static io.trino.spi.StandardErrorCode.INVALID_ROW_FILTER;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.trino.spi.connector.SchemaTableName.schemaTableName;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.spi.type.TypeUtils.writeNativeValue;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.testing.TestingConnectorSession.SESSION;
import static java.lang.Float.floatToIntBits;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Collections.singletonList;
import static java.util.Objects.requireNonNullElse;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.function.Function.identity;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;
import static pl.net.was.OpenApiErrorCode.OPENAPI_CONNECTION_ERROR;
import static pl.net.was.OpenApiErrorCode.OPENAPI_HTTP_ERROR;
import static pl.net.was.OpenApiErrorCode.OPENAPI_INVALID_RESPONSE;
import static pl.net.was.OpenApiSpec.SCHEMA_NAME;
import static pl.net.was.TestingOpenApiSpec.createSpec;

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
              /created:
                post:
                  operationId: createItems
                  x-trino:
                    sqlOperation: [select, insert]
            %s
            """.formatted(OK_RESPONSES, OK_RESPONSES, OK_RESPONSES, OK_RESPONSES, OK_RESPONSES);

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

    private static final String DECLARED_NOT_FOUND_SPEC =
            """
            openapi: 3.0.0
            info:
              title: test
              version: '1'
            paths:
              /items:
                get:
                  operationId: listItems
            %s
                    '404':
                      description: No items
            """.formatted(OK_RESPONSES);

    private static final String MEDIA_TYPES_SPEC =
            """
            openapi: 3.0.0
            info:
              title: test
              version: '1'
            paths:
              /users:
                get:
                  operationId: listUsers
            %s
              /usage:
                get:
                  operationId: listUsage
            %s
              /items:
                get:
                  operationId: listItems
            %s
            """.formatted(
            OK_RESPONSES.replace("application/json:", "application/scim+json:"),
            OK_RESPONSES.replace("application/json:", "application/json;datetime-format=rfc3339:"),
            OK_RESPONSES.replace("application/json:", "'*/*':"));

    private static final String SUCCESS_STATUSES_SPEC =
            """
            openapi: 3.0.0
            info:
              title: test
              version: '1'
            paths:
              /mirrored:
                get:
                  operationId: listMirroredItems
            %s
              /search:
                post:
                  operationId: searchItems
                  x-trino:
                    sqlOperation: select
            %s
            """.formatted(
            OK_RESPONSES.replace("'200':", "'203':"),
            OK_RESPONSES.replace("'200':", "'201':"));

    private static final String NUMBER_PARAMETERS_SPEC =
            """
            openapi: 3.0.0
            info:
              title: test
              version: '1'
            paths:
              /measurements/{size}:
                get:
                  operationId: listMeasurements
                  parameters:
                    - name: size
                      in: path
                      required: true
                      schema:
                        type: number
                        format: double
                    - name: weight
                      in: query
                      schema:
                        type: number
                        format: float
                    - name: density
                      in: header
                      schema:
                        type: number
                        format: double
            %s
            """.formatted(OK_RESPONSES);

    private static final String DELETE_SPEC =
            """
            openapi: 3.0.0
            info:
              title: test
              version: '1'
            paths:
              /items:
                get:
                  operationId: listItems
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
                                  type: string
              /items/{id}:
                parameters:
                  - name: id
                    in: path
                    required: true
                    schema:
                      type: string
                get:
                  operationId: getItem
                  responses:
                    '200':
                      description: OK
                      content:
                        application/json:
                          schema:
                            type: object
                            properties:
                              id:
                                type: string
                delete:
                  operationId: deleteItem
                  responses:
                    '204':
                      description: Deleted
            """;

    private static final String LINKED_PATH_PARAMETER_SPEC =
            """
            openapi: 3.0.0
            info:
              title: test
              version: '1'
            paths:
              /pets:
                get:
                  operationId: listPets
                  responses:
                    '200':
                      description: OK
                      content:
                        application/json:
                          schema:
                            type: array
                            items:
                              $ref: '#/components/schemas/Pet'
              /pets/{petId}:
                get:
                  operationId: getPet
                  parameters:
                    - name: petId
                      in: path
                      required: true
                      schema:
                        type: integer
                      x-trino:
                        field: id
                  responses:
                    '200':
                      description: OK
                      content:
                        application/json:
                          schema:
                            $ref: '#/components/schemas/Pet'
            components:
              schemas:
                Pet:
                  type: object
                  properties:
                    id:
                      type: integer
                    name:
                      type: string
            """;

    private static final String DELETE_WITH_REASON_SPEC = DELETE_SPEC.replace(
            """
                  operationId: deleteItem
            """,
            """
                  operationId: deleteItem
                  parameters:
                    - name: reason
                      in: query
                      schema:
                        type: string
            """);

    private static final String PAGED_SPEC =
            """
            openapi: 3.0.0
            info:
              title: test
              version: '1'
            paths:
              /paged:
                get:
                  operationId: listPaged
                  parameters:
                    - name: page
                      in: query
                      schema:
                        type: integer
                  x-trino:
                    pageParam: page
                    resultsPath: $response.body#/results
                    totalResultsPath: $response.body#/total
                  responses:
                    '200':
                      description: OK
                      content:
                        application/json:
                          schema:
                            type: object
                            properties:
                              results:
                                type: array
                                items:
                                  type: object
                                  properties:
                                    id:
                                      type: integer
                              total:
                                type: string
            """;

    private static final String DYNAMIC_KEYS_SPEC =
            """
            openapi: 3.0.0
            info:
              title: test
              version: '1'
            paths:
              /inventory:
                get:
                  responses:
                    '200':
                      description: OK
                      content:
                        application/json:
                          schema:
                            type: object
                            additionalProperties:
                              type: integer
                              format: int32
                              nullable: true
              /snippets:
                get:
                  x-trino:
                    resultsPath: $response.body#/files
                  responses:
                    '200':
                      description: OK
                      content:
                        application/json:
                          schema:
                            type: object
                            properties:
                              total:
                                type: integer
                                format: int32
                              files:
                                type: object
                                additionalProperties:
                                  type: object
                                  properties:
                                    path:
                                      type: string
                                    key:
                                      type: string
            """;

    private static final String PAGED_ERROR_PATH_SPEC = PAGED_SPEC.replace(
            "totalResultsPath: $response.body#/total\n",
            """
            totalResultsPath: $response.body#/total
                    errorPath: $response.body#/message
            """);

    private static final String CURSOR_SPEC = PAGED_SPEC
            .replace(
                    "- name: page\n          in: query\n          schema:\n            type: integer\n",
                    "- name: cursor\n          in: query\n          schema:\n            type: string\n")
            .replace("pageParam: page\n", "cursorParam: cursor\n        nextCursorPath: $response.body#/next\n");

    private static final String OFFSET_SPEC = PAGED_SPEC
            .replace("- name: page\n", "- name: offset\n")
            .replace("pageParam: page\n", "offsetParam: offset\n");

    private static final String HEADER_CURSOR_SPEC = CURSOR_SPEC.replace("$response.body#/next", "$response.header#X-Next-Cursor");

    private static final String HEADER_TOTAL_SPEC = PAGED_SPEC.replace("$response.body#/total", "$response.header#X-Total-Count");

    private static final String LINK_SPEC = PAGED_SPEC.replace("pageParam: page\n", "nextPageLinkHeader: true\n");

    private static final String LINK_CURSOR_SPEC = CURSOR_SPEC.replace("nextCursorPath: $response.body#/next", "nextPageLinkHeader: true");

    private static final String PAGED_PATH_PARAMETER_SPEC = PAGED_SPEC
            .replace("  /paged:\n", "  /{tenant}/paged:\n")
            .replace(
                    "      parameters:\n",
                    """
                          parameters:
                            - name: tenant
                              in: path
                              required: true
                              schema:
                                type: string
                    """);

    private static final String FORM_SPEC =
            """
            openapi: 3.0.0
            info:
              title: test
              version: '1'
            paths:
              /search:
                post:
                  operationId: searchItems
                  x-trino:
                    sqlOperation: select
                  requestBody:
                    content:
                      application/x-www-form-urlencoded:
                        schema:
                          type: object
                          properties:
                            text:
                              type: string
                            count:
                              type: integer
                            ratio:
                              type: number
                              format: double
                            enabled:
                              type: boolean
                            color:
                              type: string
                              enum: [red, green]
                            tags:
                              type: array
                              items:
                                type: string
                            note:
                              type: string
            %s
            """.formatted(OK_RESPONSES);

    private final Queue<HttpHandler> responses = new ConcurrentLinkedQueue<>();
    private final Queue<URI> requestUris = new ConcurrentLinkedQueue<>();
    private final Queue<Headers> requestHeaders = new ConcurrentLinkedQueue<>();
    private final Queue<String> requestBodies = new ConcurrentLinkedQueue<>();
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
        server = HttpServer.create(new InetSocketAddress(InetAddress.getLoopbackAddress(), 0), 0);
        server.createContext("/", exchange -> {
            requestCount.incrementAndGet();
            requestUris.add(exchange.getRequestURI());
            requestHeaders.add(exchange.getRequestHeaders());
            requestBodies.add(new String(exchange.getRequestBody().readAllBytes(), UTF_8));
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
        requestHeaders.clear();
        requestBodies.clear();
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
    void testPathParameterLinkedToField()
            throws IOException
    {
        responses.add(respond(200, "{\"id\":1,\"name\":\"Cat\"}"));
        assertThat(getRowsMatching(newConfig(LINKED_PATH_PARAMETER_SPEC), "pets", Map.of("id", Domain.singleValue(BIGINT, 1L))))
                .map(List::toString)
                .containsExactly("[1, Cat]");
        // rows of a list get the value of the field from every item
        responses.add(respond(200, "[{\"id\":1,\"name\":\"Cat\"},{\"id\":2,\"name\":\"Dog\"}]"));
        assertThat(getRowsMatching(newConfig(LINKED_PATH_PARAMETER_SPEC), "pets", Map.of()))
                .map(List::toString)
                .containsExactly("[1, Cat]", "[2, Dog]");
        assertThat(requestUris).containsExactly(URI.create("/pets/1"), URI.create("/pets"));
    }

    @Test
    void testNumberParametersWithoutScientificNotation()
            throws IOException
    {
        assertThat(getRowsMatching(newConfig(NUMBER_PARAMETERS_SPEC), "measurements", Map.of(
                "size", Domain.singleValue(DOUBLE, 1.0e7),
                "weight", Domain.singleValue(REAL, (long) floatToIntBits(2.5e-7f)),
                "density", Domain.singleValue(DOUBLE, 1.0e-5)))).isEmpty();
        assertThat(requestUris).containsExactly(URI.create("/measurements/10000000?weight=0.00000025"));
        assertThat(requestHeaders.peek().getFirst("density")).isEqualTo("0.00001");
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
    void testSendsContentTypeOnlyWithBody()
            throws IOException
    {
        assertThat(getRows(newConfig(), "items", null)).isEmpty();
        assertThat(getRows(newConfig(), "search", null)).isEmpty();
        responses.add(respond(204, ""));
        deleteRow(newConfig(DELETE_SPEC), "1");
        assertThat(requestHeaders)
                .map(headers -> headers.getFirst("Content-Type"))
                .containsExactly(null, "application/json; charset=utf-8", null);
    }

    @Test
    void testSendsFormRequestBodies()
            throws IOException
    {
        ArrayType tagsType = new ArrayType(VARCHAR);
        BlockBuilder tags = VARCHAR.createBlockBuilder(null, 2);
        VARCHAR.writeString(tags, "new");
        VARCHAR.writeString(tags, "on sale");
        assertThat(getRowsMatching(newConfig(FORM_SPEC), "search", Map.of(
                "text", Domain.singleValue(VARCHAR, utf8Slice("a b&c=d")),
                "count", Domain.singleValue(BIGINT, 3L),
                "ratio", Domain.singleValue(DOUBLE, 1.0e7),
                "enabled", Domain.singleValue(BOOLEAN, true),
                "color", Domain.singleValue(VARCHAR, utf8Slice("green")),
                "tags", Domain.singleValue(tagsType, tags.build())))).isEmpty();
        assertThat(requestHeaders.peek().getFirst("Content-Type")).isEqualTo("application/x-www-form-urlencoded");
        // the note isn't filtered on, so it's not sent, and arrays are sent as repeated fields
        assertThat(requestBodies).containsExactly("text=a+b%26c%3Dd&count=3&ratio=10000000&enabled=true&color=green&tags=new&tags=on+sale");
    }

    @Test
    void testFailsToSendUnsupportedFormValues()
            throws IOException
    {
        String unionSpec = FORM_SPEC.replace(
                "note:\n                  type: string",
                "note:\n                  oneOf: [{type: string}, {type: integer}]");
        // union columns hold JSON text, which can be an object, even if the alternatives are plain values
        assertThatThrownBy(() -> getRowsMatching(newConfig(unionSpec), "search", Map.of("note", Domain.singleValue(VARCHAR, utf8Slice("{\"a\":1}")))))
                .isInstanceOfSatisfying(TrinoException.class, e -> assertThat(e.getErrorCode()).isEqualTo(NOT_SUPPORTED.toErrorCode()))
                .hasMessage("Field note of the form request body of POST /search has a nested value, which can't be sent as a form field");
        assertThatThrownBy(() -> getRows(newConfig(FORM_SPEC.replace("application/x-www-form-urlencoded:", "multipart/form-data:")), "search", null))
                .isInstanceOfSatisfying(TrinoException.class, e -> assertThat(e.getErrorCode()).isEqualTo(NOT_SUPPORTED.toErrorCode()))
                .hasMessage("Operation POST /search only accepts multipart/form-data request bodies, which are not supported");
        assertThat(requestCount).hasValue(0);
    }

    @Test
    void testSendsDeclaredMediaTypes()
            throws IOException
    {
        assertThat(getRows(newConfig(MEDIA_TYPES_SPEC), "users", null)).isEmpty();
        assertThat(getRows(newConfig(MEDIA_TYPES_SPEC), "usage", null)).isEmpty();
        assertThat(getRows(newConfig(MEDIA_TYPES_SPEC), "items", null)).isEmpty();
        assertThat(requestHeaders)
                .map(headers -> headers.getFirst("Accept"))
                .containsExactly("application/scim+json", "application/json; datetime-format=rfc3339", "application/json; charset=utf-8");
    }

    @Test
    void testReadsDeclaredSuccessStatuses()
            throws IOException
    {
        responses.add(respond(203, "[{\"id\":1}]"));
        assertThat(getRows(newConfig(SUCCESS_STATUSES_SPEC), "mirrored", null))
                .map(List::toString)
                .containsExactly("[1]");
        responses.add(respond(201, "[{\"id\":2}]"));
        assertThat(getRows(newConfig(SUCCESS_STATUSES_SPEC), "search", null))
                .map(List::toString)
                .containsExactly("[2]");
        responses.add(respond(204, ""));
        assertThat(getRows(newConfig(SUCCESS_STATUSES_SPEC), "mirrored", null)).isEmpty();
    }

    @Test
    void testDoesNotFollowRedirects()
            throws IOException
    {
        responses.add(respond(302, "", "Location", "https://other.example/items?api_key=secret"));

        assertThatThrownBy(() -> getRows(newConfig(), "items", null))
                .isInstanceOfSatisfying(TrinoException.class, e -> assertThat(e.getErrorCode()).isEqualTo(OPENAPI_HTTP_ERROR.toErrorCode()))
                .hasMessage("Request GET %s/items for operation listItems failed with HTTP 302, redirecting to https://other.example/items, but redirects are not followed, so credentials are not sent to other hosts", baseUri);
        assertThat(requestCount).hasValue(1);
    }

    @Test
    void testDescribesRelativeRedirects()
            throws IOException
    {
        responses.add(respond(307, "", "Location", "/moved"));

        assertThatThrownBy(() -> getRows(newConfig(), "items", null))
                .hasMessage("Request GET %s/items for operation listItems failed with HTTP 307, redirecting to %s/moved, but redirects are not followed, so credentials are not sent to other hosts", baseUri, baseUri);
        assertThat(requestCount).hasValue(1);
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
    void testRetriesPostReads()
            throws IOException
    {
        responses.add(respond(502, ""));
        responses.add(respond(503, ""));
        responses.add(respond(200, "[{\"id\":1}]"));

        assertThat(getRows(newConfig(), "search", null)).hasSize(1);
        assertThat(requestCount).hasValue(3);
    }

    @Test
    void testDoesNotRetryNonIdempotentRequests()
            throws IOException
    {
        responses.add(respond(502, ""));

        // the same operation also inserts rows, so repeating it can create duplicates
        assertThatThrownBy(() -> getRows(newConfig(), "created", null))
                .hasMessage("Request POST %s/created for operation createItems failed with HTTP 502", baseUri);
        assertThat(requestCount).hasValue(1);

        requestCount.set(0);
        responses.add(respond(502, ""));

        // a PATCH can apply relative changes, like appending to a list, so repeating it can change the result
        assertThatThrownBy(() -> getRows(newConfig(), "patched", null))
                .hasMessage("Request PATCH %s/patched for operation patchItems failed with HTTP 502", baseUri);
        assertThat(requestCount).hasValue(1);
    }

    @Test
    void testRetriesTooManyRequestsForAnyMethod()
            throws IOException
    {
        responses.add(respond(429, ""));

        assertThat(getRows(newConfig(), "created", null)).isEmpty();
        assertThat(requestCount).hasValue(2);

        requestCount.set(0);
        responses.add(respond(429, ""));

        assertThat(getRows(newConfig(), "patched", null)).isEmpty();
        assertThat(requestCount).hasValue(2);
    }

    @Test
    void testRowIdHasValuesOfPathParameters()
            throws IOException
    {
        responses.add(respond(200, "{\"id\":\"1\"}"));
        assertThat(getRows(newConfig(DELETE_SPEC), "items", "id", "1"))
                .containsExactly(List.of("{\"id\":\"1\"}", "1"));
    }

    @Test
    void testRowIdsOfListedResources()
            throws IOException
    {
        responses.add(respond(200, "[{\"id\":\"1\"},{\"id\":\"2\"}]"));
        assertThat(getRows(newConfig(DELETE_SPEC), "items", null))
                .containsExactly(List.of("{\"id\":\"1\"}", "1"), List.of("{\"id\":\"2\"}", "2"));
        assertThat(requestUris).containsExactly(URI.create("/items"));
    }

    @Test
    void testDeletesResourcesIdentifiedByRowIds()
            throws IOException
    {
        responses.add(respond(204, ""));
        responses.add(respond(204, ""));
        // a filter matching many rows doesn't identify the resource of any of them
        Map<String, OpenApiColumn> columns = createSpec(newConfig(DELETE_SPEC)).getTables().get("items").stream()
                .collect(toImmutableMap(OpenApiColumn::getName, identity()));
        TupleDomain<ColumnHandle> constraint = TupleDomain.withColumnDomains(Map.of(
                columns.get("id").getHandle(), Domain.multipleValues(VARCHAR, List.of(utf8Slice("1"), utf8Slice("2")))));
        deleteRow(newConfig(DELETE_SPEC), constraint, "{\"id\":\"1\"}");
        deleteRow(newConfig(DELETE_SPEC), constraint, "{\"id\":\"2\"}");
        assertThat(requestUris).containsExactly(URI.create("/items/1"), URI.create("/items/2"));
    }

    @Test
    void testDeleteFailsWithoutRowIdentifiers()
            throws IOException
    {
        assertThatThrownBy(() -> deleteRow(newConfig(DELETE_SPEC), TupleDomain.all(), "{}"))
                .isInstanceOfSatisfying(TrinoException.class, e -> assertThat(e.getErrorCode()).isEqualTo(INVALID_ROW_FILTER.toErrorCode()))
                .hasMessage("Cannot DELETE rows of table items, because they don't have values of the path parameters that identify their resources: id (column id) of DELETE /items/{id}. " +
                        "Filter on the columns of these parameters, or link the parameters to the response fields with the same values, using the field property of the x-trino extension of the parameters");
        assertThat(requestCount).hasValue(0);
    }

    @Test
    void testDeleteFailsWithSeveralValuesOfParameter()
            throws IOException
    {
        Map<String, OpenApiColumn> columns = createSpec(newConfig(DELETE_WITH_REASON_SPEC)).getTables().get("items").stream()
                .collect(toImmutableMap(OpenApiColumn::getName, identity()));
        TupleDomain<ColumnHandle> constraint = TupleDomain.withColumnDomains(Map.of(
                columns.get("reason").getHandle(), Domain.multipleValues(VARCHAR, List.of(utf8Slice("duplicate"), utf8Slice("spam")))));
        assertThatThrownBy(() -> deleteRow(newConfig(DELETE_WITH_REASON_SPEC), constraint, "{\"id\":\"1\"}"))
                .isInstanceOfSatisfying(TrinoException.class, e -> assertThat(e.getErrorCode()).isEqualTo(INVALID_ROW_FILTER.toErrorCode()))
                .hasMessage("Filter on reason has more than one value, but a request can only send one");
        assertThat(requestCount).hasValue(0);
    }

    @Test
    void testRetriedDeleteOfMissingResourceSucceeds()
            throws IOException
    {
        // the first attempt deleted the resource, but the gateway timed out waiting for the response
        responses.add(respond(504, ""));
        responses.add(respond(404, ""));
        deleteRow(newConfig(DELETE_SPEC), "1");

        requestCount.set(0);
        responses.add(respond(502, ""));
        responses.add(respond(410, ""));
        deleteRow(newConfig(DELETE_SPEC), "1");
        assertThat(requestCount).hasValue(2);
    }

    @Test
    void testDeleteOfMissingResourceFails()
            throws IOException
    {
        responses.add(respond(404, ""));

        assertThatThrownBy(() -> deleteRow(newConfig(DELETE_SPEC), "1"))
                .hasMessage("Request DELETE %s/items/1 for operation deleteItem failed with HTTP 404", baseUri);
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
    void testRetriesBodyReadFailuresWithHttpCache()
            throws IOException
    {
        OpenApiConfig config = newConfig()
                .setHttpCacheDirectory(Files.createTempDirectory(tempDir, "cache").toString());
        responses.add(truncatedBody());
        responses.add(respond(200, "[{\"id\": 1}]", "Cache-Control", "max-age=60"));

        assertThat(getRows(config, "items", null)).hasSize(1);
        assertThat(requestCount).hasValue(2);
    }

    @Test
    void testBodyReadFailureWithHttpCacheIncludesRequestContext()
            throws IOException
    {
        OpenApiConfig config = newConfig()
                .setMaxRetries(1)
                .setHttpCacheDirectory(Files.createTempDirectory(tempDir, "cache").toString());
        responses.add(truncatedBody());
        responses.add(truncatedBody());

        assertThatThrownBy(() -> getRows(config, "items", "s3cr3t"))
                .isInstanceOfSatisfying(TrinoException.class, e -> assertThat(e.getErrorCode()).isEqualTo(OPENAPI_CONNECTION_ERROR.toErrorCode()))
                .hasMessageStartingWith("Request GET %s/items for operation listItems failed after 2 attempts: ", baseUri)
                .hasCauseInstanceOf(IOException.class)
                .message()
                .doesNotContain("s3cr3t");
        assertThat(requestCount).hasValue(2);
    }

    @Test
    void testHttpCacheIsSeparatedByServerAndCredentials()
            throws IOException
    {
        String cacheDirectory = Files.createTempDirectory(tempDir, "cache").toString();
        OpenApiConfig config = newConfig()
                .setAuthenticationType(AuthenticationType.HTTP)
                .setUsername("user")
                .setPassword("first-password")
                .setHttpCacheDirectory(cacheDirectory);
        responses.add(respond(200, "[{\"id\": 1}]", "Cache-Control", "max-age=60"));
        assertThat(getRows(config, "items", null)).hasSize(1);
        assertThat(getRows(config, "items", null)).hasSize(1);
        assertThat(requestCount).hasValue(1);

        // the username identifies the account, so a rotated password keeps the cached responses
        assertThat(getRows(config.setPassword("second-password"), "items", null)).hasSize(1);
        assertThat(requestCount).hasValue(1);

        // like a catalog created again with the same name, but for another account, or server
        assertThat(getRows(config.setUsername("other-user"), "items", null)).isEmpty();
        assertThat(getRows(config.setBaseUri(URI.create(baseUri + "/")), "items", null)).isEmpty();
        // tokens are the only identity of their accounts
        config.setAuthenticationType(AuthenticationType.HTTP).setAuthenticationScheme(AuthenticationScheme.BEARER);
        assertThat(getRows(config.setBearerToken("first-token"), "items", null)).isEmpty();
        assertThat(getRows(config.setBearerToken("first-token"), "items", null)).isEmpty();
        assertThat(getRows(config.setBearerToken("second-token"), "items", null)).isEmpty();
        config.setAuthenticationType(AuthenticationType.API_KEY).setApiKeyName("api_key");
        assertThat(getRows(config.setApiKeyValue("first-key"), "items", null)).isEmpty();
        assertThat(getRows(config.setApiKeyValue("second-key"), "items", null)).isEmpty();
        assertThat(requestCount).hasValue(8);
        try (Stream<Path> directories = Files.list(Path.of(cacheDirectory))) {
            assertThat(directories.map(directory -> directory.getFileName().toString()).toList())
                    .hasSize(7)
                    .allSatisfy(name -> assertThat(name).startsWith("test-").doesNotContain("password", "user", "token", "key"));
        }
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
        assertThat(getRows(newConfig().setMaxRequestsPerSecond(2.0), "items", null)).isEmpty();
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
    void testUnauthorizedWithoutChallengeIncludesErrorPathMessage()
            throws IOException
    {
        // Jetty fails 401 responses without a WWW-Authenticate header, which many APIs send, as protocol violations
        responses.add(respond(401, "{\"message\":\"Bad credentials\"}"));

        assertThatThrownBy(() -> getRows(newConfig(), "items", null))
                .isInstanceOfSatisfying(TrinoException.class, e -> assertThat(e.getErrorCode()).isEqualTo(OPENAPI_HTTP_ERROR.toErrorCode()))
                .hasMessage("Request GET %s/items for operation listItems failed with HTTP 401: \"Bad credentials\"", baseUri)
                .hasNoCause();
        assertThat(requestCount).hasValue(1);
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
    void testDecompressedResponseOverMaxSizeFails()
            throws IOException
    {
        // like a gzip bomb, it's small before it's decompressed
        responses.add(respondGzip(200, "[" + "{\"id\":1},".repeat(10_000) + "{\"id\":1}]"));

        assertThatThrownBy(() -> getRows(newConfig().setMaxResponseSize(DataSize.of(64, KILOBYTE)), "items", "s3cr3t"))
                .isInstanceOfSatisfying(TrinoException.class, e -> assertThat(e.getErrorCode()).isEqualTo(OPENAPI_INVALID_RESPONSE.toErrorCode()))
                .hasMessage("Request GET %s/items for operation listItems failed: the response is larger than 64kB, increase the max-response-size catalog property to read it", baseUri);
        assertThat(requestCount).hasValue(1);
    }

    @Test
    void testResponseOverMaxSizeFailsWithHttpCache()
            throws IOException
    {
        OpenApiConfig config = newConfig()
                .setMaxResponseSize(DataSize.of(64, KILOBYTE))
                .setHttpCacheDirectory(Files.createTempDirectory(tempDir, "cache").toString());
        responses.add(respond(200, "[" + "{\"id\":1},".repeat(10_000) + "{\"id\":1}]", "Cache-Control", "max-age=60"));

        assertThatThrownBy(() -> getRows(config, "items", null))
                .isInstanceOfSatisfying(TrinoException.class, e -> assertThat(e.getErrorCode()).isEqualTo(OPENAPI_INVALID_RESPONSE.toErrorCode()))
                .hasMessage("Request GET %s/items for operation listItems failed: the response is larger than 64kB, increase the max-response-size catalog property to read it", baseUri);
        assertThat(requestCount).hasValue(1);
    }

    @Test
    void testResponseOverHttpClientLimitIsNotRetried()
            throws IOException
    {
        // over the default limit of the real HTTP client of this test, which is lower than the one of the connector,
        // so this fails if Airlift changes the message the retry policy recognizes its limit by
        responses.add(respond(200, "[" + "{\"id\":1},".repeat(2_500_000) + "{\"id\":1}]"));

        assertThatThrownBy(() -> getRows(newConfig().setMaxResponseSize(DataSize.of(1, GIGABYTE)), "items", null))
                .isInstanceOfSatisfying(TrinoException.class, e -> assertThat(e.getErrorCode()).isEqualTo(OPENAPI_CONNECTION_ERROR.toErrorCode()))
                .hasMessageContaining("exceeded maximum length");
        assertThat(requestCount).hasValue(1);
    }

    @Test
    void testResponseBelowMaxSizeIsRead()
            throws IOException
    {
        String body = "[" + "{\"id\":1},".repeat(999) + "{\"id\":1}]";
        responses.add(respondGzip(200, body));

        assertThat(getRows(newConfig().setMaxResponseSize(DataSize.ofBytes(body.length())), "items", null)).hasSize(1000);
    }

    @Test
    void testReadsResponsesWithLoggedBodies()
            throws IOException
    {
        Logging logging = Logging.initialize();
        logging.setLevel("trino-openapi-bodies", Level.DEBUG);
        try {
            responses.add(respond(200, "[{\"id\":1}]"));
            assertThat(getRows(newConfig(), "items", null)).hasSize(1);

            responses.add(respond(200, "not JSON"));
            assertThatThrownBy(() -> getRows(newConfig(), "items", null))
                    .isInstanceOfSatisfying(TrinoException.class, e -> assertThat(e.getErrorCode()).isEqualTo(OPENAPI_INVALID_RESPONSE.toErrorCode()))
                    .hasMessage("Response to GET %s/items is not valid JSON", baseUri);
        }
        finally {
            logging.setLevel("trino-openapi-bodies", Level.INFO);
        }
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
    void testTotalSentAsString()
            throws IOException
    {
        // the server returns the last page for every page number past it, so only the total ends pagination
        responses.add(respond(200, "{\"results\":[{\"id\":1}],\"total\":\"2\"}"));
        responses.add(respond(200, "{\"results\":[{\"id\":2}],\"total\":\"2\"}"));
        responses.add(respond(200, "{\"results\":[{\"id\":2}],\"total\":\"2\"}"));

        assertThat(getRows(newConfig(PAGED_SPEC), "paged", null)).hasSize(2);
        assertThat(requestCount).hasValue(2);
    }

    @Test
    void testTotalThatIsNotANumberIsExternalError()
            throws IOException
    {
        responses.add(respond(200, "{\"results\":[{\"id\":1}],\"total\":\"internal-details\"}"));

        assertThatThrownBy(() -> getRows(newConfig(PAGED_SPEC), "paged", null))
                .isInstanceOfSatisfying(TrinoException.class, e -> assertThat(e.getErrorCode()).isEqualTo(OPENAPI_INVALID_RESPONSE.toErrorCode()))
                .hasMessage("Value of totalResultsPath /total in the response to GET %s/paged is not a whole number", baseUri)
                .satisfies(e -> assertThat(getStackTraceAsString(e)).doesNotContain("internal-details"));
    }

    @Test
    void testErrorWithStatusOkIncludesErrorPathMessage()
            throws IOException
    {
        responses.add(respond(200, "{\"message\":\"Invalid token\",\"details\":\"internal-details\"}"));

        assertThatThrownBy(() -> getRows(newConfig(PAGED_ERROR_PATH_SPEC), "paged", null))
                .isInstanceOfSatisfying(TrinoException.class, e -> assertThat(e.getErrorCode()).isEqualTo(OPENAPI_HTTP_ERROR.toErrorCode()))
                .hasMessage("Request GET %s/paged for operation listPaged failed with HTTP 200: \"Invalid token\"", baseUri)
                .satisfies(e -> assertThat(getStackTraceAsString(e)).doesNotContain("internal-details"));
        assertThat(requestCount).hasValue(1);

        // null results with an error are an error, not an empty page
        responses.add(respond(200, "{\"results\":null,\"message\":\"Invalid token\"}"));

        assertThatThrownBy(() -> getRows(newConfig(PAGED_ERROR_PATH_SPEC), "paged", null))
                .isInstanceOfSatisfying(TrinoException.class, e -> assertThat(e.getErrorCode()).isEqualTo(OPENAPI_HTTP_ERROR.toErrorCode()))
                .hasMessage("Request GET %s/paged for operation listPaged failed with HTTP 200: \"Invalid token\"", baseUri);
    }

    @Test
    void testErrorWithStatusOkWithoutErrorPath()
            throws IOException
    {
        responses.add(respond(200, "{\"message\":\"Invalid token\",\"details\":\"internal-details\"}"));

        assertThatThrownBy(() -> getRows(newConfig(PAGED_SPEC), "paged", null))
                .isInstanceOfSatisfying(TrinoException.class, e -> assertThat(e.getErrorCode()).isEqualTo(OPENAPI_INVALID_RESPONSE.toErrorCode()))
                .hasMessage("Response to GET %s/paged has no value at resultsPath /results", baseUri)
                .satisfies(e -> assertThat(getStackTraceAsString(e)).doesNotContain("Invalid token", "internal-details"));
    }

    @Test
    void testScalarResultsAreExternalError()
            throws IOException
    {
        responses.add(respond(200, "{\"results\":\"internal-details\"}"));

        assertThatThrownBy(() -> getRows(newConfig(PAGED_ERROR_PATH_SPEC), "paged", null))
                .isInstanceOfSatisfying(TrinoException.class, e -> assertThat(e.getErrorCode()).isEqualTo(OPENAPI_INVALID_RESPONSE.toErrorCode()))
                .hasMessage("Value of resultsPath /results in the response to GET %s/paged is a JSON STRING, not an array or an object", baseUri)
                .satisfies(e -> assertThat(getStackTraceAsString(e)).doesNotContain("internal-details"));
    }

    @Test
    void testNullResultsAreEmpty()
            throws IOException
    {
        responses.add(respond(200, "{\"results\":null}"));

        assertThat(getRows(newConfig(PAGED_SPEC), "paged", null)).isEmpty();
        assertThat(requestCount).hasValue(1);
    }

    @Test
    void testPlainValueResults()
            throws IOException
    {
        responses.add(respond(200, "[\"a\",\"b\"]"));
        assertThat(getRows(newConfig(plainValuesSpec("type: string")), "values", null))
                .containsExactly(List.of("a"), List.of("b"));

        responses.add(respond(200, "{\"results\":[\"c\"]}"));
        assertThat(getRows(newConfig(plainValuesSpec("type: string")), "wrapped_values", null))
                .containsExactly(List.of("c"));
    }

    @Test
    void testPlainValueTypes()
            throws IOException
    {
        responses.add(respond(200, "[1.5,2]"));
        assertThat(getRows(newConfig(plainValuesSpec("type: number\nformat: double")), "values", null))
                .containsExactly(List.of(1.5), List.of(2.0));

        responses.add(respond(200, "[1,2]"));
        assertThat(getRows(newConfig(plainValuesSpec("type: integer\nformat: int32")), "values", null))
                .containsExactly(List.of(1), List.of(2));

        responses.add(respond(200, "[true,false]"));
        assertThat(getRows(newConfig(plainValuesSpec("type: boolean")), "values", null))
                .containsExactly(List.of(true), List.of(false));
    }

    @Test
    void testNullPlainValues()
            throws IOException
    {
        responses.add(respond(200, "[\"a\",null]"));
        assertThat(getRows(newConfig(plainValuesSpec("type: string\nnullable: true")), "values", null))
                .containsExactly(List.of("a"), singletonList(null));

        responses.add(respond(200, "{\"results\":[null,2.5]}"));
        assertThat(getRows(newConfig(plainValuesSpec("type: number\nformat: double\nnullable: true")), "wrapped_values", null))
                .containsExactly(singletonList(null), List.of(2.5));
    }

    @Test
    void testDynamicKeyResults()
            throws IOException
    {
        responses.add(respond(200, "{\"a\":1,\"b\":null}"));
        assertThat(getRows(newConfig(DYNAMIC_KEYS_SPEC), "inventory", null))
                .containsExactly(List.of("a", 1), Arrays.asList("b", null));

        // the entry key column is named key_2, because the entries have a field named key
        responses.add(respond(200, "{\"total\":2,\"files\":{\"src/a.java\":{\"path\":\"a.java\",\"key\":\"k1\"},\"src/b.java\":{\"path\":\"b.java\"}}}"));
        assertThat(getRows(newConfig(DYNAMIC_KEYS_SPEC), "snippets", null))
                .containsExactly(List.of(2, "src/a.java", "a.java", "k1"), Arrays.asList(2, "src/b.java", "b.java", null));

        responses.add(respond(200, "{\"total\":0,\"files\":null}"));
        assertThat(getRows(newConfig(DYNAMIC_KEYS_SPEC), "snippets", null)).isEmpty();

        responses.add(respond(200, "{\"total\":1,\"files\":[\"internal-details\"]}"));
        assertThatThrownBy(() -> getRows(newConfig(DYNAMIC_KEYS_SPEC), "snippets", null))
                .isInstanceOfSatisfying(TrinoException.class, e -> assertThat(e.getErrorCode()).isEqualTo(OPENAPI_INVALID_RESPONSE.toErrorCode()))
                .hasMessage("Response to GET %s/snippets contains a JSON STRING where an object was expected", baseUri);
    }

    @Test
    void testCursorPagination()
            throws IOException
    {
        responses.add(respond(200, "{\"results\":[{\"id\":1}],\"next\":\"a b\"}"));
        // an empty page with a cursor doesn't end pagination
        responses.add(respond(200, "{\"results\":[],\"next\":\"c\"}"));
        responses.add(respond(200, "{\"results\":[{\"id\":2}],\"next\":null}"));

        assertThat(getRows(newConfig(CURSOR_SPEC), "paged", null))
                .map(row -> String.valueOf(row.get(1)))
                .containsExactly("1", "2");
        assertThat(requestUris).containsExactly(URI.create("/paged"), URI.create("/paged?cursor=a%20b"), URI.create("/paged?cursor=c"));
    }

    @Test
    void testEmptyCursorEndsPagination()
            throws IOException
    {
        responses.add(respond(200, "{\"results\":[{\"id\":1}],\"next\":\"\"}"));

        assertThat(getRows(newConfig(CURSOR_SPEC), "paged", null)).hasSize(1);
        assertThat(requestCount).hasValue(1);
    }

    @Test
    void testCursorFilterRequestsOnlyThatPage()
            throws IOException
    {
        responses.add(respond(200, "{\"results\":[{\"id\":2}],\"next\":\"c\"}"));

        assertThat(getRows(newConfig(CURSOR_SPEC), "paged", "cursor", "b")).hasSize(1);
        assertThat(requestUris).containsExactly(URI.create("/paged?cursor=b"));
    }

    @Test
    void testCursorThatIsNotAScalarIsExternalError()
            throws IOException
    {
        responses.add(respond(200, "{\"results\":[{\"id\":1}],\"next\":{\"token\":\"internal-details\"}}"));

        assertThatThrownBy(() -> getRows(newConfig(CURSOR_SPEC), "paged", null))
                .isInstanceOfSatisfying(TrinoException.class, e -> assertThat(e.getErrorCode()).isEqualTo(OPENAPI_INVALID_RESPONSE.toErrorCode()))
                .hasMessage("Value of nextCursorPath /next in the response to GET %s/paged is a JSON OBJECT, not a string or a whole number", baseUri)
                .satisfies(e -> assertThat(getStackTraceAsString(e)).doesNotContain("internal-details"));
    }

    @Test
    void testOffsetPagination()
            throws IOException
    {
        // the server returns fewer results than on the first page, so the offset is the number of results already read
        responses.add(respond(200, "{\"results\":[{\"id\":1},{\"id\":2}]}"));
        responses.add(respond(200, "{\"results\":[{\"id\":3}]}"));
        responses.add(respond(200, "{\"results\":[]}"));

        assertThat(getRows(newConfig(OFFSET_SPEC), "paged", null)).hasSize(3);
        assertThat(requestUris).containsExactly(URI.create("/paged?offset=0"), URI.create("/paged?offset=2"), URI.create("/paged?offset=3"));
    }

    @Test
    void testOffsetPaginationEndsWithTotal()
            throws IOException
    {
        responses.add(respond(200, "{\"results\":[{\"id\":1}],\"total\":\"2\"}"));
        responses.add(respond(200, "{\"results\":[{\"id\":2}],\"total\":\"2\"}"));

        assertThat(getRows(newConfig(OFFSET_SPEC), "paged", null)).hasSize(2);
        assertThat(requestUris).containsExactly(URI.create("/paged?offset=0"), URI.create("/paged?offset=1"));
    }

    @Test
    void testTotalInHeader()
            throws IOException
    {
        // the server returns the last page for every page number past it, so only the total ends pagination
        responses.add(respond(200, "{\"results\":[{\"id\":1}]}", "X-Total-Count", "2"));
        responses.add(respond(200, "{\"results\":[{\"id\":2}]}", "X-Total-Count", "2"));
        responses.add(respond(200, "{\"results\":[{\"id\":2}]}", "X-Total-Count", "2"));

        assertThat(getRows(newConfig(HEADER_TOTAL_SPEC), "paged", null)).hasSize(2);
        assertThat(requestCount).hasValue(2);

        responses.add(respond(200, "{\"results\":[{\"id\":1}]}", "X-Total-Count", "internal-details"));

        assertThatThrownBy(() -> getRows(newConfig(HEADER_TOTAL_SPEC), "paged", null))
                .isInstanceOfSatisfying(TrinoException.class, e -> assertThat(e.getErrorCode()).isEqualTo(OPENAPI_INVALID_RESPONSE.toErrorCode()))
                .hasMessage("Value of totalResultsPath header X-Total-Count in the response to GET %s/paged is not a whole number", baseUri)
                .satisfies(e -> assertThat(getStackTraceAsString(e)).doesNotContain("internal-details"));
    }

    @Test
    void testCursorInHeader()
            throws IOException
    {
        responses.add(respond(200, "{\"results\":[{\"id\":1}]}", "X-Next-Cursor", "b"));
        responses.add(respond(200, "{\"results\":[{\"id\":2}]}"));

        assertThat(getRows(newConfig(HEADER_CURSOR_SPEC), "paged", null)).hasSize(2);
        assertThat(requestUris).containsExactly(URI.create("/paged"), URI.create("/paged?cursor=b"));
    }

    @Test
    void testFollowsNextLinks()
            throws IOException
    {
        responses.add(respond(200, "{\"results\":[{\"id\":1}]}", "Link", "<%s/paged?page=2&per_page=1>; rel=\"next\", <%s/paged?page=3>; rel=\"last\"".formatted(baseUri, baseUri)));
        // links can be relative to the URI of the request
        responses.add(respond(200, "{\"results\":[{\"id\":2}]}", "Link", "</paged?page=3>; rel=\"next\""));
        responses.add(respond(200, "{\"results\":[{\"id\":3}]}", "Link", "</paged?page=1>; rel=\"first\""));

        assertThat(getRows(newConfig(LINK_SPEC), "paged", null)).hasSize(3);
        assertThat(requestUris).containsExactly(URI.create("/paged"), URI.create("/paged?page=2&per_page=1"), URI.create("/paged?page=3"));
    }

    @Test
    void testNextLinkToAnotherHostFails()
            throws IOException
    {
        responses.add(respond(200, "{\"results\":[{\"id\":1}]}", "Link", "<https://other.example/paged?page=2&api_key=secret>; rel=\"next\""));

        assertThatThrownBy(() -> getRows(newConfig(LINK_SPEC), "paged", null))
                .isInstanceOfSatisfying(TrinoException.class, e -> assertThat(e.getErrorCode()).isEqualTo(OPENAPI_INVALID_RESPONSE.toErrorCode()))
                .hasMessage("Response to GET %s/paged links to the next page on another host, https://other.example/paged, but requests are sent only to the server of the operation, so credentials are not sent to other hosts", baseUri)
                .satisfies(e -> assertThat(getStackTraceAsString(e)).doesNotContain("secret"));

        responses.add(respond(200, "{\"results\":[{\"id\":1}]}", "Link", "<http://127.0.0.1:1/paged?page=2>; rel=\"next\""));

        assertThatThrownBy(() -> getRows(newConfig(LINK_SPEC), "paged", null))
                .hasMessage("Response to GET %s/paged links to the next page on another host, http://127.0.0.1:1/paged, but requests are sent only to the server of the operation, so credentials are not sent to other hosts", baseUri);
        assertThat(requestCount).hasValue(2);
    }

    @Test
    void testCursorFromNextLink()
            throws IOException
    {
        // only the cursor is read from the link, so it can point to another host
        responses.add(respond(200, "{\"results\":[{\"id\":1}]}", "Link", "<https://other.example/paged?limit=1&cursor=a%20b>; rel=\"next\""));
        responses.add(respond(200, "{\"results\":[{\"id\":2}]}"));

        assertThat(getRows(newConfig(LINK_CURSOR_SPEC), "paged", null)).hasSize(2);
        assertThat(requestUris).containsExactly(URI.create("/paged"), URI.create("/paged?cursor=a%20b"));
    }

    @Test
    void testNotFoundListFails()
            throws IOException
    {
        responses.add(respond(404, "{\"message\":\"Not Found\"}"));

        assertThatThrownBy(() -> getRows(newConfig(), "items", null))
                .isInstanceOfSatisfying(TrinoException.class, e -> assertThat(e.getErrorCode()).isEqualTo(OPENAPI_HTTP_ERROR.toErrorCode()))
                .hasMessage("Request GET %s/items for operation listItems failed with HTTP 404: \"Not Found\"", baseUri);
        assertThat(requestCount).hasValue(1);
    }

    @Test
    void testNotFoundWithPathParametersIsEmpty()
            throws IOException
    {
        responses.add(respond(404, "{\"message\":\"Not Found\"}"));

        assertThat(getRows(newConfig(PATH_PARAMETER_SPEC), "items", "tenant", "missing")).isEmpty();
        assertThat(requestUris).containsExactly(URI.create("/missing/items"));
    }

    @Test
    void testDeclaredNotFoundIsEmpty()
            throws IOException
    {
        responses.add(respond(404, "{\"message\":\"Not Found\"}"));

        assertThat(getRows(newConfig(DECLARED_NOT_FOUND_SPEC), "items", null)).isEmpty();
        assertThat(requestCount).hasValue(1);
    }

    @Test
    void testNotFoundPageAfterFirstFails()
            throws IOException
    {
        responses.add(respond(200, "{\"results\":[{\"id\":1}],\"total\":2}"));
        responses.add(respond(404, "{\"message\":\"Not Found\"}"));

        assertThatThrownBy(() -> getRows(newConfig(PAGED_PATH_PARAMETER_SPEC), "paged", "tenant", "a"))
                .isInstanceOfSatisfying(TrinoException.class, e -> assertThat(e.getErrorCode()).isEqualTo(OPENAPI_HTTP_ERROR.toErrorCode()))
                .hasMessage("Request GET %s/a/paged for operation listPaged failed with HTTP 404", baseUri);
        assertThat(requestUris).containsExactly(URI.create("/a/paged?page=1"), URI.create("/a/paged?page=2"));
    }

    @Test
    void testNotFoundFirstPageWithPathParametersIsEmpty()
            throws IOException
    {
        responses.add(respond(404, "{\"message\":\"Not Found\"}"));

        assertThat(getRows(newConfig(PAGED_PATH_PARAMETER_SPEC), "paged", "tenant", "missing")).isEmpty();
        assertThat(requestCount).hasValue(1);
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

    private static String plainValuesSpec(String itemSchema)
    {
        return """
               openapi: 3.0.0
               info:
                 title: test
                 version: '1'
               paths:
                 /values:
                   get:
                     responses:
                       '200':
                         description: OK
                         content:
                           application/json:
                             schema:
                               type: array
                               items:
               %s
                 /wrapped_values:
                   get:
                     x-trino:
                       resultsPath: $response.body#/results
                     responses:
                       '200':
                         description: OK
                         content:
                           application/json:
                             schema:
                               type: object
                               properties:
                                 results:
                                   type: array
                                   items:
               %s
               """.formatted(itemSchema.indent(18).stripTrailing(), itemSchema.indent(22).stripTrailing());
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
        return getRowsMatching(config, tableName, value == null ? Map.of() : Map.of(columnName, Domain.singleValue(VARCHAR, utf8Slice(value))));
    }

    private List<List<?>> getRowsMatching(OpenApiConfig config, String tableName, Map<String, Domain> columnDomains)
    {
        OpenApiSpec spec = createSpec(config);
        OpenApiServers servers = new OpenApiServers(config, spec);
        OpenApiClient client = new OpenApiClient(config, new CatalogName("test"), httpClient, new Authentication(config, spec, servers, httpClient), new ExtraCredentialProvider(config), spec, servers);
        OpenApiTableHandle table = spec.getTableHandle(schemaTableName(SCHEMA_NAME, tableName));
        if (!columnDomains.isEmpty()) {
            Map<ColumnHandle, Domain> domains = spec.getTables().get(tableName).stream()
                    .filter(column -> columnDomains.containsKey(column.getName()))
                    .collect(toImmutableMap(OpenApiColumn::getHandle, column -> columnDomains.get(column.getName())));
            table = table.cloneWithConstraint(TupleDomain.withColumnDomains(domains));
        }
        return ImmutableList.copyOf(client.getRows(SESSION, table, 1, 1, false));
    }

    private void deleteRow(OpenApiConfig config, String id)
    {
        deleteRow(config, TupleDomain.all(), "{\"id\":\"%s\"}".formatted(id));
    }

    private void deleteRow(OpenApiConfig config, TupleDomain<ColumnHandle> constraint, String rowId)
    {
        OpenApiSpec spec = createSpec(config);
        OpenApiServers servers = new OpenApiServers(config, spec);
        OpenApiClient client = new OpenApiClient(config, new CatalogName("test"), httpClient, new Authentication(config, spec, servers, httpClient), new ExtraCredentialProvider(config), spec, servers);
        OpenApiTableHandle table = spec.getTableHandle(schemaTableName(SCHEMA_NAME, "items"))
                .cloneWithConstraint(constraint);
        client.deleteRows(SESSION, new OpenApiOutputTableHandle(table, List.of(), Map.of()), writeNativeValue(VARCHAR, utf8Slice(rowId)), 0);
    }

    private static HttpHandler disconnect()
    {
        return _ -> {
            throw new IOException("Closing the connection without a response");
        };
    }

    private static HttpHandler truncatedBody()
    {
        return exchange -> {
            exchange.getResponseHeaders().add("Cache-Control", "max-age=60");
            exchange.sendResponseHeaders(200, 100);
            OutputStream output = exchange.getResponseBody();
            output.write("[{\"id\":".getBytes(UTF_8));
            output.flush();
            // closing the exchange before the declared length was sent closes the connection
            exchange.close();
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
