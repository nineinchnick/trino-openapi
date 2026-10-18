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

import com.google.common.collect.ImmutableList;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;
import io.airlift.http.client.HttpClient;
import io.airlift.http.client.jetty.JettyHttpClient;
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

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

import static io.airlift.slice.Slices.utf8Slice;
import static io.trino.spi.connector.SchemaTableName.schemaTableName;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;
import static pl.net.was.OpenApiSpec.SCHEMA_NAME;

@TestInstance(PER_CLASS)
class TestOpenApiClientRequests
{
    private static final String OK_RESPONSES = """
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
    private static final String SPEC = """
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
            """.formatted(OK_RESPONSES, OK_RESPONSES);

    private final Queue<HttpHandler> responses = new ConcurrentLinkedQueue<>();
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
    }

    @Test
    void testErrorIncludesRequestContextAndErrorPathMessage()
            throws IOException
    {
        responses.add(respond(500, "{\"message\":\"Something broke\",\"details\":\"internal-details\"}"));

        assertThatThrownBy(() -> getRows(newConfig(), "items", "s3cr3t"))
                .isInstanceOf(TrinoException.class)
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
                .isInstanceOf(TrinoException.class)
                .hasMessageStartingWith("Request GET %s/items for operation listItems failed: ", unreachableUri)
                .hasCauseInstanceOf(IOException.class)
                .message()
                .doesNotContain("s3cr3t");
    }

    private OpenApiConfig newConfig()
            throws IOException
    {
        Path specFile = Files.writeString(tempDir.resolve("spec.yaml"), SPEC);
        return new OpenApiConfig()
                .setSpecLocation(specFile.toString())
                .setBaseUri(baseUri);
    }

    private List<List<?>> getRows(OpenApiConfig config, String tableName, String secret)
    {
        OpenApiSpec spec = new OpenApiSpec(config);
        OpenApiClient client = new OpenApiClient(config, httpClient, spec);
        OpenApiTableHandle table = spec.getTableHandle(schemaTableName(SCHEMA_NAME, tableName));
        if (secret != null) {
            OpenApiColumn column = spec.getTables().get(tableName).stream()
                    .filter(candidate -> candidate.getName().equals("secret"))
                    .findFirst()
                    .orElseThrow();
            table = table.cloneWithConstraint(TupleDomain.withColumnDomains(Map.of((ColumnHandle) column.getHandle(), Domain.singleValue(VARCHAR, utf8Slice(secret)))));
        }
        return ImmutableList.copyOf(client.getRows(table));
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
