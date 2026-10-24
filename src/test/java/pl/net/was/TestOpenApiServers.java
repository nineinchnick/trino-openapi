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
import com.sun.net.httpserver.HttpServer;
import io.airlift.http.client.HttpClient;
import io.airlift.http.client.jetty.JettyHttpClient;
import io.trino.spi.catalog.CatalogName;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.io.TempDir;
import pl.net.was.authentication.Authentication;
import pl.net.was.authentication.ExtraCredentialProvider;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;

import static io.trino.spi.connector.SchemaTableName.schemaTableName;
import static io.trino.testing.TestingConnectorSession.SESSION;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNullElse;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;
import static pl.net.was.OpenApiSpec.SCHEMA_NAME;

@TestInstance(PER_CLASS)
class TestOpenApiServers
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

    private final Queue<URI> forecastRequests = new ConcurrentLinkedQueue<>();
    private final Queue<URI> archiveRequests = new ConcurrentLinkedQueue<>();
    private final Map<String, String> specs = new ConcurrentHashMap<>();
    private HttpServer forecastServer;
    private HttpServer archiveServer;
    private URI forecastUri;
    private URI archiveUri;
    private HttpClient httpClient;

    @TempDir
    Path tempDir;

    @BeforeAll
    void setUp()
            throws IOException
    {
        forecastServer = startServer(forecastRequests);
        archiveServer = startServer(archiveRequests);
        forecastUri = URI.create("http://127.0.0.1:" + forecastServer.getAddress().getPort());
        archiveUri = URI.create("http://127.0.0.1:" + archiveServer.getAddress().getPort());
        httpClient = new JettyHttpClient();
    }

    @AfterAll
    void tearDown()
    {
        httpClient.close();
        forecastServer.stop(0);
        archiveServer.stop(0);
    }

    @BeforeEach
    void resetServers()
    {
        forecastRequests.clear();
        archiveRequests.clear();
        specs.clear();
    }

    @Test
    void testServersOfPaths()
            throws IOException
    {
        OpenApiConfig config = newConfig(
                """
                openapi: 3.0.0
                info:
                  title: test
                  version: '1'
                paths:
                  /v1/forecast:
                    servers:
                      - url: %s
                    get:
                %s
                  /v1/archive:
                    servers:
                      - url: %s
                    get:
                %s
                """.formatted(forecastUri, OK_RESPONSES, archiveUri, OK_RESPONSES));

        assertThat(getRows(config, "v1_forecast")).isEmpty();
        assertThat(getRows(config, "v1_archive")).isEmpty();
        assertThat(forecastRequests).containsExactly(URI.create("/v1/forecast"));
        assertThat(archiveRequests).containsExactly(URI.create("/v1/archive"));
    }

    @Test
    void testServerOfOperationOverridesServersOfPathAndDocument()
            throws IOException
    {
        OpenApiConfig config = newConfig(
                """
                openapi: 3.0.0
                info:
                  title: test
                  version: '1'
                servers:
                  - url: %1$s/root
                  - url: %2$s/root
                paths:
                  /operation:
                    servers:
                      - url: %1$s/path
                    get:
                      servers:
                        - url: %2$s/operation
                        - url: %1$s/operation
                %3$s
                  /path:
                    servers:
                      - url: %2$s/path
                      - url: %1$s/path
                    get:
                %3$s
                  /document:
                    get:
                %3$s
                """.formatted(forecastUri, archiveUri, OK_RESPONSES));

        assertThat(getRows(config, "operation")).isEmpty();
        assertThat(getRows(config, "path")).isEmpty();
        assertThat(getRows(config, "document")).isEmpty();
        assertThat(forecastRequests).containsExactly(URI.create("/root/document"));
        assertThat(archiveRequests).containsExactly(URI.create("/operation/operation"), URI.create("/path/path"));
    }

    @Test
    void testBaseUriOverridesServers()
            throws IOException
    {
        OpenApiConfig config = newConfig(
                """
                openapi: 3.0.0
                info:
                  title: test
                  version: '1'
                servers:
                  - url: %1$s/root
                paths:
                  /forecast:
                    servers:
                      - url: %1$s/path
                    get:
                      servers:
                        - url: %1$s/operation
                %2$s
                """.formatted(forecastUri, OK_RESPONSES))
                .setBaseUri(URI.create(archiveUri + "/v1"));

        assertThat(getRows(config, "forecast")).isEmpty();
        assertThat(forecastRequests).isEmpty();
        assertThat(archiveRequests).containsExactly(URI.create("/v1/forecast"));
    }

    @Test
    void testRelativeServers()
    {
        specs.put("/specs/openapi.yaml",
                """
                openapi: 3.0.0
                info:
                  title: test
                  version: '1'
                servers:
                  - url: /v1
                paths:
                  /root:
                    get:
                %1$s
                  /relative:
                    servers:
                      - url: api
                    get:
                %1$s
                  /authority:
                    servers:
                      - url: //%2$s/v2
                    get:
                %1$s
                """.formatted(OK_RESPONSES, archiveUri.getRawAuthority()));
        OpenApiConfig config = new OpenApiConfig()
                .setSpecLocation(forecastUri + "/specs/openapi.yaml")
                .setMaxRetries(0);

        assertThat(getRows(config, "root")).isEmpty();
        assertThat(getRows(config, "relative")).isEmpty();
        assertThat(getRows(config, "authority")).isEmpty();
        assertThat(forecastRequests).containsExactly(URI.create("/v1/root"), URI.create("/specs/api/relative"));
        assertThat(archiveRequests).containsExactly(URI.create("/v2/authority"));
    }

    @Test
    void testRelativeServerOfSpecFile()
            throws IOException
    {
        OpenApiConfig config = newConfig(
                """
                openapi: 3.0.0
                info:
                  title: test
                  version: '1'
                servers:
                  - url: /v1
                paths:
                  /forecast:
                    get:
                %s
                """.formatted(OK_RESPONSES));

        assertThatThrownBy(() -> getRows(config, "forecast"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Server URL /v1 of GET /forecast is relative, but spec-location is not a URL, so set base-uri");
        assertThat(getRows(config.setBaseUri(forecastUri), "forecast")).isEmpty();
        assertThat(forecastRequests).containsExactly(URI.create("/forecast"));
    }

    @Test
    void testServerVariables()
            throws IOException
    {
        OpenApiConfig config = newConfig(
                """
                openapi: 3.0.0
                info:
                  title: test
                  version: '1'
                servers:
                  - url: '{scheme}://127.0.0.1:{port}/{version}'
                    variables:
                      scheme:
                        default: http
                        enum:
                          - http
                          - https
                      port:
                        default: '%s'
                      version:
                        default: v1
                paths:
                  /forecast:
                    get:
                %s
                """.formatted(forecastServer.getAddress().getPort(), OK_RESPONSES));

        assertThat(getRows(config, "forecast")).isEmpty();
        assertThat(forecastRequests).containsExactly(URI.create("/v1/forecast"));
    }

    @Test
    void testServerVariableWithoutDefault()
            throws IOException
    {
        OpenApiConfig config = newConfig(
                """
                openapi: 3.0.0
                info:
                  title: test
                  version: '1'
                servers:
                  - url: 'https://{region}.example.com'
                paths:
                  /forecast:
                    get:
                %s
                """.formatted(OK_RESPONSES));

        assertThatThrownBy(() -> getRows(config, "forecast"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Server URL https://{region}.example.com of GET /forecast uses the variable region, which doesn't have a default value, so set base-uri");
    }

    @Test
    void testMissingServers()
            throws IOException
    {
        OpenApiConfig config = newConfig(
                """
                openapi: 3.0.0
                info:
                  title: test
                  version: '1'
                paths:
                  /forecast:
                    get:
                %s
                """.formatted(OK_RESPONSES));

        assertThatThrownBy(() -> newServers(config))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("The spec doesn't declare any servers, so set base-uri");
        assertThat(newServers(config.setBaseUri(forecastUri)).getUris()).containsExactly(forecastUri);
    }

    @Test
    void testMissingServersOfSomePaths()
            throws IOException
    {
        OpenApiConfig config = newConfig(
                """
                openapi: 3.0.0
                info:
                  title: test
                  version: '1'
                paths:
                  /forecast:
                    servers:
                      - url: %s
                    get:
                %s
                  /archive:
                    get:
                %2$s
                    post:
                      x-trino:
                        sqlOperation: insert
                %2$s
                """.formatted(forecastUri, OK_RESPONSES));

        assertThatThrownBy(() -> newServers(config))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("The spec doesn't declare servers for POST /archive, GET /archive, so set base-uri");
    }

    @Test
    void testCredentialsOverHttp()
            throws IOException
    {
        OpenApiConfig config = newConfig(
                """
                openapi: 3.0.0
                info:
                  title: test
                  version: '1'
                servers:
                  - url: https://api.example.com
                paths:
                  /forecast:
                    servers:
                      - url: %s
                    get:
                %s
                  /archive:
                    get:
                %2$s
                """.formatted(forecastUri, OK_RESPONSES))
                .setBearerToken("token");

        assertThatThrownBy(() -> newServers(config))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Servers declared in the spec must use HTTPS when credentials are configured, unless authentication.allow-insecure-http is set: %s", forecastUri);
        assertThat(newServers(config.setAllowInsecureHttp(true)).getUris())
                .containsExactlyInAnyOrder(forecastUri, URI.create("https://api.example.com"));
        assertThat(newServers(config.setAllowInsecureHttp(false).setBaseUri(URI.create("https://api.example.com"))).getUris())
                .containsExactly(URI.create("https://api.example.com"));
    }

    private OpenApiServers newServers(OpenApiConfig config)
    {
        return new OpenApiServers(config, new OpenApiSpec(config, httpClient));
    }

    private OpenApiConfig newConfig(String spec)
            throws IOException
    {
        Path specFile = Files.writeString(tempDir.resolve("spec.yaml"), spec);
        return new OpenApiConfig()
                .setSpecLocation(specFile.toString())
                .setMaxRetries(0);
    }

    private List<List<?>> getRows(OpenApiConfig config, String tableName)
    {
        OpenApiSpec spec = new OpenApiSpec(config, httpClient);
        OpenApiServers servers = new OpenApiServers(config, spec);
        OpenApiClient client = new OpenApiClient(config, new CatalogName("test"), httpClient, new Authentication(config, spec, servers, httpClient), new ExtraCredentialProvider(config), spec, servers);
        return ImmutableList.copyOf(client.getRows(SESSION, spec.getTableHandle(schemaTableName(SCHEMA_NAME, tableName)), 1, 1, false));
    }

    private HttpServer startServer(Queue<URI> requests)
            throws IOException
    {
        HttpServer server = HttpServer.create(new InetSocketAddress(InetAddress.getLoopbackAddress(), 0), 0);
        server.createContext("/", exchange -> {
            String spec = specs.get(exchange.getRequestURI().getPath());
            if (spec == null) {
                requests.add(exchange.getRequestURI());
            }
            exchange.getResponseHeaders().add("Content-Type", spec == null ? "application/json" : "application/yaml");
            byte[] bytes = requireNonNullElse(spec, "[]").getBytes(UTF_8);
            exchange.sendResponseHeaders(200, bytes.length);
            try (OutputStream output = exchange.getResponseBody()) {
                output.write(bytes);
            }
        });
        server.start();
        return server;
    }
}
