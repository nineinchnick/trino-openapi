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

import com.sun.net.httpserver.HttpServer;
import io.airlift.http.client.HttpClient;
import io.airlift.http.client.jetty.JettyHttpClient;
import io.airlift.units.Duration;
import io.trino.spi.TrinoException;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import pl.net.was.authentication.AuthenticationType;
import pl.net.was.authentication.CatalogCredentials;
import pl.net.was.authentication.SpecLocationAuthentication;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.URI;
import java.util.Base64;
import java.util.Map;
import java.util.Objects;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.stream.Stream;

import static io.trino.spi.StandardErrorCode.INVALID_CATALOG_PROPERTY;
import static io.trino.spi.connector.SchemaTableName.schemaTableName;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.entry;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;
import static pl.net.was.OpenApiErrorCode.OPENAPI_CONNECTION_ERROR;
import static pl.net.was.OpenApiErrorCode.OPENAPI_HTTP_ERROR;
import static pl.net.was.OpenApiErrorCode.OPENAPI_INVALID_SPEC;
import static pl.net.was.OpenApiErrorCode.OPENAPI_SPEC_MISMATCH;
import static pl.net.was.OpenApiSpec.SCHEMA_NAME;

@TestInstance(PER_CLASS)
class TestSpecLocation
{
    private static final String SPEC =
            """
            openapi: 3.0.0
            info:
              title: test
              version: '1'
            servers:
              - url: https://api.example.com
            paths:
              /items:
                get:
                  responses:
                    '200':
                      description: OK
                      content:
                        application/json:
                          schema:
                            type: array
                            items:
                              $ref: '%s'
            """;
    private static final String AUTHORIZATION = "Authorization";
    private static final String API_KEY_HEADER = "X-API-Key";
    private static final String BASIC_CREDENTIALS = "Basic " + Base64.getEncoder().encodeToString("user:secret".getBytes(UTF_8));
    private static final String ITEM_SCHEMA =
            """
            type: object
            properties:
              id:
                type: integer
            """;

    private final Map<String, String> documents = new ConcurrentHashMap<>();
    private final Queue<Integer> failures = new ConcurrentLinkedQueue<>();
    private final Queue<URI> requests = new ConcurrentLinkedQueue<>();
    private final Map<String, Map.Entry<String, String>> requiredCredentials = new ConcurrentHashMap<>();
    private final Map<String, String> receivedCredentials = new ConcurrentHashMap<>();
    private final Map<String, String> redirects = new ConcurrentHashMap<>();
    private HttpServer server;
    private URI serverUri;
    private HttpClient httpClient;

    @BeforeAll
    void setUp()
            throws IOException
    {
        server = HttpServer.create(new InetSocketAddress(InetAddress.getLoopbackAddress(), 0), 0);
        server.createContext("/", exchange -> {
            requests.add(exchange.getRequestURI());
            String path = exchange.getRequestURI().getPath();
            Stream.of(AUTHORIZATION, API_KEY_HEADER)
                    .map(exchange.getRequestHeaders()::getFirst)
                    .filter(Objects::nonNull)
                    .findFirst()
                    .ifPresent(value -> receivedCredentials.put(path, value));
            Map.Entry<String, String> required = requiredCredentials.get(path);
            String redirect = redirects.get(path);
            Integer failure = failures.poll();
            String document = documents.get(path);
            int status;
            if (required != null && !required.getValue().equals(exchange.getRequestHeaders().getFirst(required.getKey()))) {
                exchange.getResponseHeaders().add("WWW-Authenticate", "Basic realm=\"test\"");
                status = 401;
            }
            else if (redirect != null) {
                exchange.getResponseHeaders().add("Location", redirect);
                status = 302;
            }
            else {
                status = failure != null ? failure : document == null ? 404 : 200;
            }
            byte[] bytes = (status == 200 ? document : "error").getBytes(UTF_8);
            exchange.getResponseHeaders().add("Content-Type", "application/yaml");
            exchange.sendResponseHeaders(status, bytes.length);
            try (OutputStream output = exchange.getResponseBody()) {
                output.write(bytes);
            }
        });
        server.start();
        serverUri = URI.create("http://127.0.0.1:" + server.getAddress().getPort());
        httpClient = new JettyHttpClient();
    }

    @AfterAll
    void tearDown()
    {
        httpClient.close();
        server.stop(0);
    }

    @BeforeEach
    void reset()
    {
        documents.clear();
        failures.clear();
        requests.clear();
        requiredCredentials.clear();
        receivedCredentials.clear();
        redirects.clear();
    }

    @Test
    void testRemoteReferences()
    {
        documents.put("/specs/openapi.yaml", SPEC.formatted("schemas/item.yaml"));
        documents.put("/specs/schemas/item.yaml", ITEM_SCHEMA);

        OpenApiSpec spec = newSpec(newConfig(serverUri + "/specs/openapi.yaml"));

        assertThat(spec.getTables().get("items"))
                .extracting(OpenApiColumn::getName)
                .contains("id");
        assertThat(requests).containsExactly(URI.create("/specs/openapi.yaml"), URI.create("/specs/schemas/item.yaml"));
    }

    @Test
    void testRetryTransientFailure()
    {
        documents.put("/openapi.yaml", SPEC.formatted("item.yaml"));
        documents.put("/item.yaml", ITEM_SCHEMA);
        failures.add(503);
        failures.add(502);

        OpenApiSpec spec = newSpec(newConfig(serverUri + "/openapi.yaml"));

        assertThat(spec.getTables()).containsOnlyKeys("items");
        assertThat(requests).containsExactly(URI.create("/openapi.yaml"), URI.create("/openapi.yaml"), URI.create("/openapi.yaml"), URI.create("/item.yaml"));
    }

    @Test
    void testPersistentFailure()
    {
        failures.add(503);
        failures.add(503);
        failures.add(503);

        // the query string can contain credentials, so it's not included in the error
        assertThatThrownBy(() -> newSpec(newConfig(serverUri + "/openapi.yaml?token=secret")))
                .isInstanceOfSatisfying(TrinoException.class, e -> assertThat(e.getErrorCode()).isEqualTo(OPENAPI_HTTP_ERROR.toErrorCode()))
                .hasMessage("Failed to fetch the OpenAPI spec from %s/openapi.yaml with HTTP 503 after 3 attempts", serverUri);
        assertThat(requests).hasSize(3);
    }

    @Test
    void testMissingSpec()
    {
        assertThatThrownBy(() -> newSpec(newConfig(serverUri + "/openapi.yaml")))
                .isInstanceOfSatisfying(TrinoException.class, e -> assertThat(e.getErrorCode()).isEqualTo(OPENAPI_HTTP_ERROR.toErrorCode()))
                .hasMessage("Failed to fetch the OpenAPI spec from %s/openapi.yaml with HTTP 404", serverUri);
        assertThat(requests).hasSize(1);
    }

    @Test
    void testUnauthorizedWithoutChallenge()
    {
        // Jetty fails 401 responses without a WWW-Authenticate header, which many servers send, as protocol violations
        failures.add(401);

        assertThatThrownBy(() -> newSpec(newConfig(serverUri + "/openapi.yaml")))
                .isInstanceOfSatisfying(TrinoException.class, e -> assertThat(e.getErrorCode()).isEqualTo(OPENAPI_HTTP_ERROR.toErrorCode()))
                .hasMessage("Failed to fetch the OpenAPI spec from %s/openapi.yaml with HTTP 401", serverUri);
        assertThat(requests).hasSize(1);
    }

    @Test
    void testUnreachableSpec()
            throws IOException
    {
        URI unreachableUri;
        try (ServerSocket socket = new ServerSocket(0, 0, InetAddress.getLoopbackAddress())) {
            unreachableUri = URI.create("http://127.0.0.1:" + socket.getLocalPort() + "/openapi.yaml");
        }

        assertThatThrownBy(() -> newSpec(newConfig(unreachableUri.toString())))
                .isInstanceOfSatisfying(TrinoException.class, e -> assertThat(e.getErrorCode()).isEqualTo(OPENAPI_CONNECTION_ERROR.toErrorCode()))
                .hasMessageStartingWith("Failed to fetch the OpenAPI spec from %s after 3 attempts: ", unreachableUri);
    }

    @Test
    void testInvalidSpec()
    {
        documents.put("/openapi.yaml",
                """
                openapi: 3.0.0
                info:
                  version: '1'
                paths:
                  /items:
                    get:
                      parameters:
                        - name: id
                """);

        assertThatThrownBy(() -> newSpec(newConfig(serverUri + "/openapi.yaml?token=secret")))
                .isInstanceOfSatisfying(TrinoException.class, e -> assertThat(e.getErrorCode()).isEqualTo(OPENAPI_INVALID_SPEC.toErrorCode()))
                .hasMessage(
                        """
                        Failed to parse the OpenAPI spec at %s/openapi.yaml:
                        attribute info.title is missing
                        attribute paths.'/items'(get).parameters.[id].in is missing
                        attribute paths.'/items'(get).responses is missing""",
                        serverUri);
    }

    @Test
    void testChangedSpec()
    {
        documents.put("/openapi.yaml", SPEC.formatted("item.yaml"));
        documents.put("/item.yaml", ITEM_SCHEMA);
        OpenApiConfig config = newConfig(serverUri + "/openapi.yaml");
        OpenApiSpec coordinatorSpec = newSpec(config);
        OpenApiTableHandle table = coordinatorSpec.getTableHandle(schemaTableName(SCHEMA_NAME, "items"));

        coordinatorSpec.checkSameSpec(table);
        newSpec(config).checkSameSpec(table);

        documents.put("/openapi.yaml", SPEC.formatted("item.yaml").replace("version: '1'", "version: '2'"));
        OpenApiSpec workerSpec = newSpec(config);

        assertThatThrownBy(() -> workerSpec.checkSameSpec(table))
                .isInstanceOfSatisfying(TrinoException.class, e -> assertThat(e.getErrorCode()).isEqualTo(OPENAPI_SPEC_MISMATCH.toErrorCode()))
                .hasMessage("Table default.items was planned with a different OpenAPI spec than this node loaded, because the spec at spec-location changed after the coordinator loaded it; "
                        + "restart the catalog on all nodes, or use a spec that doesn't change");
    }

    @Test
    void testHttpCredentials()
    {
        documents.put("/openapi.yaml", SPEC.formatted("item.yaml"));
        documents.put("/item.yaml", ITEM_SCHEMA);
        requiredCredentials.put("/openapi.yaml", Map.entry(AUTHORIZATION, BASIC_CREDENTIALS));
        OpenApiConfig config = newConfig(serverUri + "/openapi.yaml")
                .setAuthenticationType(AuthenticationType.HTTP)
                .setUsername("user")
                .setPassword("secret")
                .setAllowInsecureHttp(true);

        assertThatThrownBy(() -> newSpec(config))
                .isInstanceOfSatisfying(TrinoException.class, e -> assertThat(e.getErrorCode()).isEqualTo(OPENAPI_HTTP_ERROR.toErrorCode()))
                .hasMessage("Failed to fetch the OpenAPI spec from %s/openapi.yaml with HTTP 401", serverUri);
        assertThat(receivedCredentials).isEmpty();

        OpenApiSpec spec = newSpec(config.setSpecLocationAuthentication(SpecLocationAuthentication.CATALOG));

        assertThat(spec.getTables()).containsOnlyKeys("items");
        assertThat(receivedCredentials).containsExactly(entry("/openapi.yaml", BASIC_CREDENTIALS));
    }

    @Test
    void testApiKeyCredentials()
    {
        documents.put("/openapi.yaml", SPEC.formatted("item.yaml"));
        documents.put("/item.yaml", ITEM_SCHEMA);
        requiredCredentials.put("/openapi.yaml", Map.entry(API_KEY_HEADER, "secret"));
        OpenApiConfig config = newConfig(serverUri + "/openapi.yaml")
                .setSpecLocationAuthentication(SpecLocationAuthentication.CATALOG)
                .setAuthenticationType(AuthenticationType.API_KEY)
                .setApiKeyName(API_KEY_HEADER)
                .setApiKeyValue("secret")
                .setAllowInsecureHttp(true);

        assertThat(newSpec(config).getTables()).containsOnlyKeys("items");
        assertThat(receivedCredentials).containsExactly(entry("/openapi.yaml", "secret"));
    }

    @Test
    void testCredentialsNotSentToReferences()
    {
        // the same server, but another host name, so the reference is to another host
        URI otherHostUri = URI.create("http://localhost:" + server.getAddress().getPort());
        documents.put("/openapi.yaml", SPEC.formatted(otherHostUri + "/item.yaml"));
        documents.put("/item.yaml", ITEM_SCHEMA);
        OpenApiConfig config = newConfig(serverUri + "/openapi.yaml")
                .setSpecLocationAuthentication(SpecLocationAuthentication.CATALOG)
                .setAuthenticationType(AuthenticationType.HTTP)
                .setUsername("user")
                .setPassword("secret")
                .setAllowInsecureHttp(true);

        assertThat(newSpec(config).getTables()).containsOnlyKeys("items");
        assertThat(requests).containsExactly(URI.create("/openapi.yaml"), URI.create("/item.yaml"));
        assertThat(receivedCredentials).containsExactly(entry("/openapi.yaml", BASIC_CREDENTIALS));
    }

    @Test
    void testRedirectNotFollowedWithCredentials()
    {
        URI otherHostUri = URI.create("http://localhost:" + server.getAddress().getPort());
        documents.put("/openapi.yaml", SPEC.formatted("item.yaml"));
        documents.put("/item.yaml", ITEM_SCHEMA);
        redirects.put("/latest.yaml", otherHostUri + "/openapi.yaml");
        OpenApiConfig config = newConfig(serverUri + "/latest.yaml")
                .setAuthenticationType(AuthenticationType.HTTP)
                .setUsername("user")
                .setPassword("secret")
                .setAllowInsecureHttp(true);

        assertThat(newSpec(config).getTables()).containsOnlyKeys("items");

        assertThatThrownBy(() -> newSpec(config.setSpecLocationAuthentication(SpecLocationAuthentication.CATALOG)))
                .isInstanceOfSatisfying(TrinoException.class, e -> assertThat(e.getErrorCode()).isEqualTo(OPENAPI_HTTP_ERROR.toErrorCode()))
                .hasMessage("Failed to fetch the OpenAPI spec from %s/latest.yaml with HTTP 302, redirecting to %s/openapi.yaml, but redirects are not followed, so credentials are not sent to other hosts", serverUri, otherHostUri);
        assertThat(receivedCredentials).containsExactly(entry("/latest.yaml", BASIC_CREDENTIALS));
    }

    @Test
    void testMissingCredentials()
    {
        documents.put("/openapi.yaml", SPEC.formatted("item.yaml"));
        OpenApiConfig config = newConfig(serverUri + "/openapi.yaml?token=secret")
                .setSpecLocationAuthentication(SpecLocationAuthentication.CATALOG)
                .setAuthenticationType(AuthenticationType.HTTP)
                .setUsername("user")
                .setAllowInsecureHttp(true);

        assertThatThrownBy(() -> newSpec(config))
                .isInstanceOfSatisfying(TrinoException.class, e -> assertThat(e.getErrorCode()).isEqualTo(INVALID_CATALOG_PROPERTY.toErrorCode()))
                .hasMessage("Cannot authenticate the GET %s/openapi.yaml request, because the http authentication type requires the authentication.password configuration property", serverUri);
        assertThat(requests).isEmpty();
    }

    private OpenApiSpec newSpec(OpenApiConfig config)
    {
        return new OpenApiSpec(config, new CatalogCredentials(config), httpClient);
    }

    private static OpenApiConfig newConfig(String specLocation)
    {
        return new OpenApiConfig()
                .setSpecLocation(specLocation)
                .setMaxRetries(2)
                .setRetryInitialDelay(new Duration(0, MILLISECONDS));
    }
}
