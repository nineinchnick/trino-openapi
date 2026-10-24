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

import com.google.common.collect.ImmutableMap;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import io.trino.Session;
import io.trino.spi.security.Identity;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.QueryRunner;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.URLDecoder;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.stream.Stream;

import static com.google.common.base.Splitter.on;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNullElse;
import static org.assertj.core.api.Assertions.assertThat;

class TestUserCredentials
        extends AbstractTestQueryFramework
{
    private static final String ITEMS_RESPONSES =
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
                                owner:
                                  type: string
            """;

    private final Queue<String> requests = new ConcurrentLinkedQueue<>();
    private final Queue<String> tokenRequests = new ConcurrentLinkedQueue<>();
    private HttpServer server;
    private Path cacheDirectory;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        server = HttpServer.create(new InetSocketAddress(InetAddress.getLoopbackAddress(), 0), 0);
        String serverUri = "http://127.0.0.1:" + server.getAddress().getPort();
        server.createContext("/openapi.yaml", exchange -> respond(exchange, "application/yaml", spec(serverUri), "no-store"));
        server.createContext("/token", this::issueToken);
        server.createContext("/", this::listItems);
        server.start();
        cacheDirectory = Files.createTempDirectory("trino-openapi-user-credentials");

        Map<String, String> credentialNames = Map.of(
                "spec-location", serverUri + "/openapi.yaml",
                "authentication.allow-insecure-http", "true",
                "authentication.username-credential-name", "openapi_username",
                "authentication.password-credential-name", "openapi_password",
                "authentication.bearer-token-credential-name", "openapi_token",
                "authentication.api-key-value-credential-name", "openapi_api_key");
        return OpenApiQueryRunner.createQueryRunner(
                Map.of(
                        "openapi", ImmutableMap.<String, String>builder()
                                .putAll(credentialNames)
                                .put("authentication.username", "catalog")
                                .put("authentication.password", "catalog-password")
                                .put("authentication.bearer-token", "catalog-token")
                                .put("authentication.api-key-value", "catalog-key")
                                .put("http-cache.directory", cacheDirectory.toString())
                                .buildOrThrow(),
                        "per_user", credentialNames),
                0);
    }

    @AfterAll
    void stopServer()
    {
        server.stop(0);
    }

    @Test
    void testBearerToken()
    {
        assertOwner(getSession(), "openapi.default.bearer_items", "catalog-token");
        assertOwner(userSession("alice", Map.of("openapi_token", "alice-token")), "openapi.default.bearer_items", "alice-token");
        assertOwner(userSession("bob", Map.of("openapi_token", "bob-token")), "openapi.default.bearer_items", "bob-token");
        // the same Trino user with other credentials must not get the responses of the previous ones
        assertOwner(userSession("alice", Map.of("openapi_token", "other-alice-token")), "openapi.default.bearer_items", "other-alice-token");
        assertThat(requestsOf("/bearer/items")).containsExactly(
                "catalog-token",
                "alice-token",
                "bob-token",
                "other-alice-token");

        // responses are cached for every credentials separately
        assertOwner(userSession("bob", Map.of("openapi_token", "bob-token")), "openapi.default.bearer_items", "bob-token");
        assertOwner(userSession("carol", Map.of("openapi_token", "alice-token")), "openapi.default.bearer_items", "alice-token");
        assertOwner(getSession(), "openapi.default.bearer_items", "catalog-token");
        assertThat(requestsOf("/bearer/items")).hasSize(4);
        try (Stream<Path> files = Files.walk(cacheDirectory)) {
            assertThat(files.map(file -> file.getFileName().toString()))
                    .noneMatch(name -> name.contains("token"));
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    void testUsernameAndPassword()
    {
        assertOwner(getSession(), "openapi.default.basic_items", "catalog:catalog-password");
        assertOwner(userSession("alice", Map.of("openapi_username", "alice", "openapi_password", "alice-password")), "openapi.default.basic_items", "alice:alice-password");
        assertQueryFails(
                userSession("bob", Map.of("openapi_username", "bob")),
                "SELECT owner FROM openapi.default.basic_items",
                "The openapi_username and openapi_password extra credentials must be passed together");
        assertQueryFails(
                userSession("bob", Map.of("openapi_password", "bob-password")),
                "SELECT owner FROM openapi.default.basic_items",
                "The openapi_username and openapi_password extra credentials must be passed together");
    }

    @Test
    void testApiKey()
    {
        assertOwner(getSession(), "openapi.default.key_items", "catalog-key");
        assertOwner(userSession("alice", Map.of("openapi_api_key", "alice-key")), "openapi.default.key_items", "alice-key");
        // other extra credentials don't replace the API key
        assertOwner(userSession("bob", Map.of("openapi_token", "bob-token")), "openapi.default.key_items", "catalog-key");
    }

    @Test
    void testOAuthPasswordGrant()
    {
        Session alice = userSession("alice", Map.of("openapi_username", "alice", "openapi_password", "alice-password"));
        Session bob = userSession("bob", Map.of("openapi_username", "bob", "openapi_password", "bob-password"));
        assertOwner(getSession(), "openapi.default.oauth_items", "token-for-catalog");
        assertOwner(alice, "openapi.default.oauth_items", "token-for-alice");
        assertOwner(bob, "openapi.default.oauth_items", "token-for-bob");
        assertOwner(alice, "openapi.default.oauth_items", "token-for-alice");
        assertOwner(getSession(), "openapi.default.oauth_items", "token-for-catalog");
        // every user gets a token once, and reuses it
        assertThat(tokenRequests).containsExactly("catalog", "alice", "bob");
    }

    @Test
    void testMissingCatalogCredentials()
    {
        assertQueryFails(
                "SELECT owner FROM per_user.default.bearer_items",
                "Cannot authenticate the GET /bearer/items request, because none of its security requirements can be satisfied: "
                        + "the bearerAuth security scheme requires the authentication.bearer-token configuration property, or the openapi_token extra credential");
        assertQueryFails(
                "SELECT owner FROM per_user.default.basic_items",
                "Cannot authenticate the GET /basic/items request, because none of its security requirements can be satisfied: "
                        + "the basicAuth security scheme requires the authentication.username and authentication.password configuration properties, "
                        + "or the openapi_username and openapi_password extra credentials");
        assertOwner(userSession("alice", Map.of("openapi_api_key", "alice-key")), "per_user.default.key_items", "alice-key");
    }

    @Test
    void testCredentialsAreNotInPlans()
    {
        Session alice = userSession("alice", Map.of("openapi_token", "plan-token", "openapi_api_key", "plan-key"));
        assertThat((String) computeActual(alice, "EXPLAIN SELECT owner FROM openapi.default.bearer_items").getOnlyValue())
                .doesNotContain("plan-token", "plan-key");
        assertThat((String) computeActual(alice, "EXPLAIN ANALYZE SELECT owner FROM openapi.default.key_items").getOnlyValue())
                .doesNotContain("plan-token", "plan-key");
    }

    private List<String> requestsOf(String path)
    {
        return requests.stream()
                .filter(request -> request.startsWith(path + " "))
                .map(request -> request.substring(path.length() + 1))
                .toList();
    }

    private void assertOwner(Session session, String table, String owner)
    {
        assertThat(computeActual(session, "SELECT owner FROM " + table).getOnlyColumnAsSet())
                .containsExactly(owner);
    }

    private Session userSession(String user, Map<String, String> extraCredentials)
    {
        return Session.builder(getSession())
                .setIdentity(Identity.forUser(user)
                        .withExtraCredentials(extraCredentials)
                        .build())
                .build();
    }

    private static String spec(String serverUri)
    {
        return
                """
                openapi: 3.0.0
                info:
                  title: test
                  version: '1'
                servers:
                  - url: %1$s
                paths:
                  /basic/items:
                    get:
                      security:
                        - basicAuth: []
                %2$s
                  /bearer/items:
                    get:
                      security:
                        - bearerAuth: []
                %2$s
                  /key/items:
                    get:
                      security:
                        - apiKey: []
                %2$s
                  /oauth/items:
                    get:
                      security:
                        - oauth: []
                %2$s
                components:
                  securitySchemes:
                    basicAuth:
                      type: http
                      scheme: basic
                    bearerAuth:
                      type: http
                      scheme: bearer
                    apiKey:
                      type: apiKey
                      in: header
                      name: X-API-Key
                    oauth:
                      type: oauth2
                      flows:
                        password:
                          tokenUrl: %1$s/token
                          scopes: {}
                """.formatted(serverUri, ITEMS_RESPONSES);
    }

    private void listItems(HttpExchange exchange)
            throws IOException
    {
        String path = exchange.getRequestURI().getPath();
        String authorization = requireNonNullElse(exchange.getRequestHeaders().getFirst("Authorization"), "");
        String owner = switch (path) {
            case "/basic/items" -> new String(Base64.getDecoder().decode(authorization.substring("Basic ".length())), UTF_8);
            case "/bearer/items", "/oauth/items" -> authorization.substring("Bearer ".length());
            case "/key/items" -> exchange.getRequestHeaders().getFirst("X-API-Key");
            default -> throw new IllegalArgumentException("Unexpected path " + path);
        };
        requests.add(path + " " + owner);
        // OAuth responses are not cached, so every query shows which token it sent
        respond(exchange, "application/json", "[{\"owner\": \"%s\"}]".formatted(owner), path.equals("/oauth/items") ? "no-store" : "max-age=60");
    }

    private void issueToken(HttpExchange exchange)
            throws IOException
    {
        Map<String, String> form = on('&').withKeyValueSeparator('=').split(new String(exchange.getRequestBody().readAllBytes(), UTF_8));
        String username = URLDecoder.decode(form.get("username"), UTF_8);
        tokenRequests.add(username);
        respond(exchange, "application/json", "{\"access_token\": \"token-for-%s\", \"token_type\": \"Bearer\", \"expires_in\": 3600}".formatted(username), "no-store");
    }

    private static void respond(HttpExchange exchange, String contentType, String body, String cacheControl)
            throws IOException
    {
        byte[] bytes = body.getBytes(UTF_8);
        exchange.getResponseHeaders().add("Content-Type", contentType);
        exchange.getResponseHeaders().add("Cache-Control", cacheControl);
        exchange.sendResponseHeaders(200, bytes.length);
        try (OutputStream output = exchange.getResponseBody()) {
            output.write(bytes);
        }
    }
}
