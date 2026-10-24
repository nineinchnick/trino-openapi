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
package pl.net.was.authentication;

import com.google.common.collect.ImmutableListMultimap;
import com.sun.net.httpserver.HttpServer;
import io.airlift.http.client.HttpClient;
import io.airlift.http.client.HttpStatus;
import io.airlift.http.client.StringResponseHandler.StringResponse;
import io.airlift.http.client.jetty.JettyHttpClient;
import io.airlift.http.client.testing.TestingHttpClient;
import io.airlift.http.client.testing.TestingResponse;
import io.airlift.units.DataSize;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import pl.net.was.OpenApiConfig;
import pl.net.was.OpenApiServers;
import pl.net.was.OpenApiSpec;
import pl.net.was.UnchallengedResponseHttpClient;
import pl.net.was.cache.CachingHttpClient;
import pl.net.was.cache.HttpCache;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Clock;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import static com.google.common.net.MediaType.JSON_UTF_8;
import static io.airlift.http.client.HeaderNames.AUTHORIZATION;
import static io.airlift.http.client.HeaderNames.CACHE_CONTROL;
import static io.airlift.http.client.HeaderNames.CONTENT_TYPE;
import static io.airlift.http.client.HeaderNames.WWW_AUTHENTICATE;
import static io.airlift.http.client.Request.Builder.prepareGet;
import static io.airlift.http.client.StringResponseHandler.createStringResponseHandler;
import static io.airlift.http.client.testing.TestingResponse.mockResponse;
import static io.airlift.units.DataSize.Unit.MEGABYTE;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;
import static pl.net.was.OpenApiClient.OPENAPI_PATH_HEADER;
import static pl.net.was.TestingOpenApiSpec.createSpec;

class TestAuthenticatingHttpClient
{
    private static final URI BASE_URI = URI.create("https://localhost:8080");
    private static final String SPEC =
            """
            openapi: 3.0.0
            info:
              title: test
              version: '1'
            security:
              - oauth: []
            paths:
              /secured:
                get:
                  responses:
                    '200':
                      description: OK
              /static:
                get:
                  security:
                    - bearerAuth: []
                  responses:
                    '200':
                      description: OK
            components:
              securitySchemes:
                oauth:
                  type: oauth2
                  flows:
                    clientCredentials:
                      tokenUrl: https://localhost:8081/token
                      scopes: {}
                bearerAuth:
                  type: http
                  scheme: bearer
            """;

    @TempDir
    Path tempDir;

    private final AtomicInteger tokenRequests = new AtomicInteger();
    private final List<String> sentAuthorizations = new ArrayList<>();

    @Test
    void testRejectedTokenIsRefreshed()
            throws Exception
    {
        HttpClient client = createClient(Set.of("token-2"), "Basic realm=\"api\", Bearer error=\"invalid_token\"");

        assertThat(get(client, "/secured").getStatusCode()).isEqualTo(200);
        assertThat(get(client, "/secured").getStatusCode()).isEqualTo(200);

        assertThat(tokenRequests).hasValue(2);
        assertThat(sentAuthorizations).containsExactly("Bearer token-1", "Bearer token-2", "Bearer token-2");
    }

    @Test
    void testRejectedTokenOfClientSecretBasicIsRefreshed()
            throws Exception
    {
        HttpClient client = createClient(Set.of("token-2"), "Bearer", ClientAuthenticationMethod.CLIENT_SECRET_BASIC);

        assertThat(get(client, "/secured").getStatusCode()).isEqualTo(200);

        assertThat(tokenRequests).hasValue(2);
        assertThat(sentAuthorizations).containsExactly("Bearer token-1", "Bearer token-2");
    }

    @Test
    void testRefreshedTokenIsSentOnlyOnce()
            throws Exception
    {
        HttpClient client = createClient(Set.of(), "Bearer");

        assertThat(get(client, "/secured").getStatusCode()).isEqualTo(401);

        assertThat(tokenRequests).hasValue(2);
        assertThat(sentAuthorizations).containsExactly("Bearer token-1", "Bearer token-2");
    }

    @Test
    void testRejectionWithoutBearerChallenge()
            throws Exception
    {
        // the comma and Bearer are part of a quoted parameter value, so they don't start another challenge
        HttpClient client = createClient(Set.of(), "Basic realm=\"api, Bearer x\"");

        assertThat(get(client, "/secured").getStatusCode()).isEqualTo(401);

        assertThat(tokenRequests).hasValue(1);
        assertThat(sentAuthorizations).containsExactly("Bearer token-1");
    }

    @Test
    void testRejectionWithoutChallengeIsNotRefreshed()
            throws Exception
    {
        // Jetty fails 401 responses without a WWW-Authenticate header, which many APIs send, as protocol violations
        HttpServer server = HttpServer.create(new InetSocketAddress(InetAddress.getLoopbackAddress(), 0), 0);
        server.createContext("/", exchange -> {
            sentAuthorizations.add(exchange.getRequestHeaders().getFirst(AUTHORIZATION.toString()));
            exchange.sendResponseHeaders(401, -1);
            exchange.close();
        });
        server.start();
        URI serverUri = URI.create("http://127.0.0.1:" + server.getAddress().getPort());
        OpenApiConfig config = new OpenApiConfig()
                .setSpecLocation(Files.writeString(tempDir.resolve("spec.yaml"), SPEC).toString())
                .setBaseUri(serverUri)
                .setAllowInsecureHttp(true)
                .setClientId("client")
                .setClientSecret("secret");
        OpenApiSpec spec = createSpec(config);
        try (HttpClient apiClient = new JettyHttpClient()) {
            // wrapped the same way as by the OpenAPI client
            HttpClient client = new UnchallengedResponseHttpClient(new AuthenticatingHttpClient(apiClient, new Authentication(new CatalogCredentials(config), spec, new OpenApiServers(config, spec), createTokenClient()), UserCredentials.NONE));

            assertThat(get(client, serverUri, "/secured").getStatusCode()).isEqualTo(401);
        }
        finally {
            server.stop(0);
        }

        assertThat(tokenRequests).hasValue(1);
        assertThat(sentAuthorizations).containsExactly("Bearer token-1");
    }

    @Test
    void testStaticBearerTokenIsNotRefreshed()
            throws Exception
    {
        HttpClient client = createClient(Set.of(), "Bearer error=\"invalid_token\"");

        assertThat(get(client, "/static").getStatusCode()).isEqualTo(401);

        assertThat(tokenRequests).hasValue(0);
        assertThat(sentAuthorizations).containsExactly("Bearer static-token");
    }

    @Test
    void testResponseAfterRefreshIsCached()
            throws Exception
    {
        HttpClient client = new CachingHttpClient(
                createClient(Set.of("token-2"), "Bearer"),
                new HttpCache(tempDir.resolve("cache"), DataSize.of(1, MEGABYTE), DataSize.of(1, MEGABYTE)),
                Clock.systemUTC());

        assertThat(get(client, "/secured").getStatusCode()).isEqualTo(200);
        assertThat(get(client, "/secured").getStatusCode()).isEqualTo(200);

        assertThat(sentAuthorizations).containsExactly("Bearer token-1", "Bearer token-2");
    }

    private HttpClient createClient(Set<String> acceptedTokens, String challenge)
            throws IOException
    {
        return createClient(acceptedTokens, challenge, ClientAuthenticationMethod.CLIENT_SECRET_POST);
    }

    private HttpClient createClient(Set<String> acceptedTokens, String challenge, ClientAuthenticationMethod clientAuthenticationMethod)
            throws IOException
    {
        Path specFile = Files.writeString(tempDir.resolve("spec.yaml"), SPEC);
        OpenApiConfig config = new OpenApiConfig()
                .setSpecLocation(specFile.toString())
                .setBaseUri(BASE_URI)
                .setClientId("client")
                .setClientSecret("secret")
                .setClientAuthenticationMethod(clientAuthenticationMethod)
                .setBearerToken("static-token");
        HttpClient apiClient = new TestingHttpClient(request -> {
            String authorization = request.getHeader(AUTHORIZATION);
            sentAuthorizations.add(authorization);
            if (acceptedTokens.contains(authorization.substring("Bearer ".length()))) {
                return new TestingResponse(
                        HttpStatus.OK,
                        ImmutableListMultimap.of(CONTENT_TYPE, JSON_UTF_8.toString(), CACHE_CONTROL, "max-age=60"),
                        "[]".getBytes(UTF_8));
            }
            return new TestingResponse(HttpStatus.UNAUTHORIZED, ImmutableListMultimap.of(WWW_AUTHENTICATE, challenge), new byte[0]);
        });
        OpenApiSpec spec = createSpec(config);
        return new AuthenticatingHttpClient(apiClient, new Authentication(new CatalogCredentials(config), spec, new OpenApiServers(config, spec), createTokenClient()), UserCredentials.NONE);
    }

    private HttpClient createTokenClient()
    {
        return new TestingHttpClient(_ -> mockResponse(
                HttpStatus.OK,
                JSON_UTF_8,
                "{\"access_token\":\"token-%d\",\"token_type\":\"Bearer\",\"expires_in\":300}".formatted(tokenRequests.incrementAndGet())));
    }

    private static StringResponse get(HttpClient client, String path)
    {
        return get(client, BASE_URI, path);
    }

    private static StringResponse get(HttpClient client, URI baseUri, String path)
    {
        return client.execute(
                prepareGet()
                        .setUri(baseUri.resolve(path))
                        .addHeader(OPENAPI_PATH_HEADER, path)
                        .build(),
                createStringResponseHandler());
    }
}
