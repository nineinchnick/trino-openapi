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

import com.sun.net.httpserver.HttpServer;
import io.airlift.http.client.HeaderName;
import io.airlift.http.client.HttpClient;
import io.airlift.http.client.HttpStatus;
import io.airlift.http.client.Request;
import io.airlift.http.client.StaticBodyGenerator;
import io.airlift.http.client.jetty.JettyHttpClient;
import io.airlift.http.client.testing.TestingHttpClient;
import io.trino.spi.ErrorCodeSupplier;
import io.trino.spi.TrinoException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import pl.net.was.OpenApiConfig;
import pl.net.was.OpenApiServers;
import pl.net.was.OpenApiSpec;

import java.io.IOException;
import java.io.OutputStream;
import java.net.ConnectException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static com.google.common.base.Throwables.getStackTraceAsString;
import static com.google.common.net.MediaType.JSON_UTF_8;
import static io.airlift.http.client.HeaderNames.AUTHORIZATION;
import static io.airlift.http.client.HeaderNames.COOKIE;
import static io.airlift.http.client.Request.Builder.fromRequest;
import static io.airlift.http.client.Request.Builder.prepareGet;
import static io.airlift.http.client.testing.TestingResponse.mockResponse;
import static io.trino.spi.StandardErrorCode.INVALID_CATALOG_PROPERTY;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static pl.net.was.OpenApiClient.OPENAPI_PATH_HEADER;
import static pl.net.was.OpenApiErrorCode.OPENAPI_CONNECTION_ERROR;
import static pl.net.was.OpenApiErrorCode.OPENAPI_HTTP_ERROR;
import static pl.net.was.OpenApiErrorCode.OPENAPI_INVALID_RESPONSE;
import static pl.net.was.TestingOpenApiSpec.createSpec;

class TestAuthentication
{
    private static final URI BASE_URI = URI.create("http://localhost:8080");
    private static final String SECURITY_SCHEMES =
            """
            components:
              securitySchemes:
                basicAuth:
                  type: http
                  scheme: basic
                apiKeyHeader:
                  type: apiKey
                  in: header
                  name: X-API-Key
                apiKeyCookie:
                  type: apiKey
                  in: cookie
                  name: session
                apiKeyQuery:
                  type: apiKey
                  in: query
                  name: api_key
                bearerAuth:
                  type: http
                  scheme: bearer
            """;
    private static final String OPERATION_RESPONSES =
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

    @TempDir
    Path tempDir;

    @Test
    void testAnonymousOperationSkipsDefaultAuthentication()
            throws Exception
    {
        Authentication authentication = createAuthentication(
                """
                openapi: 3.0.0
                info:
                  title: test
                  version: '1'
                paths:
                  /anonymous:
                    get:
                      security: []
                %s
                  /inherited:
                    get:
                %s
                %s
                """.formatted(OPERATION_RESPONSES, OPERATION_RESPONSES, SECURITY_SCHEMES));

        assertNoCredentials(authentication.filterRequest(request("/anonymous"), UserCredentials.NONE));
        assertThat(authentication.filterRequest(request("/inherited"), UserCredentials.NONE).getHeader(AUTHORIZATION))
                .isEqualTo("Basic dXNlcjpwYXNzd29yZA==");
    }

    @Test
    void testAnonymousOperationSkipsGlobalRequirements()
            throws Exception
    {
        Authentication authentication = createAuthentication(
                """
                openapi: 3.0.0
                info:
                  title: test
                  version: '1'
                security:
                  - basicAuth: []
                    apiKeyHeader: []
                    apiKeyCookie: []
                    apiKeyQuery: []
                paths:
                  /anonymous:
                    get:
                      security: []
                %s
                  /inherited:
                    get:
                %s
                %s
                """.formatted(OPERATION_RESPONSES, OPERATION_RESPONSES, SECURITY_SCHEMES));

        assertNoCredentials(authentication.filterRequest(request("/anonymous"), UserCredentials.NONE));
        Request inherited = authentication.filterRequest(request("/inherited"), UserCredentials.NONE);
        assertThat(inherited.getHeader(AUTHORIZATION)).isEqualTo("Basic dXNlcjpwYXNzd29yZA==");
        assertThat(inherited.getHeader(HeaderName.of("X-API-Key"))).isEqualTo("header-secret");
        assertThat(inherited.getHeader(COOKIE)).isEqualTo("session=cookie-secret");
        assertThat(inherited.getUri().getQuery()).isEqualTo("api_key=query-secret");
    }

    @Test
    void testGlobalAnonymousSecuritySkipsDefaultAuthentication()
            throws Exception
    {
        Authentication authentication = createAuthentication(
                """
                openapi: 3.0.0
                info:
                  title: test
                  version: '1'
                security: []
                paths:
                  /inherited:
                    get:
                %s
                  /secured:
                    get:
                      security:
                        - basicAuth: []
                %s
                %s
                """.formatted(OPERATION_RESPONSES, OPERATION_RESPONSES, SECURITY_SCHEMES));

        assertNoCredentials(authentication.filterRequest(request("/inherited"), UserCredentials.NONE));
        assertThat(authentication.filterRequest(request("/secured"), UserCredentials.NONE).getHeader(AUTHORIZATION))
                .isEqualTo("Basic dXNlcjpwYXNzd29yZA==");
    }

    @Test
    void testSpecWithoutSecuritySchemes()
            throws Exception
    {
        Authentication authentication = createAuthentication(
                """
                openapi: 3.0.0
                info:
                  title: test
                  version: '1'
                security:
                  - basicAuth: []
                paths:
                  /secured:
                    get:
                %s
                """.formatted(OPERATION_RESPONSES));

        assertThatThrownBy(() -> authentication.filterRequest(request("/secured"), UserCredentials.NONE))
                .isInstanceOf(TrinoException.class)
                .hasMessage("Cannot authenticate the GET /secured request, because none of its security requirements can be satisfied: the basicAuth security scheme is not declared in components.securitySchemes");
    }

    @Test
    void testSpecWithoutPaths()
            throws Exception
    {
        Authentication authentication = createAuthentication(
                """
                openapi: 3.1.0
                info:
                  title: test
                  version: '1'
                %s
                """.formatted(SECURITY_SCHEMES));

        assertThat(authentication.filterRequest(request("/unknown"), UserCredentials.NONE).getHeader(AUTHORIZATION))
                .isEqualTo("Basic dXNlcjpwYXNzd29yZA==");
    }

    @Test
    void testOAuthFlowsRequiringBrowser()
            throws Exception
    {
        Authentication authentication = createAuthentication(
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
                %s
                components:
                  securitySchemes:
                    oauth:
                      type: oauth2
                      flows:
                        authorizationCode:
                          authorizationUrl: http://localhost:8081/authorize
                          tokenUrl: https://localhost:8081/token
                          scopes: {}
                        implicit:
                          authorizationUrl: http://localhost:8081/authorize
                          scopes: {}
                """.formatted(OPERATION_RESPONSES));

        assertThatThrownBy(() -> authentication.filterRequest(request("/secured"), UserCredentials.NONE))
                .isInstanceOf(TrinoException.class)
                .hasMessage("Cannot authenticate the GET /secured request, because none of its security requirements can be satisfied: the oauth security scheme must declare a password or clientCredentials flow, because other flows require a user to log in using a browser");
    }

    @Test
    void testOAuthPasswordFlowWithoutCredentials()
            throws Exception
    {
        Authentication authentication = createAuthentication(
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
                %s
                components:
                  securitySchemes:
                    oauth:
                      type: oauth2
                      flows:
                        password:
                          tokenUrl: https://localhost:8081/token
                          scopes: {}
                """.formatted(OPERATION_RESPONSES),
                new OpenApiConfig()
                        .setClientId("client")
                        .setClientSecret("secret"));

        assertThatThrownBy(() -> authentication.filterRequest(request("/secured"), UserCredentials.NONE))
                .isInstanceOf(TrinoException.class)
                .hasMessage("Cannot authenticate the GET /secured request, because none of its security requirements can be satisfied: the password flow of the oauth security scheme requires the authentication.username and authentication.password configuration properties");
    }

    @Test
    void testShortLivedOAuthTokenIsReused()
            throws Exception
    {
        AtomicInteger tokenRequests = new AtomicInteger();
        Authentication authentication = createAuthentication(
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
                %s
                components:
                  securitySchemes:
                    oauth:
                      type: oauth2
                      flows:
                        clientCredentials:
                          tokenUrl: https://localhost:8081/token
                          scopes: {}
                """.formatted(OPERATION_RESPONSES),
                new OpenApiConfig()
                        .setClientId("client")
                        .setClientSecret("secret"),
                new TestingHttpClient(_ -> mockResponse(
                        HttpStatus.OK,
                        JSON_UTF_8,
                        "{\"access_token\":\"token-%d\",\"token_type\":\"Bearer\",\"expires_in\":30}".formatted(tokenRequests.incrementAndGet()))));

        assertThat(authentication.filterRequest(request("/secured"), UserCredentials.NONE).getHeader(AUTHORIZATION)).isEqualTo("Bearer token-1");
        assertThat(authentication.filterRequest(request("/secured"), UserCredentials.NONE).getHeader(AUTHORIZATION)).isEqualTo("Bearer token-1");
        assertThat(tokenRequests).hasValue(1);
    }

    @Test
    void testOAuthScopesOfSecurityRequirement()
            throws Exception
    {
        List<String> tokenRequestBodies = new ArrayList<>();
        Authentication authentication = createAuthentication(
                """
                openapi: 3.0.0
                info:
                  title: test
                  version: '1'
                security:
                  - oauth: [read]
                paths:
                  /secured:
                    get:
                %s
                  /admin:
                    get:
                      security:
                        - oauth: [read, write]
                %s
                components:
                  securitySchemes:
                    oauth:
                      type: oauth2
                      flows:
                        clientCredentials:
                          tokenUrl: https://localhost:8081/token
                          scopes:
                            read: Read items
                            write: Change items
                """.formatted(OPERATION_RESPONSES, OPERATION_RESPONSES),
                new OpenApiConfig()
                        .setClientId("client")
                        .setClientSecret("secret"),
                new TestingHttpClient(request -> {
                    tokenRequestBodies.add(getBody(request));
                    return mockResponse(HttpStatus.OK, JSON_UTF_8, "{\"access_token\":\"token-%d\",\"token_type\":\"Bearer\",\"expires_in\":300}".formatted(tokenRequestBodies.size()));
                }));

        assertThat(authentication.filterRequest(request("/secured"), UserCredentials.NONE).getHeader(AUTHORIZATION)).isEqualTo("Bearer token-1");
        assertThat(authentication.filterRequest(request("/admin"), UserCredentials.NONE).getHeader(AUTHORIZATION)).isEqualTo("Bearer token-2");
        assertThat(authentication.filterRequest(request("/secured"), UserCredentials.NONE).getHeader(AUTHORIZATION)).isEqualTo("Bearer token-1");
        assertThat(tokenRequestBodies).containsExactly(
                "grant_type=client_credentials&scope=read&client_id=client&client_secret=secret",
                "grant_type=client_credentials&scope=read+write&client_id=client&client_secret=secret");
    }

    @Test
    void testOAuthClientSecretBasic()
            throws Exception
    {
        List<Request> tokenRequests = new ArrayList<>();
        HttpClient tokenClient = new TestingHttpClient(request -> {
            tokenRequests.add(request);
            return mockResponse(HttpStatus.OK, JSON_UTF_8, "{\"access_token\":\"token\",\"token_type\":\"Bearer\",\"expires_in\":300}");
        });
        // declare both flows, because the password flow is only used when the username and password are set
        String spec =
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
                %s
                components:
                  securitySchemes:
                    oauth:
                      type: oauth2
                      flows:
                        password:
                          tokenUrl: https://localhost:8081/token
                          scopes: {}
                        clientCredentials:
                          tokenUrl: https://localhost:8081/token
                          scopes: {}
                """.formatted(OPERATION_RESPONSES);
        OpenApiConfig config = new OpenApiConfig()
                .setClientId("client:id")
                .setClientSecret("s3cr3t+/")
                .setClientAuthenticationMethod(ClientAuthenticationMethod.CLIENT_SECRET_BASIC);

        createAuthentication(spec, config, tokenClient).filterRequest(request("/secured"), UserCredentials.NONE);
        createAuthentication(spec, config.setUsername("user").setPassword("password"), tokenClient).filterRequest(request("/secured"), UserCredentials.NONE);

        // RFC 6749 requires encoding the client ID and secret as form values before they're joined with a colon
        assertThat(tokenRequests)
                .extracting(request -> request.getHeader(AUTHORIZATION))
                .containsExactly("Basic Y2xpZW50JTNBaWQ6czNjcjN0JTJCJTJG", "Basic Y2xpZW50JTNBaWQ6czNjcjN0JTJCJTJG");
        assertThat(tokenRequests)
                .extracting(TestAuthentication::getBody)
                .containsExactly("grant_type=client_credentials", "grant_type=password&username=user&password=password");
    }

    @Test
    void testOAuthClientSecretBasicRetriesTokenRequests()
            throws Exception
    {
        AtomicInteger tokenRequests = new AtomicInteger();
        Authentication authentication = createAuthentication(
                clientCredentialsSpec("https://localhost:8081/token"),
                new OpenApiConfig()
                        .setClientId("client")
                        .setClientSecret("secret")
                        .setClientAuthenticationMethod(ClientAuthenticationMethod.CLIENT_SECRET_BASIC),
                new TestingHttpClient(_ -> {
                    if (tokenRequests.incrementAndGet() == 1) {
                        return mockResponse(HttpStatus.SERVICE_UNAVAILABLE, JSON_UTF_8, "{}");
                    }
                    return mockResponse(HttpStatus.OK, JSON_UTF_8, "{\"access_token\":\"token\",\"token_type\":\"Bearer\",\"expires_in\":300}");
                }));

        assertThat(authentication.filterRequest(request("/secured"), UserCredentials.NONE).getHeader(AUTHORIZATION)).isEqualTo("Bearer token");
        assertThat(tokenRequests).hasValue(2);
    }

    @Test
    void testOAuthClientSecretBasicWithoutSecret()
            throws Exception
    {
        List<Request> tokenRequests = new ArrayList<>();
        Authentication authentication = createAuthentication(
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
                %s
                components:
                  securitySchemes:
                    oauth:
                      type: oauth2
                      flows:
                        password:
                          tokenUrl: https://localhost:8081/token
                          scopes: {}
                """.formatted(OPERATION_RESPONSES),
                new OpenApiConfig()
                        .setClientId("public-client")
                        .setClientAuthenticationMethod(ClientAuthenticationMethod.CLIENT_SECRET_BASIC)
                        .setUsername("user")
                        .setPassword("password"),
                new TestingHttpClient(request -> {
                    tokenRequests.add(request);
                    return mockResponse(HttpStatus.OK, JSON_UTF_8, "{\"access_token\":\"token\",\"token_type\":\"Bearer\",\"expires_in\":300}");
                }));

        authentication.filterRequest(request("/secured"), UserCredentials.NONE);

        assertThat(tokenRequests).singleElement().satisfies(request -> {
            assertThat(request.getHeader(AUTHORIZATION)).isNull();
            assertThat(getBody(request)).isEqualTo("grant_type=password&username=user&password=password&client_id=public-client");
        });
    }

    @Test
    void testOAuthTokenRequestDoesNotFollowRedirects()
            throws IOException
    {
        List<Request> tokenRequests = new ArrayList<>();
        HttpClient tokenClient = new TestingHttpClient(request -> {
            tokenRequests.add(request);
            return mockResponse(HttpStatus.OK, JSON_UTF_8, "{\"access_token\":\"token\",\"token_type\":\"Bearer\",\"expires_in\":30}");
        });
        OpenApiConfig config = new OpenApiConfig()
                .setClientId("client")
                .setClientSecret("secret");

        createAuthentication(clientCredentialsSpec("https://localhost:8081/token"), config, tokenClient)
                .filterRequest(request("/secured"), UserCredentials.NONE);
        createAuthentication(clientCredentialsSpec("https://localhost:8081/token"), config.setClientAuthenticationMethod(ClientAuthenticationMethod.CLIENT_SECRET_BASIC), tokenClient)
                .filterRequest(request("/secured"), UserCredentials.NONE);

        assertThat(tokenRequests)
                .extracting(Request::isFollowRedirects)
                .containsExactly(false, false);
    }

    @Test
    void testOAuthTokenRequestRejectedWithoutChallenge()
            throws IOException
    {
        // Jetty fails 401 responses without a WWW-Authenticate header, which token endpoints can send, as protocol violations
        HttpServer server = HttpServer.create(new InetSocketAddress(InetAddress.getLoopbackAddress(), 0), 0);
        server.createContext("/token", exchange -> {
            byte[] body = "{\"error\":\"invalid_client\"}".getBytes(UTF_8);
            exchange.getResponseHeaders().add("Content-Type", "application/json");
            exchange.sendResponseHeaders(401, body.length);
            try (OutputStream output = exchange.getResponseBody()) {
                output.write(body);
            }
        });
        server.start();
        try (HttpClient tokenClient = new JettyHttpClient()) {
            OpenApiConfig config = new OpenApiConfig()
                    .setClientId("client")
                    .setClientSecret("secret")
                    .setTokenUrl(URI.create("http://127.0.0.1:%d/token".formatted(server.getAddress().getPort())));

            assertThatThrownBy(() -> createAuthentication(clientCredentialsSpec("https://localhost:8081/token"), config, tokenClient).filterRequest(request("/secured"), UserCredentials.NONE))
                    .isInstanceOfSatisfying(TrinoException.class, e -> assertThat(e.getErrorCode()).isEqualTo(OPENAPI_HTTP_ERROR.toErrorCode()))
                    .hasMessage("Request POST http://127.0.0.1:%d/token for an OAuth token failed with HTTP 401", server.getAddress().getPort());
            assertThatThrownBy(() -> createAuthentication(clientCredentialsSpec("https://localhost:8081/token"), config.setClientAuthenticationMethod(ClientAuthenticationMethod.CLIENT_SECRET_BASIC), tokenClient)
                    .filterRequest(request("/secured"), UserCredentials.NONE))
                    .isInstanceOfSatisfying(TrinoException.class, e -> assertThat(e.getErrorCode()).isEqualTo(OPENAPI_HTTP_ERROR.toErrorCode()))
                    .hasMessage("Request POST http://127.0.0.1:%d/token for an OAuth token failed with HTTP 401", server.getAddress().getPort());
        }
        finally {
            server.stop(0);
        }
    }

    @Test
    void testOAuthTokenUrlFromSpec()
            throws IOException
    {
        List<URI> tokenUris = new ArrayList<>();
        HttpClient tokenClient = new TestingHttpClient(request -> {
            tokenUris.add(request.getUri());
            return mockResponse(HttpStatus.OK, JSON_UTF_8, "{\"access_token\":\"token\",\"expires_in\":30}");
        });
        OpenApiConfig config = new OpenApiConfig()
                .setClientId("client")
                .setClientSecret("secret");

        assertThatThrownBy(() -> createAuthentication(clientCredentialsSpec("http://auth.example.com/token"), config, tokenClient).filterRequest(request("/secured"), UserCredentials.NONE))
                .isInstanceOf(TrinoException.class)
                .hasMessage("Cannot authenticate the GET /secured request, because none of its security requirements can be satisfied: " +
                        "the oauth security scheme sets tokenUrl to http://auth.example.com/token, which doesn't use HTTPS and has a different host than the servers of the API, so set authentication.token-url to use it");

        // the same host as base-uri
        createAuthentication(clientCredentialsSpec("http://localhost:8080/token"), config, tokenClient)
                .filterRequest(request("/secured"), UserCredentials.NONE);
        createAuthentication(clientCredentialsSpec("http://auth.example.com/token"), config.setTokenUrl(URI.create("https://auth.example.com/override")), tokenClient)
                .filterRequest(request("/secured"), UserCredentials.NONE);

        assertThat(tokenUris).containsExactly(URI.create("http://localhost:8080/token"), URI.create("https://auth.example.com/override"));
    }

    @Test
    void testOAuthTokenUrlOnDeclaredServer()
            throws IOException
    {
        List<URI> tokenUris = new ArrayList<>();
        HttpClient tokenClient = new TestingHttpClient(request -> {
            tokenUris.add(request.getUri());
            return mockResponse(HttpStatus.OK, JSON_UTF_8, "{\"access_token\":\"token\",\"expires_in\":30}");
        });
        String spec = clientCredentialsSpec("http://localhost:8080/token").replace("paths:", "servers:\n  - url: http://localhost:8080/v1\npaths:");
        OpenApiConfig config = new OpenApiConfig()
                .setSpecLocation(Files.writeString(tempDir.resolve("spec.yaml"), spec).toString())
                .setClientId("client")
                .setClientSecret("secret")
                .setAllowInsecureHttp(true);

        newAuthentication(config, tokenClient).filterRequest(request("/secured"), UserCredentials.NONE);
        // base-uri is used instead of the declared servers, so their hosts are not trusted
        assertThatThrownBy(() -> newAuthentication(config.setBaseUri(URI.create("http://api.example.com")), tokenClient).filterRequest(request("/secured"), UserCredentials.NONE))
                .isInstanceOf(TrinoException.class)
                .hasMessageEndingWith("sets tokenUrl to http://localhost:8080/token, which doesn't use HTTPS and has a different host than the servers of the API, so set authentication.token-url to use it");

        assertThat(tokenUris).containsExactly(URI.create("http://localhost:8080/token"));
    }

    @Test
    void testOAuthTokenResponseIsNotInErrors()
            throws IOException
    {
        Authentication authentication = createClientCredentialsAuthentication("{\"access_token\":\"s3cr3t\",\"expires_in\":\"soon\"}");

        assertThatThrownBy(() -> authentication.filterRequest(request("/secured"), UserCredentials.NONE))
                .satisfies(e -> assertThat(getStackTraceAsString(e)).doesNotContain("s3cr3t"));
    }

    @Test
    void testOAuthTokenResponseWithoutAccessToken()
            throws IOException
    {
        Authentication authentication = createClientCredentialsAuthentication("{\"token_type\":\"Bearer\",\"expires_in\":30}");

        assertThatThrownBy(() -> authentication.filterRequest(request("/secured"), UserCredentials.NONE))
                .isInstanceOfSatisfying(TrinoException.class, e -> assertThat(e.getErrorCode()).isEqualTo(OPENAPI_INVALID_RESPONSE.toErrorCode()))
                .hasMessage("Response of the token endpoint https://localhost:8081/token doesn't include an access_token");
    }

    @Test
    void testOAuthTokenErrorResponse()
            throws IOException
    {
        for (HttpStatus status : List.of(HttpStatus.BAD_REQUEST, HttpStatus.INTERNAL_SERVER_ERROR)) {
            // token endpoints can repeat the credentials of the token request in the response
            HttpClient tokenClient = new TestingHttpClient(request -> mockResponse(status, JSON_UTF_8, "{\"error\":\"invalid_client\",\"error_description\":\"%s\"}".formatted(getBody(request))));
            for (ClientAuthenticationMethod method : ClientAuthenticationMethod.values()) {
                assertTokenRequestFails(
                        createAuthentication(clientCredentialsSpec("https://localhost:8081/token"), failingTokenConfig(method), tokenClient),
                        OPENAPI_HTTP_ERROR,
                        "Request POST https://localhost:8081/token for an OAuth token failed with HTTP " + status.code());
            }
            assertTokenRequestFails(
                    createAuthentication(clientCredentialsSpec("https://localhost:8081/token").replace("clientCredentials:", "password:"), failingTokenConfig(ClientAuthenticationMethod.CLIENT_SECRET_POST), tokenClient),
                    OPENAPI_HTTP_ERROR,
                    "Request POST https://localhost:8081/token for an OAuth token failed with HTTP " + status.code());
        }
    }

    @Test
    void testOAuthTokenConnectionFailure()
            throws IOException
    {
        HttpClient tokenClient = new TestingHttpClient(_ -> {
            throw new ConnectException("Connection refused");
        });
        for (ClientAuthenticationMethod method : ClientAuthenticationMethod.values()) {
            assertTokenRequestFails(
                    createAuthentication(clientCredentialsSpec("https://localhost:8081/token"), failingTokenConfig(method), tokenClient),
                    OPENAPI_CONNECTION_ERROR,
                    "Request POST https://localhost:8081/token for an OAuth token failed: Connection refused");
        }
    }

    @Test
    void testOAuthTokenRefreshErrorResponse()
            throws IOException
    {
        AtomicInteger tokenRequests = new AtomicInteger();
        HttpClient tokenClient = new TestingHttpClient(_ -> {
            if (tokenRequests.incrementAndGet() % 2 == 1) {
                return mockResponse(HttpStatus.OK, JSON_UTF_8, "{\"access_token\":\"token\",\"token_type\":\"Bearer\",\"expires_in\":300}");
            }
            return mockResponse(HttpStatus.UNAUTHORIZED, JSON_UTF_8, "{\"error\":\"invalid_client\"}");
        });
        for (ClientAuthenticationMethod method : ClientAuthenticationMethod.values()) {
            Authentication authentication = createAuthentication(clientCredentialsSpec("https://localhost:8081/token"), failingTokenConfig(method), tokenClient);
            Request authenticatedRequest = authentication.filterRequest(request("/secured"), UserCredentials.NONE);

            assertThatThrownBy(() -> authentication.refreshRejectedTokens(request("/secured"), authenticatedRequest, UserCredentials.NONE))
                    .isInstanceOfSatisfying(TrinoException.class, e -> assertThat(e.getErrorCode()).isEqualTo(OPENAPI_HTTP_ERROR.toErrorCode()))
                    .hasMessage("Request POST https://localhost:8081/token for an OAuth token failed with HTTP 401");
        }
    }

    @Test
    void testApiKeysWithReservedCharacters()
            throws Exception
    {
        Authentication authentication = createAuthentication(
                """
                openapi: 3.0.0
                info:
                  title: test
                  version: '1'
                security:
                  - apiKeyHeader: []
                    apiKeyCookie: []
                    apiKeyQuery: []
                paths:
                  /secured:
                    get:
                %s
                %s
                """.formatted(OPERATION_RESPONSES, SECURITY_SCHEMES),
                new OpenApiConfig()
                        .setApiKeys("X-API-Key=ab+c/d,session=ab+c/d;e,api_key=ab+c/d%&e"));

        Request request = authentication.filterRequest(request("/secured"), UserCredentials.NONE);
        assertThat(request.getHeader(HeaderName.of("X-API-Key"))).isEqualTo("ab+c/d");
        assertThat(request.getHeader(COOKIE)).isEqualTo("session=ab%2Bc%2Fd%3Be");
        assertThat(request.getUri().getRawQuery()).isEqualTo("api_key=ab%2Bc%2Fd%25%26e");
    }

    @Test
    void testPasswordWithReservedCharacters()
            throws Exception
    {
        Authentication authentication = createAuthentication(
                """
                openapi: 3.0.0
                info:
                  title: test
                  version: '1'
                security:
                  - basicAuth: []
                paths:
                  /secured:
                    get:
                %s
                %s
                """.formatted(OPERATION_RESPONSES, SECURITY_SCHEMES),
                new OpenApiConfig()
                        .setUsername("user")
                        .setPassword("a~~~"));

        // the standard base64 encoding of user:a~~~ ends with +, and base64url would use - instead
        assertThat(authentication.filterRequest(request("/secured"), UserCredentials.NONE).getHeader(AUTHORIZATION)).isEqualTo("Basic dXNlcjphfn5+");
    }

    @Test
    void testQueryApiKeyKeepsEncodedFilterValues()
            throws Exception
    {
        Authentication authentication = createAuthentication(
                """
                openapi: 3.0.0
                info:
                  title: test
                  version: '1'
                security:
                  - apiKeyQuery: []
                paths:
                  /{tenant}/items:
                    get:
                      parameters:
                        - name: tenant
                          in: path
                          required: true
                          schema:
                            type: string
                        - name: q
                          in: query
                          schema:
                            type: string
                %s
                %s
                """.formatted(OPERATION_RESPONSES, SECURITY_SCHEMES));

        Request request = authentication.filterRequest(prepareGet()
                .setUri(URI.create(BASE_URI + "/a%2Fb/items?q=x%26admin%3Dtrue#top"))
                .addHeader(OPENAPI_PATH_HEADER, "/{tenant}/items")
                .build(), UserCredentials.NONE);
        assertThat(request.getUri()).isEqualTo(URI.create(BASE_URI + "/a%2Fb/items?q=x%26admin%3Dtrue&api_key=query-secret#top"));
    }

    @Test
    void testPartiallyConfiguredSecurityRequirements()
            throws Exception
    {
        String spec =
                """
                openapi: 3.0.0
                info:
                  title: test
                  version: '1'
                security:
                  - apiKeyHeader: []
                    apiKeyQuery: []
                  - bearerAuth: []
                  - basicAuth: []
                paths:
                  /secured:
                    get:
                %s
                %s
                """.formatted(OPERATION_RESPONSES, SECURITY_SCHEMES);

        Authentication withBasicAuth = createAuthentication(spec, new OpenApiConfig()
                .setApiKeys("X-API-Key=header-secret")
                .setUsername("user")
                .setPassword("password"));
        Request request = withBasicAuth.filterRequest(request("/secured"), UserCredentials.NONE);
        assertThat(request.getHeaders().get(AUTHORIZATION)).containsExactly("Basic dXNlcjpwYXNzd29yZA==");
        assertThat(request.getHeader(HeaderName.of("X-API-Key"))).isNull();
        assertThat(request.getUri().getQuery()).isNull();

        Authentication withoutCredentials = createAuthentication(spec, new OpenApiConfig()
                .setApiKeys("X-API-Key=header-secret")
                .setUsername("user"));
        assertThatThrownBy(() -> withoutCredentials.filterRequest(request("/secured"), UserCredentials.NONE))
                .isInstanceOfSatisfying(TrinoException.class, e -> assertThat(e.getErrorCode()).isEqualTo(INVALID_CATALOG_PROPERTY.toErrorCode()))
                .hasMessage("Cannot authenticate the GET /secured request, because none of its security requirements can be satisfied: " +
                        "the apiKeyQuery security scheme requires the api_key key in the authentication.api-keys configuration property; " +
                        "the bearerAuth security scheme requires the authentication.bearer-token configuration property; " +
                        "the basicAuth security scheme requires the authentication.password configuration property");
    }

    @Test
    void testDefaultAuthenticationWithoutCredentials()
            throws Exception
    {
        Authentication authentication = createAuthentication(
                """
                openapi: 3.0.0
                info:
                  title: test
                  version: '1'
                paths:
                  /items:
                    get:
                %s
                """.formatted(OPERATION_RESPONSES),
                new OpenApiConfig()
                        .setAuthenticationType(AuthenticationType.HTTP)
                        .setAuthenticationScheme(AuthenticationScheme.BEARER));

        assertThatThrownBy(() -> authentication.filterRequest(request("/items"), UserCredentials.NONE))
                .isInstanceOfSatisfying(TrinoException.class, e -> assertThat(e.getErrorCode()).isEqualTo(INVALID_CATALOG_PROPERTY.toErrorCode()))
                .hasMessage("Cannot authenticate the GET /items request, because the http authentication type requires the authentication.bearer-token configuration property");
    }

    @Test
    void testCookieApiKeyIsMergedWithCookieParameters()
            throws Exception
    {
        Authentication authentication = createAuthentication(
                """
                openapi: 3.0.0
                info:
                  title: test
                  version: '1'
                security:
                  - apiKeyCookie: []
                paths:
                  /secured:
                    get:
                %s
                %s
                """.formatted(OPERATION_RESPONSES, SECURITY_SCHEMES));

        Request request = fromRequest(request("/secured"))
                .addHeader(COOKIE, "page=2")
                .build();
        assertThat(authentication.filterRequest(request, UserCredentials.NONE).getHeaders().get(COOKIE)).containsExactly("page=2; session=cookie-secret");
        assertThat(authentication.filterRequest(request("/secured"), UserCredentials.NONE).getHeaders().get(COOKIE)).containsExactly("session=cookie-secret");
    }

    @Test
    void testPathHeaderIsNotSent()
            throws Exception
    {
        Authentication authentication = createAuthentication(
                """
                openapi: 3.0.0
                info:
                  title: test
                  version: '1'
                paths:
                  /items:
                    get:
                %s
                """.formatted(OPERATION_RESPONSES));

        Request request = authentication.filterRequest(request("/items"), UserCredentials.NONE);
        assertThat(request.getHeader(OPENAPI_PATH_HEADER)).isNull();
        assertThat(request.getHeader(AUTHORIZATION)).isEqualTo("Basic dXNlcjpwYXNzd29yZA==");
    }

    private Authentication createAuthentication(String specContents)
            throws IOException
    {
        return createAuthentication(specContents, new OpenApiConfig()
                .setAuthenticationType(AuthenticationType.HTTP)
                .setAuthenticationScheme(AuthenticationScheme.BASIC)
                .setUsername("user")
                .setPassword("password")
                .setApiKeys("X-API-Key=header-secret,session=cookie-secret,api_key=query-secret"));
    }

    private Authentication createAuthentication(String specContents, OpenApiConfig config)
            throws IOException
    {
        return createAuthentication(specContents, config, new TestingHttpClient(_ -> {
            throw new UnsupportedOperationException("Unexpected request to the authentication server");
        }));
    }

    private Authentication createAuthentication(String specContents, OpenApiConfig config, HttpClient httpClient)
            throws IOException
    {
        Path specFile = Files.writeString(tempDir.resolve("spec.yaml"), specContents);
        config.setSpecLocation(specFile.toString())
                .setBaseUri(BASE_URI);
        return newAuthentication(config, httpClient);
    }

    private static Authentication newAuthentication(OpenApiConfig config, HttpClient httpClient)
    {
        OpenApiSpec spec = createSpec(config);
        return new Authentication(config, spec, new OpenApiServers(config, spec), httpClient);
    }

    private Authentication createClientCredentialsAuthentication(String tokenResponse)
            throws IOException
    {
        return createClientCredentialsAuthentication(new TestingHttpClient(_ -> mockResponse(HttpStatus.OK, JSON_UTF_8, tokenResponse)));
    }

    private static String clientCredentialsSpec(String tokenUrl)
    {
        return """
               openapi: 3.0.0
               info:
                 title: test
                 version: '1'
               security:
                 - oauth: []
               paths:
                 /secured:
                   get:
               %s
               components:
                 securitySchemes:
                   oauth:
                     type: oauth2
                     flows:
                       clientCredentials:
                         tokenUrl: %s
                         scopes: {}
               """.formatted(OPERATION_RESPONSES, tokenUrl);
    }

    private Authentication createClientCredentialsAuthentication(HttpClient tokenClient)
            throws IOException
    {
        return createAuthentication(
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
                %s
                components:
                  securitySchemes:
                    oauth:
                      type: oauth2
                      flows:
                        clientCredentials:
                          tokenUrl: https://localhost:8081/token
                          scopes: {}
                """.formatted(OPERATION_RESPONSES),
                new OpenApiConfig()
                        .setClientId("client")
                        .setClientSecret("secret"),
                tokenClient);
    }

    private static OpenApiConfig failingTokenConfig(ClientAuthenticationMethod method)
    {
        return new OpenApiConfig()
                .setClientId("client")
                .setClientSecret("client-s3cr3t")
                .setUsername("user")
                .setPassword("user-s3cr3t")
                .setClientAuthenticationMethod(method)
                .setTokenUrl(URI.create("https://localhost:8081/token?tenant=tenant-s3cr3t"));
    }

    private static void assertTokenRequestFails(Authentication authentication, ErrorCodeSupplier errorCode, String message)
    {
        assertThatThrownBy(() -> authentication.filterRequest(request("/secured"), UserCredentials.NONE))
                .isInstanceOfSatisfying(TrinoException.class, e -> {
                    assertThat(e.getErrorCode()).isEqualTo(errorCode.toErrorCode());
                    assertThat(getStackTraceAsString(e)).doesNotContain("s3cr3t", "invalid_client");
                })
                .hasMessage(message);
    }

    private static Request request(String path)
    {
        return prepareGet()
                .setUri(BASE_URI.resolve(path))
                .addHeader(OPENAPI_PATH_HEADER, path)
                .build();
    }

    private static String getBody(Request request)
    {
        return new String(((StaticBodyGenerator) request.getBodyGenerator()).getBody(), UTF_8);
    }

    private static void assertNoCredentials(Request request)
    {
        assertThat(request.getHeader(AUTHORIZATION)).isNull();
        assertThat(request.getHeader(HeaderName.of("X-API-Key"))).isNull();
        assertThat(request.getHeader(COOKIE)).isNull();
        assertThat(request.getUri().getQuery()).isNull();
    }
}
