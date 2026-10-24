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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Stopwatch;
import io.airlift.http.client.HttpClient;
import io.airlift.http.client.Request;
import io.airlift.http.client.StringResponseHandler.StringResponse;
import io.airlift.http.client.jetty.JettyHttpClient;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.io.TempDir;
import pl.net.was.KeycloakServer;
import pl.net.was.OpenApiConfig;
import pl.net.was.OpenApiServers;
import pl.net.was.OpenApiSpec;

import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;

import static com.google.common.io.BaseEncoding.base64;
import static com.google.common.io.BaseEncoding.base64Url;
import static io.airlift.http.client.HeaderNames.AUTHORIZATION;
import static io.airlift.http.client.HeaderNames.CONTENT_TYPE;
import static io.airlift.http.client.HeaderNames.WWW_AUTHENTICATE;
import static io.airlift.http.client.Request.Builder.prepareGet;
import static io.airlift.http.client.Request.Builder.preparePost;
import static io.airlift.http.client.StaticBodyGenerator.createStaticBodyGenerator;
import static io.airlift.http.client.StatusResponseHandler.createStatusResponseHandler;
import static io.airlift.http.client.StringResponseHandler.createStringResponseHandler;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;
import static pl.net.was.OpenApiClient.OPENAPI_PATH_HEADER;
import static pl.net.was.TestingOpenApiSpec.createSpec;

@TestInstance(PER_CLASS)
class TestKeycloakAuthentication
{
    // the userinfo endpoint is an API that accepts the tokens of the realm, and rejects revoked and expired ones
    private static final String USERINFO_PATH = "/protocol/openid-connect/userinfo";
    private static final String REVOKE_PATH = "/protocol/openid-connect/revoke";
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    private KeycloakServer keycloakServer;
    private HttpClient httpClient;

    @TempDir
    Path tempDir;

    @BeforeAll
    void setUp()
    {
        keycloakServer = new KeycloakServer();
        httpClient = new JettyHttpClient();
    }

    @AfterAll
    void tearDown()
    {
        httpClient.close();
        keycloakServer.close();
    }

    @Test
    void testPasswordFlow()
            throws Exception
    {
        Authentication authentication = createAuthentication(clientConfig()
                .setUsername("test-user")
                .setPassword("test-password"));

        assertThat(getTokenClaims(getToken(authentication)).path("preferred_username").asText())
                .isEqualTo("test-user");
    }

    @Test
    void testClientCredentialsFlow()
            throws Exception
    {
        for (ClientAuthenticationMethod clientAuthenticationMethod : ClientAuthenticationMethod.values()) {
            Authentication authentication = createAuthentication(clientConfig()
                    .setClientAuthenticationMethod(clientAuthenticationMethod));

            JsonNode claims = getTokenClaims(getToken(authentication));
            assertThat(claims.path("preferred_username").asText()).isEqualTo("service-account-sample-client-id");
            assertThat(claims.path("scope").asText().split(" ")).contains("openid");
        }
    }

    @Test
    void testExpiringTokenIsReplaced()
            throws Exception
    {
        // the tokens of this client expire after 4 seconds
        Authentication authentication = createAuthentication(clientConfig()
                .setClientId("short-lived-client-id"));
        HttpClient client = new AuthenticatingHttpClient(httpClient, authentication);

        String expiringToken = getToken(authentication);
        assertThat(getToken(authentication)).isEqualTo(expiringToken);
        assertThat(getUserInfo(client).getStatusCode()).isEqualTo(200);

        // Keycloak measures the lifespan in whole seconds, so the token can be accepted a bit longer
        Stopwatch stopwatch = Stopwatch.createStarted();
        while (getUserInfo(httpClient, expiringToken).getStatusCode() != 401) {
            assertThat(stopwatch.elapsed()).isLessThan(Duration.ofSeconds(30));
            Thread.sleep(Duration.ofMillis(500));
        }
        assertThat(getUserInfo(client).getStatusCode()).isEqualTo(200);
        assertThat(getToken(authentication)).isNotEqualTo(expiringToken);
    }

    @Test
    void testRevokedClientCredentialsTokenIsRefreshed()
            throws Exception
    {
        for (ClientAuthenticationMethod clientAuthenticationMethod : ClientAuthenticationMethod.values()) {
            assertRevokedTokenIsRefreshed(createAuthentication(clientConfig()
                    .setClientAuthenticationMethod(clientAuthenticationMethod)));
        }
    }

    @Test
    void testRevokedPasswordTokenIsRefreshed()
            throws Exception
    {
        for (ClientAuthenticationMethod clientAuthenticationMethod : ClientAuthenticationMethod.values()) {
            assertRevokedTokenIsRefreshed(createAuthentication(clientConfig()
                    .setClientAuthenticationMethod(clientAuthenticationMethod)
                    .setUsername("test-user")
                    .setPassword("test-password")));
        }
    }

    private void assertRevokedTokenIsRefreshed(Authentication authentication)
    {
        HttpClient client = new AuthenticatingHttpClient(httpClient, authentication);
        String revokedToken = getToken(authentication);
        assertThat(getUserInfo(client).getStatusCode()).isEqualTo(200);

        revoke(revokedToken);

        StringResponse rejected = getUserInfo(httpClient, revokedToken);
        assertThat(rejected.getStatusCode()).isEqualTo(401);
        assertThat(rejected.getHeader(WWW_AUTHENTICATE)).hasValueSatisfying(challenge -> assertThat(challenge).startsWith("Bearer "));
        assertThat(getUserInfo(client).getStatusCode()).isEqualTo(200);
        assertThat(getToken(authentication)).isNotEqualTo(revokedToken);
    }

    private OpenApiConfig clientConfig()
    {
        return new OpenApiConfig()
                .setBaseUri(URI.create(keycloakServer.getRealmUrl()))
                // Keycloak runs without TLS
                .setTokenUrl(URI.create(keycloakServer.getTokenUrl()))
                .setClientId("sample-client-id")
                .setClientSecret("secret");
    }

    private Authentication createAuthentication(OpenApiConfig config)
            throws IOException
    {
        // declare both flows to check that the password flow is only used when credentials are configured,
        // and require the openid scope, without which Keycloak rejects tokens sent to the userinfo endpoint
        Path specFile = Files.writeString(tempDir.resolve("spec.yaml"),
                """
                openapi: 3.0.0
                info:
                  title: test
                  version: '1'
                security:
                  - oauth: [openid]
                paths:
                  %1$s:
                    get:
                      responses:
                        '200':
                          description: OK
                          content:
                            application/json:
                              schema:
                                type: object
                                properties:
                                  sub:
                                    type: string
                components:
                  securitySchemes:
                    oauth:
                      type: oauth2
                      flows:
                        password:
                          tokenUrl: %2$s
                          scopes:
                            openid: OpenID Connect
                        clientCredentials:
                          tokenUrl: %2$s
                          scopes:
                            openid: OpenID Connect
                """.formatted(USERINFO_PATH, keycloakServer.getTokenUrl()));
        config.setSpecLocation(specFile.toString());
        OpenApiSpec spec = createSpec(config);
        return new Authentication(config, spec, new OpenApiServers(config, spec), httpClient);
    }

    private StringResponse getUserInfo(HttpClient client)
    {
        return client.execute(userInfoRequest().build(), createStringResponseHandler());
    }

    private StringResponse getUserInfo(HttpClient client, String token)
    {
        return client.execute(userInfoRequest().setHeader(AUTHORIZATION, "Bearer " + token).build(), createStringResponseHandler());
    }

    private Request.Builder userInfoRequest()
    {
        return prepareGet()
                .setUri(URI.create(keycloakServer.getRealmUrl() + USERINFO_PATH))
                .addHeader(OPENAPI_PATH_HEADER, USERINFO_PATH);
    }

    private void revoke(String token)
    {
        int status = httpClient.execute(
                        preparePost()
                                .setUri(URI.create(keycloakServer.getRealmUrl() + REVOKE_PATH))
                                .setHeader(AUTHORIZATION, "Basic " + base64().encode("sample-client-id:secret".getBytes(UTF_8)))
                                .setHeader(CONTENT_TYPE, "application/x-www-form-urlencoded")
                                .setBodyGenerator(createStaticBodyGenerator("token_type_hint=access_token&token=" + token, UTF_8))
                                .build(),
                        createStatusResponseHandler())
                .getStatusCode();
        assertThat(status).isEqualTo(200);
    }

    private static String getToken(Authentication authentication)
    {
        String authorization = authentication.filterRequest(prepareGet()
                        .setUri(URI.create("http://localhost" + USERINFO_PATH))
                        .addHeader(OPENAPI_PATH_HEADER, USERINFO_PATH)
                        .build())
                .getHeader(AUTHORIZATION);
        assertThat(authorization).startsWith("Bearer ");
        return authorization.substring("Bearer ".length());
    }

    private static JsonNode getTokenClaims(String token)
            throws IOException
    {
        String[] tokenParts = token.split("\\.");
        assertThat(tokenParts).hasSize(3);
        return OBJECT_MAPPER.readTree(base64Url().omitPadding().decode(tokenParts[1]));
    }
}
