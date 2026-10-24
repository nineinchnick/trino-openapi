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
import io.airlift.http.client.HttpClient;
import io.airlift.http.client.Request;
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

import static com.google.common.io.BaseEncoding.base64Url;
import static io.airlift.http.client.HeaderNames.AUTHORIZATION;
import static io.airlift.http.client.Request.Builder.prepareGet;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;
import static pl.net.was.OpenApiClient.OPENAPI_PATH_HEADER;

@TestInstance(PER_CLASS)
class TestKeycloakAuthentication
{
    private static final URI BASE_URI = URI.create("http://localhost:8080");
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

        assertThat(getTokenClaims(authentication.filterRequest(request())).path("preferred_username").asText())
                .isEqualTo("test-user");
    }

    @Test
    void testClientCredentialsFlow()
            throws Exception
    {
        Authentication authentication = createAuthentication(clientConfig());

        assertThat(getTokenClaims(authentication.filterRequest(request())).path("preferred_username").asText())
                .isEqualTo("service-account-sample-client-id");
    }

    private OpenApiConfig clientConfig()
    {
        return new OpenApiConfig()
                .setBaseUri(BASE_URI)
                // Keycloak runs on another host than the API, and without TLS
                .setTokenUrl(URI.create(keycloakServer.getTokenUrl()))
                .setClientId("sample-client-id")
                .setClientSecret("secret");
    }

    private Authentication createAuthentication(OpenApiConfig config)
            throws IOException
    {
        // declare both flows to check that the password flow is only used when credentials are configured
        Path specFile = Files.writeString(tempDir.resolve("spec.yaml"),
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
                          content:
                            application/json:
                              schema:
                                type: array
                                items:
                                  type: object
                                  properties:
                                    id:
                                      type: integer
                components:
                  securitySchemes:
                    oauth:
                      type: oauth2
                      flows:
                        password:
                          tokenUrl: %1$s
                          scopes: {}
                        clientCredentials:
                          tokenUrl: %1$s
                          scopes: {}
                """.formatted(keycloakServer.getTokenUrl()));
        config.setSpecLocation(specFile.toString());
        OpenApiSpec spec = new OpenApiSpec(config);
        return new Authentication(config, spec, new OpenApiServers(config, spec), httpClient);
    }

    private static Request request()
    {
        return prepareGet()
                .setUri(BASE_URI.resolve("/secured"))
                .addHeader(OPENAPI_PATH_HEADER, "/secured")
                .build();
    }

    private static JsonNode getTokenClaims(Request request)
            throws IOException
    {
        String authorization = request.getHeader(AUTHORIZATION);
        assertThat(authorization).startsWith("Bearer ");
        String[] tokenParts = authorization.substring("Bearer ".length()).split("\\.");
        assertThat(tokenParts).hasSize(3);
        return OBJECT_MAPPER.readTree(base64Url().omitPadding().decode(tokenParts[1]));
    }
}
