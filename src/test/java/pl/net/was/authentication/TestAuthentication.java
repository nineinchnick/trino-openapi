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

import io.airlift.configuration.InvalidConfigurationException;
import io.airlift.http.client.HttpClient;
import io.airlift.http.client.HttpStatus;
import io.airlift.http.client.Request;
import io.airlift.http.client.testing.TestingHttpClient;
import io.trino.spi.TrinoException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import pl.net.was.OpenApiConfig;
import pl.net.was.OpenApiSpec;

import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.atomic.AtomicInteger;

import static com.google.common.net.MediaType.JSON_UTF_8;
import static io.airlift.http.client.Request.Builder.prepareGet;
import static io.airlift.http.client.testing.TestingResponse.mockResponse;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class TestAuthentication
{
    private static final URI BASE_URI = URI.create("http://localhost:8080");
    private static final String SECURITY_SCHEMES = """
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
            """;
    private static final String OPERATION_RESPONSES = """
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
        Authentication authentication = createAuthentication("""
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

        assertNoCredentials(authentication.filterRequest(request("/anonymous")));
        assertThat(authentication.filterRequest(request("/inherited")).getHeader("Authorization"))
                .isEqualTo("Basic dXNlcjpwYXNzd29yZA==");
    }

    @Test
    void testAnonymousOperationSkipsGlobalRequirements()
            throws Exception
    {
        Authentication authentication = createAuthentication("""
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

        assertNoCredentials(authentication.filterRequest(request("/anonymous")));
        Request inherited = authentication.filterRequest(request("/inherited"));
        assertThat(inherited.getHeader("Authorization")).isEqualTo("Basic dXNlcjpwYXNzd29yZA==");
        assertThat(inherited.getHeader("X-API-Key")).isEqualTo("header-secret");
        assertThat(inherited.getHeader("Cookie")).isEqualTo("session=cookie-secret");
        assertThat(inherited.getUri().getQuery()).isEqualTo("api_key=query-secret");
    }

    @Test
    void testSpecWithoutSecuritySchemes()
            throws Exception
    {
        Authentication authentication = createAuthentication("""
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

        assertNoCredentials(authentication.filterRequest(request("/secured")));
    }

    @Test
    void testSpecWithoutPaths()
            throws Exception
    {
        Authentication authentication = createAuthentication("""
                openapi: 3.1.0
                info:
                  title: test
                  version: '1'
                %s
                """.formatted(SECURITY_SCHEMES));

        assertThat(authentication.filterRequest(request("/unknown")).getHeader("Authorization"))
                .isEqualTo("Basic dXNlcjpwYXNzd29yZA==");
    }

    @Test
    void testOAuthFlowsRequiringBrowser()
            throws Exception
    {
        Authentication authentication = createAuthentication("""
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
                          tokenUrl: http://localhost:8081/token
                          scopes: {}
                        implicit:
                          authorizationUrl: http://localhost:8081/authorize
                          scopes: {}
                """.formatted(OPERATION_RESPONSES));

        assertThatThrownBy(() -> authentication.filterRequest(request("/secured")))
                .isInstanceOf(TrinoException.class)
                .hasMessage("The oauth OAuth security scheme must declare a password or clientCredentials flow, because other flows require a user to log in using a browser");
    }

    @Test
    void testOAuthPasswordFlowWithoutCredentials()
            throws Exception
    {
        Authentication authentication = createAuthentication("""
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
                          tokenUrl: http://localhost:8081/token
                          scopes: {}
                """.formatted(OPERATION_RESPONSES), new OpenApiConfig()
                .setAuthenticationType(AuthenticationType.OAUTH)
                .setClientId("client")
                .setClientSecret("secret"));

        assertThatThrownBy(() -> authentication.filterRequest(request("/secured")))
                .isInstanceOf(TrinoException.class)
                .hasMessage("The password flow of the oauth OAuth security scheme requires the authentication.username and authentication.password configuration properties");
    }

    @Test
    void testShortLivedOAuthTokenIsReused()
            throws Exception
    {
        AtomicInteger tokenRequests = new AtomicInteger();
        Authentication authentication = createAuthentication("""
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
                          tokenUrl: http://localhost:8081/token
                          scopes: {}
                """.formatted(OPERATION_RESPONSES), new OpenApiConfig()
                .setAuthenticationType(AuthenticationType.OAUTH)
                .setClientId("client")
                .setClientSecret("secret"), new TestingHttpClient(_ -> mockResponse(
                HttpStatus.OK,
                JSON_UTF_8,
                "{\"access_token\":\"token-%d\",\"token_type\":\"Bearer\",\"expires_in\":30}".formatted(tokenRequests.incrementAndGet()))));

        assertThat(authentication.filterRequest(request("/secured")).getHeader("Authorization")).isEqualTo("Bearer token-1");
        assertThat(authentication.filterRequest(request("/secured")).getHeader("Authorization")).isEqualTo("Bearer token-1");
        assertThat(tokenRequests).hasValue(1);
    }

    private Authentication createAuthentication(String specContents)
            throws IOException, InvalidConfigurationException
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
        return new Authentication(config, new OpenApiSpec(config), httpClient);
    }

    private static Request request(String path)
    {
        return prepareGet()
                .setUri(BASE_URI.resolve(path))
                .addHeader("X-Trino-OpenAPI-Path", path)
                .build();
    }

    private static void assertNoCredentials(Request request)
    {
        assertThat(request.getHeader("Authorization")).isNull();
        assertThat(request.getHeader("X-API-Key")).isNull();
        assertThat(request.getHeader("Cookie")).isNull();
        assertThat(request.getUri().getQuery()).isNull();
    }
}
