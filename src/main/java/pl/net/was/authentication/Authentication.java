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

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Inject;
import io.airlift.http.client.BodyGenerator;
import io.airlift.http.client.HttpClient;
import io.airlift.http.client.HttpRequestFilter;
import io.airlift.http.client.Request;
import io.swagger.v3.oas.models.PathItem;
import io.swagger.v3.oas.models.security.SecurityRequirement;
import io.swagger.v3.oas.models.security.SecurityScheme;
import pl.net.was.OpenApiConfig;
import pl.net.was.OpenApiSpec;

import java.net.URI;
import java.net.URISyntaxException;
import java.net.URLEncoder;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.function.Supplier;

import static com.google.common.io.BaseEncoding.base64Url;
import static io.airlift.http.client.HttpUriBuilder.uriBuilderFrom;
import static io.airlift.http.client.JsonResponseHandler.createJsonResponseHandler;
import static io.airlift.http.client.Request.Builder.fromRequest;
import static io.airlift.http.client.Request.Builder.preparePost;
import static io.airlift.http.client.StaticBodyGenerator.createStaticBodyGenerator;
import static io.airlift.json.JsonCodec.jsonCodec;
import static java.lang.String.format;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;
import static java.util.Objects.requireNonNullElse;
import static java.util.stream.Collectors.joining;

public class Authentication
        implements HttpRequestFilter
{
    private final Map<String, Map<PathItem.HttpMethod, List<SecurityRequirement>>> pathSecurityRequirements;
    private final Map<String, SecurityScheme> securitySchemas;
    private final List<SecurityRequirement> securityRequirements;
    private final String defaultAuthenticationScheme;
    private final AuthenticationType defaultAuthenticationType;
    private final String username;
    private final String password;
    private final String apiKeyName;
    private final String apiKeyValue;

    private final URI baseUri;
    private final HttpClient httpClient;
    private final BodyGenerator bodyGenerator;
    private final String tokenEndpoint;
    private final String clientId;
    private final String clientSecret;
    private final Supplier<String> token = Suppliers.memoize(this::getToken);

    @Inject
    public Authentication(OpenApiConfig config,
            OpenApiSpec spec,
            @OpenApiAuthenticationClient HttpClient httpClient)
    {
        requireNonNull(config, "config is null");
        requireNonNull(spec, "spec is null");
        this.pathSecurityRequirements = spec.getPathSecurityRequirements();
        this.securityRequirements = spec.getSecurityRequirements();
        this.securitySchemas = spec.getSecuritySchemas();
        this.defaultAuthenticationScheme = config.getAuthenticationScheme().toString();
        this.defaultAuthenticationType = config.getAuthenticationType();
        this.username = config.getUsername();
        this.password = config.getPassword();
        this.apiKeyName = config.getApiKeyName();
        this.apiKeyValue = config.getApiKeyValue();

        this.baseUri = requireNonNull(config.getBaseUri(), "baseUri is null");
        this.httpClient = requireNonNull(httpClient, "httpClient is null");
        this.bodyGenerator = createStaticBodyGenerator(getBody(config.getGrantType(), config.getUsername(), config.getPassword()), UTF_8);
        this.tokenEndpoint = config.getTokenEndpoint();
        this.clientId = config.getClientId();
        this.clientSecret = config.getClientSecret();
    }

    @Override
    public Request filterRequest(Request request)
    {
        URI uri = request.getUri();
        PathItem.HttpMethod method = PathItem.HttpMethod.valueOf(request.getMethod());
        Request.Builder builder = fromRequest(request);
        List<SecurityRequirement> requirements = getRequirements(request.getHeader("X-Trino-OpenAPI-Path"), method);
        applyAuthFilters(builder, requirements, uri);
        if ((requirements == null || requirements.isEmpty()) && defaultAuthenticationType != AuthenticationType.NONE) {
            switch (defaultAuthenticationType) {
                case API_KEY -> {
                    SecurityScheme scheme = new SecurityScheme();
                    scheme.setIn(SecurityScheme.In.HEADER);
                    applyApiKeyAuth(builder, uri, scheme);
                }
                case HTTP -> applyHttpAuth(builder, null);
                case OAUTH -> applyOAuth(builder);
            }
        }
        return builder.build();
    }

    private List<SecurityRequirement> getRequirements(String path, PathItem.HttpMethod method)
    {
        requireNonNull(path, "path is null");
        requireNonNull(method, "method is null");
        if (pathSecurityRequirements.containsKey(path)&& pathSecurityRequirements.get(path).containsKey(method)) {
            return pathSecurityRequirements.get(path).get(method);
        }
        return securityRequirements;
    }

    private void applyAuthFilters(Request.Builder builder, List<SecurityRequirement> requirements, URI uri)
    {
        if (requirements == null) {
            return;
        }
        requirements.forEach(requirement -> requirement.forEach((name, options) -> {
            SecurityScheme securitySchema = securitySchemas.get(name);
            requireNonNull(securitySchema, "securitySchema is null");
            switch (securitySchema.getType()) {
                case APIKEY -> applyApiKeyAuth(builder, uri, securitySchema);
                case HTTP -> applyHttpAuth(builder, securitySchema.getScheme());
                case OAUTH2 -> applyOAuth(builder);
                default -> throw new IllegalArgumentException(format("Unsupported security schema %s", securitySchema.getType()));
            }
        }));
    }

    private void applyApiKeyAuth(Request.Builder builder, URI uri, SecurityScheme scheme)
    {
        String name = requireNonNullElse(scheme.getName(), apiKeyName);
        requireNonNull(name, "Cannot use API Key authentication method, authentication.api-key-name configuration property is not set");
        requireNonNull(apiKeyValue, "Cannot use API Key authentication method, authentication.api-key-value configuration property is not set");
        switch (scheme.getIn()) {
            case COOKIE -> builder.addHeader("Cookie", encodePair(name, apiKeyValue));
            case HEADER -> builder.addHeader(name, apiKeyValue);
            case QUERY -> {
                String query = encodePair(name, apiKeyValue);
                try {
                    builder.setUri(new URI(
                            uri.getScheme(),
                            uri.getAuthority(),
                            uri.getPath(),
                            uri.getQuery() == null ? query : uri.getQuery() + "&" + query,
                            uri.getFragment()));
                }
                catch (URISyntaxException e) {
                    throw new RuntimeException(e);
                }
            }
            default -> throw new IllegalArgumentException(format("Unsupported security schema `in` type: %s", scheme.getIn()));
        }
    }

    private Request.Builder applyHttpAuth(Request.Builder builder, String scheme)
    {
        return builder.addHeader(
                "Authorization",
                getAuthHeader(requireNonNullElse(scheme, defaultAuthenticationScheme), username, password));
    }

    private Request.Builder applyOAuth(Request.Builder builder)
    {
        // TODO pick one of supported securitySchema.getFlows(), instead of hardcoding clientCredentials
        // TODO use options as scopes
                /*
                type: oauth2
                flows:
                  implicit:
                    authorizationUrl: https://example.com/api/oauth/dialog
                    scopes:
                      write:pets: modify pets in your account
                      read:pets: read your pets
                  authorizationCode:
                    authorizationUrl: https://example.com/api/oauth/dialog
                    tokenUrl: https://example.com/api/oauth/token
                    scopes:
                      write:pets: modify pets in your account
                      read:pets: read your pets
                 */
        return builder.addHeader("Authorization", "Bearer " + token.get());
    }

    private static String getAuthHeader(String scheme, String username, String password)
    {
        return capitalize(scheme) + " " + base64Url().encode("%s:%s".formatted(username, password).getBytes(UTF_8));
    }

    private static String capitalize(String input)
    {
        return input.substring(0, 1).toUpperCase(Locale.ENGLISH) + input.substring(1).toLowerCase(Locale.ENGLISH);
    }

    private String getToken()
    {
        return httpClient.execute(
                        preparePost()
                                .setUri(uriBuilderFrom(baseUri)
                                        .replacePath(tokenEndpoint)
                                        .build())
                                .setHeader("Content-Type", "application/x-www-form-urlencoded")
                                .setHeader("Authorization", "Basic " + base64Url().encode("%s:%s".formatted(clientId, clientSecret).getBytes(UTF_8)))
                                .setBodyGenerator(bodyGenerator)
                                .build(),
                        createJsonResponseHandler(jsonCodec(Authentication.TokenResponse.class)))
                .accessToken();
    }

    private static String getBody(String grantType, String username, String password)
    {
        ImmutableMap.Builder<String, String> params = ImmutableMap.<String, String>builder()
                .put("grant_type", grantType);
        if (username != null && !username.isEmpty()) {
            params.put("username", username);
        }
        if (password != null && !password.isEmpty()) {
            params.put("password", password);
        }

        return params.build().entrySet().stream()
                .map(entry -> encodePair(entry.getKey(), entry.getValue()))
                .collect(joining("&"));
    }

    private static String encodePair(String key, String value)
    {
        return format("%s=%s", key, URLEncoder.encode(value, UTF_8));
    }

    public record TokenResponse(
            @JsonProperty("token_type") String tokenType,
            @JsonProperty("access_token") String accessToken) {}
}
