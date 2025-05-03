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
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Inject;
import io.airlift.http.client.HttpClient;
import io.airlift.http.client.HttpRequestFilter;
import io.airlift.http.client.Request;
import io.swagger.v3.oas.models.PathItem;
import io.swagger.v3.oas.models.security.OAuthFlow;
import io.swagger.v3.oas.models.security.OAuthFlows;
import io.swagger.v3.oas.models.security.SecurityRequirement;
import io.swagger.v3.oas.models.security.SecurityScheme;
import pl.net.was.OpenApiConfig;
import pl.net.was.OpenApiSpec;

import java.net.URI;
import java.net.URISyntaxException;
import java.net.URLEncoder;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import static com.google.common.base.MoreObjects.firstNonNull;
import static com.google.common.io.BaseEncoding.base64Url;
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
import static pl.net.was.authentication.AuthenticationScheme.BEARER;

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
    private final String bearerToken;
    private final Map<String, String> apiKeys;
    private final String apiKeyName;
    private final String apiKeyValue;

    private final URI baseUri;
    private final HttpClient httpClient;
    private final String clientId;
    private final String clientSecret;
    private final LoadingCache<String, String> tokens = CacheBuilder.newBuilder()
            .build(CacheLoader.from(this::getToken));

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
        this.bearerToken = config.getBearerToken();
        this.apiKeys = config.getApiKeys();
        this.apiKeyName = config.getApiKeyName();
        this.apiKeyValue = config.getApiKeyValue();

        this.baseUri = requireNonNull(config.getBaseUri(), "baseUri is null");
        this.httpClient = requireNonNull(httpClient, "httpClient is null");
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
                case OAUTH -> throw new UnsupportedOperationException("OAuth cannot be used as a default authentication method");
            }
        }
        return builder.build();
    }

    private List<SecurityRequirement> getRequirements(String path, PathItem.HttpMethod method)
    {
        requireNonNull(path, "path is null");
        requireNonNull(method, "method is null");
        if (pathSecurityRequirements.containsKey(path) && pathSecurityRequirements.get(path).containsKey(method)) {
            return pathSecurityRequirements.get(path).get(method);
        }
        return securityRequirements;
    }

    private void applyAuthFilters(Request.Builder builder, List<SecurityRequirement> requirements, URI uri)
    {
        if (requirements == null) {
            return;
        }
        // only one of the requirements needs to be satisfied - test which methods are configured, pick first one, and only fail if there are none
        for (SecurityRequirement requirement : requirements) {
            try {
                requirement.forEach((name, options) -> {
                    SecurityScheme securitySchema = securitySchemas.get(name);
                    requireNonNull(securitySchema, "securitySchema is null");
                    switch (securitySchema.getType()) {
                        case APIKEY -> applyApiKeyAuth(builder, uri, securitySchema);
                        case HTTP -> applyHttpAuth(builder, securitySchema.getScheme());
                        case OAUTH2 -> {
                            OAuthFlows flows = requireNonNull(securitySchema.getFlows(), "flows are null");
                            OAuthFlow flow = firstNonNull(
                                    flows.getPassword(),
                                    firstNonNull(
                                            flows.getAuthorizationCode(),
                                            firstNonNull(
                                                    flows.getClientCredentials(),
                                                    flows.getImplicit())));
                            applyOAuth(builder, flow.getAuthorizationUrl());
                        }
                        default -> throw new IllegalArgumentException(format("Unsupported security schema %s", securitySchema.getType()));
                    }
                });
                return;
            }
            catch (NullPointerException ignored) {
                // ignore
            }
        }
    }

    private void applyApiKeyAuth(Request.Builder builder, URI uri, SecurityScheme scheme)
    {
        String name = requireNonNullElse(scheme.getName(), apiKeyName);
        requireNonNull(name, "Cannot use API Key authentication method, authentication.api-key-name configuration property is not set");
        String value;
        if (!apiKeys.isEmpty()) {
            value = apiKeys.get(name);
            requireNonNull(value, format("Missing API Key %s in authentication.api-keys configuration property", name));
        }
        else {
            value = requireNonNull(apiKeyValue, "Cannot use API Key authentication method, authentication.api-key-value configuration property is not set");
        }
        switch (scheme.getIn()) {
            case COOKIE -> builder.addHeader("Cookie", encodePair(name, value));
            case HEADER -> builder.addHeader(name, value);
            case QUERY -> {
                String query = encodePair(name, value);
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
        scheme = requireNonNullElse(scheme, defaultAuthenticationScheme);
        String value;
        if (scheme.toUpperCase(Locale.ENGLISH).equals(BEARER.toString())) {
            value = "Bearer " + bearerToken;
        }
        else {
            value = getAuthHeader(scheme, username, password);
        }
        return builder.addHeader("Authorization", value);
    }

    private Request.Builder applyOAuth(Request.Builder builder, String authorizationUrl)
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
        return builder.addHeader("Authorization", "Bearer " + tokens.getUnchecked(authorizationUrl));
    }

    private static String getAuthHeader(String scheme, String username, String password)
    {
        return capitalize(scheme) + " " + base64Url().encode("%s:%s".formatted(username, password).getBytes(UTF_8));
    }

    private static String capitalize(String input)
    {
        return input.substring(0, 1).toUpperCase(Locale.ENGLISH) + input.substring(1).toLowerCase(Locale.ENGLISH);
    }

    private String getToken(String authorizationUrl)
    {
        return httpClient.execute(
                        preparePost()
                                .setUri(URI.create(authorizationUrl))
                                .setHeader("Content-Type", "application/x-www-form-urlencoded")
                                .setBodyGenerator(createStaticBodyGenerator(
                                        getBody("client_credentials", clientId, clientSecret),
                                        UTF_8))
                                .build(),
                        createJsonResponseHandler(jsonCodec(Authentication.TokenResponse.class)))
                .accessToken();
    }

    private static String getBody(String grantType, String clientId, String clientSecret)
    {
        requireNonNull(grantType, "grantType is null");
        ImmutableMap.Builder<String, String> params = ImmutableMap.<String, String>builder()
                .put("grant_type", grantType);
        if (clientId != null && !clientId.isEmpty()) {
            params.put("client_id", clientId);
        }
        if (clientSecret != null && !clientSecret.isEmpty()) {
            params.put("client_secret", clientSecret);
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
