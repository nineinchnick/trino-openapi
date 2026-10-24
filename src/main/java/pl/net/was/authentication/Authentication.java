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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Multimaps;
import com.google.inject.Inject;
import io.airlift.http.client.BearerTokenProvider;
import io.airlift.http.client.ClientCredentialsTokenProvider;
import io.airlift.http.client.HeaderName;
import io.airlift.http.client.HttpClient;
import io.airlift.http.client.HttpRequestFilter;
import io.airlift.http.client.Request;
import io.swagger.v3.oas.models.PathItem;
import io.swagger.v3.oas.models.security.OAuthFlow;
import io.swagger.v3.oas.models.security.OAuthFlows;
import io.swagger.v3.oas.models.security.SecurityRequirement;
import io.swagger.v3.oas.models.security.SecurityScheme;
import io.trino.spi.TrinoException;
import pl.net.was.OpenApiConfig;
import pl.net.was.OpenApiServers;
import pl.net.was.OpenApiSpec;

import java.net.URI;
import java.net.URLEncoder;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static com.google.common.base.Strings.isNullOrEmpty;
import static com.google.common.base.Strings.nullToEmpty;
import static com.google.common.io.BaseEncoding.base64;
import static io.airlift.http.client.HeaderNames.AUTHORIZATION;
import static io.airlift.http.client.HeaderNames.CONTENT_TYPE;
import static io.airlift.http.client.HeaderNames.COOKIE;
import static io.airlift.http.client.Request.Builder.preparePost;
import static io.airlift.http.client.StaticBodyGenerator.createStaticBodyGenerator;
import static io.trino.spi.StandardErrorCode.INVALID_CATALOG_PROPERTY;
import static java.lang.String.format;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;
import static java.util.Objects.requireNonNullElse;
import static java.util.stream.Collectors.joining;
import static pl.net.was.OpenApiClient.OPENAPI_PATH_HEADER;
import static pl.net.was.authentication.AuthenticationScheme.BEARER;
import static pl.net.was.authentication.ClientAuthenticationMethod.CLIENT_SECRET_BASIC;

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

    private final HttpClient httpClient;
    private final String clientId;
    private final String clientSecret;
    private final Set<URI> serverUris;
    private final ClientAuthenticationMethod clientAuthenticationMethod;
    private final URI tokenUrl;
    private final ConcurrentMap<TokenRequest, BearerTokenProvider> tokenProviders = new ConcurrentHashMap<>();

    @Inject
    public Authentication(
            OpenApiConfig config,
            OpenApiSpec spec,
            OpenApiServers servers,
            @OpenApiAuthenticationClient HttpClient httpClient)
    {
        requireNonNull(config, "config is null");
        requireNonNull(spec, "spec is null");
        requireNonNull(servers, "servers is null");
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

        this.httpClient = new NonRedirectingHttpClient(httpClient);
        this.clientId = config.getClientId();
        this.clientSecret = config.getClientSecret();
        this.serverUris = servers.getUris();
        this.clientAuthenticationMethod = config.getClientAuthenticationMethod();
        this.tokenUrl = config.getTokenUrl();
    }

    @Override
    public Request filterRequest(Request request)
    {
        Request.Builder builder = withoutPathHeader(request);
        applyCredentials(builder, request, getCredentials(request));
        return builder.build();
    }

    private List<Credential> getCredentials(Request request)
    {
        PathItem.HttpMethod method = PathItem.HttpMethod.valueOf(request.getMethod());
        String path = request.getHeader(OPENAPI_PATH_HEADER);
        List<SecurityRequirement> requirements = getRequirements(path, method);
        String requestName = method + " " + path;
        // null means the spec doesn't declare any requirements, but an empty list explicitly allows anonymous access, so even the default credentials must not be sent
        if (requirements != null) {
            return selectCredentials(requestName, requirements);
        }
        return getDefaultCredentials(requestName);
    }

    private static Request.Builder withoutPathHeader(Request request)
    {
        // the path header only tells this filter which operation is called, and Request.Builder can't remove headers copied by fromRequest()
        Request.Builder builder = new Request.Builder()
                .setUri(request.getUri())
                .setMethod(request.getMethod())
                .addHeaders(Multimaps.filterKeys(request.getHeaders(), name -> !name.equals(OPENAPI_PATH_HEADER)))
                .setBodyGenerator(request.getBodyGenerator())
                .setSpanBuilder(request.getSpanBuilder().orElse(null))
                .setFollowRedirects(request.isFollowRedirects())
                .setVersion(request.getHttpVersion().orElse(null));
        request.getRequestTimeout().ifPresent(builder::setRequestTimeout);
        request.getIdleTimeout().ifPresent(builder::setIdleTimeout);
        request.getMaxResponseContentLength().ifPresent(builder::setMaxResponseContentLength);
        return builder;
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

    private List<Credential> selectCredentials(String requestName, List<SecurityRequirement> requirements)
    {
        if (requirements.isEmpty()) {
            return List.of();
        }
        // only one of the requirements needs to be satisfied, and nothing is added to the request until one is found,
        // so credentials of different requirements are never mixed
        List<String> problems = new ArrayList<>();
        for (SecurityRequirement requirement : requirements) {
            List<Credential> credentials = new ArrayList<>();
            List<String> requirementProblems = new ArrayList<>();
            for (String schemeName : requirement.keySet()) {
                switch (resolveScheme(schemeName, requirement.get(schemeName))) {
                    case Resolved(Credential credential) -> credentials.add(credential);
                    case Unresolved(String problem) -> requirementProblems.add(problem);
                }
            }
            if (requirementProblems.isEmpty()) {
                return credentials;
            }
            problems.add(String.join(", ", requirementProblems));
        }
        throw new TrinoException(INVALID_CATALOG_PROPERTY, format(
                "Cannot authenticate the %s request, because none of its security requirements can be satisfied: %s",
                requestName,
                String.join("; ", problems)));
    }

    private List<Credential> getDefaultCredentials(String requestName)
    {
        String subject = format("the %s authentication type", defaultAuthenticationType.name().toLowerCase(Locale.ENGLISH));
        return switch (defaultAuthenticationType) {
            case NONE -> List.of();
            case API_KEY -> {
                SecurityScheme scheme = new SecurityScheme();
                scheme.setIn(SecurityScheme.In.HEADER);
                yield requireResolved(requestName, resolveApiKey(subject, scheme));
            }
            case HTTP -> requireResolved(requestName, resolveHttp(subject, null));
            case OAUTH -> throw new UnsupportedOperationException("OAuth cannot be used as a default authentication method");
        };
    }

    private static List<Credential> requireResolved(String requestName, Resolution resolution)
    {
        return switch (resolution) {
            case Resolved(Credential credential) -> List.of(credential);
            case Unresolved(String problem) -> throw new TrinoException(INVALID_CATALOG_PROPERTY, format("Cannot authenticate the %s request, because %s", requestName, problem));
        };
    }

    private Resolution resolveScheme(String schemeName, List<String> scopes)
    {
        String subject = format("the %s security scheme", schemeName);
        SecurityScheme securitySchema = securitySchemas.get(schemeName);
        if (securitySchema == null) {
            return new Unresolved(format("%s is not declared in components.securitySchemes", subject));
        }
        return switch (securitySchema.getType()) {
            case APIKEY -> resolveApiKey(subject, securitySchema);
            case HTTP -> resolveHttp(subject, securitySchema.getScheme());
            case OAUTH2 -> resolveOAuth(subject, securitySchema.getFlows(), scopes);
            default -> new Unresolved(format("%s has the unsupported %s type", subject, securitySchema.getType()));
        };
    }

    private Resolution resolveApiKey(String subject, SecurityScheme scheme)
    {
        String name = requireNonNullElse(scheme.getName(), apiKeyName);
        if (name == null) {
            return missing(subject, List.of("authentication.api-key-name"));
        }
        if (scheme.getIn() == null) {
            return new Unresolved(format("%s doesn't declare where to send the API key", subject));
        }
        String value = apiKeys.isEmpty() ? apiKeyValue : apiKeys.get(name);
        if (value == null) {
            return missing(subject, List.of(apiKeys.isEmpty() ? "authentication.api-key-value" : format("%s key in the authentication.api-keys", name)));
        }
        return new Resolved(switch (scheme.getIn()) {
            case COOKIE -> new CookieCredential(encodePair(name, value));
            case HEADER -> new HeaderCredential(HeaderName.of(name), value);
            case QUERY -> new QueryCredential(encodePair(name, value));
        });
    }

    private Resolution resolveHttp(String subject, String scheme)
    {
        scheme = requireNonNullElse(scheme, defaultAuthenticationScheme);
        if (scheme.toUpperCase(Locale.ENGLISH).equals(BEARER.toString())) {
            if (bearerToken == null) {
                return missing(subject, List.of("authentication.bearer-token"));
            }
            return new Resolved(new HeaderCredential(AUTHORIZATION, "Bearer " + bearerToken));
        }
        List<String> missingProperties = new ArrayList<>();
        if (username == null) {
            missingProperties.add("authentication.username");
        }
        if (password == null) {
            missingProperties.add("authentication.password");
        }
        if (!missingProperties.isEmpty()) {
            return missing(subject, missingProperties);
        }
        return new Resolved(new HeaderCredential(AUTHORIZATION, getAuthHeader(scheme, username, password)));
    }

    private Resolution resolveOAuth(String subject, OAuthFlows flows, List<String> scopes)
    {
        // the authorizationCode and implicit flows require a user to log in using a browser, so only flows that don't need user interaction can be used
        OAuthFlow passwordFlow = flows == null ? null : flows.getPassword();
        OAuthFlow clientCredentialsFlow = flows == null ? null : flows.getClientCredentials();
        if (passwordFlow != null && username != null && password != null) {
            return resolveTokenRequest(subject, passwordFlow, new TokenGrant(GrantType.PASSWORD, scopes));
        }
        if (clientCredentialsFlow != null) {
            return resolveTokenRequest(subject, clientCredentialsFlow, new TokenGrant(GrantType.CLIENT_CREDENTIALS, scopes));
        }
        if (passwordFlow != null) {
            return missing("the password flow of " + subject, List.of("authentication.username", "authentication.password"));
        }
        return new Unresolved(format("%s must declare a password or clientCredentials flow, because other flows require a user to log in using a browser", subject));
    }

    private Resolution resolveTokenRequest(String subject, OAuthFlow flow, TokenGrant grant)
    {
        if (tokenUrl != null) {
            return new Resolved(new OAuthCredential(new TokenRequest(tokenUrl.toString(), grant)));
        }
        // specs written for older versions of the connector could only set authorizationUrl, so keep using it when tokenUrl is missing
        String specTokenUrl = flow.getTokenUrl() != null ? flow.getTokenUrl() : flow.getAuthorizationUrl();
        if (specTokenUrl == null) {
            return new Unresolved(format("%s doesn't set tokenUrl", subject));
        }
        // the spec can come from another source than the API, so it can't decide where the client secret and the password are sent
        if (!isTrustedTokenUrl(URI.create(specTokenUrl))) {
            return new Unresolved(format("%s sets tokenUrl to %s, which doesn't use HTTPS and has a different host than the servers of the API, so set authentication.token-url to use it", subject, specTokenUrl));
        }
        return new Resolved(new OAuthCredential(new TokenRequest(specTokenUrl, grant)));
    }

    private boolean isTrustedTokenUrl(URI uri)
    {
        if ("https".equalsIgnoreCase(uri.getScheme())) {
            return true;
        }
        return serverUris.stream().anyMatch(serverUri -> Objects.equals(uri.getScheme(), serverUri.getScheme()) && Objects.equals(uri.getAuthority(), serverUri.getAuthority()));
    }

    private static Resolution missing(String subject, List<String> properties)
    {
        return new Unresolved(format("%s requires the %s configuration %s", subject, String.join(" and ", properties), properties.size() == 1 ? "property" : "properties"));
    }

    private void applyCredentials(Request.Builder builder, Request request, List<Credential> credentials)
    {
        // RFC 6265 allows only one Cookie header, so API keys are added to the one with cookie parameters
        List<String> cookies = new ArrayList<>(request.getHeaders().get(COOKIE));
        int requestCookieCount = cookies.size();
        List<String> queryParameters = new ArrayList<>();
        for (Credential credential : credentials) {
            switch (credential) {
                case HeaderCredential(HeaderName name, String value) -> builder.addHeader(name, value);
                case CookieCredential(String pair) -> cookies.add(pair);
                case QueryCredential(String pair) -> queryParameters.add(pair);
                case OAuthCredential(TokenRequest tokenRequest) -> applyOAuth(builder, tokenRequest);
            }
        }
        if (cookies.size() > requestCookieCount) {
            builder.setHeader(COOKIE, String.join("; ", cookies));
        }
        if (!queryParameters.isEmpty()) {
            builder.setUri(appendQuery(request.getUri(), String.join("&", queryParameters)));
        }
    }

    private static URI appendQuery(URI uri, String query)
    {
        // the URI is already percent-encoded, and the decoded parts would let encoded characters of filter values, like & or /, become separators
        StringBuilder result = new StringBuilder()
                .append(uri.getScheme())
                .append("://")
                .append(uri.getRawAuthority())
                .append(nullToEmpty(uri.getRawPath()))
                .append('?');
        if (uri.getRawQuery() != null) {
            result.append(uri.getRawQuery()).append('&');
        }
        result.append(query);
        if (uri.getRawFragment() != null) {
            result.append('#').append(uri.getRawFragment());
        }
        return URI.create(result.toString());
    }

    private Request.Builder applyOAuth(Request.Builder builder, TokenRequest tokenRequest)
    {
        String token = tokenProviders.computeIfAbsent(tokenRequest, this::createTokenProvider).getToken();
        return builder.addHeader(AUTHORIZATION, "Bearer " + token);
    }

    private static String getAuthHeader(String scheme, String username, String password)
    {
        return capitalize(scheme) + " " + base64().encode("%s:%s".formatted(username, password).getBytes(UTF_8));
    }

    private static String capitalize(String input)
    {
        return input.substring(0, 1).toUpperCase(Locale.ENGLISH) + input.substring(1).toLowerCase(Locale.ENGLISH);
    }

    private BearerTokenProvider createTokenProvider(TokenRequest tokenRequest)
    {
        // Airlift's provider only supports sending the client credentials using HTTP Basic authentication
        if (tokenRequest.grant().type() == GrantType.CLIENT_CREDENTIALS && isClientSecretBasic()) {
            return ClientCredentialsTokenProvider.builder(httpClient, URI.create(tokenRequest.tokenUrl()), clientId, clientSecret)
                    .scopes(tokenRequest.grant().scopes())
                    .build();
        }
        Request.Builder request = preparePost()
                .setUri(URI.create(tokenRequest.tokenUrl()))
                .setHeader(CONTENT_TYPE, "application/x-www-form-urlencoded")
                .setBodyGenerator(createStaticBodyGenerator(getBody(tokenRequest.grant()), UTF_8));
        if (isClientSecretBasic()) {
            request.setHeader(AUTHORIZATION, getClientSecretBasicHeader());
        }
        return new OAuthTokenProvider(httpClient, request.build());
    }

    // the client secret is the password of HTTP Basic authentication, so a client without one can only identify itself with client_id in the body
    private boolean isClientSecretBasic()
    {
        return clientAuthenticationMethod == CLIENT_SECRET_BASIC && !isNullOrEmpty(clientId) && !isNullOrEmpty(clientSecret);
    }

    private String getClientSecretBasicHeader()
    {
        // RFC 6749, section 2.3.1, requires encoding the client ID and secret as form values before joining them
        String credentials = URLEncoder.encode(clientId, UTF_8) + ":" + URLEncoder.encode(clientSecret, UTF_8);
        return "Basic " + base64().encode(credentials.getBytes(UTF_8));
    }

    private String getBody(TokenGrant grant)
    {
        requireNonNull(grant, "grant is null");
        ImmutableMap.Builder<String, String> params = ImmutableMap.<String, String>builder()
                .put("grant_type", grant.type().value());
        if (grant.type() == GrantType.PASSWORD) {
            params.put("username", username);
            params.put("password", password);
        }
        if (!grant.scopes().isEmpty()) {
            params.put("scope", String.join(" ", grant.scopes()));
        }
        if (!isClientSecretBasic() && !isNullOrEmpty(clientId)) {
            params.put("client_id", clientId);
        }
        if (!isClientSecretBasic() && !isNullOrEmpty(clientSecret)) {
            params.put("client_secret", clientSecret);
        }

        return params.buildOrThrow().entrySet().stream()
                .map(entry -> encodePair(entry.getKey(), entry.getValue()))
                .collect(joining("&"));
    }

    private static String encodePair(String key, String value)
    {
        return format("%s=%s", key, URLEncoder.encode(value, UTF_8));
    }

    private sealed interface Credential {}

    private record HeaderCredential(HeaderName name, String value)
            implements Credential {}

    private record CookieCredential(String pair)
            implements Credential {}

    private record QueryCredential(String pair)
            implements Credential {}

    private record OAuthCredential(TokenRequest tokenRequest)
            implements Credential {}

    private sealed interface Resolution {}

    private record Resolved(Credential credential)
            implements Resolution {}

    private record Unresolved(String problem)
            implements Resolution {}

    private record TokenRequest(String tokenUrl, TokenGrant grant)
    {
        TokenRequest
        {
            requireNonNull(tokenUrl, "tokenUrl is null");
            requireNonNull(grant, "grant is null");
        }
    }

    // operations can require different scopes, and a token is only reused for operations requiring the same ones
    private record TokenGrant(GrantType type, List<String> scopes)
    {
        TokenGrant
        {
            requireNonNull(type, "type is null");
            scopes = ImmutableList.copyOf(requireNonNullElse(scopes, List.of()));
        }
    }

    private enum GrantType
    {
        PASSWORD("password"),
        CLIENT_CREDENTIALS("client_credentials");

        private final String value;

        GrantType(String value)
        {
            this.value = value;
        }

        String value()
        {
            return value;
        }
    }
}
