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

import com.google.common.cache.Cache;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Multimaps;
import com.google.inject.Inject;
import io.airlift.http.client.BearerTokenProvider;
import io.airlift.http.client.ClientCredentialsTokenProvider;
import io.airlift.http.client.HeaderName;
import io.airlift.http.client.HttpClient;
import io.airlift.http.client.Request;
import io.swagger.v3.oas.models.PathItem;
import io.swagger.v3.oas.models.security.OAuthFlow;
import io.swagger.v3.oas.models.security.OAuthFlows;
import io.swagger.v3.oas.models.security.SecurityRequirement;
import io.swagger.v3.oas.models.security.SecurityScheme;
import io.trino.cache.EvictableCacheBuilder;
import io.trino.spi.TrinoException;
import pl.net.was.OpenApiServers;
import pl.net.was.OpenApiSpec;
import pl.net.was.UnchallengedResponseHttpClient;
import pl.net.was.authentication.UserCredentials.UsernamePassword;

import java.net.URI;
import java.net.URLEncoder;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Stream;

import static com.google.common.base.Strings.nullToEmpty;
import static com.google.common.io.BaseEncoding.base64;
import static io.airlift.http.client.HeaderNames.AUTHORIZATION;
import static io.airlift.http.client.HeaderNames.CONTENT_TYPE;
import static io.airlift.http.client.HeaderNames.COOKIE;
import static io.airlift.http.client.Request.Builder.preparePost;
import static io.airlift.http.client.StaticBodyGenerator.createStaticBodyGenerator;
import static io.trino.cache.CacheUtils.uncheckedCacheGet;
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
{
    private static final String BEARER_PREFIX = "Bearer ";
    private static final String USERNAME_PROPERTY = "authentication.username";
    private static final String PASSWORD_PROPERTY = "authentication.password";
    // users with their own credentials get their own tokens, so the providers are dropped after some time, and created again when needed
    private static final int MAX_TOKEN_PROVIDERS = 1000;
    private static final Duration TOKEN_PROVIDER_EXPIRATION = Duration.ofHours(1);

    private final Map<String, Map<PathItem.HttpMethod, List<SecurityRequirement>>> pathSecurityRequirements;
    private final Map<String, SecurityScheme> securitySchemas;
    private final List<SecurityRequirement> securityRequirements;
    private final CatalogCredentials catalogCredentials;

    private final HttpClient httpClient;
    private final Optional<String> clientId;
    private final Optional<String> clientSecret;
    private final Set<URI> serverUris;
    private final ClientAuthenticationMethod clientAuthenticationMethod;
    private final Optional<URI> tokenUrl;
    private final Cache<TokenRequest, BearerTokenProvider> tokenProviders = EvictableCacheBuilder.newBuilder()
            .maximumSize(MAX_TOKEN_PROVIDERS)
            .expireAfterWrite(TOKEN_PROVIDER_EXPIRATION)
            .build();

    @Inject
    public Authentication(
            CatalogCredentials catalogCredentials,
            OpenApiSpec spec,
            OpenApiServers servers,
            @OpenApiAuthenticationClient HttpClient httpClient)
    {
        requireNonNull(spec, "spec is null");
        requireNonNull(servers, "servers is null");
        this.pathSecurityRequirements = spec.getPathSecurityRequirements();
        this.securityRequirements = spec.getSecurityRequirements();
        this.securitySchemas = spec.getSecuritySchemas();
        this.catalogCredentials = requireNonNull(catalogCredentials, "catalogCredentials is null");

        this.httpClient = new NonRedirectingHttpClient(new UnchallengedResponseHttpClient(httpClient));
        // an empty client ID or secret is not sent, like a missing one
        this.clientId = catalogCredentials.getClientId().filter(value -> !value.isEmpty());
        this.clientSecret = catalogCredentials.getClientSecret().filter(value -> !value.isEmpty());
        this.serverUris = servers.getUris();
        this.clientAuthenticationMethod = catalogCredentials.getClientAuthenticationMethod();
        this.tokenUrl = catalogCredentials.getTokenUrl();
    }

    /**
     * Adds the credentials required by the operation of the request, using the ones of the user instead of the ones of the catalog, if the user passed them.
     */
    public Request filterRequest(Request request, UserCredentials userCredentials)
    {
        Request.Builder builder = withoutPathHeader(request);
        applyCredentials(builder, request, getCredentials(request, userCredentials));
        return builder.build();
    }

    /**
     * Adds the credentials of authentication.type to a request that is not an operation of the spec, like the one fetching the spec itself.
     */
    public static Request addDefaultCredentials(CatalogCredentials catalogCredentials, Request request, String requestName)
    {
        Request.Builder builder = Request.Builder.fromRequest(request);
        for (Credential credential : getDefaultCredentials(catalogCredentials, requestName, UserCredentials.NONE)) {
            // the default API key is always sent in a header, and the default authentication type can't be OAuth
            if (!(credential instanceof HeaderCredential(HeaderName name, String value))) {
                throw new IllegalStateException("Unexpected default credential type: " + credential.getClass().getSimpleName());
            }
            builder.addHeader(name, value);
        }
        return builder.build();
    }

    /**
     * Gets new tokens for the OAuth credentials of the request, if the authenticated request was sent with any of them.
     * Returns true if the authenticated request should be sent again with the new tokens.
     */
    public boolean refreshRejectedTokens(Request request, Request authenticatedRequest, UserCredentials userCredentials)
    {
        List<String> sentTokens = authenticatedRequest.getHeaders().get(AUTHORIZATION).stream()
                .filter(value -> value.regionMatches(true, 0, BEARER_PREFIX, 0, BEARER_PREFIX.length()))
                .map(value -> value.substring(BEARER_PREFIX.length()))
                .toList();
        boolean refreshed = false;
        for (Credential credential : getCredentials(request, userCredentials)) {
            if (!(credential instanceof OAuthCredential(TokenRequest tokenRequest))) {
                continue;
            }
            BearerTokenProvider tokenProvider = tokenProviders.getIfPresent(tokenRequest);
            if (tokenProvider == null) {
                continue;
            }
            for (String token : sentTokens) {
                refreshed |= tokenProvider.refreshToken(token);
            }
        }
        return refreshed;
    }

    private List<Credential> getCredentials(Request request, UserCredentials userCredentials)
    {
        PathItem.HttpMethod method = PathItem.HttpMethod.valueOf(request.getMethod());
        String path = request.getHeader(OPENAPI_PATH_HEADER);
        List<SecurityRequirement> requirements = getRequirements(path, method);
        String requestName = method + " " + path;
        // null means the spec doesn't declare any requirements, but an empty list explicitly allows anonymous access, so even the default credentials must not be sent
        if (requirements != null) {
            return selectCredentials(requestName, requirements, userCredentials);
        }
        return getDefaultCredentials(catalogCredentials, requestName, userCredentials);
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

    private List<Credential> selectCredentials(String requestName, List<SecurityRequirement> requirements, UserCredentials userCredentials)
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
                switch (resolveScheme(schemeName, requirement.get(schemeName), userCredentials)) {
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

    // the default credentials don't depend on the spec, so they can be used before it's loaded
    private static List<Credential> getDefaultCredentials(CatalogCredentials catalogCredentials, String requestName, UserCredentials userCredentials)
    {
        AuthenticationType authenticationType = catalogCredentials.getAuthenticationType();
        String subject = format("the %s authentication type", authenticationType.name().toLowerCase(Locale.ENGLISH));
        return switch (authenticationType) {
            case NONE -> List.of();
            case API_KEY -> {
                SecurityScheme scheme = new SecurityScheme();
                scheme.setIn(SecurityScheme.In.HEADER);
                yield requireResolved(requestName, resolveApiKey(catalogCredentials, subject, scheme, userCredentials));
            }
            case HTTP -> requireResolved(requestName, resolveHttp(catalogCredentials, subject, null, userCredentials));
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

    private Resolution resolveScheme(String schemeName, List<String> scopes, UserCredentials userCredentials)
    {
        String subject = format("the %s security scheme", schemeName);
        SecurityScheme securitySchema = securitySchemas.get(schemeName);
        if (securitySchema == null) {
            return new Unresolved(format("%s is not declared in components.securitySchemes", subject));
        }
        return switch (securitySchema.getType()) {
            case APIKEY -> resolveApiKey(catalogCredentials, subject, securitySchema, userCredentials);
            case HTTP -> resolveHttp(catalogCredentials, subject, securitySchema.getScheme(), userCredentials);
            case OAUTH2 -> resolveOAuth(subject, securitySchema.getFlows(), scopes, userCredentials);
            default -> new Unresolved(format("%s has the unsupported %s type", subject, securitySchema.getType()));
        };
    }

    private static Resolution resolveApiKey(CatalogCredentials catalogCredentials, String subject, SecurityScheme scheme, UserCredentials userCredentials)
    {
        Optional<String> keyName = Optional.ofNullable(scheme.getName()).or(catalogCredentials::getApiKeyName);
        if (keyName.isEmpty()) {
            return missing(catalogCredentials, subject, List.of("authentication.api-key-name"));
        }
        String name = keyName.get();
        if (scheme.getIn() == null) {
            return new Unresolved(format("%s doesn't declare where to send the API key", subject));
        }
        Map<String, String> apiKeys = catalogCredentials.getApiKeys();
        Optional<String> value = apiKeys.isEmpty() ? userCredentials.getApiKeyValue().or(catalogCredentials::getApiKeyValue) : Optional.ofNullable(apiKeys.get(name));
        if (value.isEmpty()) {
            return missing(catalogCredentials, subject, List.of(apiKeys.isEmpty() ? "authentication.api-key-value" : format("%s key in the authentication.api-keys", name)));
        }
        return new Resolved(switch (scheme.getIn()) {
            case COOKIE -> new CookieCredential(encodePair(name, value.get()));
            case HEADER -> new HeaderCredential(HeaderName.of(name), value.get());
            case QUERY -> new QueryCredential(encodePair(name, value.get()));
        });
    }

    private static Resolution resolveHttp(CatalogCredentials catalogCredentials, String subject, String scheme, UserCredentials userCredentials)
    {
        scheme = requireNonNullElse(scheme, catalogCredentials.getAuthenticationScheme().toString());
        if (scheme.toUpperCase(Locale.ENGLISH).equals(BEARER.toString())) {
            Optional<String> token = userCredentials.getBearerToken().or(catalogCredentials::getBearerToken);
            if (token.isEmpty()) {
                return missing(catalogCredentials, subject, List.of("authentication.bearer-token"));
            }
            return new Resolved(new HeaderCredential(AUTHORIZATION, "Bearer " + token.get()));
        }
        Optional<UsernamePassword> usernamePassword = getUsernamePassword(catalogCredentials, userCredentials);
        if (usernamePassword.isPresent()) {
            return new Resolved(new HeaderCredential(AUTHORIZATION, getAuthHeader(scheme, usernamePassword.get().username(), usernamePassword.get().password())));
        }
        List<String> missingProperties = new ArrayList<>();
        if (catalogCredentials.getUsername().isEmpty()) {
            missingProperties.add(USERNAME_PROPERTY);
        }
        if (catalogCredentials.getPassword().isEmpty()) {
            missingProperties.add(PASSWORD_PROPERTY);
        }
        return missing(catalogCredentials, subject, missingProperties);
    }

    // the username and password of the user are passed together, so they're never mixed with the ones of the catalog
    private static Optional<UsernamePassword> getUsernamePassword(CatalogCredentials catalogCredentials, UserCredentials userCredentials)
    {
        if (userCredentials.getUsernamePassword().isPresent()) {
            return userCredentials.getUsernamePassword();
        }
        if (catalogCredentials.getUsername().isEmpty() || catalogCredentials.getPassword().isEmpty()) {
            return Optional.empty();
        }
        return Optional.of(new UsernamePassword(catalogCredentials.getUsername().get(), catalogCredentials.getPassword().get()));
    }

    private Resolution resolveOAuth(String subject, OAuthFlows flows, List<String> scopes, UserCredentials userCredentials)
    {
        // the authorizationCode and implicit flows require a user to log in using a browser, so only flows that don't need user interaction can be used
        OAuthFlow passwordFlow = flows == null ? null : flows.getPassword();
        OAuthFlow clientCredentialsFlow = flows == null ? null : flows.getClientCredentials();
        Optional<UsernamePassword> usernamePassword = getUsernamePassword(catalogCredentials, userCredentials);
        if (passwordFlow != null && usernamePassword.isPresent()) {
            return resolveTokenRequest(subject, passwordFlow, new TokenGrant(GrantType.PASSWORD, scopes, usernamePassword));
        }
        if (clientCredentialsFlow != null) {
            return resolveTokenRequest(subject, clientCredentialsFlow, new TokenGrant(GrantType.CLIENT_CREDENTIALS, scopes, Optional.empty()));
        }
        if (passwordFlow != null) {
            return missing(catalogCredentials, "the password flow of " + subject, List.of(USERNAME_PROPERTY, PASSWORD_PROPERTY));
        }
        return new Unresolved(format("%s must declare a password or clientCredentials flow, because other flows require a user to log in using a browser", subject));
    }

    private Resolution resolveTokenRequest(String subject, OAuthFlow flow, TokenGrant grant)
    {
        if (tokenUrl.isPresent()) {
            return new Resolved(new OAuthCredential(new TokenRequest(tokenUrl.get().toString(), grant)));
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

    private static Resolution missing(CatalogCredentials catalogCredentials, String subject, List<String> properties)
    {
        String requirement = format("the %s configuration %s", String.join(" and ", properties), properties.size() == 1 ? "property" : "properties");
        List<String> credentialNames = getCredentialNames(catalogCredentials, properties);
        if (credentialNames.isEmpty()) {
            return new Unresolved(format("%s requires %s", subject, requirement));
        }
        return new Unresolved(format("%s requires %s, or the %s extra %s", subject, requirement, String.join(" and ", credentialNames), credentialNames.size() == 1 ? "credential" : "credentials"));
    }

    private static List<String> getCredentialNames(CatalogCredentials catalogCredentials, List<String> properties)
    {
        // the username and password of a user are only used together, so both must be passed, even when the catalog sets one of them
        boolean needsUsernamePassword = properties.contains(USERNAME_PROPERTY) || properties.contains(PASSWORD_PROPERTY);
        return Stream.of(
                        needsUsernamePassword ? catalogCredentials.getUsernameCredentialName() : Optional.<String>empty(),
                        needsUsernamePassword ? catalogCredentials.getPasswordCredentialName() : Optional.<String>empty(),
                        properties.contains("authentication.bearer-token") ? catalogCredentials.getBearerTokenCredentialName() : Optional.<String>empty(),
                        properties.contains("authentication.api-key-value") ? catalogCredentials.getApiKeyValueCredentialName() : Optional.<String>empty())
                .flatMap(Optional::stream)
                .toList();
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
        String token = uncheckedCacheGet(tokenProviders, tokenRequest, () -> new ErrorReportingTokenProvider(createTokenProvider(tokenRequest), URI.create(tokenRequest.tokenUrl()))).getToken();
        return builder.addHeader(AUTHORIZATION, BEARER_PREFIX + token);
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
            return ClientCredentialsTokenProvider.builder(httpClient, URI.create(tokenRequest.tokenUrl()), clientId.orElseThrow(), clientSecret.orElseThrow())
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
        return clientAuthenticationMethod == CLIENT_SECRET_BASIC && clientId.isPresent() && clientSecret.isPresent();
    }

    private String getClientSecretBasicHeader()
    {
        // RFC 6749, section 2.3.1, requires encoding the client ID and secret as form values before joining them
        String credentials = URLEncoder.encode(clientId.orElseThrow(), UTF_8) + ":" + URLEncoder.encode(clientSecret.orElseThrow(), UTF_8);
        return "Basic " + base64().encode(credentials.getBytes(UTF_8));
    }

    private String getBody(TokenGrant grant)
    {
        requireNonNull(grant, "grant is null");
        ImmutableMap.Builder<String, String> params = ImmutableMap.<String, String>builder()
                .put("grant_type", grant.type().value());
        if (grant.type() == GrantType.PASSWORD) {
            UsernamePassword usernamePassword = grant.usernamePassword().orElseThrow();
            params.put("username", usernamePassword.username());
            params.put("password", usernamePassword.password());
        }
        if (!grant.scopes().isEmpty()) {
            params.put("scope", String.join(" ", grant.scopes()));
        }
        if (!isClientSecretBasic() && clientId.isPresent()) {
            params.put("client_id", clientId.get());
        }
        if (!isClientSecretBasic() && clientSecret.isPresent()) {
            params.put("client_secret", clientSecret.get());
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

    // operations can require different scopes, and a token is only reused for operations requiring the same ones,
    // and users with their own username and password get their own tokens
    private record TokenGrant(GrantType type, List<String> scopes, Optional<UsernamePassword> usernamePassword)
    {
        TokenGrant
        {
            requireNonNull(type, "type is null");
            scopes = ImmutableList.copyOf(requireNonNullElse(scopes, List.of()));
            requireNonNull(usernamePassword, "usernamePassword is null");
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
