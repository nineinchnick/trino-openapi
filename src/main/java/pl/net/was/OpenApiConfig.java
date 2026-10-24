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

import com.google.common.base.Splitter;
import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;
import io.airlift.configuration.ConfigSecuritySensitive;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;
import io.airlift.units.MinDataSize;
import io.airlift.units.MinDuration;
import jakarta.validation.constraints.AssertTrue;
import jakarta.validation.constraints.DecimalMin;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotNull;
import pl.net.was.authentication.AuthenticationScheme;
import pl.net.was.authentication.AuthenticationType;
import pl.net.was.authentication.ClientAuthenticationMethod;
import pl.net.was.authentication.SpecLocationAuthentication;

import java.net.URI;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Stream;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.airlift.units.DataSize.Unit.GIGABYTE;
import static io.airlift.units.DataSize.Unit.MEGABYTE;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.SECONDS;

public class OpenApiConfig
{
    private String specLocation;
    private SpecLocationAuthentication specLocationAuthentication = SpecLocationAuthentication.NONE;
    private URI baseUri;
    private List<String> trimmedPathPrefixes = List.of();
    private AuthenticationType authenticationType = AuthenticationType.NONE;

    private AuthenticationScheme authenticationScheme = AuthenticationScheme.BASIC;

    private String username;
    private String password;
    private String bearerToken;

    private Map<String, String> apiKeys = Map.of();
    private String apiKeyName;
    private String apiKeyValue;

    private String clientId;
    private String clientSecret;
    // RFC 6749 requires servers to support client_secret_basic, but client_secret_post was the only method before it could be chosen
    private ClientAuthenticationMethod clientAuthenticationMethod = ClientAuthenticationMethod.CLIENT_SECRET_POST;
    private URI tokenUrl;
    private boolean allowInsecureHttp;

    private Optional<Double> maxRequestsPerSecond = Optional.empty();
    private Optional<Double> maxSplitsPerSecond = Optional.empty();

    private int maxRetries = 3;
    private Duration retryInitialDelay = new Duration(1, SECONDS);
    // long enough to wait for rate limits, which servers usually reset every minute
    private Duration retryMaxDelay = new Duration(1, MINUTES);

    private String httpCacheDirectory;
    private DataSize httpCacheMaxSize = DataSize.of(1, GIGABYTE);
    private DataSize httpCacheMaxEntrySize = DataSize.of(10, MEGABYTE);

    // Pushed domains are transformed into SQL IN lists
    // (or sequence of range predicates).
    // Too large IN lists cause too many requests being made, so a hard limit is required.
    private int domainExpansionLimit = 256;
    private SplitStrategy splitStrategy = SplitStrategy.OFF;
    private ProbeLimit probeLimit = ProbeLimit.NONE;

    @NotNull
    public String getSpecLocation()
    {
        return specLocation;
    }

    @Config("spec-location")
    @ConfigDescription("Path to the OpenAPI spec file")
    @ConfigSecuritySensitive
    public OpenApiConfig setSpecLocation(String value)
    {
        this.specLocation = value;
        return this;
    }

    @NotNull
    public SpecLocationAuthentication getSpecLocationAuthentication()
    {
        return specLocationAuthentication;
    }

    @Config("spec-location.authentication")
    @ConfigDescription("Credentials sent with the request fetching spec-location: none, or catalog to send the credentials of authentication.type")
    public OpenApiConfig setSpecLocationAuthentication(SpecLocationAuthentication specLocationAuthentication)
    {
        this.specLocationAuthentication = specLocationAuthentication;
        return this;
    }

    @AssertTrue(message = "spec-location.authentication can only be catalog when authentication.type is http or api_key, "
            + "because the credentials of security schemes declared in the specification are only known after loading it")
    public boolean isSpecLocationAuthenticationTypeSupported()
    {
        return specLocationAuthentication != SpecLocationAuthentication.CATALOG
                || authenticationType == AuthenticationType.HTTP
                || authenticationType == AuthenticationType.API_KEY;
    }

    @AssertTrue(message = "spec-location must be an HTTPS URL when spec-location.authentication is catalog, or an HTTP URL if authentication.allow-insecure-http is set")
    public boolean isSpecLocationTransportSecure()
    {
        if (specLocationAuthentication != SpecLocationAuthentication.CATALOG || specLocation == null) {
            return true;
        }
        String location = specLocation.toLowerCase(Locale.ENGLISH);
        return location.startsWith("https://") || (allowInsecureHttp && location.startsWith("http://"));
    }

    public URI getBaseUri()
    {
        return baseUri;
    }

    @Config("base-uri")
    @ConfigDescription("Base URI of the API, used instead of the servers declared in the spec")
    public OpenApiConfig setBaseUri(URI baseUri)
    {
        this.baseUri = baseUri;
        return this;
    }

    @NotNull
    public List<String> getTrimmedPathPrefixes()
    {
        return trimmedPathPrefixes;
    }

    @Config("trimmed-path-prefixes")
    @ConfigDescription("Comma-separated list of path prefixes, like /api/v1, left out of table names")
    public OpenApiConfig setTrimmedPathPrefixes(List<String> trimmedPathPrefixes)
    {
        this.trimmedPathPrefixes = requireNonNull(trimmedPathPrefixes, "trimmedPathPrefixes is null").stream()
                // a prefix matches whole path segments, so a trailing slash would never match
                .map(prefix -> prefix.replaceAll("/+$", ""))
                .collect(toImmutableList());
        return this;
    }

    @AssertTrue(message = "trimmed-path-prefixes must start with a slash, like /api/v1")
    public boolean isTrimmedPathPrefixesValid()
    {
        return trimmedPathPrefixes.stream().allMatch(prefix -> prefix.startsWith("/"));
    }

    @NotNull
    public AuthenticationType getAuthenticationType()
    {
        return authenticationType;
    }

    @Config("authentication.type")
    @ConfigDescription("Default authentication type if not set in the API specification")
    public OpenApiConfig setAuthenticationType(AuthenticationType authenticationType)
    {
        this.authenticationType = authenticationType;
        return this;
    }

    @AssertTrue(message = "authentication.type cannot be oauth, because OAuth needs the token URL and flows of an oauth2 security scheme declared in the specification, "
            + "so it's not supported as the default authentication; remove authentication.type if the specification declares its security requirements")
    public boolean isAuthenticationTypeSupported()
    {
        return authenticationType != AuthenticationType.OAUTH;
    }

    @NotNull
    public AuthenticationScheme getAuthenticationScheme()
    {
        return authenticationScheme;
    }

    @Config("authentication.scheme")
    @ConfigDescription("HTTP authentication scheme")
    public OpenApiConfig setAuthenticationScheme(AuthenticationScheme authenticationScheme)
    {
        this.authenticationScheme = authenticationScheme;
        return this;
    }

    public String getUsername()
    {
        return username;
    }

    @Config("authentication.username")
    @ConfigDescription("Username")
    public OpenApiConfig setUsername(String username)
    {
        this.username = username;
        return this;
    }

    public String getPassword()
    {
        return password;
    }

    @Config("authentication.password")
    @ConfigDescription("Password")
    @ConfigSecuritySensitive
    public OpenApiConfig setPassword(String password)
    {
        this.password = password;
        return this;
    }

    public String getBearerToken()
    {
        return bearerToken;
    }

    @Config("authentication.bearer-token")
    @ConfigDescription("Bearer token")
    @ConfigSecuritySensitive
    public OpenApiConfig setBearerToken(String bearerToken)
    {
        this.bearerToken = bearerToken;
        return this;
    }

    @NotNull
    public Map<String, String> getApiKeys()
    {
        return apiKeys;
    }

    @Config("authentication.api-keys")
    @ConfigDescription("Comma-separated list of API key names and values, like name1=value1,name2=value2")
    @ConfigSecuritySensitive
    public OpenApiConfig setApiKeys(String apiKeys)
    {
        this.apiKeys = Splitter
                .on(',')
                .trimResults()
                .omitEmptyStrings()
                .withKeyValueSeparator("=")
                .split(requireNonNull(apiKeys, "apiKeys is null"));
        return this;
    }

    public String getApiKeyName()
    {
        return apiKeyName;
    }

    @Config("authentication.api-key-name")
    @ConfigDescription("API key name")
    public OpenApiConfig setApiKeyName(String apiKeyName)
    {
        this.apiKeyName = apiKeyName;
        return this;
    }

    public String getApiKeyValue()
    {
        return apiKeyValue;
    }

    @Config("authentication.api-key-value")
    @ConfigDescription("API key value")
    @ConfigSecuritySensitive
    public OpenApiConfig setApiKeyValue(String apiKeyValue)
    {
        this.apiKeyValue = apiKeyValue;
        return this;
    }

    @AssertTrue(message = "authentication.api-keys cannot be combined with authentication.api-key-name or authentication.api-key-value")
    public boolean isApiKeysValid()
    {
        return apiKeys.isEmpty() || (apiKeyName == null && apiKeyValue == null);
    }

    public String getClientId()
    {
        return clientId;
    }

    @Config("authentication.client-id")
    @ConfigDescription("OAuth client ID")
    public OpenApiConfig setClientId(String clientId)
    {
        this.clientId = clientId;
        return this;
    }

    public String getClientSecret()
    {
        return clientSecret;
    }

    @Config("authentication.client-secret")
    @ConfigDescription("OAuth client secret")
    @ConfigSecuritySensitive
    public OpenApiConfig setClientSecret(String clientSecret)
    {
        this.clientSecret = clientSecret;
        return this;
    }

    @NotNull
    public ClientAuthenticationMethod getClientAuthenticationMethod()
    {
        return clientAuthenticationMethod;
    }

    @Config("authentication.client-auth-method")
    @ConfigDescription("How the OAuth client ID and secret are sent to the token endpoint: client_secret_basic or client_secret_post")
    public OpenApiConfig setClientAuthenticationMethod(ClientAuthenticationMethod clientAuthenticationMethod)
    {
        this.clientAuthenticationMethod = clientAuthenticationMethod;
        return this;
    }

    public URI getTokenUrl()
    {
        return tokenUrl;
    }

    @Config("authentication.token-url")
    @ConfigDescription("OAuth token endpoint, used instead of the tokenUrl of the spec")
    public OpenApiConfig setTokenUrl(URI tokenUrl)
    {
        this.tokenUrl = tokenUrl;
        return this;
    }

    public boolean isAllowInsecureHttp()
    {
        return allowInsecureHttp;
    }

    @Config("authentication.allow-insecure-http")
    @ConfigDescription("Allow sending credentials over HTTP, without TLS")
    public OpenApiConfig setAllowInsecureHttp(boolean allowInsecureHttp)
    {
        this.allowInsecureHttp = allowInsecureHttp;
        return this;
    }

    @AssertTrue(message = "base-uri and authentication.token-url must use HTTPS when credentials are configured, unless authentication.allow-insecure-http is set")
    public boolean isCredentialTransportSecure()
    {
        if (allowInsecureHttp || !hasCredentials()) {
            return true;
        }
        return !isInsecure(baseUri) && !isInsecure(tokenUrl);
    }

    public boolean hasCredentials()
    {
        return Stream.of(username, password, bearerToken, apiKeyValue, clientId, clientSecret).anyMatch(Objects::nonNull) || !apiKeys.isEmpty();
    }

    private static boolean isInsecure(URI uri)
    {
        return uri != null && "http".equalsIgnoreCase(uri.getScheme());
    }

    @NotNull
    public Optional<@DecimalMin(value = "0", inclusive = false, message = "must be greater than 0") Double> getMaxRequestsPerSecond()
    {
        return maxRequestsPerSecond;
    }

    @Config("max-requests-per-second")
    @ConfigDescription("Maximum number of HTTP requests per second from a single node; unlimited if not set")
    public OpenApiConfig setMaxRequestsPerSecond(Double maxRequestsPerSecond)
    {
        this.maxRequestsPerSecond = Optional.ofNullable(maxRequestsPerSecond);
        return this;
    }

    @NotNull
    public Optional<@DecimalMin(value = "0", inclusive = false, message = "must be greater than 0") Double> getMaxSplitsPerSecond()
    {
        return maxSplitsPerSecond;
    }

    @Config("max-splits-per-second")
    @ConfigDescription("Maximum number of splits per second generated for a query; unlimited if not set")
    public OpenApiConfig setMaxSplitsPerSecond(Double maxSplitsPerSecond)
    {
        this.maxSplitsPerSecond = Optional.ofNullable(maxSplitsPerSecond);
        return this;
    }

    @Min(0)
    public int getMaxRetries()
    {
        return maxRetries;
    }

    @Config("max-retries")
    @ConfigDescription("Maximum number of times a request that failed with a transient error is retried, 0 disables retries")
    public OpenApiConfig setMaxRetries(int maxRetries)
    {
        this.maxRetries = maxRetries;
        return this;
    }

    @NotNull
    @MinDuration("0ms")
    public Duration getRetryInitialDelay()
    {
        return retryInitialDelay;
    }

    @Config("retry-initial-delay")
    @ConfigDescription("Delay before the first retry, doubled for every next one")
    public OpenApiConfig setRetryInitialDelay(Duration retryInitialDelay)
    {
        this.retryInitialDelay = retryInitialDelay;
        return this;
    }

    @NotNull
    @MinDuration("0ms")
    public Duration getRetryMaxDelay()
    {
        return retryMaxDelay;
    }

    @Config("retry-max-delay")
    @ConfigDescription("Maximum delay between retries; requests are not retried if the server asks to wait longer with Retry-After")
    public OpenApiConfig setRetryMaxDelay(Duration retryMaxDelay)
    {
        this.retryMaxDelay = retryMaxDelay;
        return this;
    }

    @AssertTrue(message = "retry-max-delay must be greater than or equal to retry-initial-delay")
    public boolean isRetryMaxDelayValid()
    {
        return retryInitialDelay == null || retryMaxDelay == null || retryMaxDelay.compareTo(retryInitialDelay) >= 0;
    }

    public Optional<String> getHttpCacheDirectory()
    {
        // the Docker image sets every property, so an empty value means it's not set
        return Optional.ofNullable(httpCacheDirectory).filter(directory -> !directory.isBlank());
    }

    @Config("http-cache.directory")
    @ConfigDescription("Directory to cache HTTP responses in, in a subdirectory named after the catalog; responses are not cached if not set")
    public OpenApiConfig setHttpCacheDirectory(String httpCacheDirectory)
    {
        this.httpCacheDirectory = httpCacheDirectory;
        return this;
    }

    @NotNull
    @MinDataSize("1MB")
    public DataSize getHttpCacheMaxSize()
    {
        return httpCacheMaxSize;
    }

    @Config("http-cache.max-size")
    @ConfigDescription("Maximum size of cached responses of a catalog; the least recently used ones are removed above it")
    public OpenApiConfig setHttpCacheMaxSize(DataSize httpCacheMaxSize)
    {
        this.httpCacheMaxSize = httpCacheMaxSize;
        return this;
    }

    @NotNull
    public DataSize getHttpCacheMaxEntrySize()
    {
        return httpCacheMaxEntrySize;
    }

    @Config("http-cache.max-entry-size")
    @ConfigDescription("Maximum size of a single cached response; larger responses are not cached")
    public OpenApiConfig setHttpCacheMaxEntrySize(DataSize httpCacheMaxEntrySize)
    {
        this.httpCacheMaxEntrySize = httpCacheMaxEntrySize;
        return this;
    }

    @AssertTrue(message = "http-cache.max-entry-size must be less than or equal to http-cache.max-size")
    public boolean isHttpCacheMaxEntrySizeValid()
    {
        return httpCacheMaxSize == null || httpCacheMaxEntrySize == null || httpCacheMaxEntrySize.compareTo(httpCacheMaxSize) <= 0;
    }

    @Min(1)
    public int getDomainExpansionLimit()
    {
        return domainExpansionLimit;
    }

    @Config("domain-expansion-limit")
    @ConfigDescription("Maximum number of discrete values in a predicate domain.")
    public OpenApiConfig setDomainExpansionLimit(int domainExpansionLimit)
    {
        this.domainExpansionLimit = domainExpansionLimit;
        return this;
    }

    @NotNull
    public SplitStrategy getSplitStrategy()
    {
        return splitStrategy;
    }

    @Config("split-strategy")
    @ConfigDescription("How to read pages of results in separate splits: probe, stride, or off")
    public OpenApiConfig setSplitStrategy(SplitStrategy splitStrategy)
    {
        this.splitStrategy = splitStrategy;
        return this;
    }

    @NotNull
    public ProbeLimit getProbeLimit()
    {
        return probeLimit;
    }

    @Config("probe-limit")
    @ConfigDescription("Page size used when requesting the first page to learn the total number of results with the probe split strategy, or none to use the regular page size")
    public OpenApiConfig setProbeLimit(ProbeLimit probeLimit)
    {
        this.probeLimit = probeLimit;
        return this;
    }
}
