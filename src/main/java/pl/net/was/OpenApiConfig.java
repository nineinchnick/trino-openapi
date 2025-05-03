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
import io.airlift.configuration.InvalidConfigurationException;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotNull;
import pl.net.was.authentication.AuthenticationScheme;
import pl.net.was.authentication.AuthenticationType;

import java.net.URI;
import java.util.Map;

import static java.util.Objects.requireNonNull;

public class OpenApiConfig
{
    private String specLocation;
    private URI baseUri;
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

    private double maxRequestsPerSecond = Double.MAX_VALUE;
    private double maxSplitsPerSecond = Double.MAX_VALUE;

    // Pushed domains are transformed into SQL IN lists
    // (or sequence of range predicates).
    // Too large IN lists cause too many requests being made, so a hard limit is required.
    private int domainExpansionLimit = 256;

    @NotNull
    public String getSpecLocation()
    {
        return specLocation;
    }

    @Config("spec-location")
    @ConfigDescription("Path to the OpenAPI spec file")
    public OpenApiConfig setSpecLocation(String value)
    {
        this.specLocation = value;
        return this;
    }

    @NotNull
    public URI getBaseUri()
    {
        return baseUri;
    }

    @Config("base-uri")
    @ConfigDescription("Base URI of the API")
    public OpenApiConfig setBaseUri(URI baseUri)
    {
        this.baseUri = baseUri;
        return this;
    }

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
    public OpenApiConfig setApiKeys(String apiKeys)
            throws InvalidConfigurationException
    {
        if (apiKeys.isEmpty()) {
            return this;
        }
        if (this.apiKeyName != null || this.apiKeyValue != null) {
            throw new InvalidConfigurationException("Cannot use authentication.api-keys if authentication.api-key-name or authentication.api-key-name is set");
        }
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
            throws InvalidConfigurationException
    {
        if (!this.apiKeys.isEmpty()) {
            throw new InvalidConfigurationException("Cannot use authentication.api-key-name if authentication.api-keys is set");
        }
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
            throws InvalidConfigurationException
    {
        if (!this.apiKeys.isEmpty()) {
            throw new InvalidConfigurationException("Cannot use authentication.api-key-value if authentication.api-keys is set");
        }
        this.apiKeyValue = apiKeyValue;
        return this;
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

    public double getMaxRequestsPerSecond()
    {
        return maxRequestsPerSecond;
    }

    @Config("max-requests-per-second")
    public OpenApiConfig setMaxRequestsPerSecond(double maxRequestsPerSecond)
    {
        this.maxRequestsPerSecond = maxRequestsPerSecond;
        return this;
    }

    public double getMaxSplitsPerSecond()
    {
        return maxSplitsPerSecond;
    }

    @Config("max-splits-per-second")
    public OpenApiConfig setMaxSplitsPerSecond(double maxSplitsPerSecond)
    {
        this.maxSplitsPerSecond = maxSplitsPerSecond;
        return this;
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
}
