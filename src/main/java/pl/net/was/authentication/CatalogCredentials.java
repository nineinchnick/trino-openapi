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

import com.google.common.collect.ImmutableMap;
import com.google.inject.Inject;
import pl.net.was.OpenApiConfig;

import java.net.URI;
import java.util.Map;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

/**
 * Credentials set by the catalog properties, and the names of the extra credentials users can pass instead.
 * They must never be logged, or included in handles or splits, so this class has no JSON representation, and doesn't print them.
 */
public final class CatalogCredentials
{
    private final AuthenticationType authenticationType;
    private final AuthenticationScheme authenticationScheme;
    private final Optional<String> username;
    private final Optional<String> password;
    private final Optional<String> bearerToken;
    private final Map<String, String> apiKeys;
    private final Optional<String> apiKeyName;
    private final Optional<String> apiKeyValue;
    private final Optional<String> usernameCredentialName;
    private final Optional<String> passwordCredentialName;
    private final Optional<String> bearerTokenCredentialName;
    private final Optional<String> apiKeyValueCredentialName;
    private final Optional<String> clientId;
    private final Optional<String> clientSecret;
    private final ClientAuthenticationMethod clientAuthenticationMethod;
    private final Optional<URI> tokenUrl;

    @Inject
    public CatalogCredentials(OpenApiConfig config)
    {
        requireNonNull(config, "config is null");
        this.authenticationType = requireNonNull(config.getAuthenticationType(), "authenticationType is null");
        this.authenticationScheme = requireNonNull(config.getAuthenticationScheme(), "authenticationScheme is null");
        this.username = Optional.ofNullable(config.getUsername());
        this.password = Optional.ofNullable(config.getPassword());
        this.bearerToken = Optional.ofNullable(config.getBearerToken());
        this.apiKeys = ImmutableMap.copyOf(config.getApiKeys());
        this.apiKeyName = Optional.ofNullable(config.getApiKeyName());
        this.apiKeyValue = Optional.ofNullable(config.getApiKeyValue());
        this.usernameCredentialName = Optional.ofNullable(config.getUsernameCredentialName());
        this.passwordCredentialName = Optional.ofNullable(config.getPasswordCredentialName());
        this.bearerTokenCredentialName = Optional.ofNullable(config.getBearerTokenCredentialName());
        this.apiKeyValueCredentialName = Optional.ofNullable(config.getApiKeyValueCredentialName());
        this.clientId = Optional.ofNullable(config.getClientId());
        this.clientSecret = Optional.ofNullable(config.getClientSecret());
        this.clientAuthenticationMethod = requireNonNull(config.getClientAuthenticationMethod(), "clientAuthenticationMethod is null");
        this.tokenUrl = Optional.ofNullable(config.getTokenUrl());
    }

    public AuthenticationType getAuthenticationType()
    {
        return authenticationType;
    }

    public AuthenticationScheme getAuthenticationScheme()
    {
        return authenticationScheme;
    }

    public Optional<String> getUsername()
    {
        return username;
    }

    public Optional<String> getPassword()
    {
        return password;
    }

    public Optional<String> getBearerToken()
    {
        return bearerToken;
    }

    public Map<String, String> getApiKeys()
    {
        return apiKeys;
    }

    public Optional<String> getApiKeyName()
    {
        return apiKeyName;
    }

    public Optional<String> getApiKeyValue()
    {
        return apiKeyValue;
    }

    public Optional<String> getUsernameCredentialName()
    {
        return usernameCredentialName;
    }

    public Optional<String> getPasswordCredentialName()
    {
        return passwordCredentialName;
    }

    public Optional<String> getBearerTokenCredentialName()
    {
        return bearerTokenCredentialName;
    }

    public Optional<String> getApiKeyValueCredentialName()
    {
        return apiKeyValueCredentialName;
    }

    public Optional<String> getClientId()
    {
        return clientId;
    }

    public Optional<String> getClientSecret()
    {
        return clientSecret;
    }

    public ClientAuthenticationMethod getClientAuthenticationMethod()
    {
        return clientAuthenticationMethod;
    }

    public Optional<URI> getTokenUrl()
    {
        return tokenUrl;
    }

    @Override
    public String toString()
    {
        return "CatalogCredentials{redacted}";
    }
}
