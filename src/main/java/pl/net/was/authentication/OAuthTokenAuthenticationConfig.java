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

import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;
import io.airlift.configuration.ConfigSecuritySensitive;

import javax.validation.constraints.NotNull;

public class OAuthTokenAuthenticationConfig
{
    private String tokenEndpoint = "/oauth/v2/token";
    private String clientId;
    private String clientSecret;
    private String grantType;
    private String username;
    private String password;

    @NotNull
    public String getTokenEndpoint()
    {
        return tokenEndpoint;
    }

    @Config("authentication.token-endpoint")
    @ConfigDescription("OAuth 2 token endpoint")
    public OAuthTokenAuthenticationConfig setTokenEndpoint(String tokenEndpoint)
    {
        this.tokenEndpoint = tokenEndpoint;
        return this;
    }

    @NotNull
    public String getClientId()
    {
        return clientId;
    }

    @Config("authentication.client-id")
    @ConfigDescription("Client ID")
    public OAuthTokenAuthenticationConfig setClientId(String clientId)
    {
        this.clientId = clientId;
        return this;
    }

    @NotNull
    public String getClientSecret()
    {
        return clientSecret;
    }

    @Config("authentication.client-secret")
    @ConfigDescription("Client secret")
    @ConfigSecuritySensitive
    public OAuthTokenAuthenticationConfig setClientSecret(String clientSecret)
    {
        this.clientSecret = clientSecret;
        return this;
    }

    @NotNull
    public String getGrantType()
    {
        return grantType;
    }

    @Config("authentication.grant-type")
    @ConfigDescription("OAuth 2 grant type")
    public OAuthTokenAuthenticationConfig setGrantType(String grantType)
    {
        this.grantType = grantType;
        return this;
    }

    @NotNull
    public String getUsername()
    {
        return username;
    }

    @Config("authentication.username")
    @ConfigDescription("Username")
    public OAuthTokenAuthenticationConfig setUsername(String username)
    {
        this.username = username;
        return this;
    }

    @NotNull
    public String getPassword()
    {
        return password;
    }

    @Config("authentication.password")
    @ConfigDescription("Password")
    @ConfigSecuritySensitive
    public OAuthTokenAuthenticationConfig setPassword(String password)
    {
        this.password = password;
        return this;
    }
}
