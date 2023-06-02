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
package io.starburst;

import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;
import io.airlift.configuration.ConfigSecuritySensitive;

import javax.validation.constraints.NotNull;

public class ClientCredentialsAuthenticationConfig
{
    private String clientId;
    private String clientSecret;

    @NotNull
    public String getClientId()
    {
        return clientId;
    }

    @Config("authentication.client-id")
    @ConfigDescription("Client ID")
    public ClientCredentialsAuthenticationConfig setClientId(String clientId)
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
    public ClientCredentialsAuthenticationConfig setClientSecret(String clientSecret)
    {
        this.clientSecret = clientSecret;
        return this;
    }
}
