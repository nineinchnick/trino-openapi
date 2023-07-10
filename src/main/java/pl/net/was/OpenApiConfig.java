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

import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;
import pl.net.was.authentication.AuthenticationType;

import javax.validation.constraints.NotNull;

import java.net.URI;
import java.util.Optional;

public class OpenApiConfig
{
    private String specLocation;
    private URI baseUri;
    private Optional<AuthenticationType> authenticationType = Optional.empty();

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

    public Optional<AuthenticationType> getAuthenticationType()
    {
        return authenticationType;
    }

    @Config("authentication.type")
    @ConfigDescription("Authentication type")
    public OpenApiConfig setAuthenticationType(AuthenticationType authenticationType)
    {
        this.authenticationType = Optional.of(authenticationType);
        return this;
    }
}
