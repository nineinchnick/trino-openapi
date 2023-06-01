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

import com.google.common.collect.ImmutableMap;
import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;

import javax.validation.constraints.NotNull;

import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class OpenApiConfig
{
    private String specLocation;
    private URI baseUri;
    private Map<String, String> httpHeaders = ImmutableMap.of();

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

    @NotNull
    public Map<String, String> getHttpHeaders()
    {
        return httpHeaders;
    }

    @ConfigDescription("List of custom custom HTTP headers provided as: \"Header-Name-1: header value 1, Header-Value-2: header value 2, ...\" ")
    @Config("http-headers")
    public OpenApiConfig setHttpHeaders(List<String> httpHeaders)
    {
        try {
            this.httpHeaders = httpHeaders
                    .stream()
                    .collect(Collectors.toUnmodifiableMap(kvs -> kvs.split(":", 2)[0], kvs -> kvs.split(":", 2)[1]));
        }
        catch (IndexOutOfBoundsException e) {
            throw new IllegalArgumentException(String.format("Cannot parse http headers from property http-headers; value provided was %s, " +
                    "expected format is \"Header-Name-1: header value 1, Header-Value-2: header value 2, ...\"", String.join(", ", httpHeaders)), e);
        }
        return this;
    }
}
