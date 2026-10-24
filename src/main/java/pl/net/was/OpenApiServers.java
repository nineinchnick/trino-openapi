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

import com.google.common.collect.ImmutableSet;
import com.google.inject.Inject;

import java.net.URI;
import java.util.Set;

import static java.util.Objects.requireNonNull;

public class OpenApiServers
{
    private final URI baseUri;

    @Inject
    public OpenApiServers(OpenApiConfig config)
    {
        requireNonNull(config, "config is null");
        this.baseUri = config.getBaseUri();
    }

    public URI getUri(HttpPath httpPath)
    {
        return baseUri;
    }

    public Set<URI> getUris()
    {
        return ImmutableSet.of(baseUri);
    }
}
