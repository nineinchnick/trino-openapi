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

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.inject.Inject;
import io.swagger.v3.oas.models.servers.Server;

import java.net.URI;
import java.util.Collection;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Stream;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static io.trino.spi.connector.SchemaTableName.schemaTableName;
import static java.util.Objects.requireNonNull;
import static pl.net.was.OpenApiSpec.SCHEMA_NAME;

public class OpenApiServers
{
    private final Optional<URI> baseUri;
    private final Map<HttpPath, URI> serverUris;

    @Inject
    public OpenApiServers(OpenApiConfig config, OpenApiSpec spec)
    {
        requireNonNull(config, "config is null");
        requireNonNull(spec, "spec is null");
        this.baseUri = Optional.ofNullable(config.getBaseUri());
        this.serverUris = baseUri.isPresent() ? ImmutableMap.of() : getServerUris(spec);
    }

    public URI getUri(HttpPath httpPath)
    {
        if (baseUri.isPresent()) {
            return baseUri.get();
        }
        URI uri = serverUris.get(httpPath);
        checkState(uri != null, "The spec doesn't declare servers for %s, so set base-uri", httpPath);
        return uri;
    }

    public Set<URI> getUris()
    {
        return baseUri.<Set<URI>>map(ImmutableSet::of).orElseGet(() -> ImmutableSet.copyOf(serverUris.values()));
    }

    private static Map<HttpPath, URI> getServerUris(OpenApiSpec spec)
    {
        ImmutableMap.Builder<HttpPath, URI> serverUris = ImmutableMap.builder();
        for (HttpPath httpPath : getHttpPaths(spec)) {
            spec.getServer(httpPath).ifPresent(server -> serverUris.put(httpPath, getServerUri(httpPath, server)));
        }
        return serverUris.buildOrThrow();
    }

    private static Set<HttpPath> getHttpPaths(OpenApiSpec spec)
    {
        return spec.getTables().keySet().stream()
                .map(tableName -> spec.getTableHandle(schemaTableName(SCHEMA_NAME, tableName)))
                .flatMap(table -> Stream.of(table.getSelectPaths(), table.getInsertPaths(), table.getUpdatePaths(), table.getDeletePaths()))
                .flatMap(Collection::stream)
                .collect(toImmutableSet());
    }

    private static URI getServerUri(HttpPath httpPath, Server server)
    {
        URI uri = URI.create(server.getUrl());
        checkArgument(uri.isAbsolute(), "Server URL %s of %s is relative, so set base-uri", server.getUrl(), httpPath);
        return uri;
    }
}
