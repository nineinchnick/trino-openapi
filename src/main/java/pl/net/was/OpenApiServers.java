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
import io.swagger.v3.oas.models.servers.ServerVariable;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.Collection;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static io.trino.spi.connector.SchemaTableName.schemaTableName;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static pl.net.was.OpenApiSpec.SCHEMA_NAME;

public class OpenApiServers
{
    private static final Pattern VARIABLE_PATTERN = Pattern.compile("\\{([^{}]+)}");

    private final Optional<URI> baseUri;
    private final Map<HttpPath, URI> serverUris;

    @Inject
    public OpenApiServers(OpenApiConfig config, OpenApiSpec spec)
    {
        requireNonNull(config, "config is null");
        requireNonNull(spec, "spec is null");
        this.baseUri = Optional.ofNullable(config.getBaseUri());
        this.serverUris = baseUri.isPresent() ? ImmutableMap.of() : getServerUris(spec, getSpecUri(config.getSpecLocation()));
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

    private static Optional<URI> getSpecUri(String specLocation)
    {
        String location = specLocation.toLowerCase(Locale.ENGLISH);
        if (!location.startsWith("http://") && !location.startsWith("https://")) {
            return Optional.empty();
        }
        return Optional.of(URI.create(specLocation));
    }

    private static Map<HttpPath, URI> getServerUris(OpenApiSpec spec, Optional<URI> specUri)
    {
        ImmutableMap.Builder<HttpPath, URI> serverUris = ImmutableMap.builder();
        for (HttpPath httpPath : getHttpPaths(spec)) {
            spec.getServer(httpPath).ifPresent(server -> serverUris.put(httpPath, getServerUri(httpPath, server, specUri)));
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

    private static URI getServerUri(HttpPath httpPath, Server server, Optional<URI> specUri)
    {
        String url = fillVariables(httpPath, server);
        URI uri;
        try {
            uri = new URI(url);
        }
        catch (URISyntaxException e) {
            throw new IllegalArgumentException(format("Invalid server URL %s of %s", url, httpPath), e);
        }
        if (uri.isAbsolute()) {
            return uri;
        }
        // like the OpenAPI specification defines, relative URLs are relative to where the spec is served from
        checkArgument(specUri.isPresent(), "Server URL %s of %s is relative, but spec-location is not a URL, so set base-uri", url, httpPath);
        return specUri.get().resolve(uri);
    }

    private static String fillVariables(HttpPath httpPath, Server server)
    {
        Map<String, ServerVariable> variables = server.getVariables() == null ? ImmutableMap.of() : server.getVariables();
        Matcher matcher = VARIABLE_PATTERN.matcher(server.getUrl());
        return matcher.replaceAll(match -> {
            ServerVariable variable = variables.get(match.group(1));
            checkArgument(variable != null && variable.getDefault() != null, "Server URL %s of %s uses the variable %s, which doesn't have a default value, so set base-uri", server.getUrl(), httpPath, match.group(1));
            return Matcher.quoteReplacement(variable.getDefault());
        });
    }
}
