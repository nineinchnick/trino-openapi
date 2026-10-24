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
import com.google.common.collect.ImmutableSet;
import io.airlift.http.client.HttpClient;
import io.airlift.http.client.HttpStatus;
import io.airlift.http.client.testing.TestingHttpClient;
import io.swagger.v3.oas.models.PathItem;
import io.trino.spi.Page;
import io.trino.spi.TrinoException;
import io.trino.spi.block.ArrayBlockBuilder;
import io.trino.spi.block.Block;
import io.trino.spi.block.MapBlockBuilder;
import io.trino.spi.catalog.CatalogName;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.MapType;
import org.junit.jupiter.api.Test;
import pl.net.was.authentication.Authentication;
import pl.net.was.authentication.CatalogCredentials;
import pl.net.was.authentication.ExtraCredentialProvider;

import java.net.URI;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.OptionalInt;

import static com.google.common.net.MediaType.JSON_UTF_8;
import static io.airlift.http.client.testing.TestingResponse.mockResponse;
import static io.airlift.slice.Slices.utf8Slice;
import static io.trino.spi.StandardErrorCode.JSON_INPUT_CONVERSION_ERROR;
import static io.trino.spi.connector.SchemaTableName.schemaTableName;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.testing.TestingConnectorSession.SESSION;
import static java.util.Collections.nCopies;
import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static pl.net.was.OpenApiSpec.SCHEMA_NAME;
import static pl.net.was.TestingOpenApiSpec.createSpec;

class TestOpenApiClient
{
    @Test
    void shouldSerializeMapPredicatesInRequestBodies()
    {
        OpenApiSpec spec = loadSpec("echo.yml");
        OpenApiColumn column = getColumn(spec, "echo_map", "labels");
        MapType mapType = (MapType) column.getType();
        MapBlockBuilder blockBuilder = mapType.createBlockBuilder(null, 1);
        blockBuilder.buildEntry((keyBuilder, valueBuilder) -> {
            VARCHAR.writeString(keyBuilder, "a");
            mapType.getValueType().writeLong(valueBuilder, 1);
        });
        Object labels = mapType.getObject(blockBuilder.build(), 0);

        assertThat(serializeMap(spec, "echo_map", Domain.singleValue(mapType, labels), column).toString())
                .isEqualTo("{\"labels\":{\"a\":1}}");
    }

    @Test
    void shouldSerializeUnionPredicatesInRequestBodiesAsRawJson()
    {
        OpenApiSpec spec = loadSpec("echo.yml");
        OpenApiColumn column = getColumn(spec, "echo_union_value", "value");

        assertThat(serializeMap(spec, "echo_union_value", Domain.singleValue(column.getType(), utf8Slice("{\"number\":42}")), column).toString())
                .isEqualTo("{\"value\":{\"number\":42}}");
    }

    @Test
    void shouldSerializeArraysInAllOfInRequestBodies()
    {
        OpenApiSpec spec = loadSpec("echo.yml");
        OpenApiColumn column = getColumn(spec, "echo_numbers", "tags");
        ArrayType arrayType = (ArrayType) column.getType();
        ArrayBlockBuilder blockBuilder = arrayType.createBlockBuilder(null, 1);
        blockBuilder.buildEntry(elementBuilder -> {
            VARCHAR.writeString(elementBuilder, "a");
            VARCHAR.writeString(elementBuilder, "b");
        });
        Block tags = blockBuilder.build();
        OpenApiClient client = createClient(spec);
        OpenApiTableHandle table = spec.getTableHandle(schemaTableName(SCHEMA_NAME, "echo_numbers"));

        // rows written by INSERT and UPDATE
        assertThat(client.serializePage(new OpenApiOutputTableHandle(table, List.of(column.getHandle()), Map.of()), new HttpPath(PathItem.HttpMethod.POST, "/echo_numbers"), new Page(tags), 0).toString())
                .isEqualTo("{\"tags\":[\"a\",\"b\"]}");
        // filters of operations reading rows using a request body
        assertThat(serializeMap(spec, "echo_numbers", Domain.singleValue(arrayType, arrayType.getObject(tags, 0)), column).toString())
                .isEqualTo("{\"tags\":[\"a\",\"b\"]}");
    }

    @Test
    void shouldRejectUnionPredicatesInRequestBodiesThatAreNotJson()
    {
        OpenApiSpec spec = loadSpec("echo.yml");
        OpenApiColumn column = getColumn(spec, "echo_union_value", "value");

        for (String value : List.of("plain text", "", "1 2")) {
            assertThatThrownBy(() -> serializeMap(spec, "echo_union_value", Domain.singleValue(column.getType(), utf8Slice(value)), column))
                    .isInstanceOfSatisfying(TrinoException.class, e -> assertThat(e.getErrorCode()).isEqualTo(JSON_INPUT_CONVERSION_ERROR.toErrorCode()))
                    .hasMessage("Value of column value is not valid JSON");
        }
    }

    @Test
    void shouldProbeWithLimitedPageSize()
    {
        List<URI> requests = new ArrayList<>();
        OpenApiClient client = createPagedClient(requests);
        OpenApiTableHandle table = getPagedTable();

        assertThat(client.getSplitsAfterFirstPage(SESSION, table, new ProbeLimit(OptionalInt.of(0)), false))
                .extracting(split -> getFilter(split, "page"))
                .containsExactly(2L, 3L);
        // the order of parameters is not defined
        assertThat(requests)
                .extracting(uri -> ImmutableSet.copyOf(Splitter.on('&').split(uri.getQuery())))
                .containsExactly(ImmutableSet.of("page=1", "size=0"));
    }

    @Test
    void shouldProbeWithRegularPageSize()
    {
        List<URI> requests = new ArrayList<>();
        OpenApiClient client = createPagedClient(requests);
        OpenApiTableHandle table = getPagedTable();

        assertThat(client.getSplitsAfterFirstPage(SESSION, table, ProbeLimit.NONE, false))
                .extracting(split -> getFilter(split, "page"))
                .containsExactly(2L, 3L);
        assertThat(requests)
                .extracting(URI::getQuery)
                .containsExactly("page=1");
    }

    private static OpenApiClient createPagedClient(List<URI> requests)
    {
        OpenApiConfig config = new OpenApiConfig().setBaseUri(URI.create("http://localhost"));
        // 25 results, with 10 on every page, so the probe must be followed by 2 more pages
        String firstPage = "{\"total\": 25, \"results\": [%s]}".formatted(String.join(", ", nCopies(10, "{\"name\": \"a\"}")));
        OpenApiSpec spec = loadSpec("paged.yml");
        return newClient(config, spec, new TestingHttpClient(request -> {
            requests.add(request.getUri());
            return mockResponse(HttpStatus.OK, JSON_UTF_8, firstPage);
        }));
    }

    private static OpenApiClient createClient(OpenApiSpec spec)
    {
        OpenApiConfig config = new OpenApiConfig().setBaseUri(URI.create("http://localhost"));
        return newClient(config, spec, new TestingHttpClient(_ -> {
            throw new UnsupportedOperationException();
        }));
    }

    private static OpenApiClient newClient(OpenApiConfig config, OpenApiSpec spec, HttpClient httpClient)
    {
        OpenApiServers servers = new OpenApiServers(config, spec);
        return new OpenApiClient(config, new CatalogName("test"), httpClient, new Authentication(new CatalogCredentials(config), spec, servers, httpClient), new CatalogCredentials(config), new ExtraCredentialProvider(config), spec, servers);
    }

    private static OpenApiTableHandle getPagedTable()
    {
        return loadSpec("paged.yml").getTableHandle(schemaTableName(SCHEMA_NAME, "items"));
    }

    private static Object getFilter(OpenApiSplit split, String columnName)
    {
        return split.getTableHandle().getConstraint().getDomains().orElseThrow().entrySet().stream()
                .filter(entry -> ((OpenApiColumnHandle) entry.getKey()).getName().equals(columnName))
                .map(entry -> entry.getValue().getSingleValue())
                .findFirst()
                .orElseThrow();
    }

    private static Object serializeMap(OpenApiSpec spec, String tableName, Domain domain, OpenApiColumn column)
    {
        OpenApiClient client = createClient(spec);
        OpenApiTableHandle table = spec.getTableHandle(schemaTableName(SCHEMA_NAME, tableName))
                .cloneWithConstraint(TupleDomain.withColumnDomains(Map.of((ColumnHandle) column.getHandle(), domain)));
        return client.serializeMap(table, new HttpPath(PathItem.HttpMethod.POST, "/" + tableName));
    }

    private static OpenApiColumn getColumn(OpenApiSpec spec, String tableName, String columnName)
    {
        return spec.getTables().get(tableName).stream()
                .filter(column -> column.getName().equals(columnName))
                .findFirst()
                .orElseThrow();
    }

    private static OpenApiSpec loadSpec(String name)
    {
        OpenApiConfig config = new OpenApiConfig();
        URL specResource = requireNonNull(TestOpenApiClient.class.getClassLoader().getResource(name));
        config.setSpecLocation(specResource.getFile());
        return createSpec(config);
    }
}
