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

import io.airlift.http.client.testing.TestingHttpClient;
import io.swagger.v3.oas.models.PathItem;
import io.trino.spi.block.MapBlockBuilder;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.type.MapType;
import org.junit.jupiter.api.Test;

import java.net.URL;
import java.util.Map;

import static io.airlift.slice.Slices.utf8Slice;
import static io.trino.spi.connector.SchemaTableName.schemaTableName;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;
import static pl.net.was.OpenApiSpec.SCHEMA_NAME;

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

    private static Object serializeMap(OpenApiSpec spec, String tableName, Domain domain, OpenApiColumn column)
    {
        OpenApiClient client = new OpenApiClient(new OpenApiConfig(), new CatalogName("test"), new TestingHttpClient(_ -> {
            throw new UnsupportedOperationException();
        }), spec);
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
        return new OpenApiSpec(config);
    }
}
