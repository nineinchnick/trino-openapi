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

import io.swagger.v3.oas.models.OpenAPI;
import io.swagger.v3.oas.models.Operation;
import io.swagger.v3.oas.models.PathItem;
import io.swagger.v3.oas.models.media.Schema;
import io.swagger.v3.parser.OpenAPIV3Parser;
import io.swagger.v3.parser.core.models.ParseOptions;
import io.swagger.v3.parser.core.models.SwaggerParseResult;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.MapType;
import io.trino.spi.type.RowType;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeOperators;

import javax.inject.Inject;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static java.util.Objects.requireNonNull;

public class OpenApiSpec
{
    private final OpenAPI openApi;

    private final Map<String, List<ColumnMetadata>> tables;

    @Inject
    public OpenApiSpec(OpenApiConfig config)
    {
        this.openApi = requireNonNull(parse(config.getSpecLocation()), "openApi is null");

        this.tables = openApi.getPaths().values().stream()
                .filter(pathItem -> {
                    Operation op = pathItem.getGet();
                    return op != null && (op.getDeprecated() == null || !op.getDeprecated()) && op.getResponses().get("200") != null && op.getResponses().get("200").getContent().get("application/json") != null;
                })
                .map(PathItem::getGet)
                .collect(Collectors.toMap(
                        op -> getIdentifier(op.getOperationId()),
                        this::getColumns));
    }

    public Map<String, List<ColumnMetadata>> getTables()
    {
        return tables;
    }

    private List<ColumnMetadata> getColumns(Operation op)
    {
        Map<String, Schema> properties = op.getResponses()
                .get("200").getContent()
                .get("application/json").getSchema()
                .getProperties();
        return properties.entrySet().stream()
                .filter(property -> convertType(property.getValue()).isPresent())
                .map(property -> new ColumnMetadata(getIdentifier(property.getKey()), convertType(property.getValue()).orElseThrow()))
                .collect(Collectors.toList());
    }

    public static String getIdentifier(String string)
    {
        return string.replaceAll("([a-z])([A-Z]+)", "$1_$2").toLowerCase();
    }

    private Optional<Type> convertType(Schema property)
    {
        // composite type
        if (property.getType() == null) {
            Map<String, Schema> subProperties = property.getProperties();
            requireNonNull(subProperties, "subProperties of " + property + " is null");
            List<RowType.Field> fields = subProperties.entrySet()
                    .stream()
                    .map(subprop -> RowType.field(
                            subprop.getKey(),
                            convertType(subprop.getValue()).orElseThrow()))
                    .collect(Collectors.toList());
            return Optional.of(RowType.from(fields));
        }
        if (property.getType().equals("object")) {
            return Optional.of(new MapType(VARCHAR, convertType((Schema) property.getAdditionalProperties()).orElseThrow(), new TypeOperators()));
        }
        if (property.getType().equals("array")) {
            return Optional.of(new ArrayType(convertType(property.getItems()).orElseThrow()));
        }
        if (property.getType().equals("integer")) {
            // TODO use format to detect precision
            return Optional.of(BIGINT);
        }
        if (property.getType().equals("string")) {
            // TODO use format to detect date/time/timestamp types, or check if property instanceof DateTimeSchema
            return Optional.of(VARCHAR);
        }
        if (property.getType().equals("boolean")) {
            return Optional.of(BOOLEAN);
        }
        // TODO log?
        return Optional.empty();
    }

    private static OpenAPI parse(String specLocation)
    {
        ParseOptions parseOptions = new ParseOptions();
        parseOptions.setResolveFully(true);
        SwaggerParseResult result = new OpenAPIV3Parser().readLocation(specLocation, null, parseOptions);
        OpenAPI openAPI = result.getOpenAPI();

        if (result.getMessages() != null && result.getMessages().size() != 0) {
            throw new IllegalArgumentException("Failed to parse the OpenAPI spec: " + String.join(", ", result.getMessages()));
        }

        return openAPI;
    }
}
