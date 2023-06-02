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

import com.google.common.base.CaseFormat;
import io.starburst.adapters.GalaxyAdapter;
import io.swagger.v3.oas.models.OpenAPI;
import io.swagger.v3.oas.models.Operation;
import io.swagger.v3.oas.models.PathItem;
import io.swagger.v3.oas.models.info.Info;
import io.swagger.v3.oas.models.media.ArraySchema;
import io.swagger.v3.oas.models.media.BooleanSchema;
import io.swagger.v3.oas.models.media.DateSchema;
import io.swagger.v3.oas.models.media.DateTimeSchema;
import io.swagger.v3.oas.models.media.IntegerSchema;
import io.swagger.v3.oas.models.media.MapSchema;
import io.swagger.v3.oas.models.media.NumberSchema;
import io.swagger.v3.oas.models.media.Schema;
import io.swagger.v3.oas.models.media.StringSchema;
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

import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.DecimalType.createDecimalType;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.spi.type.TimestampType.TIMESTAMP_MILLIS;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static java.util.Objects.requireNonNull;

public class OpenApiSpec
{
    private final Map<String, List<ColumnMetadata>> tables;
    private final Map<String, Map<String, Schema<?>>> originalColumnTypes;
    private final Map<String, String> paths;
    private static final Map<String, OpenApiSpecAdapter> adapters = Map.of(
            "Starburst Galaxy Public API", new GalaxyAdapter());
    private final Optional<OpenApiSpecAdapter> adapter;

    @Inject
    public OpenApiSpec(OpenApiConfig config)
    {
        OpenAPI openApi = requireNonNull(parse(config.getSpecLocation()), "openApi is null");

        Info info = openApi.getInfo();
        this.adapter = Optional.ofNullable(info != null ? adapters.get(info.getTitle()) : null);

        this.tables = openApi.getPaths().values().stream()
                .filter(this::filterPaths)
                .map(PathItem::getGet)
                .collect(Collectors.toMap(
                        op -> getIdentifier(op.getOperationId()),
                        this::getColumns));
        this.originalColumnTypes = openApi.getPaths().values().stream()
                .filter(this::filterPaths)
                .map(PathItem::getGet)
                .collect(Collectors.toMap(
                        op -> getIdentifier(op.getOperationId()),
                        this::getOriginalColumnTypes));
        this.paths = openApi.getPaths().entrySet().stream()
                .filter(entry -> filterPaths(entry.getValue()))
                .collect(Collectors.toMap(entry -> getIdentifier(entry.getValue().getGet().getOperationId()), Map.Entry::getKey));
    }

    private static OpenAPI parse(String specLocation)
    {
        ParseOptions parseOptions = new ParseOptions();
        parseOptions.setResolveFully(true);
        SwaggerParseResult result = new OpenAPIV3Parser().readLocation(specLocation, null, parseOptions);
        OpenAPI openAPI = result.getOpenAPI();

        if (result.getMessages() != null && !result.getMessages().isEmpty()) {
            throw new IllegalArgumentException("Failed to parse the OpenAPI spec: " + String.join(", ", result.getMessages()));
        }

        return openAPI;
    }

    private boolean filterPaths(PathItem pathItem)
    {
        Operation op = pathItem.getGet();
        return op != null &&
                (op.getDeprecated() == null || !op.getDeprecated()) &&
                op.getResponses().get("200") != null &&
                op.getResponses().get("200").getContent().get("application/json") != null;
    }

    public Map<String, List<ColumnMetadata>> getTables()
    {
        return tables;
    }

    public Map<String, String> getPaths()
    {
        return paths;
    }

    public Map<String, Schema<?>> getOriginalColumnTypes(String tableName)
    {
        return originalColumnTypes.get(tableName);
    }

    public Optional<OpenApiSpecAdapter> getAdapter()
    {
        return adapter;
    }

    private Map<String, Schema> get200JsonSchema(Operation op)
    {
        Map<String, Schema> properties = op.getResponses()
                .get("200").getContent()
                .get("application/json").getSchema()
                .getProperties();

        return adapter.map(adapter -> adapter.runAdapter(op.getOperationId(), properties)).orElse(properties);
    }

    private List<ColumnMetadata> getColumns(Operation op)
    {
        return get200JsonSchema(op).entrySet().stream()
                .filter(property -> convertType(property.getValue()).isPresent())
                .map(property -> {
                    Schema<?> value = property.getValue();
                    return ColumnMetadata.builder()
                            .setName(getIdentifier(property.getKey()))
                            .setType(convertType(value).orElseThrow())
                            .setNullable(Optional.ofNullable(value.getNullable()).orElse(false))
                            .setComment(Optional.ofNullable(value.getDescription()))
                            .build();
                })
                .toList();
    }

    private Map<String, Schema<?>> getOriginalColumnTypes(Operation op)
    {
        return get200JsonSchema(op).entrySet().stream()
                .filter(property -> convertType(property.getValue()).isPresent())
                .collect(toImmutableMap(entry -> getIdentifier(entry.getKey()), Map.Entry::getValue));
    }

    public static String getIdentifier(String string)
    {
        return CaseFormat.LOWER_CAMEL.to(CaseFormat.LOWER_UNDERSCORE, string);
    }

    private Optional<Type> convertType(Schema<?> property)
    {
        if (property instanceof MapSchema map) {
            return Optional.of(new MapType(VARCHAR, convertType((Schema<?>) map.getAdditionalProperties()).orElseThrow(), new TypeOperators()));
        }
        if (property instanceof ArraySchema array) {
            return Optional.of(new ArrayType(convertType(array.getItems()).orElseThrow()));
        }
        if (property instanceof IntegerSchema) {
            return Optional.of(INTEGER);
        }
        if (property instanceof NumberSchema) {
            Optional<String> format = Optional.ofNullable(property.getFormat());
            if (format.filter("float"::equals).isPresent()) {
                return Optional.of(REAL);
            }
            if (format.filter("double"::equals).isPresent()) {
                return Optional.of(DOUBLE);
            }
            // arbitrary scale and precision but should fit most numbers
            return Optional.of(createDecimalType(18, 8));
        }
        if (property instanceof StringSchema) {
            return Optional.of(VARCHAR);
        }
        if (property instanceof DateSchema) {
            return Optional.of(DATE);
        }
        if (property instanceof DateTimeSchema) {
            // according to ISO-8601 can be any precision actually so might not fit
            return Optional.of(TIMESTAMP_MILLIS);
        }
        if (property instanceof BooleanSchema) {
            return Optional.of(BOOLEAN);
        }
        // composite type
        Map<String, Schema> properties = property.getProperties();
        requireNonNull(properties, "properties of " + property + " is null");
        List<RowType.Field> fields = properties.entrySet()
                .stream()
                .map(prop -> RowType.field(
                        prop.getKey(),
                        convertType(prop.getValue()).orElseThrow()))
                .toList();
        return Optional.of(RowType.from(fields));
    }
}
