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

import com.google.common.base.CaseFormat;
import com.google.inject.Inject;
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
import io.swagger.v3.oas.models.media.ObjectSchema;
import io.swagger.v3.oas.models.media.Schema;
import io.swagger.v3.oas.models.media.StringSchema;
import io.swagger.v3.oas.models.parameters.Parameter;
import io.swagger.v3.oas.models.security.SecurityRequirement;
import io.swagger.v3.oas.models.security.SecurityScheme;
import io.swagger.v3.parser.OpenAPIV3Parser;
import io.swagger.v3.parser.core.models.ParseOptions;
import io.swagger.v3.parser.core.models.SwaggerParseResult;
import io.trino.spi.connector.SchemaNotFoundException;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.connector.TableNotFoundException;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.MapType;
import io.trino.spi.type.RowType;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeOperators;
import pl.net.was.adapters.GalaxyAdapter;

import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.IntStream;
import java.util.stream.Stream;

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
import static java.util.function.Function.identity;
import static java.util.stream.Collectors.groupingBy;
import static java.util.stream.Collectors.toMap;

public class OpenApiSpec
{
    public static final String SCHEMA_NAME = "default";
    public static final String ROW_ID = "__trino_row_id";
    private final Map<String, List<OpenApiColumn>> tables;
    private final Map<String, Map<PathItem.HttpMethod, String>> paths;
    private static final Map<String, OpenApiSpecAdapter> adapters = Map.of(
            "Starburst Galaxy Public API", new GalaxyAdapter());
    private final Optional<OpenApiSpecAdapter> adapter;

    private final Map<String, Map<PathItem.HttpMethod, List<SecurityRequirement>>> pathSecurityRequirements;
    private final Map<String, SecurityScheme> securitySchemas;
    private final List<SecurityRequirement> securityRequirements;

    @Inject
    public OpenApiSpec(OpenApiConfig config)
    {
        OpenAPI openApi = requireNonNull(parse(config.getSpecLocation()), "openApi is null");

        Info info = openApi.getInfo();
        this.adapter = Optional.ofNullable(info != null ? adapters.get(info.getTitle()) : null);

        this.tables = openApi.getPaths().entrySet().stream()
                .filter(entry -> hasOpsWithJson(entry.getValue()))
                .collect(toMap(
                        entry -> getIdentifier(stripPathParams(entry.getKey())),
                        entry -> getColumns(entry.getValue()),
                        (a, b) -> Stream.concat(a.stream(), b.stream()).distinct().toList()))
                .entrySet().stream()
                .collect(toMap(Map.Entry::getKey, entry -> mergeColumns(entry.getValue())));
        this.paths = openApi.getPaths().entrySet().stream()
                .map(pathEntry -> new AbstractMap.SimpleEntry<>(
                        getIdentifier(stripPathParams(pathEntry.getKey())),
                        pathEntry.getValue().readOperationsMap().keySet().stream()
                                .filter(method -> filterPath(pathEntry.getKey(), method))
                                .collect(toMap(identity(), method -> pathEntry.getKey()))))
                .collect(toMap(
                        Map.Entry::getKey,
                        Map.Entry::getValue,
                        // merge both maps
                        (a, b) -> Stream.concat(a.entrySet().stream(), b.entrySet().stream())
                                .collect(toMap(Map.Entry::getKey, Map.Entry::getValue, (x, y) -> x.length() < y.length() ? x : y))));
        this.pathSecurityRequirements = openApi.getPaths().entrySet().stream()
                .map(pathEntry -> new AbstractMap.SimpleEntry<>(
                        pathEntry.getKey(),
                        pathEntry.getValue().readOperationsMap().entrySet().stream()
                                .filter(opEntry -> opEntry.getValue().getSecurity() != null)
                                .map(opEntry -> new AbstractMap.SimpleEntry<>(
                                        opEntry.getKey(),
                                        opEntry.getValue().getSecurity()))
                                .collect(toImmutableMap(AbstractMap.SimpleEntry::getKey, AbstractMap.SimpleEntry::getValue))))
                .collect(toImmutableMap(AbstractMap.SimpleEntry::getKey, AbstractMap.SimpleEntry::getValue));
        this.securitySchemas = openApi.getComponents().getSecuritySchemes();
        this.securityRequirements = openApi.getSecurity();
    }

    private List<OpenApiColumn> mergeColumns(List<OpenApiColumn> columns)
    {
        return columns.stream()
                // merge all columns with same name and data type
                .collect(groupingBy(OpenApiColumn::getPrimaryKey))
                .values().stream()
                .map(sameColumns -> OpenApiColumn.builderFrom(sameColumns.get(0))
                        .setRequiresPredicate(sameColumns.stream()
                                .map(OpenApiColumn::getRequiresPredicate)
                                .flatMap(map -> map.entrySet().stream())
                                .collect(toMap(Map.Entry::getKey, Map.Entry::getValue, (a, b) -> a)))
                        .build())
                .collect(groupingBy(OpenApiColumn::getName))
                .values().stream()
                // make sure column names are also unique, append incrementing suffixes for columns of different types
                .flatMap(sameColumns -> IntStream
                        .range(0, sameColumns.size())
                        .mapToObj(i -> OpenApiColumn.builderFrom(sameColumns.get(i))
                                .setName(sameColumns.get(i).getName() + (i > 0 ? "_" + i : ""))
                                .build()))
                .toList();
    }

    private String stripPathParams(String key)
    {
        return key.replaceAll("/\\{[^\\}]+\\}", "");
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

    private static boolean hasOpsWithJson(PathItem pathItem)
    {
        return pathItem.readOperations().stream().anyMatch(OpenApiSpec::hasJson);
    }

    private static boolean hasJson(Operation op)
    {
        return op != null && (op.getDeprecated() == null || !op.getDeprecated()) &&
                op.getResponses().get("200") != null &&
                op.getResponses().get("200").getContent() != null &&
                op.getResponses().get("200").getContent().get("application/json") != null;
    }

    public Map<String, List<OpenApiColumn>> getTables()
    {
        return tables;
    }

    public OpenApiTableHandle getTableHandle(SchemaTableName name)
    {
        if (!name.getSchemaName().equals(SCHEMA_NAME)) {
            throw new SchemaNotFoundException(name.getSchemaName());
        }
        Map<PathItem.HttpMethod, String> paths = this.paths.get(name.getTableName());
        if (paths == null) {
            throw new TableNotFoundException(name);
        }
        return new OpenApiTableHandle(
                name,
                paths.get(PathItem.HttpMethod.GET),
                paths.get(PathItem.HttpMethod.POST),
                paths.containsKey(PathItem.HttpMethod.PUT) ? paths.get(PathItem.HttpMethod.PUT) : paths.get(PathItem.HttpMethod.POST),
                paths.get(PathItem.HttpMethod.DELETE),
                TupleDomain.none());
    }

    public Optional<OpenApiSpecAdapter> getAdapter()
    {
        return adapter;
    }

    private Map<String, Schema> getSchemaProperties(Schema<?> schema, String operationId)
    {
        Map<String, Schema> properties;
        if (schema instanceof ArraySchema) {
            properties = schema.getItems().getProperties();
        }
        else {
            properties = schema.getProperties();
        }
        if (properties == null) {
            return new LinkedHashMap<>();
        }

        if (adapter.isPresent()) {
            properties = adapter.get().runAdapter(operationId, properties);
        }
        return properties;
    }

    private List<OpenApiColumn> getColumns(PathItem pathItem)
    {
        Stream<OpenApiColumn> columns = pathItem.readOperationsMap().entrySet().stream().flatMap(entry -> {
            PathItem.HttpMethod method = entry.getKey();
            Operation op = entry.getValue();
            Map<String, Schema> properties = new LinkedHashMap<>();
            List<String> requiredProperties = new ArrayList<>();

            Map<String, Parameter> requiredParameters;
            if (op.getParameters() != null && filterPath(pathItem.toString(), method)) {
                // add required parameters as columns, so they can be set as predicates;
                // predicate values will be saved in the table handle and copied to result rows
                requiredParameters = op.getParameters().stream()
                        .filter(Parameter::getRequired)
                        .collect(toMap(Parameter::getName, identity()));
                requiredParameters.forEach((key, value) -> properties.put(key, value.getSchema()));
            }
            else {
                requiredParameters = Map.of();
            }
            if (op.getRequestBody() != null && op.getRequestBody().getContent().get("application/json") != null) {
                Schema<?> schema = op.getRequestBody()
                        .getContent()
                        .get("application/json")
                        .getSchema();
                properties.putAll(getSchemaProperties(schema, op.getOperationId()));
                if (schema.getRequired() != null) {
                    requiredProperties.addAll(schema.getRequired());
                }
            }
            if (op.getResponses().get("200") != null
                    && op.getResponses().get("200").getContent() != null
                    && op.getResponses().get("200").getContent().get("application/json") != null) {
                Schema<?> schema = op.getResponses()
                        .get("200").getContent()
                        .get("application/json")
                        .getSchema();
                properties.putAll(getSchemaProperties(schema, op.getOperationId()));
                if (schema.getRequired() != null) {
                    requiredProperties.addAll(schema.getRequired());
                }
            }

            return properties.entrySet().stream()
                    .filter(property -> convertType(property.getValue()).isPresent())
                    .map(property -> {
                        Schema<?> value = property.getValue();
                        return OpenApiColumn.builder()
                                .setName(getIdentifier(property.getKey()))
                                .setSourceName(property.getKey())
                                .setType(convertType(value).orElseThrow())
                                .setSourceType(value)
                                .setRequiresPredicate(requiredParameters.containsKey(property.getKey()) ? Map.of(method, requiredParameters.get(property.getKey()).getIn()) : Map.of())
                                .setIsNullable(Optional.ofNullable(value.getNullable()).orElse(!requiredProperties.contains(property.getKey())))
                                .setComment(value.getDescription())
                                .build();
                    });
        }).distinct();
        if (pathItem.getPost() != null || pathItem.getPut() != null || pathItem.getDelete() != null) {
            // the ROW_ID column is required for MERGE operation, including UPDATE and DELETE
            return Stream.concat(
                            Stream.of(OpenApiColumn.builder()
                                    .setName(ROW_ID)
                                    .setType(VARCHAR)
                                    .setSourceType(new StringSchema())
                                    .build()),
                            columns)
                    .toList();
        }
        return columns.toList();
    }

    private boolean filterPath(String path, PathItem.HttpMethod method)
    {
        // ignore POST operations on paths with parameters, because INSERT doesn't require predicates
        return (!method.equals(PathItem.HttpMethod.POST) || !path.contains("{"))
                // ignore PUT operations on paths without parameters, because UPDATE always require a predicate and the required parameter will be the primary key
                // TODO what if there's no PUT, only POST, on a parametrized endpoint?
                && (!method.equals(PathItem.HttpMethod.PUT) || path.contains("{"));
    }

    public static String getIdentifier(String string)
    {
        return CaseFormat.LOWER_CAMEL.to(CaseFormat.LOWER_UNDERSCORE, string.replaceAll("^/", "").replace('/', '_'));
    }

    private Optional<Type> convertType(Schema<?> property)
    {
        // ignore arrays with `oneOf`, `anyOf`, `allOf` and `multipleOf` response type
        if (property.getOneOf() != null
                || property.getAnyOf() != null
                || property.getAllOf() != null
                || property.getMultipleOf() != null
                // ignore `$ref` as well
                || property.get$ref() != null) {
            return Optional.empty();
        }
        if (property instanceof ArraySchema array) {
            return convertType(array.getItems()).map(ArrayType::new);
        }
        if (property instanceof MapSchema map && map.getAdditionalProperties() instanceof Schema<?> valueSchema) {
            return convertType(valueSchema).map(type -> new MapType(VARCHAR, type, new TypeOperators()));
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
        if (property instanceof ObjectSchema) {
            // composite type
            Map<String, Schema> properties = property.getProperties();
            if (properties == null) {
                return Optional.empty();
            }
            requireNonNull(properties, "properties of " + property + " is null");
            List<RowType.Field> fields = properties.entrySet()
                    .stream()
                    .map(prop -> Map.entry(prop.getKey(), convertType(prop.getValue())))
                    .filter(prop -> prop.getValue().isPresent())
                    .map(prop -> RowType.field(prop.getKey(), prop.getValue().get()))
                    .toList();
            if (fields.isEmpty()) {
                return Optional.empty();
            }
            return Optional.of(RowType.from(fields));
        }
        // TODO log unknown type
        return Optional.of(VARCHAR);
    }

    public Map<String, Map<PathItem.HttpMethod, List<SecurityRequirement>>> getPathSecurityRequirements()
    {
        return pathSecurityRequirements;
    }

    public Map<String, SecurityScheme> getSecuritySchemas()
    {
        return securitySchemas;
    }

    public List<SecurityRequirement> getSecurityRequirements()
    {
        return securityRequirements;
    }
}
