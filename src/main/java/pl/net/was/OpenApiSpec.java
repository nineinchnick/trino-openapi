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

import com.fasterxml.jackson.core.JsonPointer;
import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.base.CaseFormat;
import com.google.common.base.Enums;
import com.google.common.base.Splitter;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ListMultimap;
import com.google.common.collect.MultimapBuilder;
import com.google.inject.Inject;
import io.airlift.log.Logger;
import io.swagger.v3.oas.models.Components;
import io.swagger.v3.oas.models.OpenAPI;
import io.swagger.v3.oas.models.Operation;
import io.swagger.v3.oas.models.PathItem;
import io.swagger.v3.oas.models.media.ArraySchema;
import io.swagger.v3.oas.models.media.ComposedSchema;
import io.swagger.v3.oas.models.media.ObjectSchema;
import io.swagger.v3.oas.models.media.Schema;
import io.swagger.v3.oas.models.media.StringSchema;
import io.swagger.v3.oas.models.parameters.Parameter;
import io.swagger.v3.oas.models.security.SecurityRequirement;
import io.swagger.v3.oas.models.security.SecurityScheme;
import io.swagger.v3.parser.OpenAPIV3Parser;
import io.swagger.v3.parser.core.models.ParseOptions;
import io.swagger.v3.parser.core.models.SwaggerParseResult;
import io.swagger.v3.parser.util.DeserializationUtils;
import io.trino.spi.connector.SchemaNotFoundException;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.connector.TableNotFoundException;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.MapType;
import io.trino.spi.type.RowType;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeOperators;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static com.google.common.base.MoreObjects.firstNonNull;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static com.google.common.collect.Sets.toImmutableEnumSet;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.DecimalType.createDecimalType;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.spi.type.TimestampType.TIMESTAMP_MILLIS;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static java.lang.Boolean.FALSE;
import static java.lang.Boolean.TRUE;
import static java.util.Comparator.comparingInt;
import static java.util.Objects.requireNonNull;
import static java.util.Objects.requireNonNullElse;
import static java.util.stream.Collectors.groupingBy;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toMap;

public class OpenApiSpec
{
    private static final Logger log = Logger.get(OpenApiSpec.class);

    public static final String SCHEMA_NAME = "default";
    public static final String ROW_ID = "__trino_row_id";
    public static final String HTTP_OK = "200";
    public static final String MIME_JSON = "application/json";

    private static final TypeTuple FALLBACK_TYPE = new TypeTuple(VARCHAR, new StringSchema());

    private static final String SPEC_EXTENSION = "x-trino";
    private static final String LEGACY_SPEC_EXTENSION = "x-pagination";
    private static final String PAGINATION_RESULTS_PATH = "resultsPath";
    private static final String PAGINATION_TOTAL_RESULTS_PATH = "totalResultsPath";
    private static final String ERROR_PATH = "errorPath";
    private static final String PAGINATION_PAGE_PARAM = "pageParam";
    private static final String PAGINATION_LIMIT_PARAM = "limitParam";
    private static final String PAGINATION_TOTAL_PAGES_PATH = "totalPagesPath";
    private static final String PAGINATION_HAS_MORE_PATH = "hasMorePath";
    private static final String PAGINATION_MAX_PAGES = "maxPages";
    private static final String SQL_OPERATION = "sqlOperation";
    private static final Pattern JSON_POINTER_PATTERN = Pattern.compile("\\$response\\.body#(/.*)");

    // should only be used to manually resolving references
    private final OpenAPI openApi;
    private final Map<String, List<OpenApiColumn>> tables;
    private final Map<String, OpenApiTableHandle> handles;
    private final Map<String, Map<HttpPath, JsonPointer>> errorPointers;
    private final Map<String, Map<HttpPath, JsonPointer>> resultsPointers;
    private final Map<String, Map<HttpPath, Pagination>> paginations;

    private final Map<String, Map<PathItem.HttpMethod, List<SecurityRequirement>>> pathSecurityRequirements;
    private final Map<String, SecurityScheme> securitySchemas;
    private final List<SecurityRequirement> securityRequirements;

    @Inject
    public OpenApiSpec(OpenApiConfig config)
    {
        this(parse(requireNonNull(config, "config is null").getSpecLocation()));
    }

    OpenApiSpec(OpenAPI openApi)
    {
        this.openApi = requireNonNull(openApi, "openApi is null");

        /*
        Path params are assumed to be primary keys, so paths without any params are merged with same path with params.
        For example, /orgs and /orgs/{org} will be represented by a single table named orgs.
        If there are more paths with different params, they'll only be merged with the base path.
        For example, the following paths:
        * /orgs
        * /orgs/{org}
        * /orgs/{security_product}/{enablement}
        Will be merged into two tables:
        * orgs_org
        * orgs_security_product_enablement
        Where both tables will include columns created from the /orgs path response.
         */
        Map<String, PathItem> paths = requireNonNullElse(openApi.getPaths(), ImmutableMap.of());
        Map<String, List<Map.Entry<String, PathItem>>> pathGroups = paths.entrySet().stream()
                .filter(entry -> hasOpsWithJson(entry.getValue()))
                .filter(entry -> !getIdentifier(stripPathParams(entry.getKey())).isEmpty())
                // TODO group paths by the response type, otherwise it's not possible to create both unique and easy to use table names
                .collect(groupingBy(entry -> getIdentifier(stripPathParams(entry.getKey()))));
        ImmutableMap.Builder<String, List<OpenApiColumn>> tables = ImmutableMap.builder();
        ImmutableMap.Builder<String, OpenApiTableHandle> handles = ImmutableMap.builder();
        ImmutableMap.Builder<String, Map<HttpPath, JsonPointer>> errorPointers = ImmutableMap.builder();
        ImmutableMap.Builder<String, Map<HttpPath, JsonPointer>> resultsPointers = ImmutableMap.builder();
        ImmutableMap.Builder<String, Map<HttpPath, Pagination>> paginations = ImmutableMap.builder();
        for (Map.Entry<String, List<Map.Entry<String, PathItem>>> groupEntry : pathGroups.entrySet()) {
            List<PathItem> pathItems = groupEntry.getValue().stream()
                    .map(Map.Entry::getValue)
                    .toList();
            if (pathItems.size() == 1) {
                // create a new entry with the value unwrapped out of a list
                Map.Entry<String, PathItem> firstEntry = groupEntry.getValue().getFirst();
                tables.put(Map.entry(
                        groupEntry.getKey(),
                        mergeColumns(getColumns(pathItems.getFirst(), firstEntry.getKey()))));
                Map<SqlOperation, List<HttpPath>> tablePaths = operationPaths(firstEntry.getValue(), firstEntry.getKey());
                errorPointers.put(groupEntry.getKey(), errorPointers(firstEntry.getValue(), firstEntry.getKey()));
                resultsPointers.put(groupEntry.getKey(), resultsPointers(firstEntry.getValue(), firstEntry.getKey()));
                paginations.put(groupEntry.getKey(), paginations(firstEntry.getValue(), firstEntry.getKey()));
                handles.put(groupEntry.getKey(), tableHandle(groupEntry.getKey(), tablePaths));
                continue;
            }
            Map.Entry<String, PathItem> baseEntry = groupEntry.getValue().stream()
                    .min(comparingInt(entry -> entry.getKey().length()))
                    .orElseThrow();
            List<OpenApiColumn> baseColumns = getColumns(baseEntry.getValue(), baseEntry.getKey());
            Map<SqlOperation, List<HttpPath>> baseOperations = operationPaths(baseEntry.getValue(), baseEntry.getKey());
            Map<HttpPath, JsonPointer> baseErrorPointers = errorPointers(baseEntry.getValue(), baseEntry.getKey());
            Map<HttpPath, JsonPointer> baseResultsPointers = resultsPointers(baseEntry.getValue(), baseEntry.getKey());
            Map<HttpPath, Pagination> basePaginations = paginations(baseEntry.getValue(), baseEntry.getKey());
            // treat all combinations of path params as primary keys, which means every path with params is mapped to a separate table,
            // but combine it with columns from the base path
            groupEntry.getValue().stream()
                    .filter(entry -> !entry.equals(baseEntry))
                    .forEach(entry -> {
                        String tableName = getIdentifier(pathItems.size() == 2 ? groupEntry.getKey() : entry.getKey());
                        tables.put(
                                tableName,
                                mergeColumns(Stream.concat(
                                                baseColumns.stream(),
                                                getColumns(entry.getValue(), entry.getKey()).stream())
                                        .distinct()
                                        .toList()));
                        Map<SqlOperation, List<HttpPath>> tablePaths = Stream.concat(
                                        baseOperations.entrySet().stream(),
                                        operationPaths(entry.getValue(), entry.getKey()).entrySet().stream())
                                .collect(toImmutableMap(
                                        Map.Entry::getKey,
                                        Map.Entry::getValue,
                                        (x, y) -> Stream.concat(x.stream(), y.stream()).distinct().collect(toImmutableList())));
                        errorPointers.put(tableName, Stream.concat(
                                        baseErrorPointers.entrySet().stream(),
                                        errorPointers(entry.getValue(), entry.getKey()).entrySet().stream())
                                .collect(toImmutableMap(Map.Entry::getKey, Map.Entry::getValue)));
                        resultsPointers.put(tableName, Stream.concat(
                                        baseResultsPointers.entrySet().stream(),
                                        resultsPointers(entry.getValue(), entry.getKey()).entrySet().stream())
                                .collect(toImmutableMap(Map.Entry::getKey, Map.Entry::getValue)));
                        paginations.put(tableName, Stream.concat(
                                        basePaginations.entrySet().stream(),
                                        paginations(entry.getValue(), entry.getKey()).entrySet().stream())
                                .collect(toImmutableMap(Map.Entry::getKey, Map.Entry::getValue)));

                        handles.put(tableName, tableHandle(tableName, tablePaths));
                    });
        }
        this.handles = handles.buildOrThrow();
        this.tables = tables.buildOrThrow().entrySet().stream()
                .collect(toImmutableMap(Map.Entry::getKey, entry -> addRowIdColumn(
                        allowInsertsWithoutResponseColumns(entry.getValue(), this.handles.get(entry.getKey())),
                        this.handles.get(entry.getKey()))));
        this.errorPointers = errorPointers.buildOrThrow();
        this.resultsPointers = resultsPointers.buildOrThrow();
        this.paginations = paginations.buildOrThrow();

        this.handles.entrySet().stream()
                .sorted(Map.Entry.comparingByKey())
                .flatMap(entry -> Stream.concat(Stream.of(
                        "SELECT FROM " + entry.getKey() + " maps to: " + pathsToString(entry.getValue().getSelectPaths()),
                        "INSERT INTO " + entry.getKey() + " maps to: " + pathsToString(entry.getValue().getInsertPaths()),
                        "UPDATE " + entry.getKey() + " maps to: " + pathsToString(entry.getValue().getUpdatePaths()),
                        "DELETE FROM " + entry.getKey() + " maps to: " + pathsToString(entry.getValue().getDeletePaths())),
                        this.tables.get(entry.getKey()).stream().filter(column -> !column.getRequiresPredicate().isEmpty() || !column.getOptionalPredicate().isEmpty())
                                .map(column -> entry.getKey() + "." + column.getName() + " is " +
                                        (column.isPageNumber() ? "the page number, " : "") +
                                        "required for: " + column.getRequiresPredicate() + ", " +
                                        "optional for: " + column.getOptionalPredicate())))
                .forEach(log::info);

        this.pathSecurityRequirements = paths.entrySet().stream()
                .map(pathEntry -> Map.entry(
                        pathEntry.getKey(),
                        pathEntry.getValue().readOperationsMap().entrySet().stream()
                                .filter(opEntry -> opEntry.getValue().getSecurity() != null)
                                .map(opEntry -> Map.entry(
                                        opEntry.getKey(),
                                        opEntry.getValue().getSecurity()))
                                .collect(toImmutableMap(Map.Entry::getKey, Map.Entry::getValue))))
                .collect(toImmutableMap(Map.Entry::getKey, Map.Entry::getValue));
        this.securitySchemas = Optional.ofNullable(openApi.getComponents())
                .map(Components::getSecuritySchemes)
                .orElse(ImmutableMap.of());
        this.securityRequirements = openApi.getSecurity();
    }

    private static String pathsToString(List<HttpPath> paths)
    {
        return Optional.of(String.join(
                        ", ",
                        paths.stream().map(HttpPath::toString).toList()))
                .filter(value -> !value.isBlank())
                .orElse("<none>");
    }

    private String stripPathParams(String key)
    {
        return key.replaceAll("/\\{[^\\}]+\\}", "");
    }

    private static OpenAPI parse(String specLocation)
    {
        // specs of large APIs, like Datadog, exceed the default limit of 3 MB for YAML documents,
        // which protects against untrusted input, but the spec location is set by the catalog administrator.
        // The limit can only be changed globally, and the parser is loaded only by this plugin's class loader.
        DeserializationUtils.getOptions().setMaxYamlCodePoints(Integer.MAX_VALUE);
        ParseOptions parseOptions = new ParseOptions();
        parseOptions.setResolveFully(true);
        EmptySecurityDetectingParser parser = new EmptySecurityDetectingParser();
        SwaggerParseResult result = parser.readLocation(specLocation, null, parseOptions);
        OpenAPI openAPI = result.getOpenAPI();

        if (result.getMessages() != null && !result.getMessages().isEmpty()) {
            throw new IllegalArgumentException("Failed to parse the OpenAPI spec: " + String.join(", ", result.getMessages()));
        }

        // the parser drops an empty top-level security list, but unlike a missing one, it explicitly allows anonymous access to all operations
        if (openAPI.getSecurity() == null && parser.hasEmptyRootSecurity()) {
            openAPI.setSecurity(new ArrayList<>());
        }
        return openAPI;
    }

    private static class EmptySecurityDetectingParser
            extends OpenAPIV3Parser
    {
        private boolean rootParsed;
        private boolean emptyRootSecurity;

        @Override
        public SwaggerParseResult parseJsonNode(String path, JsonNode node, ParseOptions options)
        {
            // the root document is parsed first, before any external references are resolved
            if (!rootParsed) {
                rootParsed = true;
                JsonNode security = node.get("security");
                emptyRootSecurity = security != null && security.isArray() && security.isEmpty();
            }
            return super.parseJsonNode(path, node, options);
        }

        boolean hasEmptyRootSecurity()
        {
            return emptyRootSecurity;
        }
    }

    private static boolean hasOpsWithJson(PathItem pathItem)
    {
        return pathItem.readOperations().stream().anyMatch(OpenApiSpec::hasJsonResponse);
    }

    private static boolean hasJsonResponse(Operation op)
    {
        return op != null && (op.getDeprecated() == null || !op.getDeprecated()) &&
                op.getResponses().get(HTTP_OK) != null &&
                op.getResponses().get(HTTP_OK).getContent() != null &&
                op.getResponses().get(HTTP_OK).getContent().get(MIME_JSON) != null;
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
        OpenApiTableHandle handle = this.handles.get(name.getTableName());
        if (handle == null) {
            throw new TableNotFoundException(name);
        }
        return handle;
    }

    public Map<HttpPath, JsonPointer> getErrorPointers(SchemaTableName name)
    {
        if (!name.getSchemaName().equals(SCHEMA_NAME)) {
            throw new SchemaNotFoundException(name.getSchemaName());
        }
        Map<HttpPath, JsonPointer> result = this.errorPointers.get(name.getTableName());
        if (result == null) {
            throw new TableNotFoundException(name);
        }
        return result;
    }

    public Map<HttpPath, JsonPointer> getResultsPointers(SchemaTableName name)
    {
        if (!name.getSchemaName().equals(SCHEMA_NAME)) {
            throw new SchemaNotFoundException(name.getSchemaName());
        }
        Map<HttpPath, JsonPointer> result = this.resultsPointers.get(name.getTableName());
        if (result == null) {
            throw new TableNotFoundException(name);
        }
        return result;
    }

    public Map<HttpPath, Pagination> getPaginations(SchemaTableName name)
    {
        if (!name.getSchemaName().equals(SCHEMA_NAME)) {
            throw new SchemaNotFoundException(name.getSchemaName());
        }
        Map<HttpPath, Pagination> result = this.paginations.get(name.getTableName());
        if (result == null) {
            throw new TableNotFoundException(name);
        }
        return result;
    }

    public Optional<String> getOperationId(HttpPath httpPath)
    {
        return Optional.ofNullable(openApi.getPaths())
                .map(paths -> paths.get(httpPath.path()))
                .map(pathItem -> pathItem.readOperationsMap().get(httpPath.method()))
                .map(Operation::getOperationId)
                .filter(operationId -> !operationId.isBlank());
    }

    private List<OpenApiColumn> getColumns(PathItem pathItem, String path)
    {
        return pathItem.readOperationsMap().entrySet().stream()
                .flatMap(entry -> getColumn(path, pathItem.getParameters(), entry))
                .distinct()
                .toList();
    }

    private Stream<OpenApiColumn> getColumn(String path, List<Parameter> pathParameters, Map.Entry<PathItem.HttpMethod, Operation> entry)
    {
        PathItem.HttpMethod method = entry.getKey();
        Operation op = entry.getValue();
        List<Parameter> parameters = getParameters(pathParameters, op);
        List<OpenApiColumn> result = new ArrayList<>();

        Map<String, String> specExtension = op.getExtensions() == null ?
                ImmutableMap.of() :
                getMapOfStrings(firstNonNull(
                        op.getExtensions().get(SPEC_EXTENSION),
                        firstNonNull(op.getExtensions().get(LEGACY_SPEC_EXTENSION), ImmutableMap.of())));
        JsonPointer resultsPointer;
        try {
            resultsPointer = parseJsonPointer(specExtension.get(PAGINATION_RESULTS_PATH));
        }
        catch (IllegalArgumentException e) {
            throw new IllegalArgumentException("Invalid value of %s: %s. %s".formatted(PAGINATION_RESULTS_PATH, specExtension.get(PAGINATION_RESULTS_PATH), e.getMessage()));
        }

        Schema<?> schema = getResponseSchema(op);
        if (schema != null) {
            List<String> requiredProperties = getRequiredProperties(schema);
            getAlternativesProperties(schema)
                    .entrySet().stream()
                    .filter(propEntry -> !resultsPointer.matchesProperty(propEntry.getKey()))
                    .map(propEntry -> getResultColumn(
                            propEntry.getKey(),
                            propEntry.getValue(),
                            !requiredProperties.contains(propEntry.getKey()),
                            propEntry.getKey().equals(specExtension.get(PAGINATION_PAGE_PARAM))))
                    .filter(Optional::isPresent)
                    .forEach(column -> result.add(column.get()));
            Schema<?> resultsSchema = getResultsSchema(schema, resultsPointer);
            List<String> requiredResultsProperties = getRequiredProperties(resultsSchema);
            getAlternativesProperties(resultsSchema)
                    .entrySet().stream()
                    .map(propEntry -> getResultColumn(
                            propEntry.getKey(),
                            propEntry.getValue(),
                            !requiredResultsProperties.contains(propEntry.getKey()),
                            propEntry.getKey().equals(specExtension.get(PAGINATION_PAGE_PARAM))))
                    .filter(Optional::isPresent)
                    .forEach(column -> result.add(column.get()));
        }
        schema = getRequestSchema(op);
        if (schema != null) {
            ListMultimap<String, OpenApiColumn.PrimaryKey> keys = result.stream()
                    .collect(ArrayListMultimap::create,
                            (map, element) -> map.put(element.getName(), element.getPrimaryKey()),
                            ArrayListMultimap::putAll);
            List<String> requiredProperties = getRequiredProperties(schema);
            getSchemaProperties(schema)
                    .entrySet().stream()
                    .map(propEntry -> getPredicateColumn(
                            propEntry.getKey(),
                            propEntry.getValue(),
                            requiredProperties.contains(propEntry.getKey()) ? ImmutableMap.of(new HttpPath(method, path), ParameterLocation.BODY) : ImmutableMap.of(),
                            !requiredProperties.contains(propEntry.getKey()) ? ImmutableMap.of(new HttpPath(method, path), ParameterLocation.BODY) : ImmutableMap.of(),
                            ImmutableMap.of(),
                            !requiredProperties.contains(propEntry.getKey()),
                            false,
                            propEntry.getKey().equals(specExtension.get(PAGINATION_PAGE_PARAM))))
                    .filter(Optional::isPresent)
                    .map(Optional::get)
                    .map(column -> {
                        while (hasAmbiguousName(column, keys)) {
                            // if the request param is also a response field,
                            // append `_req` to its name to disambiguate it,
                            // otherwise it'll get a number suffix, like `_2`
                            column = OpenApiColumn.builderFrom(column)
                                    .setName(column.getName() + "_req")
                                    .build();
                        }
                        return column;
                    })
                    .forEach(result::add);
        }
        if (!parameters.isEmpty()) {
            ListMultimap<String, OpenApiColumn.PrimaryKey> keys = result.stream()
                    .collect(ArrayListMultimap::create,
                            (map, element) -> map.put(element.getName(), element.getPrimaryKey()),
                            ArrayListMultimap::putAll);
            // add required parameters as columns, so they can be set as predicates;
            // predicate values will be saved in the table handle and copied to result rows
            parameters.stream()
                    .filter(parameter -> hasSchema(parameter, method, path))
                    .map(parameter -> {
                        ParameterLocation parameterLocation = parameter.getIn() == null ? ParameterLocation.NONE : ParameterLocation.valueOf(parameter.getIn().toUpperCase(Locale.ENGLISH));
                        boolean isRequired = TRUE.equals(parameter.getRequired());
                        ParameterStyle parameterStyle = ParameterStyle.of(parameter.getStyle(), parameter.getExplode(), parameter.getAllowReserved(), parameterLocation);
                        return getPredicateColumn(
                                parameter.getName(),
                                parameter.getSchema(),
                                isRequired ? ImmutableMap.of(new HttpPath(method, path), parameterLocation) : ImmutableMap.of(),
                                !isRequired ? ImmutableMap.of(new HttpPath(method, path), parameterLocation) : ImmutableMap.of(),
                                // only save styles different from the default one for the parameter location
                                !parameterStyle.equals(ParameterStyle.defaultFor(parameterLocation))
                                        ? ImmutableMap.of(new HttpPath(method, path), parameterStyle)
                                        : ImmutableMap.of(),
                                // always nullable, because they're only required as predicates, not in INSERT statements
                                true,
                                // keep pagination parameters as hidden columns, so it's possible to
                                // see the page number (how many requests were made) and change the default per-page limit
                                specExtension.containsValue(parameter.getName()),
                                parameter.getName().equals(specExtension.get(PAGINATION_PAGE_PARAM)));
                    })
                    .filter(Optional::isPresent)
                    .map(Optional::get)
                    .map(column -> {
                        while (hasAmbiguousName(column, keys)) {
                            // if the request param is also a response field,
                            // append `_req` to its name to disambiguate it,
                            // otherwise it'll get a number suffix, like `_2`
                            column = OpenApiColumn.builderFrom(column)
                                    .setName(column.getName() + "_req")
                                    .build();
                        }
                        return column;
                    })
                    .forEach(result::add);
        }

        return result.stream();
    }

    private static List<Parameter> getParameters(List<Parameter> pathParameters, Operation op)
    {
        // operation parameters override path item parameters with the same name and location
        Map<ParameterKey, Parameter> parameters = new LinkedHashMap<>();
        Stream.concat(
                        Optional.ofNullable(pathParameters).stream().flatMap(List::stream),
                        Optional.ofNullable(op.getParameters()).stream().flatMap(List::stream))
                .forEach(parameter -> parameters.put(new ParameterKey(parameter.getName(), parameter.getIn()), parameter));
        return ImmutableList.copyOf(parameters.values());
    }

    private static boolean hasSchema(Parameter parameter, PathItem.HttpMethod method, String path)
    {
        if (parameter.getSchema() != null) {
            return true;
        }
        // parameters serialized using a media type, defined in `content` instead of `schema`, are not supported
        log.warn("Ignoring parameter %s in %s of %s %s, because it has no schema", parameter.getName(), parameter.getIn(), method, path);
        return false;
    }

    private record ParameterKey(String name, String in) {}

    private static JsonPointer parseJsonPointer(String expression)
    {
        if (expression == null) {
            return JsonPointer.empty();
        }
        Matcher matcher = JSON_POINTER_PATTERN.matcher(expression);
        if (matcher.matches()) {
            return JsonPointer.compile(matcher.group(1));
        }
        if (!expression.contains("/") && (expression.contains(".") || expression.contains("["))) {
            // it might be a JSON path, which are not supported, so ignore them
            return JsonPointer.empty();
        }
        if (expression.startsWith("$")) {
            throw new IllegalArgumentException("Complex JSON pointer or JSON path expressions are not supported");
        }
        if (!expression.startsWith("/")) {
            expression = "/" + expression;
        }
        return JsonPointer.compile(expression);
    }

    private static Schema<?> getResponseSchema(Operation op)
    {
        if (op.getResponses() == null
                || op.getResponses().get(HTTP_OK) == null
                || op.getResponses().get(HTTP_OK).getContent() == null
                || op.getResponses().get(HTTP_OK).getContent().get(MIME_JSON) == null) {
            return null;
        }
        return op.getResponses()
                .get(HTTP_OK)
                .getContent()
                .get(MIME_JSON)
                .getSchema();
    }

    private static Schema<?> getRequestSchema(Operation op)
    {
        if (op.getRequestBody() == null
                || op.getRequestBody().getContent() == null
                || op.getRequestBody().getContent().get(MIME_JSON) == null
                || op.getRequestBody().getContent().get(MIME_JSON).getSchema() == null) {
            return null;
        }
        return op.getRequestBody()
                .getContent()
                .get(MIME_JSON)
                .getSchema();
    }

    private static Map<String, String> getMapOfStrings(Object object)
    {
        if (!(object instanceof Map<?, ?>)) {
            return ImmutableMap.of();
        }

        return ((Map<?, ?>) object).entrySet().stream()
                .filter(entry -> entry.getKey() instanceof String && entry.getValue() instanceof String)
                .collect(toImmutableMap(entry -> (String) entry.getKey(), entry -> (String) entry.getValue()));
    }

    private static Map<String, Schema> getSchemaProperties(Schema<?> schema)
    {
        Map<String, Schema> properties;
        if (schema instanceof ArraySchema || schema.getItems() != null) {
            properties = schema.getItems().getProperties();
        }
        else {
            properties = schema.getProperties();
        }
        if (properties == null) {
            return Map.of();
        }
        return properties;
    }

    private static List<String> getRequiredProperties(Schema<?> schema)
    {
        List<String> required = requireNonNullElse(schema.getRequired(), List.of());
        if (!(schema instanceof ArraySchema) && schema.getItems() == null) {
            return required;
        }
        // some specs, like Cloudflare's, list required item properties in the array schema
        return Stream.concat(required.stream(), requireNonNullElse(schema.getItems().getRequired(), List.<String>of()).stream())
                .distinct()
                .collect(toImmutableList());
    }

    private Schema<?> getResultsSchema(Schema<?> schema, JsonPointer resultsPointer)
    {
        while (resultsPointer != JsonPointer.empty()) {
            if (resultsPointer.getMatchingIndex() != -1) {
                // skip over arrays
                resultsPointer = resultsPointer.tail();
                continue;
            }
            String name = resultsPointer.getMatchingProperty();
            schema = getAlternativesProperties(schema).get(name);
            if (schema == null) {
                throw new IllegalArgumentException("Invalid value of %s: unknown field %s".formatted(JSON_POINTER_PATTERN, resultsPointer));
            }
            // TODO validate that the schema is an array?
            resultsPointer = resultsPointer.tail();
        }
        return schema;
    }

    private Map<String, Schema> getAlternativesProperties(Schema<?> schema)
    {
        // properties of a union, like a oneOf with a success and an error object, are defined in its alternatives,
        // and a response can match any of them, so all of their properties are included
        ListMultimap<String, Schema> alternatives = MultimapBuilder.linkedHashKeys().arrayListValues().build();
        collectProperties(schema, alternatives);
        return alternatives.asMap().entrySet().stream()
                .collect(toMap(
                        Map.Entry::getKey,
                        entry -> mergeAlternatives(ImmutableList.copyOf(entry.getValue())),
                        (a, b) -> a,
                        LinkedHashMap::new));
    }

    private static void collectProperties(Schema<?> schema, ListMultimap<String, Schema> properties)
    {
        if (isArray(schema)) {
            // a response can be either a single object or an array of objects, like a file or a directory listing,
            // and every array element becomes a row, so the item properties are merged with the object properties;
            // arrays of arrays are not expanded into rows
            if (schema.getItems() != null && !isArray(schema.getItems())) {
                collectProperties(schema.getItems(), properties);
            }
            return;
        }
        if (schema.getProperties() != null) {
            schema.getProperties().forEach(properties::put);
        }
        getAlternatives(schema).forEach(alternative -> collectProperties(alternative, properties));
    }

    private static boolean isArray(Schema<?> schema)
    {
        return schema instanceof ArraySchema || schema.getItems() != null;
    }

    private static List<Schema> getAlternatives(Schema<?> schema)
    {
        return Stream.of(schema.getAllOf(), schema.getOneOf(), schema.getAnyOf())
                .filter(Objects::nonNull)
                .flatMap(List::stream)
                .toList();
    }

    private Schema<?> mergeAlternatives(List<Schema> schemas)
    {
        if (schemas.size() == 1) {
            return schemas.getFirst();
        }
        long typesCount = schemas.stream()
                .map(schema -> convertType(schema).map(TypeTuple::type))
                .distinct()
                .count();
        if (typesCount == 1) {
            return schemas.getFirst();
        }
        // a property can't be decoded as different types from a single field, so treat it as a union of its alternatives,
        // which merges objects into a single row, and maps other types to VARCHAR with the raw JSON value
        ComposedSchema union = new ComposedSchema();
        union.setOneOf(schemas);
        union.setDescription(schemas.stream()
                .map(Schema::getDescription)
                .filter(Objects::nonNull)
                .findFirst()
                .orElse(null));
        return union;
    }

    private static boolean hasAmbiguousName(OpenApiColumn column, ListMultimap<String, OpenApiColumn.PrimaryKey> keys)
    {
        return keys.get(column.getName()).stream().anyMatch(existingKey -> !existingKey.equals(column.getPrimaryKey()));
    }

    private Optional<OpenApiColumn> getResultColumn(
            String sourceName,
            Schema<?> schema,
            boolean isNullable,
            boolean isPageNumber)
    {
        String name = getIdentifier(sourceName);
        return convertType(schema).map(type -> OpenApiColumn.builder()
                .setName(name)
                .setSourceName(sourceName)
                .setType(type.type())
                .setSourceType(type.schema())
                // optional properties can be missing from the response, even if they're not nullable
                .setIsNullable(isNullable || isNullable(schema, false))
                .setIsHidden(false)
                .setIsPageNumber(isPageNumber)
                .setComment(schema.getDescription())
                .build());
    }

    private Optional<OpenApiColumn> getPredicateColumn(
            String sourceName,
            Schema<?> schema,
            Map<HttpPath, ParameterLocation> requiredPredicate,
            Map<HttpPath, ParameterLocation> optionalPredicate,
            Map<HttpPath, ParameterStyle> parameterStyles,
            boolean isNullable,
            boolean isHidden,
            boolean isPageNumber)
    {
        String name = getIdentifier(sourceName);
        return convertType(schema).map(type -> OpenApiColumn.builder()
                .setName(name)
                .setSourceName(sourceName)
                .setType(type.type())
                .setSourceType(type.schema())
                .setRequiresPredicate(requiredPredicate)
                .setOptionalPredicate(optionalPredicate)
                .setParameterStyles(parameterStyles)
                .setIsNullable(isNullable(schema, isNullable))
                .setIsHidden(isHidden)
                .setIsPageNumber(isPageNumber)
                .setComment(schema.getDescription())
                .build());
    }

    private static boolean isNullable(Schema<?> schema, boolean defaultValue)
    {
        if (schema.getTypes() != null && schema.getTypes().contains("null")) {
            return true;
        }
        return Optional.ofNullable(schema.getNullable()).orElse(defaultValue);
    }

    private static Map<SqlOperation, List<HttpPath>> operationPaths(PathItem pathItem, String path)
    {
        Map<PathItem.HttpMethod, Operation> operations = pathItem.readOperationsMap();
        ListMultimap<SqlOperation, HttpPath> result = MultimapBuilder.enumKeys(SqlOperation.class).arrayListValues().build();
        operations.forEach((method, operation) -> getSqlOperations(new HttpPath(method, path), operation)
                .forEach(sqlOperation -> result.put(sqlOperation, new HttpPath(method, path))));

        HttpPath put = new HttpPath(PathItem.HttpMethod.PUT, path);
        HttpPath patch = new HttpPath(PathItem.HttpMethod.PATCH, path);
        List<HttpPath> updatePaths = result.get(SqlOperation.UPDATE);
        if (updatePaths.contains(put) && updatePaths.contains(patch)) {
            // an UPDATE changes only some columns, and a PUT replaces the whole resource, so it's used only when explicitly requested
            boolean isOnlyPutExplicit = getSqlOperationExtension(operations.get(PathItem.HttpMethod.PUT)).isPresent()
                    && getSqlOperationExtension(operations.get(PathItem.HttpMethod.PATCH)).isEmpty();
            updatePaths.remove(isOnlyPutExplicit ? patch : put);
        }
        return result.asMap().entrySet().stream()
                .collect(toImmutableMap(Map.Entry::getKey, entry -> ImmutableList.copyOf(entry.getValue())));
    }

    private static Set<SqlOperation> getSqlOperations(HttpPath httpPath, Operation operation)
    {
        Optional<Object> extension = getSqlOperationExtension(operation);
        if (extension.isPresent()) {
            return parseSqlOperations(httpPath, extension.get());
        }
        // POST is used by APIs both to create and to search resources, so using it for SELECT requires the extension
        return switch (httpPath.method()) {
            case GET -> ImmutableSet.of(SqlOperation.SELECT);
            case POST -> ImmutableSet.of(SqlOperation.INSERT);
            case PUT, PATCH -> ImmutableSet.of(SqlOperation.UPDATE);
            case DELETE -> ImmutableSet.of(SqlOperation.DELETE);
            default -> ImmutableSet.of();
        };
    }

    private static Optional<Object> getSqlOperationExtension(Operation operation)
    {
        if (operation.getExtensions() == null || !(operation.getExtensions().get(SPEC_EXTENSION) instanceof Map<?, ?> specExtension)) {
            return Optional.empty();
        }
        return Optional.ofNullable(specExtension.get(SQL_OPERATION));
    }

    private static Set<SqlOperation> parseSqlOperations(HttpPath httpPath, Object value)
    {
        List<?> values = value instanceof List<?> list ? list : Splitter.on(',').trimResults().splitToList(value.toString());
        checkArgument(!values.isEmpty(), "Invalid value of %s in %s: it must not be empty", SQL_OPERATION, httpPath);
        return values.stream()
                .map(element -> {
                    Optional<SqlOperation> sqlOperation = Enums.getIfPresent(SqlOperation.class, String.valueOf(element).trim().toUpperCase(Locale.ENGLISH)).toJavaUtil();
                    checkArgument(sqlOperation.isPresent(), "Invalid value of %s in %s: '%s', must be one of: select, insert, update, delete", SQL_OPERATION, httpPath, element);
                    return sqlOperation.get();
                })
                .collect(toImmutableEnumSet());
    }

    private Map<HttpPath, JsonPointer> errorPointers(PathItem pathItem, String path)
    {
        return pointers(pathItem, path, ERROR_PATH);
    }

    private Map<HttpPath, JsonPointer> resultsPointers(PathItem pathItem, String path)
    {
        return pointers(pathItem, path, PAGINATION_RESULTS_PATH);
    }

    private Map<HttpPath, Pagination> paginations(PathItem pathItem, String path)
    {
        return pathItem.readOperationsMap().entrySet().stream()
                .filter(entry -> entry.getValue().getExtensions() != null && entry.getValue().getExtensions().containsKey(SPEC_EXTENSION))
                .collect(toImmutableMap(
                        entry -> new HttpPath(entry.getKey(), path),
                        entry -> {
                            Object rawExtension = entry.getValue().getExtensions().get(SPEC_EXTENSION);
                            Map<String, String> specExtension = getMapOfStrings(rawExtension);
                            return Pagination.builder()
                                    .setLimitParam(Optional.ofNullable(specExtension.get(PAGINATION_LIMIT_PARAM)))
                                    .setTotalResultsPointer(pointer(specExtension, PAGINATION_TOTAL_RESULTS_PATH))
                                    .setTotalPagesPointer(pointer(specExtension, PAGINATION_TOTAL_PAGES_PATH))
                                    .setHasMorePointer(pointer(specExtension, PAGINATION_HAS_MORE_PATH))
                                    .setMaxPages(getMaxPages(rawExtension))
                                    .build();
                        }));
    }

    private static OptionalInt getMaxPages(Object specExtension)
    {
        // YAML and JSON specs can have the number either as a number or a string
        Object value = specExtension instanceof Map<?, ?> map ? map.get(PAGINATION_MAX_PAGES) : null;
        if (value == null) {
            return OptionalInt.empty();
        }
        try {
            return OptionalInt.of(value instanceof Number number ? number.intValue() : Integer.parseInt(value.toString()));
        }
        catch (NumberFormatException e) {
            throw new IllegalArgumentException("Invalid value of %s: %s".formatted(PAGINATION_MAX_PAGES, value), e);
        }
    }

    private Map<HttpPath, JsonPointer> pointers(PathItem pathItem, String path, String extensionName)
    {
        return pathItem.readOperationsMap().entrySet().stream()
                .filter(entry -> entry.getValue().getExtensions() != null && entry.getValue().getExtensions().containsKey(SPEC_EXTENSION))
                .map(entry -> Map.entry(entry.getKey(), getMapOfStrings(entry.getValue().getExtensions().get(SPEC_EXTENSION))))
                // an empty pointer selects the whole response, which would include the whole body of error responses in error messages
                .filter(entry -> entry.getValue().containsKey(extensionName))
                .collect(toImmutableMap(
                        entry -> new HttpPath(entry.getKey(), path),
                        entry -> pointer(entry.getValue(), extensionName)));
    }

    private static JsonPointer pointer(Map<String, String> specExtension, String extensionName)
    {
        try {
            return parseJsonPointer(specExtension.get(extensionName));
        }
        catch (IllegalArgumentException e) {
            throw new IllegalArgumentException("Invalid value of %s: %s. %s".formatted(extensionName, specExtension.get(extensionName), e.getMessage()));
        }
    }

    public static String getIdentifier(String string)
    {
        return CaseFormat.LOWER_CAMEL.to(
                CaseFormat.LOWER_UNDERSCORE,
                string
                        .replaceAll("^/", "")
                        .replaceAll("[{}]", "")
                        .replace('/', '_')
                        .replace('-', '_'));
    }

    private Optional<TypeTuple> convertType(Schema<?> property)
    {
        if (JsonTrinoConverter.isUnion(property)) {
            if (isObjectUnion(property)) {
                // a value matches only some of the alternatives, and fields of the other ones are NULL
                return convertRow(mergeObjectUnion(property));
            }
            if (isObjectArrayUnion(property)) {
                // elements of arrays of different objects can match any of them, so they're merged into rows the same way
                return convertArray(mergeObjectArrayUnion(property));
            }
            // TODO oneOf types can be incompatible (object and an array), so it would require generating separate fields for every type
            return Optional.of(new TypeTuple(VARCHAR, property));
        }
        // OpenAPI 3.1 schemas are parsed into generic schemas, not subclasses like IntegerSchema or ObjectSchema,
        // so schemas of all versions are mapped using only their type name and format
        String type = getTypeName(property);
        if (type == null) {
            return Optional.of(FALLBACK_TYPE);
        }
        String format = property.getFormat();
        return switch (type) {
            case "array" -> convertArray(property);
            case "object" -> convertObject(property);
            case "integer" -> Optional.of(new TypeTuple("int32".equals(format) ? INTEGER : BIGINT, property));
            case "number" -> Optional.of(new TypeTuple(convertNumber(format), property));
            case "string" -> Optional.of(new TypeTuple(convertString(format), property));
            case "boolean" -> Optional.of(new TypeTuple(BOOLEAN, property));
            // non-standard types used by some specs
            case "int" -> Optional.of(new TypeTuple(INTEGER, property));
            case "float" -> Optional.of(new TypeTuple(REAL, property));
            default -> convertComponentType(type);
        };
    }

    private static String getTypeName(Schema<?> property)
    {
        if (property.getType() != null) {
            return property.getType();
        }
        if (property.getTypes() == null) {
            return null;
        }
        // OpenAPI 3.1 declares nullable values with a list of types including null, instead of the nullable keyword
        List<String> types = property.getTypes().stream()
                .filter(type -> !type.equals("null"))
                .toList();
        if (types.size() != 1) {
            return null;
        }
        return types.getFirst();
    }

    private static boolean isObjectUnion(Schema<?> union)
    {
        List<Schema> alternatives = getNonNullAlternatives(union);
        return !alternatives.isEmpty() && alternatives.stream().allMatch(OpenApiSpec::isObjectAlternative);
    }

    private static boolean isObjectArrayUnion(Schema<?> union)
    {
        List<Schema> alternatives = getNonNullAlternatives(union);
        return !alternatives.isEmpty() && alternatives.stream()
                .allMatch(alternative -> isArray(alternative) && alternative.getItems() != null && isObjectAlternative(alternative.getItems()));
    }

    private static List<Schema> getNonNullAlternatives(Schema<?> union)
    {
        return getAlternatives(union).stream()
                // OpenAPI 3.1 declares nullable values with a null alternative
                .filter(alternative -> !isNullType(alternative))
                .toList();
    }

    private static boolean isNullType(Schema<?> schema)
    {
        return "null".equals(schema.getType()) || (schema.getTypes() != null && schema.getTypes().equals(Set.of("null")));
    }

    private static boolean isObjectAlternative(Schema<?> schema)
    {
        if (JsonTrinoConverter.isUnion(schema)) {
            return isObjectUnion(schema);
        }
        String type = getTypeName(schema);
        if (type == null) {
            return schema.getProperties() != null;
        }
        if (!type.equals("object")) {
            return false;
        }
        Object additionalProperties = schema.getAdditionalProperties();
        if (additionalProperties instanceof Schema<?> booleanSchema && booleanSchema.getBooleanSchemaValue() != null) {
            additionalProperties = booleanSchema.getBooleanSchemaValue();
        }
        // objects with additional properties are maps, not rows
        return additionalProperties == null || FALSE.equals(additionalProperties);
    }

    private Schema<?> mergeObjectUnion(Schema<?> union)
    {
        ObjectSchema merged = new ObjectSchema();
        merged.setProperties(getAlternativesProperties(union));
        merged.setDescription(union.getDescription());
        merged.addExtension(JsonTrinoConverter.MERGED_UNION_EXTENSION, true);
        return merged;
    }

    private static Schema<?> mergeObjectArrayUnion(Schema<?> union)
    {
        ComposedSchema items = new ComposedSchema();
        items.setOneOf(getNonNullAlternatives(union).stream()
                .<Schema>map(Schema::getItems)
                .toList());
        ArraySchema merged = new ArraySchema();
        merged.setItems(items);
        merged.setDescription(union.getDescription());
        return merged;
    }

    private Optional<TypeTuple> convertArray(Schema<?> array)
    {
        if (array.getItems() == null) {
            // the parser drops items when it merges allOf alternatives referencing an array schema,
            // so elements of an unknown type can only be kept as serialized JSON
            return Optional.of(FALLBACK_TYPE);
        }
        return convertType(array.getItems()).map(elementType -> new TypeTuple(
                new ArrayType(elementType.type()),
                array.items(elementType.schema())));
    }

    private Optional<TypeTuple> convertObject(Schema<?> object)
    {
        Object additionalProperties = object.getAdditionalProperties();
        // OpenAPI 3.1 parses boolean schemas, like `additionalProperties: true`, into schemas holding only that boolean value
        if (additionalProperties instanceof Schema<?> booleanSchema && booleanSchema.getBooleanSchemaValue() != null) {
            additionalProperties = booleanSchema.getBooleanSchemaValue();
        }
        if (additionalProperties instanceof Schema<?> valueSchema) {
            return convertMap(object, valueSchema);
        }
        if (TRUE.equals(additionalProperties)) {
            // values of any type can only be kept as serialized JSON
            return Optional.of(FALLBACK_TYPE);
        }
        return convertRow(object);
    }

    private Optional<TypeTuple> convertMap(Schema<?> map, Schema<?> valueSchema)
    {
        Optional<TypeTuple> mapType = convertType(valueSchema);
        if (mapType.isEmpty()) {
            // fallback for invalid types - the value will be serialized json,
            // which can be later processed using SQL json functions
            return Optional.of(FALLBACK_TYPE);
        }
        return mapType.map(type -> new TypeTuple(
                new MapType(VARCHAR, type.type(), new TypeOperators()),
                map.additionalProperties(type.schema())));
    }

    private static Type convertNumber(String format)
    {
        return switch (format) {
            case "float" -> REAL;
            case "double" -> DOUBLE;
            // arbitrary scale and precision but should fit most numbers
            case null, default -> createDecimalType(18, 8);
        };
    }

    private static Type convertString(String format)
    {
        return switch (format) {
            case "date" -> DATE;
            // according to ISO-8601 can be any precision actually so might not fit
            case "date-time" -> TIMESTAMP_MILLIS;
            case null, default -> VARCHAR;
        };
    }

    private Optional<TypeTuple> convertRow(Schema<?> object)
    {
        // composite type
        Map<String, Schema> properties = object.getProperties();
        if (properties == null) {
            return Optional.of(FALLBACK_TYPE);
        }
        Map<String, TypeTuple> fieldTypes = properties.entrySet().stream()
                .map(prop -> Map.entry(prop.getKey(), convertType(prop.getValue())))
                .filter(entry -> entry.getValue().isPresent())
                .collect(toMap(
                        Map.Entry::getKey,
                        entry -> entry.getValue().get(),
                        (a, b) -> a,
                        LinkedHashMap::new));
        List<RowType.Field> fields = fieldTypes.entrySet().stream()
                .map(prop -> RowType.field(prop.getKey(), prop.getValue().type()))
                .toList();
        if (fields.isEmpty()) {
            return Optional.of(FALLBACK_TYPE);
        }
        Map<String, Schema> newProperties = fieldTypes.entrySet().stream()
                .collect(toMap(
                        Map.Entry::getKey,
                        entry -> entry.getValue().schema(),
                        (a, b) -> a,
                        LinkedHashMap::new));
        return Optional.of(new TypeTuple(RowType.from(fields), object.properties(newProperties)));
    }

    private Optional<TypeTuple> convertComponentType(String name)
    {
        Optional<Schema<?>> referenced = getComponentSchema(name);
        if (referenced.isEmpty()) {
            // unknown and unsupported types will be returned as strings, which at least can be parsed with json functions
            return Optional.of(FALLBACK_TYPE);
        }
        return convertType(referenced.get()).map(convertedType -> new TypeTuple(convertedType.type(), referenced.get()));
    }

    private Optional<Schema<?>> getComponentSchema(String name)
    {
        return Optional.ofNullable(openApi.getComponents())
                .map(Components::getSchemas)
                .map(schemas -> schemas.get(name));
    }

    private record TypeTuple(Type type, Schema<?> schema) {}

    private enum SqlOperation
    {
        SELECT,
        INSERT,
        UPDATE,
        DELETE,
    }

    private record MergeKey(OpenApiColumn.PrimaryKey primaryKey, String sourceName) {}

    private List<OpenApiColumn> mergeColumns(List<OpenApiColumn> columns)
    {
        return columns.stream()
                // merge all columns with same name, data type, and source name;
                // distinct source names can normalize to the same name, and they must remain separate columns
                .collect(groupingBy(column -> new MergeKey(column.getPrimaryKey(), column.getSourceName()), LinkedHashMap::new, toList()))
                .values().stream()
                .map(sameColumns -> OpenApiColumn.builderFrom(sameColumns.get(0))
                        .setIsNullable(sameColumns.stream().anyMatch(column -> column.getMetadata().isNullable()))
                        .setRequiresPredicate(sameColumns.stream()
                                .map(OpenApiColumn::getRequiresPredicate)
                                .flatMap(map -> map.entrySet().stream())
                                .collect(toMap(Map.Entry::getKey, Map.Entry::getValue, (a, b) -> a, LinkedHashMap::new)))
                        .setOptionalPredicate(sameColumns.stream()
                                .map(OpenApiColumn::getOptionalPredicate)
                                .flatMap(map -> map.entrySet().stream())
                                .collect(toMap(Map.Entry::getKey, Map.Entry::getValue, (a, b) -> a, LinkedHashMap::new)))
                        .setParameterStyles(sameColumns.stream()
                                .map(OpenApiColumn::getParameterStyles)
                                .flatMap(map -> map.entrySet().stream())
                                .collect(toMap(Map.Entry::getKey, Map.Entry::getValue, (a, b) -> a, LinkedHashMap::new)))
                        .build())
                .collect(groupingBy(OpenApiColumn::getName, LinkedHashMap::new, toList()))
                .values().stream()
                // make sure column names are also unique, append incrementing suffixes for columns of different types
                .flatMap(sameColumns -> IntStream
                        .range(0, sameColumns.size())
                        .mapToObj(i -> OpenApiColumn.builderFrom(sameColumns.get(i))
                                .setName(sameColumns.get(i).getName() + (i > 0 ? "_" + (i + 1) : ""))
                                .build()))
                .toList();
    }

    private static List<OpenApiColumn> addRowIdColumn(List<OpenApiColumn> columns, OpenApiTableHandle tableHandle)
    {
        if (tableHandle.getUpdatePaths().isEmpty() && tableHandle.getDeletePaths().isEmpty()) {
            return columns;
        }
        // the ROW_ID column is required for MERGE operation, including UPDATE and DELETE
        return Stream.concat(
                        Stream.of(OpenApiColumn.builder()
                                .setName(ROW_ID)
                                .setType(VARCHAR)
                                .setSourceType(new StringSchema())
                                .setIsHidden(true)
                                .build()),
                        columns.stream())
                .toList();
    }

    private static List<OpenApiColumn> allowInsertsWithoutResponseColumns(List<OpenApiColumn> columns, OpenApiTableHandle tableHandle)
    {
        if (tableHandle.getInsertPaths().isEmpty()) {
            return columns;
        }
        // columns that only exist in responses are never sent in INSERT requests, so they can't be set,
        // but Trino rejects INSERT statements that would leave a NOT NULL column empty
        return columns.stream()
                .map(column -> isResponseOnly(column) && !column.getMetadata().isNullable()
                        ? OpenApiColumn.builderFrom(column).setIsNullable(true).build()
                        : column)
                .toList();
    }

    private static boolean isResponseOnly(OpenApiColumn column)
    {
        return column.getRequiresPredicate().isEmpty()
                && column.getOptionalPredicate().isEmpty()
                && !column.getMetadata().isHidden();
    }

    private static OpenApiTableHandle tableHandle(String tableName, Map<SqlOperation, List<HttpPath>> tablePaths)
    {
        return OpenApiTableHandle.builder(SchemaTableName.schemaTableName(SCHEMA_NAME, tableName))
                .setSelectPaths(tablePaths.getOrDefault(SqlOperation.SELECT, ImmutableList.of()))
                .setInsertPaths(tablePaths.getOrDefault(SqlOperation.INSERT, ImmutableList.of()))
                .setUpdatePaths(tablePaths.getOrDefault(SqlOperation.UPDATE, ImmutableList.of()))
                .setDeletePaths(tablePaths.getOrDefault(SqlOperation.DELETE, ImmutableList.of()))
                .build();
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
