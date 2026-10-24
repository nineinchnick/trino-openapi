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
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
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

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.OptionalLong;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static com.google.common.collect.Sets.toImmutableEnumSet;
import static io.airlift.slice.Slices.utf8Slice;
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
import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toMap;

public class OpenApiSpec
{
    private static final Logger log = Logger.get(OpenApiSpec.class);

    public static final String SCHEMA_NAME = "default";
    public static final String ROW_ID = "__trino_row_id";
    public static final String HTTP_OK = "200";
    public static final String MIME_JSON = "application/json";

    private static final TypeTuple FALLBACK_TYPE = new TypeTuple(VARCHAR, new Schema<>().extensions(ImmutableMap.of(JsonTrinoConverter.JSON_TEXT_EXTENSION, true)));

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
    private static final String PAGINATION_MAX_RESULTS = "maxResults";
    private static final String PAGINATION_SPLIT_STRATEGY = "splitStrategy";
    private static final String PAGINATION_STRIDE_SPLITS = "strideSplits";
    private static final String PAGINATION_PROBE_LIMIT = "probeLimit";
    private static final String SPLIT_STRATEGIES = Arrays.stream(SplitStrategy.values())
            .map(splitStrategy -> splitStrategy.name().toLowerCase(Locale.ENGLISH))
            .collect(joining(", "));
    private static final String SQL_OPERATION = "sqlOperation";
    private static final String SORT_PARAM = "sortParam";
    private static final String SORT_VALUES = "sortValues";
    private static final String SORT_VALUE = "value";
    private static final String SORT_BINARY_ORDER = "binaryOrder";
    private static final String SORT_DIRECTION_PARAM = "directionParam";
    private static final String SORT_ASCENDING_VALUE = "ascendingValue";
    private static final String SORT_DESCENDING_VALUE = "descendingValue";
    private static final String RANGE_FILTERS = "rangeFilters";
    private static final String RANGE_LOWER_PARAM = "lowerParam";
    private static final String RANGE_LOWER_INCLUSIVE = "lowerInclusive";
    private static final String RANGE_UPPER_PARAM = "upperParam";
    private static final String RANGE_UPPER_INCLUSIVE = "upperInclusive";
    private static final String RANGE_EXACT = "exact";
    private static final String PARAMETER_DEFAULTS = "parameterDefaults";
    private static final String RATE_LIMIT_RESET_HEADER = "rateLimitResetHeader";
    private static final String RATE_LIMIT_RESET_FORMAT = "rateLimitResetFormat";
    private static final String RATE_LIMIT_REMAINING_HEADER = "rateLimitRemainingHeader";
    private static final String PARAMETER_FIELD = "field";
    private static final Set<String> DOCUMENT_EXTENSION_PROPERTIES = ImmutableSet.of(ERROR_PATH, RATE_LIMIT_RESET_HEADER, RATE_LIMIT_RESET_FORMAT, RATE_LIMIT_REMAINING_HEADER);
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
    private final Optional<RateLimitHeaders> rateLimitHeaders;
    private final Optional<JsonPointer> documentErrorPointer;

    @Inject
    public OpenApiSpec(OpenApiConfig config)
    {
        this(parse(requireNonNull(config, "config is null").getSpecLocation()));
    }

    OpenApiSpec(OpenAPI openApi)
    {
        this.openApi = requireNonNull(openApi, "openApi is null");
        Map<String, String> documentExtension = getDocumentExtension(openApi);
        this.rateLimitHeaders = getRateLimitHeaders(documentExtension);
        // errors are usually reported the same way by all operations of an API
        this.documentErrorPointer = getDocumentErrorPointer(documentExtension);

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
        Map<String, String> tableNames = getTableNames(pathGroups.values());
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
                String tableName = tableNames.get(firstEntry.getKey());
                tables.put(Map.entry(
                        tableName,
                        mergeColumns(getColumns(pathItems.getFirst(), firstEntry.getKey()))));
                Map<SqlOperation, List<HttpPath>> tablePaths = operationPaths(firstEntry.getValue(), firstEntry.getKey());
                errorPointers.put(tableName, errorPointers(firstEntry.getValue(), firstEntry.getKey()));
                resultsPointers.put(tableName, resultsPointers(firstEntry.getValue(), firstEntry.getKey()));
                paginations.put(tableName, paginations(firstEntry.getValue(), firstEntry.getKey()));
                handles.put(tableName, tableHandle(tableName, tablePaths));
                continue;
            }
            Map.Entry<String, PathItem> baseEntry = getBaseEntry(groupEntry.getValue());
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
                        String tableName = tableNames.get(entry.getKey());
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
        Map<String, List<OpenApiColumn>> mergedTables = tables.buildOrThrow();
        mergedTables.forEach(OpenApiSpec::validateParameterFields);
        this.tables = mergedTables.entrySet().stream()
                .collect(toImmutableMap(Map.Entry::getKey, entry -> addRowIdColumn(
                        allowInsertsWithoutResponseColumns(
                                allowNullsMissingFromSelectResponses(entry.getValue(), this.handles.get(entry.getKey())),
                                this.handles.get(entry.getKey())),
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

    private static Map<String, String> getDocumentExtension(OpenAPI openApi)
    {
        Object extension = openApi.getExtensions() == null ? null : openApi.getExtensions().get(SPEC_EXTENSION);
        if (extension == null) {
            return ImmutableMap.of();
        }
        checkArgument(extension instanceof Map<?, ?>, "Invalid value of the document-level %s extension: expected an object", SPEC_EXTENSION);
        // properties of operations set for the whole document would change every operation, which is likely a mistake
        ((Map<?, ?>) extension).forEach((key, value) -> {
            checkArgument(DOCUMENT_EXTENSION_PROPERTIES.contains(key), "Invalid property of the document-level %s extension: %s, must be one of: %s", SPEC_EXTENSION, key, String.join(", ", DOCUMENT_EXTENSION_PROPERTIES));
            checkArgument(value instanceof String, "Invalid value of %s: expected a string", key);
        });
        return getMapOfStrings(extension);
    }

    private static Optional<JsonPointer> getDocumentErrorPointer(Map<String, String> documentExtension)
    {
        if (!documentExtension.containsKey(ERROR_PATH)) {
            return Optional.empty();
        }
        JsonPointer pointer = pointer(documentExtension, ERROR_PATH);
        // an empty pointer selects the whole response, which would include the whole body of error responses of every operation in error messages
        checkArgument(!pointer.equals(JsonPointer.empty()), "Invalid value of %s: %s, must point to a field of the response", ERROR_PATH, documentExtension.get(ERROR_PATH));
        return Optional.of(pointer);
    }

    private static Optional<RateLimitHeaders> getRateLimitHeaders(Map<String, String> documentExtension)
    {
        String resetHeader = documentExtension.get(RATE_LIMIT_RESET_HEADER);
        if (resetHeader == null) {
            checkArgument(!documentExtension.containsKey(RATE_LIMIT_RESET_FORMAT), "%s requires %s", RATE_LIMIT_RESET_FORMAT, RATE_LIMIT_RESET_HEADER);
            checkArgument(!documentExtension.containsKey(RATE_LIMIT_REMAINING_HEADER), "%s requires %s", RATE_LIMIT_REMAINING_HEADER, RATE_LIMIT_RESET_HEADER);
            return Optional.empty();
        }
        // a reset time read in the wrong format would make requests wait too long, or retry too early, so it must be declared
        String resetFormat = documentExtension.get(RATE_LIMIT_RESET_FORMAT);
        checkArgument(resetFormat != null, "%s is required with %s", RATE_LIMIT_RESET_FORMAT, RATE_LIMIT_RESET_HEADER);
        Optional<RateLimitHeaders.ResetFormat> format = Stream.of(RateLimitHeaders.ResetFormat.values())
                .filter(value -> value.getSpecValue().equals(resetFormat))
                .findFirst();
        checkArgument(format.isPresent(), "Invalid value of %s: %s, must be one of: %s", RATE_LIMIT_RESET_FORMAT, resetFormat, Stream.of(RateLimitHeaders.ResetFormat.values())
                .map(RateLimitHeaders.ResetFormat::getSpecValue)
                .collect(joining(", ")));
        return Optional.of(new RateLimitHeaders(resetHeader, format.get(), Optional.ofNullable(documentExtension.get(RATE_LIMIT_REMAINING_HEADER))));
    }

    // keyed by the path a table is named after, which is the path that is not the base path in groups of more than one path
    private Map<String, String> getTableNames(Collection<List<Map.Entry<String, PathItem>>> pathGroups)
    {
        ImmutableMap.Builder<String, String> tableNames = ImmutableMap.builder();
        for (List<Map.Entry<String, PathItem>> pathGroup : pathGroups) {
            if (pathGroup.size() == 1) {
                String path = pathGroup.getFirst().getKey();
                tableNames.put(path, getIdentifier(stripPathParams(path)));
                continue;
            }
            Map.Entry<String, PathItem> baseEntry = getBaseEntry(pathGroup);
            pathGroup.stream()
                    .filter(entry -> !entry.equals(baseEntry))
                    // in groups of more than 2 paths, path parameters are included to tell the tables apart
                    .forEach(entry -> tableNames.put(entry.getKey(), getIdentifier(pathGroup.size() == 2 ? stripPathParams(entry.getKey()) : entry.getKey())));
        }
        return tableNames.buildOrThrow();
    }

    private static Map.Entry<String, PathItem> getBaseEntry(List<Map.Entry<String, PathItem>> pathGroup)
    {
        return pathGroup.stream()
                .min(comparingInt(entry -> entry.getKey().length()))
                .orElseThrow();
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
        PreprocessingParser parser = new PreprocessingParser();
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

    private static class PreprocessingParser
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
                // the parser merges allOf alternatives into a single schema, but drops items, so arrays must be merged before
                mergeArrayAlternatives(node, node);
            }
            return super.parseJsonNode(path, node, options);
        }

        boolean hasEmptyRootSecurity()
        {
            return emptyRootSecurity;
        }

        private static void mergeArrayAlternatives(JsonNode root, JsonNode node)
        {
            if (node instanceof ObjectNode schema) {
                getArrayAlternatives(root, schema).ifPresent(alternatives -> inlineArrayAlternatives(schema, alternatives));
            }
            // this also visits the merged items, which can have allOf alternatives of their own
            node.forEach(child -> mergeArrayAlternatives(root, child));
        }

        private static Optional<List<JsonNode>> getArrayAlternatives(JsonNode root, ObjectNode schema)
        {
            if (!(schema.get("allOf") instanceof ArrayNode allOf) || allOf.isEmpty()) {
                return Optional.empty();
            }
            List<JsonNode> alternatives = new ArrayList<>();
            for (JsonNode alternative : allOf) {
                JsonNode resolved = resolveLocalReference(root, alternative);
                if (!resolved.has("items")) {
                    return Optional.empty();
                }
                alternatives.add(resolved);
            }
            return Optional.of(alternatives);
        }

        private static JsonNode resolveLocalReference(JsonNode root, JsonNode schema)
        {
            JsonNode reference = schema.get("$ref");
            // references to other documents are resolved by the parser later, so their schemas are not known yet
            if (reference == null || !reference.isTextual() || !reference.asText().startsWith("#/")) {
                return schema;
            }
            return root.at(reference.asText().substring(1));
        }

        private static void inlineArrayAlternatives(ObjectNode schema, List<JsonNode> alternatives)
        {
            schema.remove("allOf");
            List<JsonNode> items = new ArrayList<>();
            Optional.ofNullable(schema.remove("items")).ifPresent(items::add);
            for (JsonNode alternative : alternatives) {
                items.add(alternative.get("items"));
                // keywords of the schema itself take precedence, like in a merge done by the parser
                alternative.properties().stream()
                        .filter(property -> !property.getKey().equals("items"))
                        .forEach(property -> schema.putIfAbsent(property.getKey(), property.getValue().deepCopy()));
            }
            if (items.size() == 1) {
                schema.set("items", items.getFirst().deepCopy());
                return;
            }
            // the parser merges items that are not arrays, and items that are arrays are merged when visiting them
            ObjectNode mergedItems = schema.objectNode();
            ArrayNode itemsAlternatives = mergedItems.putArray("allOf");
            items.forEach(item -> itemsAlternatives.add(item.deepCopy()));
            schema.set("items", mergedItems);
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
        return getOperation(httpPath)
                .map(Operation::getOperationId)
                .filter(operationId -> !operationId.isBlank());
    }

    public boolean hasArrayRequestBody(HttpPath httpPath)
    {
        return getOperation(httpPath)
                .map(OpenApiSpec::getRequestSchema)
                .filter(OpenApiSpec::isArray)
                .isPresent();
    }

    public boolean declaresResponse(HttpPath httpPath, String statusCode)
    {
        return getOperation(httpPath)
                .map(Operation::getResponses)
                .map(responses -> responses.containsKey(statusCode))
                .orElse(false);
    }

    private Optional<Operation> getOperation(HttpPath httpPath)
    {
        return Optional.ofNullable(openApi.getPaths())
                .map(paths -> paths.get(httpPath.path()))
                .map(pathItem -> pathItem.readOperationsMap().get(httpPath.method()));
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
                getMapOfStrings(requireNonNullElse(
                        op.getExtensions().get(SPEC_EXTENSION),
                        requireNonNullElse(op.getExtensions().get(LEGACY_SPEC_EXTENSION), ImmutableMap.of())));
        JsonPointer resultsPointer;
        try {
            resultsPointer = parseJsonPointer(specExtension.get(PAGINATION_RESULTS_PATH));
        }
        catch (IllegalArgumentException e) {
            throw new IllegalArgumentException("Invalid value of %s: %s. %s".formatted(PAGINATION_RESULTS_PATH, specExtension.get(PAGINATION_RESULTS_PATH), e.getMessage()));
        }

        Schema<?> schema = getResponseSchema(op);
        if (schema != null) {
            Schema<?> resultsSchema = getResultsSchema(schema, resultsPointer);
            Map<String, Schema> resultsProperties = getAlternativesProperties(resultsSchema);
            boolean hasEnvelope = !resultsPointer.equals(JsonPointer.empty());
            List<String> requiredProperties = getRequiredProperties(schema);
            getAlternativesProperties(schema)
                    .entrySet().stream()
                    .filter(propEntry -> !resultsPointer.matchesProperty(propEntry.getKey()))
                    // fields of the results are read from the results, even if the response has a field with the same name next to them
                    .filter(propEntry -> !hasEnvelope || !resultsProperties.containsKey(propEntry.getKey()))
                    .map(propEntry -> getResultColumn(
                            new HttpPath(method, path),
                            propEntry.getKey(),
                            propEntry.getValue(),
                            !requiredProperties.contains(propEntry.getKey()),
                            propEntry.getKey().equals(specExtension.get(PAGINATION_PAGE_PARAM))))
                    .filter(Optional::isPresent)
                    // fields next to the results, like the total number of results, describe pages, not rows
                    .map(column -> hasEnvelope
                            ? OpenApiColumn.builderFrom(column.get()).setIsHidden(true).setEnvelopePaths(Set.of(new HttpPath(method, path))).build()
                            : column.get())
                    .forEach(result::add);
            List<String> requiredResultsProperties = getRequiredProperties(resultsSchema);
            resultsProperties
                    .entrySet().stream()
                    .map(propEntry -> getResultColumn(
                            new HttpPath(method, path),
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
                        Optional<String> field = getParameterField(parameter, new HttpPath(method, path));
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
                                parameter.getName().equals(specExtension.get(PAGINATION_PAGE_PARAM)))
                                .map(column -> field.isEmpty() ? column : linkToField(column, field.get(), parameter.getName(), new HttpPath(method, path)));
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

        HttpPath httpPath = new HttpPath(method, path);
        Object rawExtension = op.getExtensions() == null ? null : op.getExtensions().get(SPEC_EXTENSION);
        List<OpenApiColumn> columns = setParameterDefaults(result, getParameterDefaults(rawExtension), httpPath, specExtension.get(PAGINATION_PAGE_PARAM));
        Map<String, RangeFilter> rangeFilters = getRangeFilters(rawExtension);
        if (rangeFilters.isEmpty()) {
            return columns.stream();
        }
        validateRangeFilters(rangeFilters, columns, httpPath);
        return columns.stream()
                .map(column -> rangeFilters.containsKey(column.getSourceName()) && !isParameter(column)
                        ? OpenApiColumn.builderFrom(column).setRangeFilters(Map.of(httpPath, rangeFilters.get(column.getSourceName()))).build()
                        : column);
    }

    private static Optional<String> getParameterField(Parameter parameter, HttpPath httpPath)
    {
        Object extension = parameter.getExtensions() == null ? null : parameter.getExtensions().get(SPEC_EXTENSION);
        if (extension == null) {
            return Optional.empty();
        }
        String parameterName = parameter.getName();
        checkArgument(extension instanceof Map<?, ?>, "Invalid value of the %s extension of parameter %s of %s: expected an object", SPEC_EXTENSION, parameterName, httpPath);
        ((Map<?, ?>) extension).keySet().forEach(key -> checkArgument(
                PARAMETER_FIELD.equals(key),
                "Invalid property of the %s extension of parameter %s of %s: %s, must be: %s",
                SPEC_EXTENSION,
                parameterName,
                httpPath,
                key,
                PARAMETER_FIELD));
        Object field = ((Map<?, ?>) extension).get(PARAMETER_FIELD);
        checkArgument(field instanceof String name && !name.isBlank(), "Invalid value of %s in the %s extension of parameter %s of %s: expected the name of a response field", PARAMETER_FIELD, SPEC_EXTENSION, parameterName, httpPath);
        // only path parameters identify a resource, and other parameters are usually filters, matching many values of a field
        checkArgument("path".equals(parameter.getIn()), "Invalid value of %s in the %s extension of parameter %s of %s: only path parameters can be linked to a response field", PARAMETER_FIELD, SPEC_EXTENSION, parameterName, httpPath);
        return Optional.of((String) field);
    }

    private static OpenApiColumn linkToField(OpenApiColumn column, String field, String parameterName, HttpPath httpPath)
    {
        // a column named after the field is merged with the column of the field, so rows get its value from the response
        return OpenApiColumn.builderFrom(column)
                .setName(getIdentifier(field))
                .setSourceName(field)
                .setParameterNames(Map.of(httpPath, parameterName))
                .build();
    }

    private static void validateParameterFields(String tableName, List<OpenApiColumn> columns)
    {
        for (OpenApiColumn column : columns) {
            column.getParameterNames().forEach((httpPath, parameterName) -> {
                String field = column.getSourceName();
                if (column.isRequestOnly()) {
                    Optional<OpenApiColumn> fieldColumn = columns.stream()
                            .filter(candidate -> candidate.getSourceName().equals(field) && !candidate.isRequestOnly())
                            .findFirst();
                    checkArgument(fieldColumn.isPresent(), "Invalid value of %s in the %s extension of parameter %s of %s: %s is not a response field of table %s", PARAMETER_FIELD, SPEC_EXTENSION, parameterName, httpPath, field, tableName);
                    throw new IllegalArgumentException("Invalid value of %s in the %s extension of parameter %s of %s: the parameter has type %s, but field %s has type %s".formatted(
                            PARAMETER_FIELD,
                            SPEC_EXTENSION,
                            parameterName,
                            httpPath,
                            column.getType(),
                            field,
                            fieldColumn.get().getType()));
                }
                // otherwise, the same parameter would be a separate column in other operations, without values in rows
                columns.stream()
                        .filter(other -> other != column)
                        .flatMap(other -> Stream.concat(other.getRequiresPredicate().entrySet().stream(), other.getOptionalPredicate().entrySet().stream())
                                .filter(entry -> entry.getValue() == ParameterLocation.PATH
                                        && entry.getKey().path().equals(httpPath.path())
                                        && other.getParameterName(entry.getKey()).equals(parameterName))
                                .map(Map.Entry::getKey))
                        .findFirst()
                        .ifPresent(otherPath -> {
                            throw new IllegalArgumentException("Parameter %s of %s is linked to field %s in the %s extension, but not in %s, so set the same %s in all operations of the path, or in the parameters of the path".formatted(
                                    parameterName,
                                    httpPath,
                                    field,
                                    SPEC_EXTENSION,
                                    otherPath,
                                    PARAMETER_FIELD));
                        });
            });
        }
    }

    private static Map<String, Object> getParameterDefaults(Object specExtension)
    {
        if (!(specExtension instanceof Map<?, ?> extension) || extension.get(PARAMETER_DEFAULTS) == null) {
            return ImmutableMap.of();
        }
        checkArgument(extension.get(PARAMETER_DEFAULTS) instanceof Map<?, ?>, "Invalid value of %s: expected an object with parameter names as keys", PARAMETER_DEFAULTS);
        return ((Map<?, ?>) extension.get(PARAMETER_DEFAULTS)).entrySet().stream()
                .collect(toImmutableMap(entry -> entry.getKey().toString(), entry -> {
                    checkArgument(entry.getValue() != null, "Invalid value of %s for %s: expected a value", PARAMETER_DEFAULTS, entry.getKey());
                    return entry.getValue();
                }));
    }

    private static List<OpenApiColumn> setParameterDefaults(List<OpenApiColumn> columns, Map<String, Object> defaults, HttpPath httpPath, String pageParam)
    {
        if (defaults.isEmpty()) {
            return columns;
        }
        String operation = httpPath.method() + " " + httpPath.path();
        Map<String, Object> nativeDefaults = defaults.entrySet().stream()
                .collect(toImmutableMap(Map.Entry::getKey, entry -> {
                    String parameter = entry.getKey();
                    // a page number filter makes the connector request only that page
                    checkArgument(!parameter.equals(pageParam), "Invalid value of %s: %s is the page number parameter of %s", PARAMETER_DEFAULTS, parameter, operation);
                    Optional<OpenApiColumn> column = columns.stream()
                            .filter(candidate -> parameter.equals(candidate.getSourceName()) && isDefaultableParameter(candidate, httpPath))
                            .findFirst();
                    checkArgument(column.isPresent(), "Invalid value of %s: %s is not an optional query, header, or cookie parameter of %s", PARAMETER_DEFAULTS, parameter, operation);
                    return getNativeDefault(column.get(), entry.getValue(), operation);
                }));
        return columns.stream()
                .map(column -> isDefaultableParameter(column, httpPath) && nativeDefaults.containsKey(column.getSourceName())
                        ? OpenApiColumn.builderFrom(column).setParameterDefaults(Map.of(httpPath, nativeDefaults.get(column.getSourceName()))).build()
                        : column)
                .toList();
    }

    private static boolean isDefaultableParameter(OpenApiColumn column, HttpPath httpPath)
    {
        // required parameters must be set by the query, and path parameters select resources
        ParameterLocation location = column.getOptionalPredicate().get(httpPath);
        return location == ParameterLocation.QUERY || location == ParameterLocation.HEADER || location == ParameterLocation.COOKIE;
    }

    private static Object getNativeDefault(OpenApiColumn column, Object value, String operation)
    {
        String parameter = column.getSourceName();
        Type type = column.getType();
        checkArgument(
                type.equals(VARCHAR) || type.equals(BIGINT) || type.equals(INTEGER) || type.equals(BOOLEAN),
                "Invalid value of %s: parameter %s of %s has type %s, but only string, integer, and boolean parameters can have defaults",
                PARAMETER_DEFAULTS,
                parameter,
                operation,
                type);
        boolean matchesType = (type.equals(VARCHAR) && value instanceof String)
                || ((type.equals(BIGINT) || type.equals(INTEGER)) && (value instanceof Integer || value instanceof Long))
                || (type.equals(BOOLEAN) && value instanceof Boolean);
        checkArgument(matchesType, "Invalid value of %s: %s for parameter %s of %s doesn't have type %s", PARAMETER_DEFAULTS, value, parameter, operation, type);
        Schema<?> schema = column.getSourceType();
        checkArgument(
                schema.getEnum() == null || schema.getEnum().stream().anyMatch(allowed -> String.valueOf(allowed).equals(value.toString())),
                "Invalid value of %s: %s for parameter %s of %s is not one of %s",
                PARAMETER_DEFAULTS,
                value,
                parameter,
                operation,
                schema.getEnum());
        if (value instanceof Number number) {
            BigDecimal decimal = BigDecimal.valueOf(number.longValue());
            checkArgument(schema.getMinimum() == null || decimal.compareTo(schema.getMinimum()) >= 0, "Invalid value of %s: %s for parameter %s of %s is less than its minimum %s", PARAMETER_DEFAULTS, value, parameter, operation, schema.getMinimum());
            checkArgument(schema.getMaximum() == null || decimal.compareTo(schema.getMaximum()) <= 0, "Invalid value of %s: %s for parameter %s of %s is greater than its maximum %s", PARAMETER_DEFAULTS, value, parameter, operation, schema.getMaximum());
            return number.longValue();
        }
        if (value instanceof String string) {
            return utf8Slice(string);
        }
        return value;
    }

    private static Map<String, RangeFilter> getRangeFilters(Object specExtension)
    {
        if (!(specExtension instanceof Map<?, ?> extension) || extension.get(RANGE_FILTERS) == null) {
            return ImmutableMap.of();
        }
        checkArgument(extension.get(RANGE_FILTERS) instanceof Map<?, ?>, "Invalid value of %s: expected an object with response fields as keys", RANGE_FILTERS);
        return ((Map<?, ?>) extension.get(RANGE_FILTERS)).entrySet().stream()
                .collect(toImmutableMap(entry -> entry.getKey().toString(), entry -> getRangeFilter(entry.getKey().toString(), entry.getValue())));
    }

    private static RangeFilter getRangeFilter(String field, Object value)
    {
        checkArgument(value instanceof Map<?, ?>, "Invalid value of %s for %s: expected an object", RANGE_FILTERS, field);
        Map<?, ?> properties = (Map<?, ?>) value;
        try {
            return new RangeFilter(
                    getRangeBound(properties, RANGE_LOWER_PARAM, RANGE_LOWER_INCLUSIVE),
                    getRangeBound(properties, RANGE_UPPER_PARAM, RANGE_UPPER_INCLUSIVE),
                    getBoolean(properties, RANGE_EXACT).orElse(false));
        }
        catch (IllegalArgumentException e) {
            throw new IllegalArgumentException("Invalid value of %s for %s: %s".formatted(RANGE_FILTERS, field, e.getMessage()), e);
        }
    }

    private static Optional<RangeFilter.Bound> getRangeBound(Map<?, ?> properties, String parameterKey, String inclusiveKey)
    {
        Object parameter = properties.get(parameterKey);
        if (parameter == null) {
            return Optional.empty();
        }
        // a wrong default would make the API skip rows equal to the bound, so it must be declared
        Optional<Boolean> inclusive = getBoolean(properties, inclusiveKey);
        checkArgument(inclusive.isPresent(), "%s is required with %s", inclusiveKey, parameterKey);
        return Optional.of(new RangeFilter.Bound(parameter.toString(), inclusive.get()));
    }

    private static Optional<Boolean> getBoolean(Map<?, ?> properties, String key)
    {
        // YAML and JSON specs can have booleans either as booleans or strings
        Object value = properties.get(key);
        if (value == null) {
            return Optional.empty();
        }
        if (value instanceof Boolean booleanValue) {
            return Optional.of(booleanValue);
        }
        checkArgument(value.equals("true") || value.equals("false"), "%s must be true or false, got %s", key, value);
        return Optional.of(Boolean.parseBoolean(value.toString()));
    }

    private static void validateRangeFilters(Map<String, RangeFilter> rangeFilters, List<OpenApiColumn> columns, HttpPath httpPath)
    {
        String operation = httpPath.method() + " " + httpPath.path();
        Set<String> boundParameters = new HashSet<>();
        rangeFilters.forEach((field, filter) -> {
            List<OpenApiColumn> fieldColumns = columns.stream()
                    .filter(column -> field.equals(column.getSourceName()) && !isParameter(column))
                    .toList();
            checkArgument(!fieldColumns.isEmpty(), "Invalid value of %s: %s is not a response field of %s", RANGE_FILTERS, field, operation);
            Stream.of(filter.lower(), filter.upper()).flatMap(Optional::stream).forEach(bound -> {
                checkArgument(boundParameters.add(bound.parameter()), "Invalid value of %s: parameter %s is used more than once in %s", RANGE_FILTERS, bound.parameter(), operation);
                Optional<OpenApiColumn> parameter = columns.stream()
                        .filter(column -> bound.parameter().equals(column.getSourceName()) && RangePushdown.isNonPathParameter(column, httpPath))
                        .findFirst();
                checkArgument(parameter.isPresent(), "Invalid value of %s: %s is not a query, header, cookie, or body parameter of %s", RANGE_FILTERS, bound.parameter(), operation);
                fieldColumns.forEach(fieldColumn -> checkArgument(
                        fieldColumn.getType().equals(parameter.get().getType()),
                        "Invalid value of %s: parameter %s has type %s, but field %s has type %s",
                        RANGE_FILTERS,
                        bound.parameter(),
                        parameter.get().getType(),
                        field,
                        fieldColumn.getType()));
            });
        });
    }

    private static boolean isParameter(OpenApiColumn column)
    {
        return !column.getRequiresPredicate().isEmpty() || !column.getOptionalPredicate().isEmpty();
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
                        (a, _) -> a,
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
            HttpPath httpPath,
            String sourceName,
            Schema<?> schema,
            boolean isNullable,
            boolean isPageNumber)
    {
        String name = getIdentifier(sourceName);
        boolean isResponseRequired = !isNullable && !isNullable(schema, false);
        return convertType(schema).map(type -> OpenApiColumn.builder()
                .setName(name)
                .setSourceName(sourceName)
                .setType(type.type())
                .setSourceType(type.schema())
                // optional properties can be missing from the response, even if they're not nullable
                .setIsNullable(!isResponseRequired)
                .setResponseRequiredPaths(isResponseRequired ? Set.of(httpPath) : Set.of())
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
                .setIsRequestOnly(true)
                .setComment(schema.getDescription())
                .build());
    }

    private static boolean isNullable(Schema<?> schema, boolean defaultValue)
    {
        if (schema.getTypes() != null && schema.getTypes().contains("null")) {
            return true;
        }
        if (getAlternatives(schema).stream().anyMatch(OpenApiSpec::isNullType)) {
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
        Map<HttpPath, JsonPointer> operationPointers = pointers(pathItem, path, ERROR_PATH);
        if (documentErrorPointer.isEmpty()) {
            return operationPointers;
        }
        return pathItem.readOperationsMap().keySet().stream()
                .map(method -> new HttpPath(method, path))
                .collect(toImmutableMap(httpPath -> httpPath, httpPath -> operationPointers.getOrDefault(httpPath, documentErrorPointer.get())));
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
                                    .setMaxPages(getOptionalInt(rawExtension, PAGINATION_MAX_PAGES))
                                    .setMaxResults(getMaxResults(rawExtension))
                                    .setSorting(getSorting(specExtension, rawExtension))
                                    .setSplitStrategy(getSplitStrategy(rawExtension))
                                    .setStrideSplits(getOptionalInt(rawExtension, PAGINATION_STRIDE_SPLITS))
                                    .setProbeLimit(getProbeLimit(rawExtension))
                                    .build();
                        }));
    }

    private static OptionalInt getOptionalInt(Object specExtension, String extensionName)
    {
        // YAML and JSON specs can have the number either as a number or a string
        Object value = specExtension instanceof Map<?, ?> map ? map.get(extensionName) : null;
        if (value == null) {
            return OptionalInt.empty();
        }
        try {
            return OptionalInt.of(value instanceof Number number ? number.intValue() : Integer.parseInt(value.toString()));
        }
        catch (NumberFormatException e) {
            throw new IllegalArgumentException("Invalid value of %s: %s".formatted(extensionName, value), e);
        }
    }

    private static OptionalLong getMaxResults(Object specExtension)
    {
        Object value = specExtension instanceof Map<?, ?> map ? map.get(PAGINATION_MAX_RESULTS) : null;
        if (value == null) {
            return OptionalLong.empty();
        }
        try {
            return OptionalLong.of(value instanceof Number number ? number.longValue() : Long.parseLong(value.toString()));
        }
        catch (NumberFormatException e) {
            throw new IllegalArgumentException("Invalid value of %s: %s".formatted(PAGINATION_MAX_RESULTS, value), e);
        }
    }

    private static Optional<Sorting> getSorting(Map<String, String> specExtension, Object rawExtension)
    {
        String sortParam = specExtension.get(SORT_PARAM);
        if (sortParam == null) {
            return Optional.empty();
        }
        Map<?, ?> sortValues = rawExtension instanceof Map<?, ?> map && map.get(SORT_VALUES) instanceof Map<?, ?> values ? values : ImmutableMap.of();
        ImmutableMap.Builder<String, String> sortParamValues = ImmutableMap.builder();
        ImmutableSet.Builder<String> binaryOrderFields = ImmutableSet.builder();
        for (Map.Entry<?, ?> sortValue : sortValues.entrySet()) {
            String field = sortValue.getKey().toString();
            checkArgument(sortValue.getValue() != null, "Invalid value of %s for %s: expected a value of %s or an object", SORT_VALUES, field, SORT_PARAM);
            if (!(sortValue.getValue() instanceof Map<?, ?> properties)) {
                sortParamValues.put(field, sortValue.getValue().toString());
                continue;
            }
            checkArgument(properties.get(SORT_VALUE) != null, "Invalid value of %s for %s: %s is required", SORT_VALUES, field, SORT_VALUE);
            sortParamValues.put(field, properties.get(SORT_VALUE).toString());
            if (getBoolean(properties, SORT_BINARY_ORDER).orElse(false)) {
                binaryOrderFields.add(field);
            }
        }
        return Optional.of(Sorting.builder()
                .setSortParam(sortParam)
                .setSortValues(sortParamValues.buildOrThrow())
                .setBinaryOrderFields(binaryOrderFields.build())
                .setDirectionParam(specExtension.get(SORT_DIRECTION_PARAM))
                .setAscendingValue(specExtension.get(SORT_ASCENDING_VALUE))
                .setDescendingValue(specExtension.get(SORT_DESCENDING_VALUE))
                .build());
    }

    private static Optional<SplitStrategy> getSplitStrategy(Object specExtension)
    {
        Object value = specExtension instanceof Map<?, ?> map ? map.get(PAGINATION_SPLIT_STRATEGY) : null;
        if (value == null) {
            return Optional.empty();
        }
        // YAML 1.1 parses an unquoted off as false
        if (Boolean.FALSE.equals(value)) {
            return Optional.of(SplitStrategy.OFF);
        }
        Optional<SplitStrategy> splitStrategy = Enums.getIfPresent(SplitStrategy.class, value.toString().trim().toUpperCase(Locale.ENGLISH)).toJavaUtil();
        checkArgument(splitStrategy.isPresent(), "Invalid value of %s: '%s', must be one of: %s", PAGINATION_SPLIT_STRATEGY, value, SPLIT_STRATEGIES);
        return splitStrategy;
    }

    private static Optional<ProbeLimit> getProbeLimit(Object specExtension)
    {
        Object value = specExtension instanceof Map<?, ?> map ? map.get(PAGINATION_PROBE_LIMIT) : null;
        if (value == null) {
            return Optional.empty();
        }
        try {
            return Optional.of(ProbeLimit.fromString(value.toString()));
        }
        catch (IllegalArgumentException e) {
            throw new IllegalArgumentException("Invalid value of %s: %s".formatted(PAGINATION_PROBE_LIMIT, value), e);
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
            Optional<Schema<?>> nullableAlternative = getNullableAlternative(property);
            if (nullableAlternative.isPresent()) {
                // a union of a single type and null, like the ones generated for optional fields by Pydantic, holds values of that type
                return convertType(nullableAlternative.get());
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
            case "integer" -> Optional.of(new TypeTuple(convertInteger(format), property));
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

    private static Optional<Schema<?>> getNullableAlternative(Schema<?> union)
    {
        List<Schema> alternatives = getAlternatives(union);
        List<Schema> nonNullAlternatives = getNonNullAlternatives(union);
        if (nonNullAlternatives.size() != 1 || alternatives.size() == nonNullAlternatives.size()) {
            return Optional.empty();
        }
        return Optional.of(nonNullAlternatives.getFirst());
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

    private static Type convertInteger(String format)
    {
        return switch (format) {
            case "int32" -> INTEGER;
            // unsigned 64-bit values don't fit in a BIGINT
            case "uint64" -> createDecimalType(20, 0);
            case null, default -> BIGINT;
        };
    }

    private static Type convertNumber(String format)
    {
        return switch (format) {
            case "float" -> REAL;
            case "double" -> DOUBLE;
            // the widest decimal, with more fractional digits than most APIs send, and room for large values, like timestamps in milliseconds
            case null, default -> createDecimalType(38, 10);
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
                        (a, _) -> a,
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
                        (a, _) -> a,
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
        return convertType(referenced.get()).map(convertedType -> JsonTrinoConverter.isJsonText(convertedType.schema())
                ? convertedType
                : new TypeTuple(convertedType.type(), referenced.get()));
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
                        // a field next to the results of one operation can be a field of the results of another operation
                        .setIsHidden(sameColumns.stream().allMatch(column -> column.getMetadata().isHidden()))
                        .setIsRequestOnly(sameColumns.stream().allMatch(OpenApiColumn::isRequestOnly))
                        .setResponseRequiredPaths(sameColumns.stream()
                                .flatMap(column -> column.getResponseRequiredPaths().stream())
                                .collect(toImmutableSet()))
                        .setEnvelopePaths(sameColumns.stream()
                                .flatMap(column -> column.getEnvelopePaths().stream())
                                .collect(toImmutableSet()))
                        .setRequiresPredicate(sameColumns.stream()
                                .map(OpenApiColumn::getRequiresPredicate)
                                .flatMap(map -> map.entrySet().stream())
                                .collect(toMap(Map.Entry::getKey, Map.Entry::getValue, (a, _) -> a, LinkedHashMap::new)))
                        .setOptionalPredicate(sameColumns.stream()
                                .map(OpenApiColumn::getOptionalPredicate)
                                .flatMap(map -> map.entrySet().stream())
                                .collect(toMap(Map.Entry::getKey, Map.Entry::getValue, (a, _) -> a, LinkedHashMap::new)))
                        .setParameterStyles(sameColumns.stream()
                                .map(OpenApiColumn::getParameterStyles)
                                .flatMap(map -> map.entrySet().stream())
                                .collect(toMap(Map.Entry::getKey, Map.Entry::getValue, (a, _) -> a, LinkedHashMap::new)))
                        .setParameterNames(sameColumns.stream()
                                .map(OpenApiColumn::getParameterNames)
                                .flatMap(map -> map.entrySet().stream())
                                .collect(toMap(Map.Entry::getKey, Map.Entry::getValue, (a, _) -> a, LinkedHashMap::new)))
                        .setRangeFilters(sameColumns.stream()
                                .map(OpenApiColumn::getRangeFilters)
                                .flatMap(map -> map.entrySet().stream())
                                .collect(toMap(Map.Entry::getKey, Map.Entry::getValue, (a, _) -> a, LinkedHashMap::new)))
                        .setParameterDefaults(sameColumns.stream()
                                .map(OpenApiColumn::getParameterDefaults)
                                .flatMap(map -> map.entrySet().stream())
                                .collect(toMap(Map.Entry::getKey, Map.Entry::getValue, (a, _) -> a, LinkedHashMap::new)))
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

    private static List<OpenApiColumn> allowNullsMissingFromSelectResponses(List<OpenApiColumn> columns, OpenApiTableHandle tableHandle)
    {
        // a field required in the response of one operation can be missing from the responses of other operations used for SELECT,
        // and the operation used depends on the query predicates
        return columns.stream()
                .map(column -> !column.isRequestOnly() && !column.getMetadata().isNullable() && !column.isResponseRequired(tableHandle.getSelectPaths())
                        ? OpenApiColumn.builderFrom(column).setIsNullable(true).build()
                        : column)
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

    public Optional<RateLimitHeaders> getRateLimitHeaders()
    {
        return rateLimitHeaders;
    }
}
