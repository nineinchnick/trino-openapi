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
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.StreamWriteFeature;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.json.JsonMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.POJONode;
import com.fasterxml.jackson.databind.node.TextNode;
import com.fasterxml.jackson.databind.util.RawValue;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.io.CharStreams;
import com.google.common.util.concurrent.RateLimiter;
import com.google.inject.Inject;
import io.airlift.http.client.BodyGenerator;
import io.airlift.http.client.HeaderName;
import io.airlift.http.client.HttpClient;
import io.airlift.http.client.HttpStatus;
import io.airlift.http.client.Request;
import io.airlift.http.client.Response;
import io.airlift.http.client.ResponseHandler;
import io.airlift.log.Logger;
import io.airlift.slice.Slice;
import io.swagger.v3.oas.models.PathItem;
import io.swagger.v3.oas.models.media.Schema;
import io.trino.spi.Page;
import io.trino.spi.TrinoException;
import io.trino.spi.block.Block;
import io.trino.spi.block.SqlMap;
import io.trino.spi.block.SqlRow;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.NullableValue;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.type.BigintType;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.MapType;
import io.trino.spi.type.RowType;
import io.trino.spi.type.StandardTypes;
import io.trino.spi.type.TimestampType;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.UncheckedIOException;
import java.math.BigDecimal;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.OptionalLong;
import java.util.Set;
import java.util.function.Function;
import java.util.function.IntFunction;
import java.util.function.Predicate;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static io.airlift.http.client.HeaderNames.ACCEPT;
import static io.airlift.http.client.HeaderNames.CONTENT_TYPE;
import static io.airlift.http.client.HeaderNames.COOKIE;
import static io.airlift.http.client.HeaderNames.USER_AGENT;
import static com.google.common.net.MediaType.JSON_UTF_8;
import static io.airlift.http.client.StaticBodyGenerator.createStaticBodyGenerator;
import static io.trino.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static io.trino.spi.StandardErrorCode.INVALID_ROW_FILTER;
import static io.trino.spi.type.TypeUtils.writeNativeValue;
import static java.lang.Float.intBitsToFloat;
import static java.lang.Math.min;
import static java.lang.String.format;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.AbstractMap.SimpleEntry;
import static java.util.Objects.requireNonNull;
import static java.util.function.Function.identity;
import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toMap;
import static pl.net.was.OpenApiSpec.ROW_ID;

public class OpenApiClient
{
    private static final Logger log = Logger.get(OpenApiRecordSetProvider.class);
    // small decimals would otherwise be written in the scientific notation, like 1E-8, which some servers can't parse
    private static final ObjectMapper OBJECT_MAPPER = JsonMapper.builder()
            .enable(StreamWriteFeature.WRITE_BIGDECIMAL_AS_PLAIN)
            // doubles can't hold all values of DECIMAL columns, so keep the exact value of every number with a fraction
            .enable(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS)
            .build();
    public static final HeaderName OPENAPI_PATH_HEADER = HeaderName.of("X-Trino-OpenAPI-Path");
    public static final String USER_AGENT_VALUE = "trino-openapi/" + OpenApiClient.class.getPackage().getImplementationVersion();

    private final URI baseUri;

    private final HttpClient httpClient;
    private final OpenApiSpec openApiSpec;
    private final RateLimiter rateLimiter;

    @Inject
    public OpenApiClient(OpenApiConfig config, @ForOpenApi HttpClient httpClient, OpenApiSpec openApiSpec)
    {
        this.baseUri = config.getBaseUri();
        this.httpClient = httpClient;
        this.openApiSpec = openApiSpec;
        if (config.getMaxRequestsPerSecond() != Double.MAX_VALUE) {
            rateLimiter = RateLimiter.create(config.getMaxRequestsPerSecond());
        }
        else {
            rateLimiter = null;
        }
    }

    public Iterable<List<?>> getRows(OpenApiTableHandle table)
    {
        List<HttpPath> selectPaths = table.getSelectPaths();
        checkState(!selectPaths.isEmpty(), "paths are empty");
        Map.Entry<HttpPath, Map<OpenApiColumn, Object>> pathWithParams = selectPath(table, selectPaths, (column) -> getFilter(column, table.getConstraint()));
        HttpPath httpPath = pathWithParams.getKey();
        Optional<OpenApiColumn> pageColumn = openApiSpec.getTables().get(table.getSchemaTableName().getTableName()).stream()
                .filter(OpenApiColumn::isPageNumber)
                .filter(column -> column.getRequiresPredicate().containsKey(httpPath) || column.getOptionalPredicate().containsKey(httpPath))
                .findFirst();
        if (pageColumn.isEmpty() || getFilter(pageColumn.get(), table.getConstraint()) != null) {
            return makeRequest(table, httpPath, pathWithParams.getValue(), getBodyGenerator(table, httpPath), getResponseHandler(table, httpPath)).rows();
        }
        return pageIterator(
                page -> {
                    TupleDomain<ColumnHandle> pageConstraint = TupleDomain.fromFixedValues(Map.of(
                            pageColumn.get().getHandle(),
                            NullableValue.of(
                                    pageColumn.get().getType(),
                                    pageColumn.get().getType() instanceof BigintType ? (long) page : page)));
                    OpenApiTableHandle pageTable = table.cloneWithConstraint(table.getConstraint().isNone() ? pageConstraint : table.getConstraint().intersect(pageConstraint));
                    // the body must be serialized for every page, since it can contain the page number,
                    // and the response handler copies the page number to every row
                    return makeRequest(pageTable, httpPath, pathWithParams.getValue(), getBodyGenerator(pageTable, httpPath), getResponseHandler(pageTable, httpPath));
                },
                0,
                Long.MAX_VALUE,
                1,
                getPageSize(table, httpPath),
                openApiSpec.getPaginations(table.getSchemaTableName()).getOrDefault(httpPath, Pagination.none()).maxPages());
    }

    private OptionalLong getPageSize(OpenApiTableHandle table, HttpPath httpPath)
    {
        Optional<String> limitParam = openApiSpec.getPaginations(table.getSchemaTableName()).getOrDefault(httpPath, Pagination.none()).limitParam();
        if (limitParam.isEmpty()) {
            return OptionalLong.empty();
        }
        Optional<OpenApiColumn> limitColumn = openApiSpec.getTables().get(table.getSchemaTableName().getTableName()).stream()
                // the row ID column has no source name
                .filter(column -> limitParam.get().equals(column.getSourceName()) && isPredicate(column, httpPath, null))
                .findFirst();
        if (limitColumn.isEmpty()) {
            return OptionalLong.empty();
        }
        Schema<?> schema = limitColumn.get().getSourceType();
        Object limit = getFilter(limitColumn.get(), table.getConstraint());
        if (limit == null) {
            limit = schema.getDefault();
        }
        if (!(limit instanceof Number limitNumber)) {
            return OptionalLong.empty();
        }
        // servers often return fewer results than requested when the limit is too high, so it must not exceed the declared maximum
        if (schema.getMaximum() != null) {
            return OptionalLong.of(min(limitNumber.longValue(), schema.getMaximum().longValue()));
        }
        return OptionalLong.of(limitNumber.longValue());
    }

    private JsonResponseHandler getResponseHandler(OpenApiTableHandle table, HttpPath httpPath)
    {
        return new JsonResponseHandler(
                table,
                httpPath,
                openApiSpec.getErrorPointers(table.getSchemaTableName()).get(httpPath),
                openApiSpec.getResultsPointers(table.getSchemaTableName()).get(httpPath),
                openApiSpec.getPaginations(table.getSchemaTableName()).getOrDefault(httpPath, Pagination.none()));
    }

    private BodyGenerator getBodyGenerator(OpenApiTableHandle table, HttpPath httpPath)
    {
        if (!hasRequestBody(httpPath.method())) {
            return null;
        }
        try {
            return createStaticBodyGenerator(toBytes(serializeMap(table, httpPath)));
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private static boolean hasRequestBody(PathItem.HttpMethod method)
    {
        return method == PathItem.HttpMethod.POST || method == PathItem.HttpMethod.PUT || method == PathItem.HttpMethod.PATCH;
    }

    public void insertRows(OpenApiOutputTableHandle table, Page page, int position)
    {
        writeRows(table, "INSERT", table.getTableHandle().getInsertPaths(), page, position, Optional.empty());
    }

    public void updateRows(OpenApiOutputTableHandle table, Page page, int position, Set<OpenApiColumnHandle> updatedColumns)
    {
        writeRows(table, "UPDATE", table.getTableHandle().getUpdatePaths(), page, position, Optional.of(updatedColumns));
    }

    public void deleteRows(OpenApiOutputTableHandle table, Block rowIds, int position)
    {
        table.getTableHandle().checkHasPaths("DELETE", table.getTableHandle().getDeletePaths());
        // don't have to decode the rowId since it's value is copied from the predicate that's still present in the table handle
        Map.Entry<HttpPath, Map<OpenApiColumn, Object>> pathWithParams = selectPath(
                table.getTableHandle(),
                table.getTableHandle().getDeletePaths(),
                (column) -> getFilter(column, table.getTableHandle().getConstraint()));
        HttpPath httpPath = pathWithParams.getKey();
        makeRequest(
                table.getTableHandle(),
                httpPath,
                pathWithParams.getValue(),
                null,
                new AnyResponseHandler(openApiSpec.getErrorPointers(table.getTableHandle().getSchemaTableName()).get(httpPath)));
    }

    private void writeRows(OpenApiOutputTableHandle table, String statement, List<HttpPath> paths, Page page, int position, Optional<Set<OpenApiColumnHandle>> updatedColumns)
    {
        table.getTableHandle().checkHasPaths(statement, paths);
        Function<OpenApiColumn, Object> rowValues = column -> getRowValue(table, page, position, column);
        Map.Entry<HttpPath, Map<OpenApiColumn, Object>> pathWithParams = selectPath(table.getTableHandle(), paths, rowValues);
        HttpPath httpPath = pathWithParams.getKey();
        // a PATCH changes only the fields it includes, and other methods usually replace the whole resource
        ObjectNode body = updatedColumns.isPresent() && httpPath.method() == PathItem.HttpMethod.PATCH
                ? serializeUpdatedColumns(table, httpPath, page, position, updatedColumns.get())
                : serializePage(table, httpPath, page, position);
        try {
            makeRequest(
                    table.getTableHandle(),
                    httpPath,
                    pathWithParams.getValue(),
                    rowValues,
                    createStaticBodyGenerator(toBytes(body)),
                    new AnyResponseHandler(openApiSpec.getErrorPointers(table.getTableHandle().getSchemaTableName()).get(httpPath)));
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    public <T> T makeRequest(
            OpenApiTableHandle table,
            HttpPath httpPath,
            Map<OpenApiColumn, Object> pathParameters,
            BodyGenerator bodyGenerator,
            ResponseHandler<T, RuntimeException> responseHandler)
    {
        return makeRequest(table, httpPath, pathParameters, column -> getFilter(column, table.getConstraint()), bodyGenerator, responseHandler);
    }

    private <T> T makeRequest(
            OpenApiTableHandle table,
            HttpPath httpPath,
            Map<OpenApiColumn, Object> pathParameters,
            Function<OpenApiColumn, Object> valueProvider,
            BodyGenerator bodyGenerator,
            ResponseHandler<T, RuntimeException> responseHandler)
    {
        String uriPath = httpPath.path();
        for (Map.Entry<OpenApiColumn, Object> entry : pathParameters.entrySet()) {
            OpenApiColumn column = entry.getKey();
            String value = getParameterStyle(column, httpPath, ParameterLocation.PATH).serializePathValue(column.getSourceName(), toJsonNode(column, entry.getValue()));
            uriPath = uriPath.replace(format("{%s}", column.getSourceName()), value);
        }
        URI uri;
        try {
            uri = buildUri(baseUri, uriPath, getQueryParams(table, httpPath, valueProvider));
        }
        catch (URISyntaxException e) {
            throw new TrinoException(GENERIC_INTERNAL_ERROR, format("Failed to construct the API URL: %s", e));
        }
        Request.Builder builder = new Request.Builder()
                .setMethod(httpPath.method().name())
                .setBodyGenerator(bodyGenerator)
                .setUri(uri)
                .addHeader(USER_AGENT, USER_AGENT_VALUE)
                .addHeader(CONTENT_TYPE, JSON_UTF_8.toString())
                .addHeader(ACCEPT, JSON_UTF_8.toString())
                .addHeader(OPENAPI_PATH_HEADER, httpPath.path());
        getFilterColumnValues(table, httpPath, ParameterLocation.HEADER, valueProvider)
                .forEach((column, value) -> builder.addHeader(
                        HeaderName.of(column.getSourceName()),
                        getParameterStyle(column, httpPath, ParameterLocation.HEADER).serializeHeaderValue(column.getSourceName(), toJsonNode(column, value))));
        String cookies = getFilterColumnValues(table, httpPath, ParameterLocation.COOKIE, valueProvider).entrySet().stream()
                .map(entry -> entry.getKey().getSourceName() + "=" + getParameterStyle(entry.getKey(), httpPath, ParameterLocation.COOKIE)
                        .serializeCookieValue(entry.getKey().getSourceName(), toJsonNode(entry.getKey(), entry.getValue())))
                .collect(joining("; "));
        if (!cookies.isEmpty()) {
            builder.addHeader(COOKIE, cookies);
        }

        Request request = builder.build();
        log.debug(request.toString());
        if (rateLimiter != null) {
            rateLimiter.acquire();
        }
        return httpClient.execute(request, responseHandler);
    }

    private Map.Entry<HttpPath, Map<OpenApiColumn, Object>> selectPath(OpenApiTableHandle table, List<HttpPath> paths, Function<OpenApiColumn, Object> valueProvider)
    {
        String tableName = table.getSchemaTableName().getTableName();
        List<OpenApiColumn> columns = openApiSpec.getTables().get(tableName);
        Predicate<OpenApiColumn> hasValue = column -> valueProvider.apply(column) != null;
        HttpPath httpPath = PathSelector.selectPath(columns, paths, hasValue)
                .orElseThrow(() -> new TrinoException(INVALID_ROW_FILTER, PathSelector.describeMissingPredicates(columns, paths, hasValue)));
        Map<OpenApiColumn, Object> pathParams = columns.stream()
                .filter(column -> isPredicate(column, httpPath, ParameterLocation.PATH))
                .map(column -> new SimpleEntry<>(column, valueProvider.apply(column)))
                .filter(entry -> entry.getValue() != null)
                .collect(toMap(Map.Entry::getKey, Map.Entry::getValue));
        return Map.entry(httpPath, pathParams);
    }

    private static URI buildUri(URI uri, String path, List<String> queryParams)
            throws URISyntaxException
    {
        // both the path and query parameters are already percent-encoded, according to their parameter styles,
        // and HttpUriBuilder would decode and encode them again, for example turning encoded slashes into path separators
        URI resolved = uri.resolve(new URI(uri.getRawPath() + path));
        if (queryParams.isEmpty()) {
            return resolved;
        }
        return new URI(resolved + "?" + String.join("&", queryParams));
    }

    private List<String> getQueryParams(OpenApiTableHandle table, HttpPath httpPath, Function<OpenApiColumn, Object> valueProvider)
    {
        return getFilterColumnValues(table, httpPath, ParameterLocation.QUERY, valueProvider).entrySet().stream()
                .flatMap(entry -> getParameterStyle(entry.getKey(), httpPath, ParameterLocation.QUERY)
                        .serializeQueryParameter(entry.getKey().getSourceName(), toJsonNode(entry.getKey(), entry.getValue()))
                        .stream())
                .toList();
    }

    private static ParameterStyle getParameterStyle(OpenApiColumn column, HttpPath httpPath, ParameterLocation location)
    {
        return column.getParameterStyles().getOrDefault(httpPath, ParameterStyle.defaultFor(location));
    }

    private static JsonNode toJsonNode(OpenApiColumn column, Object value)
    {
        // values read from pages are already converted, and values read from predicates are native Trino values
        if (value instanceof JsonNode node) {
            return resolveRawValues(node);
        }
        if (value instanceof RawValue rawValue) {
            return parseRawValue(rawValue);
        }
        if (value instanceof Block || value instanceof SqlRow || value instanceof SqlMap) {
            return resolveRawValues((JsonNode) JsonTrinoConverter.convert(writeNativeValue(column.getType(), value), 0, column.getType(), column.getSourceType(), OBJECT_MAPPER));
        }
        return TextNode.valueOf(formatParamValue(value));
    }

    /**
     * Replaces values of union types, which are converted to raw JSON, with parsed JSON nodes,
     * so they're serialized like any other value, instead of as the string representation of RawValue.
     */
    private static JsonNode resolveRawValues(JsonNode node)
    {
        if (node instanceof POJONode pojoNode && pojoNode.getPojo() instanceof RawValue rawValue) {
            return parseRawValue(rawValue);
        }
        if (node.isArray()) {
            ArrayNode result = OBJECT_MAPPER.createArrayNode();
            node.forEach(element -> result.add(resolveRawValues(element)));
            return result;
        }
        if (node.isObject()) {
            ObjectNode result = OBJECT_MAPPER.createObjectNode();
            node.properties().forEach(field -> result.set(field.getKey(), resolveRawValues(field.getValue())));
            return result;
        }
        return node;
    }

    private static JsonNode parseRawValue(RawValue rawValue)
    {
        String text = rawValue.rawValue().toString();
        try {
            JsonNode node = OBJECT_MAPPER.readTree(text);
            if (!node.isMissingNode()) {
                return node;
            }
        }
        catch (JsonProcessingException _) {
            // union columns are VARCHAR, so they can also hold plain text, which is sent as-is
        }
        return TextNode.valueOf(text);
    }

    private static String formatParamValue(Object value)
    {
        // BigDecimal.toString() uses the scientific notation for small values
        if (value instanceof BigDecimal decimalValue) {
            return decimalValue.toPlainString();
        }
        return value.toString();
    }

    private Map<String, Object> getFilterValues(OpenApiTableHandle table, HttpPath httpPath, ParameterLocation in)
    {
        return getFilterColumnValues(table, httpPath, in, column -> getFilter(column, table.getConstraint())).entrySet().stream()
                .collect(toMap(entry -> entry.getKey().getSourceName(), Map.Entry::getValue));
    }

    private Map<String, Object> getRecordFilterValues(OpenApiTableHandle table, HttpPath httpPath)
    {
        // records need the Trino native representation of values, not the one used to encode request parameters
        String tableName = table.getSchemaTableName().getTableName();
        return openApiSpec.getTables().get(tableName).stream()
                .filter(column -> isPredicate(column, httpPath, null))
                .map(column -> new SimpleEntry<>(column.getSourceName(), getNativeFilter(column, table.getConstraint())))
                .filter(entry -> entry.getValue() != null)
                .collect(toMap(Map.Entry::getKey, Map.Entry::getValue));
    }

    private Map<OpenApiColumn, Object> getFilterColumnValues(OpenApiTableHandle table, HttpPath httpPath, ParameterLocation in, Function<OpenApiColumn, Object> valueProvider)
    {
        checkState(!ParameterLocation.PATH.equals(in), "getFilterValues cannot be used for path parameters");
        String tableName = table.getSchemaTableName().getTableName();
        List<OpenApiColumn> columns = openApiSpec.getTables().get(tableName);
        return columns.stream()
                .filter(column -> isPredicate(column, httpPath, in))
                .map(column -> {
                    Object value = valueProvider.apply(column);
                    if (value == null && isRequiredPredicate(column, httpPath, in)) {
                        throw new TrinoException(INVALID_ROW_FILTER, "Missing required constraint for " + column.getName());
                    }
                    return new SimpleEntry<>(column, value);
                })
                .filter(entry -> entry.getValue() != null)
                .collect(toMap(Map.Entry::getKey, Map.Entry::getValue));
    }

    private static boolean isPredicate(OpenApiColumn column, HttpPath httpPath, ParameterLocation in)
    {
        return isRequiredPredicate(column, httpPath, in) || isOptionalPredicate(column, httpPath, in);
    }

    private static boolean isRequiredPredicate(OpenApiColumn column, HttpPath httpPath, ParameterLocation in)
    {
        ParameterLocation requiredIn = column.getRequiresPredicate().get(httpPath);
        return requiredIn != null && (in == null || requiredIn.equals(in));
    }

    private static boolean isOptionalPredicate(OpenApiColumn column, HttpPath httpPath, ParameterLocation in)
    {
        ParameterLocation optionalIn = column.getOptionalPredicate().get(httpPath);
        return optionalIn != null && (in == null || optionalIn.equals(in));
    }

    private static Object getRequiredFilter(OpenApiColumn column, TupleDomain<ColumnHandle> constraint)
    {
        Object value = getFilter(column, constraint);
        if (value == null) {
            throw new TrinoException(INVALID_ROW_FILTER, "Missing required constraint for " + column.getName());
        }
        return value;
    }

    private static Object getFilter(OpenApiColumn column, TupleDomain<ColumnHandle> constraint)
    {
        Object value = getNativeFilter(column, constraint);
        if (value == null) {
            return null;
        }
        return switch (column.getType().getBaseName()) {
            case StandardTypes.BIGINT, StandardTypes.INTEGER, StandardTypes.SMALLINT, StandardTypes.TINYINT -> value;
            case StandardTypes.REAL -> intBitsToFloat(((Long) value).intValue());
            case StandardTypes.DOUBLE -> (Double) value;
            case StandardTypes.DECIMAL -> JsonTrinoConverter.toBigDecimal(value, (DecimalType) column.getType());
            case StandardTypes.VARCHAR -> ((Slice) value).toStringUtf8();
            case StandardTypes.DATE -> DateTimeConverter.formatDate((Long) value, column.getSourceType());
            case StandardTypes.TIMESTAMP -> DateTimeConverter.formatTimestamp(value, (TimestampType) column.getType(), column.getSourceType());
            case StandardTypes.BOOLEAN -> value;
            case StandardTypes.MAP -> (SqlMap) value;
            case StandardTypes.ARRAY -> (Block) value;
            case StandardTypes.ROW -> (SqlRow) value;
            default -> throw new TrinoException(INVALID_ROW_FILTER, "Unexpected constraint for " + column.getName() + "(" + column.getType().getBaseName() + ")");
        };
    }

    private static Object getNativeFilter(OpenApiColumn column, TupleDomain<ColumnHandle> constraint)
    {
        requireNonNull(column, "column is null");
        Domain domain = null;
        if (constraint.getDomains().isPresent()) {
            domain = constraint.getDomains().get().get(column.getHandle());
        }
        if (domain == null || domain.isOnlyNull()) {
            return null;
        }
        return domain.getSingleValue();
    }

    private static Object getRowValue(OpenApiOutputTableHandle table, Page page, int position, OpenApiColumn column)
    {
        Object value = getFilter(column, page, position, table.getColumns().indexOf(column.getHandle()));
        if (value != null) {
            return value;
        }
        // hidden columns are not included in pages, but they could have been used as predicates, like in an UPDATE
        return getFilter(column, table.getTableHandle().getConstraint());
    }

    private static Object getFilter(OpenApiColumn column, Page page, int position, int channel)
    {
        requireNonNull(column, "column is null");
        requireNonNull(page, "page is null");
        if (channel < 0) {
            return null;
        }
        Block block = page.getBlock(channel);
        if (block.isNull(position)) {
            return null;
        }
        return JsonTrinoConverter.convert(block, position, column.getType(), column.getSourceType(), OBJECT_MAPPER);
    }

    public ObjectNode serializePage(OpenApiOutputTableHandle table, HttpPath httpPath, Page page, int position)
    {
        ObjectNode node = OBJECT_MAPPER.createObjectNode();
        String tableName = table.getTableHandle().getSchemaTableName().getTableName();
        Map<OpenApiColumnHandle, OpenApiColumn> columns = openApiSpec.getTables().get(tableName).stream()
                .collect(toImmutableMap(OpenApiColumn::getHandle, identity()));
        for (int channel = 0; channel < table.getColumns().size(); channel++) {
            Block block = page.getBlock(channel);
            if (block.isNull(position)) {
                continue;
            }

            OpenApiColumn column = columns.get(table.getColumns().get(channel));
            // all fields from a request object param should be marked as predicates
            if (!isPredicate(column, httpPath, ParameterLocation.BODY)) {
                continue;
            }
            Object value = JsonTrinoConverter.convert(block, position, column.getType(), column.getSourceType(), OBJECT_MAPPER);
            JsonTrinoConverter.putValue(node, column.getSourceName(), value);
        }
        return node;
    }

    private ObjectNode serializeUpdatedColumns(OpenApiOutputTableHandle table, HttpPath httpPath, Page page, int position, Set<OpenApiColumnHandle> updatedColumns)
    {
        ObjectNode node = OBJECT_MAPPER.createObjectNode();
        String tableName = table.getTableHandle().getSchemaTableName().getTableName();
        Map<OpenApiColumnHandle, OpenApiColumn> columns = openApiSpec.getTables().get(tableName).stream()
                .collect(toImmutableMap(OpenApiColumn::getHandle, identity()));
        for (int channel = 0; channel < table.getColumns().size(); channel++) {
            OpenApiColumnHandle columnHandle = table.getColumns().get(channel);
            OpenApiColumn column = columns.get(columnHandle);
            if (!isPredicate(column, httpPath, ParameterLocation.BODY)) {
                continue;
            }
            boolean isUpdated = updatedColumns.contains(columnHandle);
            Block block = page.getBlock(channel);
            // required fields, like identifiers, must be sent even if they're not updated
            if (!isUpdated && (block.isNull(position) || !isRequiredPredicate(column, httpPath, ParameterLocation.BODY))) {
                continue;
            }
            // updating a column to NULL sends an explicit null, which clears the field
            Object value = block.isNull(position) ? null : JsonTrinoConverter.convert(block, position, column.getType(), column.getSourceType(), OBJECT_MAPPER);
            JsonTrinoConverter.putValue(node, column.getSourceName(), value);
        }
        return node;
    }

    public ObjectNode serializeMap(OpenApiTableHandle table, HttpPath httpPath)
    {
        String tableName = table.getSchemaTableName().getTableName();
        List<OpenApiColumn> columns = openApiSpec.getTables().get(tableName);

        ObjectNode node = OBJECT_MAPPER.createObjectNode();
        for (OpenApiColumn column : columns) {
            if (!isPredicate(column, httpPath, ParameterLocation.BODY)) {
                continue;
            }
            Object value = getFilter(column, table.getConstraint());
            if (value == null) {
                if (isRequiredPredicate(column, httpPath, ParameterLocation.BODY)) {
                    throw new TrinoException(INVALID_ROW_FILTER, "Missing required constraint for " + column.getName());
                }
                continue;
            }
            if (value instanceof Block block) {
                value = JsonTrinoConverter.convert(block, 0, column.getType(), column.getSourceType(), OBJECT_MAPPER);
            }
            else if (value instanceof String stringValue && JsonTrinoConverter.isUnion(column.getSourceType())) {
                value = new RawValue(stringValue);
            }
            else if (value instanceof SqlRow sqlRow) {
                ObjectNode rowNode = OBJECT_MAPPER.createObjectNode();
                JsonTrinoConverter.convertRow(rowNode, sqlRow, (RowType) column.getType(), column.getSourceType(), OBJECT_MAPPER);
                value = rowNode;
            }
            else if (value instanceof SqlMap sqlMap) {
                value = JsonTrinoConverter.convertMap(sqlMap, (MapType) column.getType(), column.getSourceType(), OBJECT_MAPPER);
            }
            JsonTrinoConverter.putValue(node, column.getSourceName(), value);
        }
        return node;
    }

    private record ResultsPage(Iterable<List<?>> rows, OptionalLong totalResults, OptionalLong totalPages, Optional<Boolean> hasMore) {}

    private class JsonResponseHandler
            implements ResponseHandler<ResultsPage, RuntimeException>
    {
        private final OpenApiTableHandle table;
        private final HttpPath httpPath;
        private final JsonPointer errorPointer;
        private final JsonPointer resultsPointer;
        private final Pagination pagination;

        JsonResponseHandler(OpenApiTableHandle table, HttpPath httpPath, JsonPointer errorPointer, JsonPointer resultsPointer, Pagination pagination)
        {
            this.table = requireNonNull(table, "table is null");
            this.httpPath = requireNonNull(httpPath, "httpPath is null");
            this.errorPointer = errorPointer;
            this.resultsPointer = resultsPointer;
            this.pagination = requireNonNull(pagination, "pagination is null");
        }

        @Override
        public ResultsPage handleException(Request request, Exception exception)
        {
            throw new RuntimeException(exception);
        }

        @Override
        public ResultsPage handle(Request request, Response response)
        {
            if (response.getStatusCode() == HttpStatus.NOT_FOUND.code()) {
                return new ResultsPage(List.of(), OptionalLong.empty(), OptionalLong.empty(), Optional.empty());
            }
            String result = "";
            try {
                result = CharStreams.toString(new InputStreamReader(response.getInputStream(), UTF_8));
            }
            catch (IOException e) {
                throw new RuntimeException(e);
            }
            log.debug("Received response code " + response.getStatusCode() + ": " + result);
            if (response.getStatusCode() != HttpStatus.OK.code()) {
                errorResponse(response.getStatusCode(), result, errorPointer);
            }

            JsonNode jsonNode;
            try {
                jsonNode = OBJECT_MAPPER.readTree(result);
            }
            catch (JsonProcessingException ex) {
                String message = format("Could not marshal JSON from API response: %s", result);
                throw new TrinoException(GENERIC_INTERNAL_ERROR, message, ex);
            }
            return new ResultsPage(
                    convertJson(table, httpPath, resultsPointer, jsonNode),
                    getLong(jsonNode, pagination.totalResultsPointer()),
                    getLong(jsonNode, pagination.totalPagesPointer()),
                    getBoolean(jsonNode, pagination.hasMorePointer()));
        }

        private static OptionalLong getLong(JsonNode jsonNode, JsonPointer pointer)
        {
            if (JsonPointer.empty().equals(pointer)) {
                return OptionalLong.empty();
            }
            JsonNode valueNode = jsonNode.at(pointer);
            if (!valueNode.canConvertToExactIntegral() || !valueNode.canConvertToLong()) {
                return OptionalLong.empty();
            }
            return OptionalLong.of(valueNode.asLong());
        }

        private static Optional<Boolean> getBoolean(JsonNode jsonNode, JsonPointer pointer)
        {
            if (JsonPointer.empty().equals(pointer)) {
                return Optional.empty();
            }
            JsonNode valueNode = jsonNode.at(pointer);
            if (!valueNode.isBoolean()) {
                return Optional.empty();
            }
            return Optional.of(valueNode.booleanValue());
        }
    }

    private static void errorResponse(int statusCode, String result, JsonPointer errorPointer)
    {
        if (errorPointer != null) {
            try {
                JsonNode errorNode = OBJECT_MAPPER.readTree(result).at(errorPointer);
                String message = format("Server responded with error %s: %s", statusCode, errorNode);
                throw new TrinoException(GENERIC_INTERNAL_ERROR, message, new IllegalArgumentException(result));
            }
            catch (JsonProcessingException ex) {
                // ignore, so a generic exception will be thrown
            }
        }
        String message = format("Server responded with error %s", statusCode);
        throw new TrinoException(GENERIC_INTERNAL_ERROR, message, new IllegalArgumentException(result));
    }

    private Iterable<List<?>> convertJson(OpenApiTableHandle table, HttpPath httpPath, JsonPointer resultsPointer, JsonNode jsonNode)
    {
        ImmutableList.Builder<List<?>> resultRecordsBuilder = ImmutableList.builder();

        String rowId = getFilterValues(table, httpPath, null).values().stream().findFirst().map(Object::toString).orElse(null);
        Map<String, Object> recordParams = getRecordFilterValues(table, httpPath);

        if (jsonNode instanceof ArrayNode arrayNode) {
            for (JsonNode jsonRecord : arrayNode) {
                resultRecordsBuilder.addAll(convertJsonToRecords(table, httpPath, resultsPointer, rowId, recordParams, jsonRecord));
            }
        }
        else {
            resultRecordsBuilder.addAll(convertJsonToRecords(table, httpPath, resultsPointer, rowId, recordParams, jsonNode));
        }

        return resultRecordsBuilder.build();
    }

    private Iterable<List<?>> convertJsonToRecords(OpenApiTableHandle table, HttpPath httpPath, JsonPointer resultsPointer, String rowId, Map<String, Object> recordParams, JsonNode jsonNode)
    {
        if (!jsonNode.isObject()) {
            throw new TrinoException(GENERIC_INTERNAL_ERROR, format("JsonNode is not an object: %s", jsonNode));
        }

        Iterable<JsonNode> resultNodes = List.of(jsonNode);
        List<OpenApiColumn> columns = openApiSpec.getTables().get(table.getSchemaTableName().getTableName());
        if (resultsPointer != null && !JsonPointer.empty().equals(resultsPointer)) {
            resultNodes = getResultNodes(jsonNode.at(resultsPointer));
        }

        ImmutableList.Builder<List<?>> resultRecordsBuilder = ImmutableList.builder();
        for (JsonNode resultNode : resultNodes) {
            List<Object> recordBuilder = new ArrayList<>();
            for (OpenApiColumn column : columns) {
                if (column.getName().equals(ROW_ID)) {
                    // TODO this is dangerous, make it configurable and required?
                    recordBuilder.add(rowId);
                    continue;
                }
                String parameterName = column.getSourceName();
                // request parameters are usually not included in the response, so copy them from predicates
                if (isPredicate(column, httpPath, null) && !(column.getName().equals(column.getSourceName()) && resultNode.has(parameterName))) {
                    recordBuilder.add(recordParams.getOrDefault(parameterName, null));
                    continue;
                }
                recordBuilder.add(
                        JsonTrinoConverter.convertToRecordValue(
                                resultNode.get(parameterName),
                                column.getType(),
                                column.getSourceType()));
            }
            resultRecordsBuilder.add(recordBuilder);
        }

        return resultRecordsBuilder.build();
    }

    private static Iterable<JsonNode> getResultNodes(JsonNode resultNode)
    {
        if (resultNode instanceof ArrayNode) {
            return resultNode;
        }
        // results can be a union of a single object and an array of objects
        if (resultNode.isObject()) {
            return List.of(resultNode);
        }
        throw new IllegalArgumentException("Result path points to a node that's not an array");
    }

    private byte[] toBytes(JsonNode rootNode)
            throws IOException
    {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        OBJECT_MAPPER.createGenerator(baos).writeTree(rootNode);
        byte[] output = baos.toByteArray();
        log.debug("Request body: " + new String(output, UTF_8));
        return output;
    }

    private static class AnyResponseHandler
            implements ResponseHandler<Void, RuntimeException>
    {
        private final JsonPointer errorPointer;

        AnyResponseHandler(JsonPointer errorPointer)
        {
            this.errorPointer = errorPointer;
        }

        @Override
        public Void handleException(Request request, Exception exception)
        {
            throw new RuntimeException(exception);
        }

        @Override
        public Void handle(Request request, Response response)
        {
            String result = "";
            try {
                result = CharStreams.toString(new InputStreamReader(response.getInputStream(), UTF_8));
            }
            catch (IOException e) {
                throw new RuntimeException(e);
            }
            log.debug("Received response code " + response.getStatusCode() + ": " + result);
            // servers often respond to writes with 201 Created, 202 Accepted, or 204 No Content
            if (HttpStatus.familyForStatusCode(response.getStatusCode()) != HttpStatus.Family.SUCCESSFUL) {
                errorResponse(response.getStatusCode(), result, errorPointer);
            }
            return null;
        }
    }

    private Iterable<List<?>> pageIterator(
            IntFunction<ResultsPage> getter,
            int offset,
            long limit,
            int pageIncrement,
            OptionalLong pageSize,
            OptionalInt maxPages)
    {
        return () -> new Iterator<>()
        {
            long resultSize;
            long resultLimit = limit;
            int page = offset + 1;
            int fetchedPages;
            boolean isLastPage;
            Iterator<List<?>> rows;

            @Override
            public boolean hasNext()
            {
                if (rows != null && rows.hasNext()) {
                    return true;
                }
                if (isLastPage || resultSize >= resultLimit) {
                    return false;
                }
                ResultsPage resultsPage = getter.apply(page);
                int fetchedPage = page;
                page += pageIncrement;
                fetchedPages++;
                // stop on the last page, even if it's not empty, because servers might return the last page for every page number past it
                resultsPage.totalResults().ifPresent(totalResults -> resultLimit = min(resultLimit, totalResults));
                if (pageSize.isPresent() && Iterables.size(resultsPage.rows()) < pageSize.getAsLong()) {
                    isLastPage = true;
                }
                if (resultsPage.totalPages().isPresent() && fetchedPage >= resultsPage.totalPages().getAsLong()) {
                    isLastPage = true;
                }
                if (resultsPage.hasMore().isPresent() && !resultsPage.hasMore().get()) {
                    isLastPage = true;
                }
                if (maxPages.isPresent() && fetchedPages >= maxPages.getAsInt()) {
                    isLastPage = true;
                }
                rows = resultsPage.rows().iterator();
                return rows.hasNext();
            }

            @Override
            public List<?> next()
            {
                resultSize += 1;
                return rows.next();
            }
        };
    }
}
