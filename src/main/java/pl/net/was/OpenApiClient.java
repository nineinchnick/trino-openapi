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
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.util.RawValue;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.CharStreams;
import com.google.common.util.concurrent.RateLimiter;
import com.google.inject.Inject;
import io.airlift.http.client.BodyGenerator;
import io.airlift.http.client.HttpClient;
import io.airlift.http.client.HttpStatus;
import io.airlift.http.client.HttpUriBuilder;
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
import io.trino.spi.type.Int128;
import io.trino.spi.type.RowType;
import io.trino.spi.type.StandardTypes;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.UncheckedIOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.text.NumberFormat;
import java.time.Instant;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.function.IntFunction;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static com.google.common.net.HttpHeaders.ACCEPT;
import static com.google.common.net.HttpHeaders.CONTENT_TYPE;
import static com.google.common.net.HttpHeaders.USER_AGENT;
import static com.google.common.net.MediaType.JSON_UTF_8;
import static io.airlift.http.client.StaticBodyGenerator.createStaticBodyGenerator;
import static io.trino.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static io.trino.spi.StandardErrorCode.INVALID_ROW_FILTER;
import static io.trino.spi.type.DecimalConversions.longDecimalToDouble;
import static io.trino.spi.type.DecimalConversions.shortDecimalToDouble;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static java.lang.Float.intBitsToFloat;
import static java.lang.String.format;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.AbstractMap.SimpleEntry;
import static java.util.Comparator.comparing;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.groupingBy;
import static java.util.stream.Collectors.toMap;
import static pl.net.was.OpenApiSpec.ROW_ID;

public class OpenApiClient
{
    private static final Logger log = Logger.get(OpenApiRecordSetProvider.class);
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    public static final String USER_AGENT_VALUE = "trino-openapi/" + OpenApiClient.class.getPackage().getImplementationVersion();

    private final URI baseUri;

    private final HttpClient httpClient;
    private final OpenApiSpec openApiSpec;
    private final RateLimiter rateLimiter;

    static final long[] POWERS_OF_TEN = {
            1L,
            10L,
            100L,
            1000L,
            10_000L,
            100_000L,
            1_000_000L,
            10_000_000L,
            100_000_000L,
            1_000_000_000L,
            10_000_000_000L,
            100_000_000_000L,
            1000_000_000_000L
    };

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
        PathItem.HttpMethod method = table.getSelectMethod();
        Optional<OpenApiColumn> pageColumn = openApiSpec.getTables().get(table.getSchemaTableName().getTableName()).stream()
                .filter(OpenApiColumn::isPageNumber)
                .findFirst();
        List<String> selectPaths = table.getSelectPaths();
        checkState(!selectPaths.isEmpty(), "paths are empty");
        Map.Entry<String, Map<String, Object>> pathWithParams = selectPath(table, method, selectPaths, (column) -> getFilter(column, table.getConstraint()));
        HttpPath httpPath = new HttpPath(method, pathWithParams.getKey());
        BodyGenerator bodyGenerator = getBodyGenerator(table, httpPath);
        JsonResponseHandler responseHandler = new JsonResponseHandler(table, httpPath, openApiSpec.getErrorPointers(table.getSchemaTableName()).get(httpPath));
        if (pageColumn.isEmpty() || getFilter(pageColumn.get(), table.getConstraint()) != null) {
            return makeRequest(table, httpPath, pathWithParams.getValue(), bodyGenerator, responseHandler);
        }
        return pageIterator(
                page -> {
                    TupleDomain<ColumnHandle> pageConstraint = TupleDomain.fromFixedValues(Map.of(
                            pageColumn.get().getHandle(),
                            NullableValue.of(
                                    pageColumn.get().getType(),
                                    pageColumn.get().getType() instanceof BigintType ? (long) page : page)));
                    OpenApiTableHandle pageTable = table.cloneWithConstraint(table.getConstraint().intersect(pageConstraint));
                    return makeRequest(pageTable, httpPath, pathWithParams.getValue(), bodyGenerator, responseHandler);
                },
                0,
                Integer.MAX_VALUE,
                1);
    }

    private BodyGenerator getBodyGenerator(OpenApiTableHandle table, HttpPath httpPath)
    {
        if (httpPath.method() != PathItem.HttpMethod.POST) {
            return null;
        }
        try {
            return createStaticBodyGenerator(toBytes(serializeMap(table, httpPath)));
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    public void postRows(OpenApiOutputTableHandle table, Page page, int position)
    {
        List<OpenApiColumn> columns = openApiSpec.getTables().get(table.getTableHandle().getSchemaTableName().getTableName());
        Map.Entry<String, Map<String, Object>> pathWithParams = selectPath(
                table.getTableHandle(),
                PathItem.HttpMethod.POST,
                table.getTableHandle().getInsertPaths(),
                (column) -> getFilter(column, page, position, columns.indexOf(column)));
        HttpPath httpPath = new HttpPath(PathItem.HttpMethod.POST, pathWithParams.getKey());
        postRows(table, httpPath, pathWithParams.getValue(), serializePage(table.getTableHandle(), httpPath, page, position));
    }

    public void postRows(OpenApiOutputTableHandle table, HttpPath httpPath, Map<String, Object> pathParams, JsonNode data)
    {
        try {
            makeRequest(
                    table.getTableHandle(),
                    httpPath,
                    pathParams,
                    createStaticBodyGenerator(toBytes(data)),
                    new AnyResponseHandler(openApiSpec.getErrorPointers(table.getTableHandle().getSchemaTableName()).get(httpPath)));
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    public void putRows(OpenApiOutputTableHandle table, Page page, int position)
    {
        List<OpenApiColumn> columns = openApiSpec.getTables().get(table.getTableHandle().getSchemaTableName().getTableName());
        Map.Entry<String, Map<String, Object>> pathWithParams = selectPath(
                table.getTableHandle(),
                PathItem.HttpMethod.PUT,
                table.getTableHandle().getUpdatePaths(),
                (column) -> getFilter(column, page, position, columns.indexOf(column)));
        HttpPath httpPath = new HttpPath(PathItem.HttpMethod.PUT, pathWithParams.getKey());
        putRows(table, httpPath, pathWithParams.getValue(), serializePage(table.getTableHandle(), httpPath, page, position));
    }

    public void putRows(OpenApiOutputTableHandle table, HttpPath httpPath, Map<String, Object> pathParams, JsonNode data)
    {
        try {
            makeRequest(
                    table.getTableHandle(),
                    httpPath,
                    pathParams,
                    createStaticBodyGenerator(toBytes(data)),
                    new AnyResponseHandler(openApiSpec.getErrorPointers(table.getTableHandle().getSchemaTableName()).get(httpPath)));
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    public void deleteRows(OpenApiOutputTableHandle table, Block rowIds, int position)
    {
        // don't have to decode the rowId since it's value is copied from the predicate that's still present in the table handle
        Map.Entry<String, Map<String, Object>> pathWithParams = selectPath(
                table.getTableHandle(),
                PathItem.HttpMethod.DELETE,
                table.getTableHandle().getDeletePaths(),
                (column) -> getFilter(column, table.getTableHandle().getConstraint()));
        HttpPath httpPath = new HttpPath(PathItem.HttpMethod.DELETE, pathWithParams.getKey());
        makeRequest(
                table.getTableHandle(),
                httpPath,
                pathWithParams.getValue(),
                null,
                new AnyResponseHandler(openApiSpec.getErrorPointers(table.getTableHandle().getSchemaTableName()).get(httpPath)));
    }

    public <T> T makeRequest(
            OpenApiTableHandle table,
            HttpPath httpPath,
            Map<String, Object> parameters,
            BodyGenerator bodyGenerator,
            ResponseHandler<T, RuntimeException> responseHandler)
    {
        String uriPath = httpPath.path();
        for (Map.Entry<String, Object> entry : parameters.entrySet()) {
            uriPath = uriPath.replace(format("{%s}", entry.getKey()), entry.getValue().toString());
        }
        URI uri;
        try {
            uri = buildUri(baseUri, uriPath, getFilterValues(table, httpPath, ParameterLocation.QUERY));
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
                .addHeader("X-Trino-OpenAPI-Path", httpPath.path());
        getFilterValues(table, httpPath, ParameterLocation.HEADER)
                .forEach((key, value) -> builder.addHeader(key, value.toString()));

        Request request = builder.build();
        log.debug(request.toString());
        if (rateLimiter != null) {
            rateLimiter.acquire();
        }
        return httpClient.execute(request, responseHandler);
    }

    private Map.Entry<String, Map<String, Object>> selectPath(OpenApiTableHandle table, PathItem.HttpMethod method, List<String> paths, Function<OpenApiColumn, Object> valueProvider)
    {
        String tableName = table.getSchemaTableName().getTableName();
        List<OpenApiColumn> columns = openApiSpec.getTables().get(tableName);
        // ignore paths where required params are not present
        List<String> invalidPaths = columns.stream()
                .flatMap(column -> column.getRequiresPredicate().entrySet().stream()
                        .filter(entry -> entry.getValue() == ParameterLocation.PATH)
                        .map(entry -> new SimpleEntry<>(entry.getKey(), valueProvider.apply(column)))
                        .filter(entry -> entry.getValue() == null)
                        .map(entry -> entry.getKey().path()))
                .toList();
        // index params (created from column predicates) by path
        Map<HttpPath, Map<String, Object>> pathParams = columns.stream()
                .flatMap(column -> Stream.concat(
                                column.getOptionalPredicate().entrySet().stream(),
                                column.getRequiresPredicate().entrySet().stream())
                        .filter(entry -> entry.getValue() == ParameterLocation.PATH)
                        .map(entry -> new SimpleEntry<>(
                                entry.getKey(),
                                valueProvider.apply(column)))
                        .filter(entry -> entry.getValue() != null)
                        .map(entry -> Map.entry(entry.getKey(), Map.entry(column, entry.getValue()))))
                .collect(groupingBy(
                        Map.Entry::getKey,
                        toMap(
                                entry -> entry.getValue().getKey().getSourceName(),
                                entry -> entry.getValue().getValue())));

        // pick the path that has the highest number of params present in query predicates, and the shortest if there are ties
        Map<HttpPath, Integer> pathParamCounts = pathParams.entrySet().stream()
                .collect(toImmutableMap(Map.Entry::getKey, entry -> entry.getValue().size()));
        Comparator<Object> comparator = comparing(path -> requireNonNull(pathParamCounts.getOrDefault(new HttpPath(method, (String) path), 0)))
                .thenComparing(path -> -((String) path).length());
        String selectedPath = paths.stream()
                .filter(path -> !invalidPaths.contains(path))
                .max(comparator)
                .orElseThrow();
        return Map.entry(selectedPath, pathParams.getOrDefault(new HttpPath(method, selectedPath), ImmutableMap.of()));
    }

    private static URI buildUri(URI uri, String path, Map<String, Object> queryParams)
            throws URISyntaxException
    {
        URI oldUri = uri.resolve(uri.getPath() + path);
        HttpUriBuilder builder = HttpUriBuilder.uriBuilderFrom(oldUri);
        queryParams.forEach((key, value) -> builder.addParameter(key, encodeQueryParamValue(value)));
        return builder.build();
    }

    private static Iterable<String> encodeQueryParamValue(Object value)
    {
        if (value instanceof Block block) {
            return IntStream
                    .range(0, block.getPositionCount())
                    .mapToObj(i -> VARCHAR.getSlice(block, i).toStringUtf8())
                    .toList();
        }
        return List.of(value.toString());
    }

    private Map<String, Object> getFilterValues(OpenApiTableHandle table, HttpPath httpPath, ParameterLocation in)
    {
        checkState(!ParameterLocation.PATH.equals(in), "getFilterValues cannot be used for path parameters");
        String tableName = table.getSchemaTableName().getTableName();
        List<OpenApiColumn> columns = openApiSpec.getTables().get(tableName);
        return columns.stream()
                .filter(column -> isPredicate(column, httpPath, in))
                .map(column -> {
                    Object value = getFilter(column, table.getConstraint());
                    if (value == null && isRequiredPredicate(column, httpPath, in)) {
                        throw new TrinoException(INVALID_ROW_FILTER, "Missing required constraint for " + column.getName());
                    }
                    return new SimpleEntry<>(column.getSourceName(), value);
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
        requireNonNull(column, "column is null");
        Domain domain = null;
        if (constraint.getDomains().isPresent()) {
            domain = constraint.getDomains().get().get(column.getHandle());
        }
        if (domain == null || domain.isOnlyNull()) {
            return null;
        }
        return switch (column.getType().getBaseName()) {
            case StandardTypes.BIGINT, StandardTypes.INTEGER, StandardTypes.SMALLINT, StandardTypes.TINYINT -> domain.getSingleValue();
            case StandardTypes.REAL -> intBitsToFloat(((Long) domain.getSingleValue()).intValue());
            case StandardTypes.DOUBLE -> (Double) domain.getSingleValue();
            case StandardTypes.DECIMAL -> toDecimal(domain.getSingleValue(), (DecimalType) column.getType());
            case StandardTypes.VARCHAR -> ((Slice) domain.getSingleValue()).toStringUtf8();
            case StandardTypes.DATE -> toDate((Long) domain.getSingleValue(), column.getSourceType());
            case StandardTypes.TIMESTAMP -> toDatetime((Long) domain.getSingleValue(), column.getSourceType());
            case StandardTypes.BOOLEAN -> domain.getSingleValue();
            case StandardTypes.MAP -> (SqlMap) domain.getSingleValue();
            case StandardTypes.ARRAY -> (Block) domain.getSingleValue();
            case StandardTypes.ROW -> (SqlRow) domain.getSingleValue();
            default -> throw new TrinoException(INVALID_ROW_FILTER, "Unexpected constraint for " + column.getName() + "(" + column.getType().getBaseName() + ")");
        };
    }

    private static Object getFilter(OpenApiColumn column, Page page, int position, int channel)
    {
        requireNonNull(column, "column is null");
        requireNonNull(page, "page is null");
        Block block = page.getBlock(channel);
        if (block.isNull(position)) {
            return null;
        }
        return JsonTrinoConverter.convert(block, position, column.getType(), column.getSourceType(), OBJECT_MAPPER);
    }

    private static String toDecimal(Object object, DecimalType type)
    {
        double value;
        if (type.isShort()) {
            value = shortDecimalToDouble((Long) object, POWERS_OF_TEN[type.getScale()]);
        }
        else {
            value = longDecimalToDouble((Int128) object, type.getScale());
        }
        return NumberFormat.getInstance().format(value);
    }

    private static String toDate(long days, Schema<?> schema)
    {
        String format = schema.getFormat();
        if (format.equals("date")) {
            format = "yyyy-MM-dd";
        }
        DateTimeFormatter dateFormatter = DateTimeFormatter.ofPattern(format);
        return dateFormatter.format(LocalDate.ofEpochDay(days));
    }

    private static String toDatetime(long millis, Schema<?> schema)
    {
        String format = schema.getFormat();
        if (format.equals("date-time")) {
            format = "yyyy-MM-dd'T'HH:mm:ss[.SSSSSSSSS][.SSSSSS][.SSS]XXX";
        }
        DateTimeFormatter dateFormatter = DateTimeFormatter.ofPattern(format);
        return dateFormatter.format(Instant.ofEpochMilli(millis));
    }

    public ObjectNode serializePage(OpenApiTableHandle table, HttpPath httpPath, Page page, int position)
    {
        ObjectNode node = OBJECT_MAPPER.createObjectNode();
        String tableName = table.getSchemaTableName().getTableName();
        List<OpenApiColumn> columns = openApiSpec.getTables().get(tableName).stream()
                .filter(column -> !column.getName().equals(ROW_ID))
                .toList();
        for (int channel = 0; channel < columns.size(); channel++) {
            Block block = page.getBlock(channel);
            if (block.isNull(position)) {
                continue;
            }

            OpenApiColumn column = columns.get(channel);
            // all fields from a request object param should be marked as predicates
            if (!isPredicate(column, httpPath, ParameterLocation.BODY)) {
                continue;
            }
            Object value = JsonTrinoConverter.convert(block, position, column.getType(), column.getSourceType(), OBJECT_MAPPER);
            nodePut(node, column.getSourceName(), value);
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
            else if (value instanceof SqlRow sqlRow) {
                ObjectNode rowNode = OBJECT_MAPPER.createObjectNode();
                JsonTrinoConverter.convertRow(rowNode, sqlRow, (RowType) column.getType(), column.getSourceType(), OBJECT_MAPPER);
                value = rowNode;
            }
            nodePut(node, column.getSourceName(), value);
        }
        return node;
    }

    private void nodePut(ObjectNode node, String name, Object value)
    {
        if (value == null) {
            node.putNull(name);
            return;
        }
        if (value instanceof Boolean booleanValue) {
            node.put(name, booleanValue);
        }
        else if (value instanceof Long longValue) {
            // includes all integer types
            node.put(name, longValue);
        }
        else if (value instanceof Double doubleValue) {
            node.put(name, doubleValue);
        }
        else if (value instanceof String stringValue) {
            // also includes dates and timestamps
            node.put(name, stringValue);
        }
        else if (value instanceof ArrayNode || value instanceof ObjectNode) {
            node.set(name, (JsonNode) value);
        }
        else if (value instanceof RawValue rawValue) {
            node.putRawValue(name, rawValue);
        }
        else {
            throw new RuntimeException(format("Unsupported object of class %s", value.getClass()));
        }
    }

    private class JsonResponseHandler
            implements ResponseHandler<Iterable<List<?>>, RuntimeException>
    {
        private final OpenApiTableHandle table;
        private final HttpPath httpPath;
        private final JsonPointer errorPointer;

        JsonResponseHandler(OpenApiTableHandle table, HttpPath httpPath, JsonPointer errorPointer)
        {
            this.table = requireNonNull(table, "table is null");
            this.httpPath = requireNonNull(httpPath, "httpPath is null");
            this.errorPointer = errorPointer;
        }

        @Override
        public Iterable<List<?>> handleException(Request request, Exception exception)
        {
            throw new RuntimeException(exception);
        }

        @Override
        public Iterable<List<?>> handle(Request request, Response response)
        {
            if (response.getStatusCode() == HttpStatus.NOT_FOUND.code()) {
                return List.of();
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

            try {
                return convertJson(table, httpPath, OBJECT_MAPPER.readTree(result));
            }
            catch (JsonProcessingException ex) {
                String message = format("Could not marshal JSON from API response: %s", result);
                throw new TrinoException(GENERIC_INTERNAL_ERROR, message, ex);
            }
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
        String message = format("Response code for getRows request was not 200: %s", statusCode);
        throw new TrinoException(GENERIC_INTERNAL_ERROR, message, new IllegalArgumentException(result));
    }

    private Iterable<List<?>> convertJson(OpenApiTableHandle table, HttpPath httpPath, JsonNode jsonNode)
    {
        ImmutableList.Builder<List<?>> resultRecordsBuilder = ImmutableList.builder();

        Map<String, Object> params = getFilterValues(table, httpPath, null);

        if (jsonNode instanceof ArrayNode arrayNode) {
            for (JsonNode jsonRecord : arrayNode) {
                resultRecordsBuilder.addAll(convertJsonToRecords(table, httpPath, params, jsonRecord));
            }
        }
        else {
            resultRecordsBuilder.addAll(convertJsonToRecords(table, httpPath, params, jsonNode));
        }

        return resultRecordsBuilder.build();
    }

    private Iterable<List<?>> convertJsonToRecords(OpenApiTableHandle table, HttpPath httpPath, Map<String, Object> params, JsonNode jsonNode)
    {
        if (!jsonNode.isObject()) {
            throw new TrinoException(GENERIC_INTERNAL_ERROR, format("JsonNode is not an object: %s", jsonNode));
        }

        Iterable<JsonNode> resultNodes = List.of(jsonNode);
        List<OpenApiColumn> columns = openApiSpec.getTables().get(table.getSchemaTableName().getTableName());
        Optional<JsonPointer> resultsPointer = columns.stream()
                .map(OpenApiColumn::getResultsPointer)
                .filter(pointer -> pointer != null && pointer.length() != 0)
                .reduce((a, b) -> {
                    if (!a.equals(b)) {
                        throw new IllegalStateException("More than one results pointer found");
                    }
                    return a;
                });
        if (resultsPointer.isPresent()) {
            JsonNode resultNode = jsonNode.at(resultsPointer.get());
            if (!(resultNode instanceof ArrayNode)) {
                throw new IllegalArgumentException("Result path points to a node that's not an array");
            }
            resultNodes = jsonNode.at(resultsPointer.get());
        }

        ImmutableList.Builder<List<?>> resultRecordsBuilder = ImmutableList.builder();
        for (JsonNode resultNode : resultNodes) {
            List<Object> recordBuilder = new ArrayList<>();
            for (OpenApiColumn column : columns) {
                if (column.getName().equals(ROW_ID)) {
                    // TODO this is dangerous, make it configurable and required?
                    recordBuilder.add(params.values().stream().findFirst().map(Object::toString).orElse(null));
                    continue;
                }
                String parameterName = column.getSourceName();
                if (column.getResultsPointer() != null && column.getResultsPointer().length() != 0) {
                    recordBuilder.add(
                            JsonTrinoConverter.convert(
                                    resultNode.get(parameterName),
                                    column.getType(),
                                    column.getSourceType()));
                    continue;
                }
                if (isPredicate(column, httpPath, null)) {
                    if (column.getName().equals(column.getSourceName()) && jsonNode.has(parameterName)) {
                        recordBuilder.add(
                                JsonTrinoConverter.convert(
                                        jsonNode.get(parameterName),
                                        column.getType(),
                                        column.getSourceType()));
                    }
                    else {
                        recordBuilder.add(params.getOrDefault(parameterName, null));
                    }
                    continue;
                }
                recordBuilder.add(
                        JsonTrinoConverter.convert(
                                jsonNode.get(parameterName),
                                column.getType(),
                                column.getSourceType()));
            }
            resultRecordsBuilder.add(recordBuilder);
        }

        return resultRecordsBuilder.build();
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
            if (response.getStatusCode() != HttpStatus.OK.code()) {
                errorResponse(response.getStatusCode(), result, errorPointer);
            }
            return null;
        }
    }

    private Iterable<List<?>> pageIterator(
            IntFunction<Iterable<List<?>>> getter,
            int offset,
            final int limit,
            int pageIncrement)
    {
        return () -> new Iterator<>()
        {
            int resultSize;
            int page = offset + 1;
            Iterator<List<?>> rows;

            @Override
            public boolean hasNext()
            {
                if (rows != null && rows.hasNext()) {
                    return true;
                }
                if (resultSize >= limit) {
                    return false;
                }
                Iterable<List<?>> items = getter.apply(page);
                page += pageIncrement;
                rows = items.iterator();
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
