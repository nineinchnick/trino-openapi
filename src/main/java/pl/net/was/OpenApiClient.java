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
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.IntFunction;
import java.util.stream.IntStream;

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
import static java.util.Objects.requireNonNull;
import static java.util.function.Function.identity;
import static java.util.stream.Collectors.toMap;
import static pl.net.was.OpenApiSpec.ROW_ID;

public class OpenApiClient
{
    private static final Logger log = Logger.get(OpenApiRecordSetProvider.class);
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
        BodyGenerator bodyGenerator = getBodyGenerator(table);
        Optional<OpenApiColumn> pageColumn = openApiSpec.getTables().get(table.getSchemaTableName().getTableName()).stream()
                .filter(OpenApiColumn::isPageNumber)
                .findFirst();
        if (pageColumn.isEmpty() || getFilter(pageColumn.get(), table.getConstraint(), null) != null) {
            return makeRequest(table, method, table.getSelectPath(), bodyGenerator, new JsonResponseHandler(table));
        }
        return pageIterator(
                page -> {
                    TupleDomain<ColumnHandle> pageConstraint = TupleDomain.fromFixedValues(Map.of(
                            pageColumn.get().getHandle(),
                            NullableValue.of(pageColumn.get().getType(), pageColumn.get().getType() instanceof BigintType ? (long) page : page)));
                    TupleDomain<ColumnHandle> constraint = table.getConstraint().isNone() ? pageConstraint : table.getConstraint().intersect(pageConstraint);
                    OpenApiTableHandle pageTable = table.cloneWithConstraint(constraint);
                    return makeRequest(pageTable, method, table.getSelectPath(), bodyGenerator, new JsonResponseHandler(table));
                },
                0,
                Integer.MAX_VALUE,
                1);
    }

    private BodyGenerator getBodyGenerator(OpenApiTableHandle table)
    {
        if (table.getSelectMethod() != PathItem.HttpMethod.POST) {
            return null;
        }
        Map<String, Object> bodyParams = getFilterValues(table, PathItem.HttpMethod.POST, "body");
        try {
            return createStaticBodyGenerator(toBytes(serializeMap(table, bodyParams)));
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    public void postRows(OpenApiOutputTableHandle table, Page page, int position)
    {
        postRows(table, serializePage(table.getTableHandle(), page, position));
    }

    public void postRows(OpenApiOutputTableHandle table, JsonNode data)
    {
        try {
            makeRequest(table.getTableHandle(), PathItem.HttpMethod.POST, table.getTableHandle().getInsertPath(), createStaticBodyGenerator(toBytes(data)), new AnyResponseHandler());
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    public void putRows(OpenApiOutputTableHandle table, Page page, int position)
    {
        putRows(table, serializePage(table.getTableHandle(), page, position));
    }

    public void putRows(OpenApiOutputTableHandle table, JsonNode data)
    {
        try {
            makeRequest(table.getTableHandle(), PathItem.HttpMethod.PUT, table.getTableHandle().getUpdatePath(), createStaticBodyGenerator(toBytes(data)), new AnyResponseHandler());
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    public void deleteRows(OpenApiOutputTableHandle table, Block rowIds, int position)
    {
        // don't have to decode the rowId since it's value is copied from the predicate that's still present in the table handle
        makeRequest(table.getTableHandle(), PathItem.HttpMethod.DELETE, table.getTableHandle().getDeletePath(), null, new AnyResponseHandler());
    }

    public <T> T makeRequest(OpenApiTableHandle table, PathItem.HttpMethod method, String path, BodyGenerator bodyGenerator, ResponseHandler<T, RuntimeException> responseHandler)
    {
        URI uri;
        try {
            uri = buildUri(baseUri, buildPath(table, method, path), getFilterValues(table, method, "query"));
        }
        catch (URISyntaxException e) {
            throw new TrinoException(GENERIC_INTERNAL_ERROR, format("Failed to construct the API URL: %s", e));
        }
        Request.Builder builder = new Request.Builder()
                .setMethod(method.name())
                .setBodyGenerator(bodyGenerator)
                .setUri(uri)
                .addHeader(USER_AGENT, USER_AGENT_VALUE)
                .addHeader(CONTENT_TYPE, JSON_UTF_8.toString())
                .addHeader(ACCEPT, JSON_UTF_8.toString())
                .addHeader("X-Trino-OpenAPI-Path", path);
        getFilterValues(table, method, "header").forEach((key, value) -> builder.addHeader(key, value.toString()));

        Request request = builder.build();
        log.debug(request.toString());
        if (rateLimiter != null) {
            rateLimiter.acquire();
        }
        return httpClient.execute(request, responseHandler);
    }

    private String buildPath(OpenApiTableHandle table, PathItem.HttpMethod method, String path)
    {
        String uriPath = requireNonNull(path, "path is null");
        Map<String, Object> pathParams = getFilterValues(table, method, "path");
        for (Map.Entry<String, Object> entry : pathParams.entrySet()) {
            uriPath = uriPath.replace(format("{%s}", entry.getKey()), entry.getValue().toString());
        }
        return uriPath;
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

    private Map<String, Object> getFilterValues(OpenApiTableHandle table, PathItem.HttpMethod method, String in)
    {
        String tableName = table.getSchemaTableName().getTableName();
        List<OpenApiColumn> columns = openApiSpec.getTables().get(tableName);
        return columns.stream()
                .filter(column -> isPredicate(column, method, in))
                .map(column -> {
                    Object value = getFilter(column, table.getConstraint(), null);
                    // don't require params in paths, since paths without params can be merged with others
                    if (!"path".equals(in) && value == null && isRequiredPredicate(column, method, in)) {
                        throw new TrinoException(INVALID_ROW_FILTER, "Missing required constraint for " + column.getName());
                    }
                    return new SimpleEntry<>(column.getSourceName(), value);
                })
                .filter(entry -> entry.getValue() != null)
                .collect(toMap(Map.Entry::getKey, Map.Entry::getValue));
    }

    private static boolean isPredicate(OpenApiColumn column, PathItem.HttpMethod method, String in)
    {
        return isRequiredPredicate(column, method, in) || isOptionalPredicate(column, method, in);
    }

    private static boolean isRequiredPredicate(OpenApiColumn column, PathItem.HttpMethod method, String in)
    {
        String requiredIn = column.getRequiresPredicate().get(method);
        return requiredIn != null && (in == null || requiredIn.equals(in));
    }

    private static boolean isOptionalPredicate(OpenApiColumn column, PathItem.HttpMethod method, String in)
    {
        String optionalIn = column.getOptionalPredicate().get(method);
        return optionalIn != null && (in == null || optionalIn.equals(in));
    }

    private static Object getFilter(OpenApiColumn column, TupleDomain<ColumnHandle> constraint, Object defaultValue)
    {
        requireNonNull(column, "column is null");
        Domain domain = null;
        if (constraint.getDomains().isPresent()) {
            domain = constraint.getDomains().get().get(column.getHandle());
        }
        if (domain == null) {
            return defaultValue;
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

    public ObjectNode serializePage(OpenApiTableHandle table, Page page, int position)
    {
        ObjectMapper objectMapper = new ObjectMapper();
        ObjectNode node = objectMapper.createObjectNode();
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
            Object value = JsonTrinoConverter.convert(block, position, column.getType(), column.getSourceType(), objectMapper);
            nodePut(node, column.getName(), value);
        }
        return node;
    }

    public ObjectNode serializeMap(OpenApiTableHandle table, Map<String, Object> params)
    {
        ObjectMapper objectMapper = new ObjectMapper();
        ObjectNode node = objectMapper.createObjectNode();
        String tableName = table.getSchemaTableName().getTableName();
        Map<String, OpenApiColumn> columns = openApiSpec.getTables().get(tableName).stream()
                .filter(column -> !column.getName().equals(ROW_ID))
                // only get columns for body params to avoid name conflicts
                .filter(column -> column.getRequiresPredicate().getOrDefault(PathItem.HttpMethod.POST, "").equals("body")
                        || column.getOptionalPredicate().getOrDefault(PathItem.HttpMethod.POST, "").equals("body"))
                .collect(toMap(OpenApiColumn::getSourceName, identity()));
        for (Map.Entry<String, Object> entry : params.entrySet()) {
            String name = entry.getKey();
            Object value = entry.getValue();
            OpenApiColumn column = columns.get(name);
            if (column == null) {
                continue;
            }
            if (value instanceof Block block) {
                value = JsonTrinoConverter.convert(block, 0, column.getType(), column.getSourceType(), objectMapper);
            }
            else if (value instanceof SqlRow sqlRow) {
                ObjectNode rowNode = objectMapper.createObjectNode();
                JsonTrinoConverter.convertRow(rowNode, sqlRow, (RowType) column.getType(), column.getSourceType(), objectMapper);
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
        private static final ObjectMapper objectMapper = new ObjectMapper();

        private final OpenApiTableHandle table;

        JsonResponseHandler(OpenApiTableHandle table)
        {
            this.table = requireNonNull(table, "table is null");
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
                throw new TrinoException(GENERIC_INTERNAL_ERROR, format("Response code for getRows request was not 200: %s", response.getStatusCode()), new IllegalArgumentException(result));
            }

            try {
                return convertJson(table, PathItem.HttpMethod.valueOf(request.getMethod()), objectMapper.readTree(result));
            }
            catch (JsonProcessingException ex) {
                throw new TrinoException(GENERIC_INTERNAL_ERROR, format("Could not marshal JSON from API response: %s", result), ex);
            }
        }
    }

    private Iterable<List<?>> convertJson(OpenApiTableHandle table, PathItem.HttpMethod httpMethod, JsonNode jsonNode)
    {
        ImmutableList.Builder<List<?>> resultRecordsBuilder = ImmutableList.builder();

        Map<String, Object> params = getFilterValues(table, httpMethod, null);

        if (jsonNode instanceof ArrayNode arrayNode) {
            for (JsonNode jsonRecord : arrayNode) {
                resultRecordsBuilder.addAll(convertJsonToRecords(table, params, jsonRecord));
            }
        }
        else {
            resultRecordsBuilder.addAll(convertJsonToRecords(table, params, jsonNode));
        }

        return resultRecordsBuilder.build();
    }

    private Iterable<List<?>> convertJsonToRecords(OpenApiTableHandle table, Map<String, Object> params, JsonNode jsonNode)
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
                if (isPredicate(column, PathItem.HttpMethod.GET, null) || isPredicate(column, PathItem.HttpMethod.POST, null)) {
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
        ObjectMapper objectMapper = new ObjectMapper();
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        objectMapper.createGenerator(baos).writeTree(rootNode);
        byte[] output = baos.toByteArray();
        log.debug("Request body: " + new String(output, UTF_8));
        return output;
    }

    private static class AnyResponseHandler
            implements ResponseHandler<Void, RuntimeException>
    {
        AnyResponseHandler()
        {
        }

        @Override
        public Void handleException(Request request, Exception exception)
        {
            throw new RuntimeException(exception);
        }

        @Override
        public Void handle(Request request, Response response)
        {
            if (response.getStatusCode() != HttpStatus.OK.code()) {
                throw new TrinoException(GENERIC_INTERNAL_ERROR, format("Response code for postRows request was not 200: %s", response.getStatusCode()));
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
