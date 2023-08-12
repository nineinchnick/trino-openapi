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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.base.CaseFormat;
import com.google.common.collect.ImmutableList;
import com.google.common.io.CharStreams;
import com.google.inject.Inject;
import io.airlift.http.client.BodyGenerator;
import io.airlift.http.client.HttpClient;
import io.airlift.http.client.HttpStatus;
import io.airlift.http.client.Request;
import io.airlift.http.client.Response;
import io.airlift.http.client.ResponseHandler;
import io.airlift.log.Logger;
import io.airlift.slice.Slice;
import io.swagger.v3.oas.models.PathItem;
import io.swagger.v3.oas.models.media.Schema;
import io.swagger.v3.oas.models.parameters.Parameter;
import io.trino.spi.Page;
import io.trino.spi.TrinoException;
import io.trino.spi.block.Block;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.ConnectorTableMetadata;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.type.StandardTypes;
import io.trino.spi.type.Type;

import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URLEncoder;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static com.google.common.net.HttpHeaders.ACCEPT;
import static com.google.common.net.HttpHeaders.CONTENT_TYPE;
import static com.google.common.net.MediaType.JSON_UTF_8;
import static io.airlift.http.client.Request.Builder.prepareDelete;
import static io.airlift.http.client.Request.Builder.prepareGet;
import static io.airlift.http.client.Request.Builder.preparePost;
import static io.airlift.http.client.Request.Builder.preparePut;
import static io.trino.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static io.trino.spi.StandardErrorCode.INVALID_ROW_FILTER;
import static java.lang.String.format;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toMap;
import static pl.net.was.OpenApiSpec.ROW_ID;

public class OpenApiClient
{
    private static final Logger log = Logger.get(OpenApiRecordSetProvider.class);

    private final URI baseUri;

    private final OpenApiMetadata metadata;
    private final HttpClient httpClient;
    private final OpenApiSpec openApiSpec;

    @Inject
    public OpenApiClient(OpenApiConfig config, OpenApiMetadata metadata, @ForOpenApi HttpClient httpClient, OpenApiSpec openApiSpec)
    {
        this.baseUri = config.getBaseUri();
        this.metadata = metadata;
        this.httpClient = httpClient;
        this.openApiSpec = openApiSpec;
    }

    public Iterable<List<?>> getRows(OpenApiTableHandle table)
    {
        return makeRequest(table, PathItem.HttpMethod.GET, table.getSelectPath(), prepareGet(), new JsonResponseHandler(table));
    }

    public void postRows(OpenApiOutputTableHandle table, Page page, int position)
    {
        postRows(table, serializePage(table, page, position));
    }

    public void postRows(OpenApiOutputTableHandle table, JsonNode data)
    {
        Request.Builder builder = preparePost().setBodyGenerator(new JsonBodyGenerator(data));
        makeRequest(table.getTableHandle(), PathItem.HttpMethod.POST, table.getTableHandle().getInsertPath(), builder, new AnyResponseHandler());
    }

    public void putRows(OpenApiOutputTableHandle table, Page page, int position)
    {
        putRows(table, serializePage(table, page, position));
    }

    public void putRows(OpenApiOutputTableHandle table, JsonNode data)
    {
        Request.Builder builder = preparePut().setBodyGenerator(new JsonBodyGenerator(data));
        makeRequest(table.getTableHandle(), PathItem.HttpMethod.PUT, table.getTableHandle().getUpdatePath(), builder, new AnyResponseHandler());
    }

    public void deleteRows(OpenApiOutputTableHandle table, Block rowIds, int position)
    {
        // don't have to decode the rowId since it's value is copied from the predicate that's still present in the table handle
        makeRequest(table.getTableHandle(), PathItem.HttpMethod.DELETE, table.getTableHandle().getDeletePath(), prepareDelete(), new AnyResponseHandler());
    }

    public <T> T makeRequest(OpenApiTableHandle table, PathItem.HttpMethod method, String path, Request.Builder builder, ResponseHandler<T, RuntimeException> responseHandler)
    {
        String uriPath = requireNonNull(path, "path is null");
        Map<String, String> pathParams = getFilterValues(table, path, method, "path");
        for (Map.Entry<String, String> entry : pathParams.entrySet()) {
            // TODO we shouldn't have to do a reverse name mapping, we should iterate over tuples of spec properties and trino types
            String parameterName = CaseFormat.LOWER_UNDERSCORE.to(CaseFormat.LOWER_CAMEL, entry.getKey());
            uriPath = uriPath.replace(format("{%s}", parameterName), entry.getValue());
        }

        URI uri;
        try {
            uri = buildUri(baseUri, uriPath, getFilterValues(table, path, method, "query"));
        }
        catch (URISyntaxException e) {
            throw new TrinoException(GENERIC_INTERNAL_ERROR, format("Failed to construct the API URL: %s", e));
        }
        builder
                .setUri(uri)
                .addHeader(CONTENT_TYPE, JSON_UTF_8.toString())
                .addHeader(ACCEPT, JSON_UTF_8.toString())
                .addHeader("X-Trino-OpenAPI-Path", path);
        getFilterValues(table, path, method, "header").forEach(builder::addHeader);

        return httpClient.execute(builder.build(), responseHandler);
    }

    private static URI buildUri(URI uri, String path, Map<String, String> queryParams)
            throws URISyntaxException
    {
        URI oldUri = uri.resolve(uri.getPath() + path);
        String query = queryParams.entrySet().stream()
                .map(entry -> format("%s=%s", entry.getKey(), URLEncoder.encode(entry.getValue(), UTF_8)))
                .collect(joining("&"));
        return new URI(
                oldUri.getScheme(),
                oldUri.getAuthority(),
                oldUri.getPath(),
                oldUri.getQuery() == null ? query : oldUri.getQuery() + "&" + query,
                oldUri.getFragment());
    }

    private Map<String, String> getFilterValues(OpenApiTableHandle table, String path)
    {
        return getFilterValues(table, path, PathItem.HttpMethod.GET, null);
    }

    private Map<String, String> getFilterValues(OpenApiTableHandle table, String path, PathItem.HttpMethod method, String in)
    {
        Map<String, OpenApiColumnHandle> columns = metadata.getColumnHandles(table);
        if (!openApiSpec.getRequiredParameters().containsKey(path) || !openApiSpec.getRequiredParameters().get(path).containsKey(method)) {
            return Map.of();
        }
        Map<String, Parameter> requiredParams = openApiSpec.getRequiredParameters().get(path).get(method);
        Stream<Map.Entry<String, Parameter>> stream = requiredParams.entrySet().stream();
        if (in != null) {
            stream = stream.filter(entry -> entry.getValue().getIn().equals(in));
        }
        return stream
                .collect(toMap(Map.Entry::getKey, entry -> {
                    Object value = getFilter(columns.get(entry.getKey()), table.getConstraint(), null);
                    if (value == null) {
                        throw new TrinoException(INVALID_ROW_FILTER, "Missing required constraint for " + entry.getKey());
                    }
                    // TODO this requires proper type mapping, reverse of JsonTrinoConverter.writeTo()
                    return value.toString();
                }));
    }

    private static Object getFilter(OpenApiColumnHandle column, TupleDomain<ColumnHandle> constraint, Object defaultValue)
    {
        requireNonNull(column, "column is null");
        Domain domain = null;
        if (constraint.getDomains().isPresent()) {
            domain = constraint.getDomains().get().get(column);
        }
        if (domain == null) {
            return defaultValue;
        }
        return switch (column.getType().getBaseName()) {
            case StandardTypes.VARCHAR -> ((Slice) domain.getSingleValue()).toStringUtf8();
            case StandardTypes.BIGINT, StandardTypes.INTEGER -> domain.getSingleValue();
            default -> throw new TrinoException(INVALID_ROW_FILTER, "Unexpected constraint for " + column.getName() + "(" + column.getType().getBaseName() + ")");
        };
    }

    public ObjectNode serializePage(OpenApiOutputTableHandle table, Page page, int position)
    {
        Map<String, Schema<?>> columnTypes = openApiSpec.getOriginalColumnTypes(table.getTableHandle().getSchemaTableName().getTableName());
        requireNonNull(columnTypes, "columnTypes is null");

        ObjectMapper objectMapper = new ObjectMapper();
        ObjectNode node = objectMapper.createObjectNode();
        // TODO iterate over non-hidden columns, not channels
        for (int channel = 0; channel < table.getColumnTypes().size(); channel++) {
            Block block = page.getBlock(channel);
            if (block.isNull(position)) {
                continue;
            }

            String columnName = table.getColumnNames().get(channel);
            Type type = table.getColumnTypes().get(channel);
            Object value = JsonTrinoConverter.convert(block, position, type, columnTypes.get(columnName), objectMapper);
            nodePut(node, columnName, value);
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
        else {
            throw new RuntimeException(format("Unsupported object of class %s", value.getClass()));
        }
    }

    private class JsonResponseHandler
            implements ResponseHandler<Iterable<List<?>>, RuntimeException>
    {
        private static final ObjectMapper objectMapper = new ObjectMapper();

        private final OpenApiTableHandle table;
        private final ConnectorTableMetadata tableMetadata;

        JsonResponseHandler(OpenApiTableHandle table)
        {
            this.table = requireNonNull(table, "table is null");
            this.tableMetadata = metadata.getTableMetadata(table);
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
            if (response.getStatusCode() != HttpStatus.OK.code()) {
                throw new TrinoException(GENERIC_INTERNAL_ERROR, format("Response code for getRows request was not 200: %s", response.getStatusCode()));
            }
            String result = "";
            try {
                result = CharStreams.toString(new InputStreamReader(response.getInputStream(), UTF_8));
            }
            catch (IOException e) {
                throw new RuntimeException(e);
            }
            log.debug("Received response code " + response.getStatusCode() + ": " + result);

            try {
                JsonNode jsonNode = objectMapper.readTree(result);

                log.debug("Marshalled response to json %s", jsonNode);

                JsonNode jsonNodeToUse = openApiSpec.getAdapter().map(adapter -> adapter.runAdapter(tableMetadata, jsonNode)).orElse(jsonNode);

                return convertJson(table, jsonNodeToUse);
            }
            catch (JsonProcessingException ex) {
                throw new TrinoException(GENERIC_INTERNAL_ERROR, format("Could not marshal JSON from API response: %s", result), ex);
            }
        }
    }

    private Iterable<List<?>> convertJson(OpenApiTableHandle table, JsonNode jsonNode)
    {
        ImmutableList.Builder<List<?>> resultRecordsBuilder = ImmutableList.builder();

        ConnectorTableMetadata tableMetadata = metadata.getTableMetadata(table);
        Map<String, String> pathParams = getFilterValues(table, table.getSelectPath());
        if (jsonNode instanceof ArrayNode arrayNode) {
            for (JsonNode jsonRecord : arrayNode) {
                resultRecordsBuilder.add(convertJsonToRecord(tableMetadata, pathParams, jsonRecord));
            }
        }
        else {
            resultRecordsBuilder.add(convertJsonToRecord(tableMetadata, pathParams, jsonNode));
        }

        return resultRecordsBuilder.build();
    }

    private List<?> convertJsonToRecord(ConnectorTableMetadata tableMetadata, Map<String, String> pathParams, JsonNode jsonNode)
    {
        if (!jsonNode.isObject()) {
            throw new TrinoException(GENERIC_INTERNAL_ERROR, format("JsonNode is not an object: %s", jsonNode));
        }

        List<Object> recordBuilder = new ArrayList<>();
        for (ColumnMetadata columnMetadata : tableMetadata.getColumns()) {
            if (columnMetadata.getName().equals(ROW_ID)) {
                // TODO this is dangerous, make it configurable and required?
                recordBuilder.add(pathParams.values().stream().findFirst().orElse(null));
                continue;
            }
            // TODO we shouldn't have to do a reverse name mapping, we should iterate over tuples of spec properties and trino types
            String parameterName = CaseFormat.LOWER_UNDERSCORE.to(CaseFormat.LOWER_CAMEL, columnMetadata.getName());
            if (!jsonNode.has(parameterName)) {
                // this might be a virtual column for a required parameter, if so, copy the value from the constraint
                recordBuilder.add(pathParams.getOrDefault(parameterName, null));
                continue;
            }
            Map<String, Schema<?>> columnTypes = openApiSpec.getOriginalColumnTypes(tableMetadata.getTable().getTableName());
            requireNonNull(columnTypes, "columnTypes is null");
            recordBuilder.add(
                    JsonTrinoConverter.convert(
                            jsonNode.get(parameterName),
                            columnMetadata.getType(),
                            columnTypes.get(columnMetadata.getName())));
        }

        return recordBuilder;
    }

    private static class JsonBodyGenerator
            implements BodyGenerator
    {
        private final JsonNode rootNode;

        protected JsonBodyGenerator(JsonNode rootNode)
        {
            this.rootNode = rootNode;
        }

        @Override
        public void write(OutputStream out)
                throws IOException
        {
            ObjectMapper objectMapper = new ObjectMapper();
            objectMapper.createGenerator(out).writeTree(rootNode);
        }
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
}
