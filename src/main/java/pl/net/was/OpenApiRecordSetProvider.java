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
import com.google.common.base.CaseFormat;
import com.google.common.collect.ImmutableList;
import com.google.common.io.CharStreams;
import io.airlift.http.client.HttpClient;
import io.airlift.http.client.Request;
import io.airlift.http.client.Response;
import io.airlift.http.client.ResponseHandler;
import io.airlift.log.Logger;
import io.airlift.slice.Slice;
import io.swagger.v3.oas.models.parameters.Parameter;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.ConnectorRecordSetProvider;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplit;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTableMetadata;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.connector.InMemoryRecordSet;
import io.trino.spi.connector.RecordSet;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.type.StandardTypes;
import io.trino.spi.type.Type;

import javax.inject.Inject;

import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URLEncoder;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static com.google.common.net.HttpHeaders.ACCEPT;
import static com.google.common.net.HttpHeaders.CONTENT_TYPE;
import static com.google.common.net.MediaType.JSON_UTF_8;
import static io.airlift.http.client.Request.Builder.prepareGet;
import static io.trino.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static io.trino.spi.StandardErrorCode.INVALID_ROW_FILTER;
import static java.lang.String.format;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toMap;

public class OpenApiRecordSetProvider
        implements ConnectorRecordSetProvider
{
    private static final Logger log = Logger.get(OpenApiRecordSetProvider.class);
    private final URI baseUri;

    private final OpenApiMetadata metadata;
    private final HttpClient httpClient;
    private final OpenApiSpec openApiSpec;

    @Inject
    public OpenApiRecordSetProvider(OpenApiConfig config, OpenApiMetadata metadata, @OpenApiClient HttpClient httpClient, OpenApiSpec openApiSpec)
    {
        this.baseUri = config.getBaseUri();
        this.metadata = metadata;
        this.httpClient = httpClient;
        this.openApiSpec = openApiSpec;
    }

    @Override
    public RecordSet getRecordSet(
            ConnectorTransactionHandle connectorTransactionHandle,
            ConnectorSession connectorSession,
            ConnectorSplit connectorSplit,
            ConnectorTableHandle table,
            List<? extends ColumnHandle> list)
    {
        List<OpenApiColumnHandle> columnHandles = list.stream()
                .map(c -> (OpenApiColumnHandle) c)
                .collect(toList());
        ConnectorTableMetadata tableMetadata = metadata.getTableMetadata(connectorSession, table);

        List<Integer> columnIndexes = columnHandles.stream()
                .map(column -> {
                    int index = 0;
                    for (ColumnMetadata columnMetadata : tableMetadata.getColumns()) {
                        if (columnMetadata.getName().equalsIgnoreCase(column.getName())) {
                            return index;
                        }
                        index++;
                    }
                    throw new IllegalStateException("Unknown column: " + column.getName());
                })
                .collect(toList());

        Iterable<List<?>> rows = getRows(connectorSession, (OpenApiTableHandle) table);
        Iterable<List<?>> mappedRows = StreamSupport.stream(rows.spliterator(), false)
                .map(row -> columnIndexes.stream()
                        .map(row::get)
                        .collect(toList())).collect(toList());

        List<Type> mappedTypes = columnHandles.stream()
                .map(OpenApiColumnHandle::getType)
                .collect(toList());
        return new InMemoryRecordSet(mappedTypes, mappedRows);
    }

    private Iterable<List<?>> getRows(ConnectorSession connectorSession, OpenApiTableHandle table)
    {
        String path = table.getPath();
        Map<String, String> pathParams = getFilterValues(connectorSession, table, "path");
        for (Map.Entry<String, String> entry : pathParams.entrySet()) {
            // TODO we shouldn't have to do a reverse name mapping, we should iterate over tuples of spec properties and trino types
            String parameterName = CaseFormat.LOWER_UNDERSCORE.to(CaseFormat.LOWER_CAMEL, entry.getKey());
            path = path.replace(format("{%s}", parameterName), entry.getValue());
        }

        URI uri;
        try {
            uri = buildUri(baseUri, path, getFilterValues(connectorSession, table, "query"));
        }
        catch (URISyntaxException e) {
            throw new TrinoException(GENERIC_INTERNAL_ERROR, format("Failed to construct the API URL: %s", e));
        }
        Request.Builder builder = prepareGet()
                .setUri(uri)
                .addHeader(CONTENT_TYPE, JSON_UTF_8.toString())
                .addHeader(ACCEPT, JSON_UTF_8.toString());
        getFilterValues(connectorSession, table, "header").forEach(builder::addHeader);
        Request request = builder.build();

        return httpClient.execute(request, new ApiResponseHandler(connectorSession, table));
    }

    public static URI buildUri(URI uri, String path, Map<String, String> queryParams)
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

    private Map<String, String> getFilterValues(ConnectorSession connectorSession, OpenApiTableHandle table)
    {
        return getFilterValues(connectorSession, table, null);
    }

    private Map<String, String> getFilterValues(ConnectorSession connectorSession, OpenApiTableHandle table, String in)
    {
        Map<String, ColumnHandle> columns = metadata.getColumnHandles(connectorSession, table);
        String tableName = table.getSchemaTableName().getTableName();
        Map<String, Parameter> requiredParams = openApiSpec.getRequiredParameters().get(tableName);
        Stream<Map.Entry<String, Parameter>> stream = requiredParams.entrySet().stream();
        if (in != null) {
            stream = stream.filter(entry -> entry.getValue().getIn().equals(in));
        }
        return stream
                .collect(toMap(Map.Entry::getKey, entry -> {
                    // TODO this requires proper type mapping, reverse of JsonTrinoConverter.writeTo()
                    String value = getFilter((OpenApiColumnHandle) columns.get(entry.getKey()), table.getConstraint(), null).toString();
                    if (value == null) {
                        throw new TrinoException(INVALID_ROW_FILTER, "Missing required constraint for " + entry.getKey());
                    }
                    return value;
                }));
    }

    private static Object getFilter(OpenApiColumnHandle column, TupleDomain<ColumnHandle> constraint, Object defaultValue)
    {
        requireNonNull(column, "column is null");
        Domain domain = null;
        if (constraint.getDomains().isPresent()) {
            domain = constraint.getDomains().get().get(column);
        }
        switch (column.getType().getBaseName()) {
            case StandardTypes.VARCHAR:
                if (domain == null) {
                    return defaultValue;
                }
                return ((Slice) domain.getSingleValue()).toStringUtf8();
            case StandardTypes.BIGINT:
            case StandardTypes.INTEGER:
                if (domain == null) {
                    return defaultValue;
                }
                return domain.getSingleValue();
            default:
                throw new TrinoException(INVALID_ROW_FILTER, "Unexpected constraint for " + column.getName() + "(" + column.getType().getBaseName() + ")");
        }
    }

    private class ApiResponseHandler
            implements ResponseHandler<Iterable<List<?>>, RuntimeException>
    {
        private static final ObjectMapper objectMapper = new ObjectMapper();

        private final ConnectorSession connectorSession;
        private final OpenApiTableHandle table;
        private final ConnectorTableMetadata tableMetadata;

        ApiResponseHandler(ConnectorSession connectorSession, OpenApiTableHandle table)
        {
            this.connectorSession = requireNonNull(connectorSession, "connectorSession is null");
            this.table = requireNonNull(table, "table is null");
            this.tableMetadata = metadata.getTableMetadata(connectorSession, table);
        }

        @Override
        public Iterable<List<?>> handleException(Request request, Exception exception)
        {
            throw new RuntimeException(exception);
        }

        @Override
        public Iterable<List<?>> handle(Request request, Response response)
        {
            if (response.getStatusCode() != 200) {
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

                return convertJson(connectorSession, table, jsonNodeToUse);
            }
            catch (JsonProcessingException ex) {
                throw new TrinoException(GENERIC_INTERNAL_ERROR, format("Could not marshal JSON from API response: %s", result), ex);
            }
        }
    }

    private Iterable<List<?>> convertJson(ConnectorSession connectorSession, OpenApiTableHandle table, JsonNode jsonNode)
    {
        ImmutableList.Builder<List<?>> resultRecordsBuilder = ImmutableList.builder();

        ConnectorTableMetadata tableMetadata = metadata.getTableMetadata(connectorSession, table);
        Map<String, String> pathParams = getFilterValues(connectorSession, table);
        if (jsonNode instanceof ArrayNode) {
            ArrayNode jsonArray = (ArrayNode) jsonNode;
            for (JsonNode jsonRecord : jsonArray) {
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
            // TODO we shouldn't have to do a reverse name mapping, we should iterate over tuples of spec properties and trino types
            String parameterName = CaseFormat.LOWER_UNDERSCORE.to(CaseFormat.LOWER_CAMEL, columnMetadata.getName());
            if (!jsonNode.has(parameterName)) {
                // this might be a virtual column for a required parameter, if so, copy the value from the constraint
                recordBuilder.add(pathParams.getOrDefault(parameterName, null));
                continue;
            }
            recordBuilder.add(
                    JsonTrinoConverter.convert(
                            jsonNode.get(parameterName),
                            columnMetadata.getType(),
                            openApiSpec.getOriginalColumnTypes(tableMetadata.getTable().getTableName()).get(columnMetadata.getName())));
        }

        return recordBuilder;
    }
}
