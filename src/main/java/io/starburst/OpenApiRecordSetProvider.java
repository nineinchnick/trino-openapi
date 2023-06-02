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
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Multimaps;
import com.google.common.io.CharStreams;
import io.airlift.http.client.HttpClient;
import io.airlift.http.client.Request;
import io.airlift.http.client.Response;
import io.airlift.http.client.ResponseHandler;
import io.airlift.log.Logger;
import io.starburst.adapters.GalaxyAdapter;
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
import io.trino.spi.type.Type;
import org.json.JSONArray;
import org.json.JSONObject;

import javax.inject.Inject;

import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.StreamSupport;

import static com.google.common.net.HttpHeaders.CONTENT_TYPE;
import static com.google.common.net.MediaType.JSON_UTF_8;
import static io.airlift.http.client.Request.Builder.prepareGet;
import static java.lang.String.format;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.stream.Collectors.toList;

public class OpenApiRecordSetProvider
        implements ConnectorRecordSetProvider
{
    private static final Logger log = Logger.get(OpenApiRecordSetProvider.class);
    private final URI baseUri;

    private final OpenApiMetadata metadata;
    private final HttpClient httpClient;
    private final OpenApiSpec openApiSpec;
    private final Map<String, String> httpHeaders;

    private final Optional<OpenApiSpecAdapter> adapter = Optional.of(new GalaxyAdapter());

    @Inject
    public OpenApiRecordSetProvider(OpenApiConfig config, OpenApiMetadata metadata, @OpenApiClient HttpClient httpClient, OpenApiSpec openApiSpec)
    {
        this.baseUri = config.getBaseUri();
        this.metadata = metadata;
        this.httpClient = httpClient;
        this.openApiSpec = openApiSpec;
        this.httpHeaders = ImmutableMap.copyOf(config.getHttpHeaders());
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
        ConnectorTableMetadata tableMetadata = metadata.getTableMetadata(connectorSession, table);
        Request request = prepareGet()
                .setUri(baseUri.resolve(table.getPath()))
                .addHeaders(Multimaps.forMap(httpHeaders))
                .addHeader(CONTENT_TYPE, JSON_UTF_8.toString())
                .build();
        return httpClient.execute(request, new ResponseHandler<>()
        {
            @Override
            public Iterable<List<?>> handleException(Request request, Exception exception)
            {
                throw new RuntimeException(exception);
            }

            @Override
            public Iterable<List<?>> handle(Request request, Response response)
            {
                if (response.getStatusCode() != 200) {
                    throw new RuntimeException(format("Response code for getRows request was not 200: %s", response.getStatusCode()));
                }
                String result = "";
                try {
                    result = CharStreams.toString(new InputStreamReader(response.getInputStream(), UTF_8));
                }
                catch (IOException e) {
                    throw new RuntimeException(e);
                }
                log.debug("Received response code " + response.getStatusCode() + ": " + result);

                JSONObject json = new JSONObject(result);

                log.debug("Serialized to json %s", json);

                JSONArray jsonArray = adapter.map(adapter -> adapter.runAdapter(tableMetadata, json)).orElseThrow();

                return convertJson(jsonArray, tableMetadata);

                /*RowType rowType = RowType.from(List.of(RowType.field("aa", VARCHAR)));
                ArrayType arrayType = new ArrayType(rowType);
                PageBuilder pageBuilder = new PageBuilder(ImmutableList.of(arrayType));

                BlockBuilder blockBuilder = pageBuilder.getBlockBuilder(0);
                BlockBuilder entryBuilder = blockBuilder.beginBlockEntry();

                // TODO populate entries
                //entryBuilder.closeEntry();
                blockBuilder.closeEntry();
                pageBuilder.declarePosition();
                Block block = arrayType.getObject(blockBuilder, blockBuilder.getPositionCount() - 1);
                return List.of(
                        List.of("x", block));*/
            }
        });
    }

    private Iterable<List<?>> convertJson(JSONArray jsonArray, ConnectorTableMetadata tableMetadata)
    {
        ImmutableList.Builder<List<?>> resultRecordsBuilder = ImmutableList.builder();
        for (Object obj : jsonArray) {
            if (!(obj instanceof JSONObject)) {
                throw new RuntimeException(format("JSONArray object is not JSONObject: %s", obj.getClass()));
            }
            JSONObject recordObject = (JSONObject) obj;

            ImmutableList.Builder<Object> recordBuilder = ImmutableList.builder();
            for (ColumnMetadata columnMetadata : tableMetadata.getColumns()) {
                // TODO Hack to deal with the fact that some clusters are missing these two columns
                if (columnMetadata.getName().equals("idle_stop_minutes") && !recordObject.has("idle_stop_minutes")) {
                    recordBuilder.add(Integer.valueOf(5));
                    continue;
                }
                if (columnMetadata.getName().equals("trino_uri") && !recordObject.has("trino_uri")) {
                    recordBuilder.add("trinoplane.com");
                    continue;
                }

                // TODO we shouldn't have to do a reverse name mapping, we should iterate over tuples of spec properties and trino types
                Object columnValue = recordObject.get(CaseFormat.LOWER_UNDERSCORE.to(CaseFormat.LOWER_CAMEL, columnMetadata.getName()));

                if (columnValue == null) {
                    throw new RuntimeException(format("JSON record missing column: %s, has columns: %s", columnMetadata.getName(), recordObject.keySet()));
                }

                recordBuilder.add(
                        JsonTrinoConverter.convert(
                                columnValue,
                                columnMetadata.getType(),
                                openApiSpec.getOriginalColumnTypes(tableMetadata.getTable().getTableName()).get(columnMetadata.getName())));
            }
            resultRecordsBuilder.add(recordBuilder.build());
        }
        return resultRecordsBuilder.build();
    }
}
