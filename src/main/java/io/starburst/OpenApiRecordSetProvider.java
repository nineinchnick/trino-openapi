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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Multimaps;
import com.google.common.io.CharStreams;
import io.airlift.http.client.HttpClient;
import io.airlift.http.client.Request;
import io.airlift.http.client.Response;
import io.airlift.http.client.ResponseHandler;
import io.airlift.log.Logger;
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
    private final Map<String, String> httpHeaders;

    @Inject
    public OpenApiRecordSetProvider(OpenApiConfig config, OpenApiMetadata metadata, @OpenApiClient HttpClient httpClient)
    {
        this.baseUri = config.getBaseUri();
        this.metadata = metadata;
        this.httpClient = httpClient;
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
        // TODO for building blocks we need a pageBuilder of a certain array type, get that from metadata
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

                return convertJson(json, tableMetadata);

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

    private Iterable<List<?>> convertJson(JSONObject jsonObject, ConnectorTableMetadata tableMetadata)
    {
        Object result = jsonObject.get("result");
        if (result == null) {
            throw new RuntimeException(format("No result key found for jsonObject with keys: %s", jsonObject.keySet()));
        }

        if (!(result instanceof JSONArray)) {
            throw new RuntimeException(format("'result' object is not JSONArray: %s", result.getClass().getName()));
        }
        JSONArray resultArray = (JSONArray) result;

        ImmutableList.Builder<List<?>> resultRecordsBuilder = ImmutableList.builder();
        for (Object obj : resultArray) {
            if (!(obj instanceof JSONObject)) {
                throw new RuntimeException(format("JSONArray object is not JSONObject: %s", obj.getClass()));
            }
            JSONObject recordObject = (JSONObject) obj;

            ImmutableList.Builder<Object> recordBuilder = ImmutableList.builder();
            for (ColumnMetadata columnMetadata : tableMetadata.getColumns()) {
                // Currently fails here due to it looking for the paginated object column 'next_page_token'
                Object columnValue = recordObject.get(columnMetadata.getName());
                if (columnValue == null) {
                    throw new RuntimeException(format("JSON record missing column: %s, has columns: %s", columnMetadata.getName(), recordObject.keySet()));
                }

                recordBuilder.add(
                        JsonTrinoConverter.convert(
                                recordObject,
                                columnMetadata.getName(),
                                columnMetadata.getType(),
                                metadata.getSpec().getOriginalColumnTypes(tableMetadata.getTable().getTableName()).get(columnMetadata.getName())));
            }
            resultRecordsBuilder.add(recordBuilder.build());
        }
        return resultRecordsBuilder.build();
    }
}
