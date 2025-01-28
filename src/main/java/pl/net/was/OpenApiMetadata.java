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

import com.google.inject.Inject;
import io.airlift.slice.Slice;
import io.trino.spi.StandardErrorCode;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.ConnectorInsertTableHandle;
import io.trino.spi.connector.ConnectorMergeTableHandle;
import io.trino.spi.connector.ConnectorMetadata;
import io.trino.spi.connector.ConnectorOutputMetadata;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTableMetadata;
import io.trino.spi.connector.ConnectorTableVersion;
import io.trino.spi.connector.Constraint;
import io.trino.spi.connector.ConstraintApplicationResult;
import io.trino.spi.connector.RetryMode;
import io.trino.spi.connector.RowChangeParadigm;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.connector.SchemaTablePrefix;
import io.trino.spi.connector.TableColumnsMetadata;
import io.trino.spi.connector.TableNotFoundException;
import io.trino.spi.statistics.ComputedStatistics;

import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.trino.spi.connector.RowChangeParadigm.CHANGE_ONLY_UPDATED_COLUMNS;
import static java.util.function.UnaryOperator.identity;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toMap;
import static pl.net.was.OpenApiSpec.ROW_ID;

public class OpenApiMetadata
        implements ConnectorMetadata
{
    public static final String SCHEMA_NAME = "default";

    private final OpenApiSpec spec;
    private final int domainExpansionLimit;

    @Inject
    public OpenApiMetadata(OpenApiSpec spec, OpenApiConfig config)
    {
        this.spec = spec;
        this.domainExpansionLimit = config.getDomainExpansionLimit();
    }

    @Override
    public List<String> listSchemaNames(ConnectorSession connectorSession)
    {
        return List.of(SCHEMA_NAME);
    }

    @Override
    public ConnectorTableHandle getTableHandle(
            ConnectorSession connectorSession,
            SchemaTableName schemaTableName,
            Optional<ConnectorTableVersion> startVersion,
            Optional<ConnectorTableVersion> endVersion)
    {
        OpenApiTableHandle handle = spec.getTableHandle(schemaTableName);
        if (startVersion.isPresent() || endVersion.isPresent()) {
            throw new TrinoException(StandardErrorCode.NOT_SUPPORTED, "This connector does not support versioned tables");
        }
        return handle;
    }

    @Override
    public ConnectorTableMetadata getTableMetadata(
            ConnectorSession connectorSession,
            ConnectorTableHandle connectorTableHandle)
    {
        return getTableMetadata(connectorTableHandle);
    }

    public ConnectorTableMetadata getTableMetadata(ConnectorTableHandle connectorTableHandle)
    {
        OpenApiTableHandle tableHandle = (OpenApiTableHandle) connectorTableHandle;
        SchemaTableName schemaTableName = tableHandle.getSchemaTableName();
        List<OpenApiColumn> columns = spec.getTables().get(schemaTableName.getTableName());
        if (columns == null) {
            throw new TableNotFoundException(schemaTableName);
        }
        return new ConnectorTableMetadata(schemaTableName, columns.stream().map(OpenApiColumn::getMetadata).toList());
    }

    @Override
    public List<SchemaTableName> listTables(ConnectorSession session, Optional<String> schemaName)
    {
        return spec.getTables()
                .keySet()
                .stream()
                .map(table -> new SchemaTableName(SCHEMA_NAME, table))
                .collect(toList());
    }

    @Override
    public Map<String, ColumnHandle> getColumnHandles(
            ConnectorSession connectorSession,
            ConnectorTableHandle connectorTableHandle)
    {
        OpenApiTableHandle tableHandle = (OpenApiTableHandle) connectorTableHandle;
        return spec.getTables().get(tableHandle.getSchemaTableName().getTableName()).stream()
                .collect(toMap(OpenApiColumn::getName, OpenApiColumn::getHandle));
    }

    public Map<String, OpenApiColumnHandle> getColumnHandles(ConnectorTableHandle connectorTableHandle)
    {
        OpenApiTableHandle tableHandle = (OpenApiTableHandle) connectorTableHandle;
        return spec.getTables().get(tableHandle.getSchemaTableName().getTableName()).stream()
                .collect(toMap(OpenApiColumn::getName, OpenApiColumn::getHandle));
    }

    public Map<String, OpenApiColumn> getColumns(ConnectorTableHandle connectorTableHandle)
    {
        OpenApiTableHandle tableHandle = (OpenApiTableHandle) connectorTableHandle;
        return spec.getTables().get(tableHandle.getSchemaTableName().getTableName()).stream()
                .collect(toMap(OpenApiColumn::getName, identity()));
    }

    @Override
    public ColumnMetadata getColumnMetadata(
            ConnectorSession connectorSession,
            ConnectorTableHandle connectorTableHandle,
            ColumnHandle columnHandle)
    {
        OpenApiColumnHandle handle = (OpenApiColumnHandle) columnHandle;
        return new ColumnMetadata(handle.getName(), handle.getType());
    }

    @Override
    public Iterator<TableColumnsMetadata> streamTableColumns(ConnectorSession session, SchemaTablePrefix prefix)
    {
        return spec.getTables().entrySet().stream()
                .map(entry -> TableColumnsMetadata.forTable(
                        new SchemaTableName(prefix.getSchema().orElse(""), entry.getKey()),
                        entry.getValue().stream().map(OpenApiColumn::getMetadata).toList()))
                .iterator();
    }

    @Override
    public Optional<ConstraintApplicationResult<ConnectorTableHandle>> applyFilter(
            ConnectorSession session,
            ConnectorTableHandle table,
            Constraint constraint)
    {
        OpenApiTableHandle openApiTable = (OpenApiTableHandle) table;
        return openApiTable.applyFilter(constraint, getColumns(table), domainExpansionLimit);
    }

    @Override
    public ConnectorInsertTableHandle beginInsert(
            ConnectorSession session,
            ConnectorTableHandle tableHandle,
            List<ColumnHandle> columns,
            RetryMode retryMode)
    {
        if (retryMode != RetryMode.NO_RETRIES) {
            throw new TrinoException(NOT_SUPPORTED, "This connector does not support query retries");
        }
        return new OpenApiOutputTableHandle((OpenApiTableHandle) tableHandle);
    }

    @Override
    public Optional<ConnectorOutputMetadata> finishInsert(
            ConnectorSession session,
            ConnectorInsertTableHandle insertHandle,
            List<ConnectorTableHandle> sourceTableHandles,
            Collection<Slice> fragments,
            Collection<ComputedStatistics> computedStatistics)
    {
        return Optional.empty();
    }

    @Override
    public RowChangeParadigm getRowChangeParadigm(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        return CHANGE_ONLY_UPDATED_COLUMNS;
    }

    @Override
    public ColumnHandle getMergeRowIdColumnHandle(
            ConnectorSession session,
            ConnectorTableHandle tableHandle)
    {
        OpenApiTableHandle table = (OpenApiTableHandle) tableHandle;
        Map<String, OpenApiColumnHandle> columns = getColumnHandles(table);
        return columns.values().stream()
                .filter(column -> column.getName().equals(ROW_ID))
                .findFirst()
                .orElseThrow(() -> new TrinoException(NOT_SUPPORTED, MODIFYING_ROWS_MESSAGE));
    }

    @Override
    public ConnectorMergeTableHandle beginMerge(
            ConnectorSession session,
            ConnectorTableHandle tableHandle,
            Map<Integer, Collection<ColumnHandle>> updateCaseColumns,
            RetryMode retryMode)
    {
        if (retryMode != RetryMode.NO_RETRIES) {
            throw new TrinoException(NOT_SUPPORTED, "This connector does not support query retries");
        }
        return new OpenApiOutputTableHandle((OpenApiTableHandle) tableHandle);
    }

    @Override
    public void finishMerge(
            ConnectorSession session,
            ConnectorMergeTableHandle tableHandle,
            List<ConnectorTableHandle> sourceTableHandles,
            Collection<Slice> fragments,
            Collection<ComputedStatistics> computedStatistics)
    {
    }
}
