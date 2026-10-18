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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
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
import io.trino.spi.connector.LimitApplicationResult;
import io.trino.spi.connector.RelationColumnsMetadata;
import io.trino.spi.connector.RetryMode;
import io.trino.spi.connector.RowChangeParadigm;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.connector.SortItem;
import io.trino.spi.connector.TableNotFoundException;
import io.trino.spi.connector.TopNApplicationResult;
import io.trino.spi.statistics.ComputedStatistics;

import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.UnaryOperator;
import java.util.stream.Stream;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.trino.spi.connector.RowChangeParadigm.CHANGE_ONLY_UPDATED_COLUMNS;
import static java.util.Collections.emptyIterator;
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
    public Iterator<RelationColumnsMetadata> streamRelationColumns(
            ConnectorSession session,
            Optional<String> schemaName,
            UnaryOperator<Set<SchemaTableName>> relationFilter)
    {
        if (schemaName.isPresent() && !schemaName.get().equals(SCHEMA_NAME)) {
            return emptyIterator();
        }
        Map<SchemaTableName, List<OpenApiColumn>> tables = spec.getTables().entrySet().stream()
                .collect(toImmutableMap(entry -> new SchemaTableName(SCHEMA_NAME, entry.getKey()), Map.Entry::getValue));
        return relationFilter.apply(tables.keySet()).stream()
                .map(table -> RelationColumnsMetadata.forTable(
                        table,
                        tables.get(table).stream().map(OpenApiColumn::getMetadata).toList()))
                .iterator();
    }

    @Override
    public Optional<ConstraintApplicationResult<ConnectorTableHandle>> applyFilter(
            ConnectorSession session,
            ConnectorTableHandle table,
            Constraint constraint)
    {
        OpenApiTableHandle openApiTable = (OpenApiTableHandle) table;
        if (openApiTable.getLimit().isPresent()) {
            // the limit applies to rows before this filter, so sending the filter in requests would change the results
            return Optional.empty();
        }
        return openApiTable.applyFilter(constraint, getColumns(table), domainExpansionLimit);
    }

    @Override
    public Optional<LimitApplicationResult<ConnectorTableHandle>> applyLimit(
            ConnectorSession session,
            ConnectorTableHandle table,
            long limit)
    {
        // Trino pushes a limit only into a scan without a remaining filter, so every row read from the server is returned,
        // and pagination can stop once the limit is reached
        OpenApiTableHandle openApiTable = (OpenApiTableHandle) table;
        if (openApiTable.getLimit().isPresent() && openApiTable.getLimit().getAsLong() <= limit) {
            return Optional.empty();
        }
        // every split reads up to the limit, and the last page can have more rows than needed, so Trino must still apply it
        return Optional.of(new LimitApplicationResult<>(openApiTable.cloneWithLimit(limit), false, false));
    }

    @Override
    public Optional<TopNApplicationResult<ConnectorTableHandle>> applyTopN(
            ConnectorSession session,
            ConnectorTableHandle table,
            long topNCount,
            List<SortItem> sortItems,
            Map<String, ColumnHandle> assignments)
    {
        // like with applyLimit, Trino pushes a TopN only into a scan without a remaining filter
        OpenApiTableHandle openApiTable = (OpenApiTableHandle) table;
        if (openApiTable.getLimit().isPresent() || sortItems.size() != 1) {
            return Optional.empty();
        }
        SortItem sortItem = sortItems.getFirst();
        Map<String, OpenApiColumn> columns = getColumns(table);
        OpenApiColumn column = columns.get(((OpenApiColumnHandle) assignments.get(sortItem.getName())).getName());
        // the server might place nulls differently than NULLS FIRST or NULLS LAST in the query
        if (!column.isResponseRequired(openApiTable.getSelectPaths())) {
            return Optional.empty();
        }
        List<Sorting> sortings = spec.getPaginations(openApiTable.getSchemaTableName()).values().stream()
                .flatMap(pagination -> pagination.sorting().stream())
                .toList();
        if (sortings.stream().noneMatch(sorting -> sorting.sortValues().containsKey(column.getSourceName()))) {
            return Optional.empty();
        }
        // rows contain values of request parameters, so sending the sort parameters would change values of their columns
        Set<String> sortParams = sortings.stream()
                .flatMap(sorting -> Stream.of(sorting.sortParam(), sorting.directionParam()))
                .collect(toImmutableSet());
        boolean readsSortParams = assignments.values().stream()
                .map(assignment -> columns.get(((OpenApiColumnHandle) assignment).getName()))
                .anyMatch(assignedColumn -> isRequestParameter(assignedColumn) && sortParams.contains(assignedColumn.getSourceName()));
        if (readsSortParams) {
            return Optional.empty();
        }
        // every split reads up to the limit, and the last page can have more rows than needed, so Trino must still apply it
        return Optional.of(new TopNApplicationResult<>(
                openApiTable.cloneWithTopN(new SortItem(column.getName(), sortItem.getSortOrder()), topNCount),
                false,
                false));
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
        return new OpenApiOutputTableHandle(
                (OpenApiTableHandle) tableHandle,
                columns.stream()
                        .map(OpenApiColumnHandle.class::cast)
                        .collect(toImmutableList()),
                ImmutableMap.of());
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
        // tables have the row ID column only when they have operations for UPDATE or DELETE
        table.checkHasPaths("UPDATE or DELETE", ImmutableList.<HttpPath>builder()
                .addAll(table.getUpdatePaths())
                .addAll(table.getDeletePaths())
                .build());
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
        OpenApiTableHandle table = (OpenApiTableHandle) tableHandle;
        // merge pages include values for all visible columns
        List<OpenApiColumnHandle> dataColumns = spec.getTables().get(table.getSchemaTableName().getTableName()).stream()
                .filter(column -> !column.getMetadata().isHidden())
                .map(OpenApiColumn::getHandle)
                .collect(toImmutableList());
        // the row change paradigm makes merge pages include values of all columns, not only the updated ones,
        // so the updated columns are known only from the merge cases
        return new OpenApiOutputTableHandle(
                table,
                dataColumns,
                updateCaseColumns.entrySet().stream()
                        .collect(toImmutableMap(
                                Map.Entry::getKey,
                                entry -> entry.getValue().stream()
                                        .map(OpenApiColumnHandle.class::cast)
                                        .collect(toImmutableSet()))));
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

    private static boolean isRequestParameter(OpenApiColumn column)
    {
        return !column.getRequiresPredicate().isEmpty() || !column.getOptionalPredicate().isEmpty();
    }
}
