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
import com.google.inject.Inject;
import io.airlift.slice.Slice;
import io.swagger.v3.oas.models.PathItem;
import io.swagger.v3.oas.models.parameters.Parameter;
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
import io.trino.spi.connector.ConnectorTableProperties;
import io.trino.spi.connector.Constraint;
import io.trino.spi.connector.ConstraintApplicationResult;
import io.trino.spi.connector.RetryMode;
import io.trino.spi.connector.RowChangeParadigm;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.connector.SchemaTablePrefix;
import io.trino.spi.connector.TableColumnsMetadata;
import io.trino.spi.connector.TableNotFoundException;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.Range;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.statistics.ComputedStatistics;
import io.trino.spi.type.Type;

import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.spi.StandardErrorCode.INVALID_ROW_FILTER;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.trino.spi.connector.RowChangeParadigm.CHANGE_ONLY_UPDATED_COLUMNS;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toMap;
import static pl.net.was.OpenApiSpec.ROW_ID;

public class OpenApiMetadata
        implements ConnectorMetadata
{
    public static final String SCHEMA_NAME = "default";

    private final OpenApiSpec spec;

    @Inject
    public OpenApiMetadata(OpenApiSpec spec)
    {
        this.spec = spec;
    }

    @Override
    public List<String> listSchemaNames(ConnectorSession connectorSession)
    {
        return List.of(SCHEMA_NAME);
    }

    @Override
    public ConnectorTableHandle getTableHandle(ConnectorSession connectorSession, SchemaTableName schemaTableName)
    {
        if (!schemaTableName.getSchemaName().equals(SCHEMA_NAME)) {
            return null;
        }
        Map<PathItem.HttpMethod, String> paths = spec.getPaths().get(schemaTableName.getTableName());
        if (paths == null) {
            throw new TableNotFoundException(schemaTableName);
        }
        return new OpenApiTableHandle(
                schemaTableName,
                paths.get(PathItem.HttpMethod.GET),
                paths.get(PathItem.HttpMethod.POST),
                paths.containsKey(PathItem.HttpMethod.PUT) ? paths.get(PathItem.HttpMethod.PUT) : paths.get(PathItem.HttpMethod.POST),
                paths.get(PathItem.HttpMethod.DELETE),
                TupleDomain.none());
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
        List<ColumnMetadata> columns = spec.getTables().get(schemaTableName.getTableName());
        if (columns == null) {
            throw new TableNotFoundException(schemaTableName);
        }
        return new ConnectorTableMetadata(schemaTableName, columns);
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
        return getTableMetadata(connectorTableHandle).getColumns().stream()
                .collect(toMap(ColumnMetadata::getName, column -> new OpenApiColumnHandle(column.getName(), column.getType())));
    }

    public Map<String, OpenApiColumnHandle> getColumnHandles(ConnectorTableHandle connectorTableHandle)
    {
        return getTableMetadata(connectorTableHandle).getColumns().stream()
                .collect(toMap(ColumnMetadata::getName, column -> new OpenApiColumnHandle(column.getName(), column.getType())));
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
    public ConnectorTableProperties getTableProperties(ConnectorSession session, ConnectorTableHandle table)
    {
        return new ConnectorTableProperties();
    }

    @Override
    public Iterator<TableColumnsMetadata> streamTableColumns(ConnectorSession session, SchemaTablePrefix prefix)
    {
        return spec.getTables().entrySet().stream()
                .map(entry -> TableColumnsMetadata.forTable(
                        new SchemaTableName(prefix.getSchema().orElse(""), entry.getKey()),
                        entry.getValue()))
                .iterator();
    }

    @Override
    public Optional<ConstraintApplicationResult<ConnectorTableHandle>> applyFilter(
            ConnectorSession session,
            ConnectorTableHandle table,
            Constraint constraint)
    {
        OpenApiTableHandle openApiTable = (OpenApiTableHandle) table;

        Map<String, OpenApiColumnHandle> columns = getColumnHandles(table);
        TupleDomain<ColumnHandle> summary = constraint.getSummary();
        // the only reason not to use isNone is so the linter doesn't complain about not checking an Optional
        if (summary.isAll() || summary.getDomains().isEmpty()) {
            return Optional.empty();
        }

        TupleDomain<ColumnHandle> currentConstraint = openApiTable.getConstraint();

        boolean found = false;
        Map<String, Parameter> requiredParameters = spec.getRequiredParameters().get(openApiTable.getSelectPath()).get(PathItem.HttpMethod.GET);
        requireNonNull(requiredParameters, "requiredParameters is null");
        for (Map.Entry<String, Parameter> entry : requiredParameters.entrySet()) {
            String columnName = entry.getKey();
            OpenApiColumnHandle column = columns.get(columnName);

            TupleDomain<ColumnHandle> newConstraint = normalizeConstraint(column, summary);
            if (newConstraint == null || newConstraint.getDomains().isEmpty()) {
                continue;
            }
            if (!validateConstraint(column, currentConstraint, newConstraint)) {
                continue;
            }
            // merge with other pushed down constraints
            Domain domain = newConstraint.getDomains().get().get(column);
            if (currentConstraint.getDomains().isEmpty()) {
                currentConstraint = newConstraint;
            }
            else if (!currentConstraint.getDomains().get().containsKey(column)) {
                Map<ColumnHandle, Domain> domains = new HashMap<>(currentConstraint.getDomains().get());
                domains.put(column, domain);
                currentConstraint = TupleDomain.withColumnDomains(domains);
            }
            else {
                currentConstraint.getDomains().get().get(column).union(domain);
            }
            found = true;
            // remove from remaining constraints
            summary = summary.filter(
                    (columnHandle, tupleDomain) -> !columnHandle.equals(column));
        }
        if (!found) {
            return Optional.empty();
        }

        return Optional.of(new ConstraintApplicationResult<>(
                openApiTable.cloneWithConstraint(currentConstraint),
                summary,
                true));
    }

    private TupleDomain<ColumnHandle> normalizeConstraint(OpenApiColumnHandle column, TupleDomain<ColumnHandle> constraint)
    {
        //noinspection OptionalGetWithoutIsPresent
        Domain domain = constraint.getDomains().get().get(column);
        if (domain == null) {
            return null;
        }
        TupleDomain<ColumnHandle> newConstraint = constraint.filter(
                (columnHandle, tupleDomain) -> columnHandle.equals(column));
        if (!domain.getType().isOrderable()) {
            return newConstraint;
        }
        if (!domain.getValues().isDiscreteSet() && !domain.getValues().getRanges().getOrderedRanges().stream().allMatch(Range::isSingleValue)) {
            //log.warning(format("Not pushing down filter on %s because it's not a discrete set: %s", column.getName(), domain));
            return null;
        }
        return newConstraint;
    }

    private boolean validateConstraint(OpenApiColumnHandle column, TupleDomain<ColumnHandle> currentConstraint, TupleDomain<ColumnHandle> newConstraint)
    {
        if (currentConstraint.getDomains().isEmpty() || !currentConstraint.getDomains().get().containsKey(column)) {
            return true;
        }
        Domain currentDomain = currentConstraint.getDomains().get().get(column);
        Domain newDomain = newConstraint.getDomains().get().get(column);
        if (currentDomain.equals(newDomain)) {
            // it is important to avoid processing same constraint multiple times
            // so that planner doesn't get stuck in a loop
            return false;
        }
        // can push down only the first predicate against this column
        throw new TrinoException(INVALID_ROW_FILTER, "Already pushed down a predicate for " + column.getName() + " which only supports a single value");
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
        List<OpenApiColumnHandle> columnHandles = columns.stream()
                .map(OpenApiColumnHandle.class::cast)
                .collect(toImmutableList());
        ImmutableList.Builder<String> columnNames = ImmutableList.builder();
        ImmutableList.Builder<Type> columnTypes = ImmutableList.builder();
        for (OpenApiColumnHandle column : columnHandles) {
            if (getColumnMetadata(session, tableHandle, column).isHidden()) {
                continue;
            }
            columnNames.add(column.getName());
            columnTypes.add(column.getType());
        }
        return new OpenApiOutputTableHandle((OpenApiTableHandle) tableHandle, columnNames.build(), columnTypes.build());
    }

    @Override
    public Optional<ConnectorOutputMetadata> finishInsert(
            ConnectorSession session,
            ConnectorInsertTableHandle insertHandle,
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
            RetryMode retryMode)
    {
        if (retryMode != RetryMode.NO_RETRIES) {
            throw new TrinoException(NOT_SUPPORTED, "This connector does not support query retries");
        }
        ImmutableList.Builder<String> columnNames = ImmutableList.builder();
        ImmutableList.Builder<Type> columnTypes = ImmutableList.builder();
        getTableMetadata(tableHandle).getColumns().stream()
                .filter(column -> !column.isHidden())
                .forEach(column -> {
                    columnNames.add(column.getName());
                    columnTypes.add(column.getType());
                });
        return new OpenApiOutputTableHandle((OpenApiTableHandle) tableHandle, columnNames.build(), columnTypes.build());
    }

    @Override
    public void finishMerge(
            ConnectorSession session,
            ConnectorMergeTableHandle tableHandle,
            Collection<Slice> fragments,
            Collection<ComputedStatistics> computedStatistics)
    {
    }
}
