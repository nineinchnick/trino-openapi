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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.airlift.slice.SizeOf;
import io.swagger.v3.oas.models.PathItem;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.Constraint;
import io.trino.spi.connector.ConstraintApplicationResult;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.TupleDomain;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.spi.StandardErrorCode.INVALID_ROW_FILTER;
import static java.util.Objects.requireNonNull;

public class OpenApiTableHandle
        implements ConnectorTableHandle, Cloneable
{
    private static final int INSTANCE_SIZE = SizeOf.instanceSize(OpenApiTableHandle.class);

    private final SchemaTableName schemaTableName;
    private final List<String> selectPaths;
    private final PathItem.HttpMethod selectMethod;
    private final List<String> insertPaths;
    private final PathItem.HttpMethod insertMethod;
    private final List<String> updatePaths;
    private final PathItem.HttpMethod updateMethod;
    private final List<String> deletePaths;
    private final PathItem.HttpMethod deleteMethod;
    private TupleDomain<ColumnHandle> constraint;

    @JsonCreator
    public OpenApiTableHandle(
            SchemaTableName schemaTableName,
            List<String> selectPaths,
            PathItem.HttpMethod selectMethod,
            List<String> insertPaths,
            PathItem.HttpMethod insertMethod,
            List<String> updatePaths,
            PathItem.HttpMethod updateMethod,
            List<String> deletePaths,
            PathItem.HttpMethod deleteMethod,
            TupleDomain<ColumnHandle> constraint)
    {
        this.schemaTableName = schemaTableName;
        this.selectPaths = requireNonNull(selectPaths, "selectPaths is null");
        this.selectMethod = selectMethod;
        this.insertPaths = requireNonNull(insertPaths, "insertPaths is null");
        this.insertMethod = insertMethod;
        this.updatePaths = requireNonNull(updatePaths, "updatePaths is null");
        this.updateMethod = updateMethod;
        this.deletePaths = requireNonNull(deletePaths, "deletePaths is null");
        this.deleteMethod = deleteMethod;
        this.constraint = constraint;
    }

    @JsonProperty
    public SchemaTableName getSchemaTableName()
    {
        return schemaTableName;
    }

    @JsonProperty
    public List<String> getSelectPaths()
    {
        return selectPaths;
    }

    @JsonProperty
    public PathItem.HttpMethod getSelectMethod()
    {
        return selectMethod;
    }

    @JsonProperty
    public List<String> getInsertPaths()
    {
        return insertPaths;
    }

    @JsonProperty
    public PathItem.HttpMethod getInsertMethod()
    {
        return insertMethod;
    }

    @JsonProperty
    public List<String> getUpdatePaths()
    {
        return updatePaths;
    }

    @JsonProperty
    public PathItem.HttpMethod getUpdateMethod()
    {
        return updateMethod;
    }

    @JsonProperty
    public List<String> getDeletePaths()
    {
        return deletePaths;
    }

    @JsonProperty
    public PathItem.HttpMethod getDeleteMethod()
    {
        return deleteMethod;
    }

    @JsonProperty("constraint")
    public TupleDomain<ColumnHandle> getConstraint()
    {
        return constraint;
    }

    @Override
    public String toString()
    {
        return schemaTableName.getTableName();
    }

    public long getRetainedSizeInBytes()
    {
        return (long) INSTANCE_SIZE
                + schemaTableName.getRetainedSizeInBytes()
                + SizeOf.estimatedSizeOf(selectPaths, String::length)
                + SizeOf.estimatedSizeOf(insertPaths, String::length)
                + SizeOf.estimatedSizeOf(updatePaths, String::length)
                + SizeOf.estimatedSizeOf(deletePaths, String::length)
                + SizeOf.estimatedSizeOf(selectMethod.toString())
                + SizeOf.estimatedSizeOf(insertMethod.toString())
                + SizeOf.estimatedSizeOf(updateMethod.toString())
                + SizeOf.estimatedSizeOf(deleteMethod.toString())
                + constraint.getRetainedSizeInBytes(column -> ((OpenApiColumnHandle) column).getRetainedSizeInBytes());
    }

    @Override
    public OpenApiTableHandle clone()
    {
        try {
            return (OpenApiTableHandle) super.clone();
        }
        catch (CloneNotSupportedException e) {
            throw new RuntimeException(e);
        }
    }

    public OpenApiTableHandle cloneWithConstraint(TupleDomain<ColumnHandle> constraint)
    {
        OpenApiTableHandle tableHandle = this.clone();
        tableHandle.constraint = constraint;
        return tableHandle;
    }

    public Optional<ConstraintApplicationResult<ConnectorTableHandle>> applyFilter(Constraint constraint, Map<String, OpenApiColumn> columns, int domainExpansionLimit)
    {
        TupleDomain<ColumnHandle> summary = constraint.getSummary();
        // the only reason not to use isNone is so the linter doesn't complain about not checking an Optional
        if (summary.isAll() || summary.getDomains().isEmpty()) {
            return Optional.empty();
        }

        TupleDomain<ColumnHandle> currentConstraint = getConstraint();

        boolean found = false;
        for (OpenApiColumn column : columns.values()) {
            if (column.getRequiresPredicate().isEmpty() && column.getOptionalPredicate().isEmpty()) {
                continue;
            }

            TupleDomain<ColumnHandle> newConstraint = normalizeConstraint(column.getHandle(), summary, domainExpansionLimit);
            if (newConstraint == null || newConstraint.getDomains().isEmpty()) {
                continue;
            }
            if (!validateConstraint(column.getHandle(), currentConstraint, newConstraint)) {
                continue;
            }
            // merge with other pushed down constraints
            Domain domain = newConstraint.getDomains().get().get(column.getHandle());
            if (currentConstraint.getDomains().isEmpty()) {
                currentConstraint = newConstraint;
            }
            else if (!currentConstraint.getDomains().get().containsKey(column.getHandle())) {
                Map<ColumnHandle, Domain> domains = new HashMap<>(currentConstraint.getDomains().get());
                domains.put(column.getHandle(), domain);
                currentConstraint = TupleDomain.withColumnDomains(domains);
            }
            else {
                currentConstraint.getDomains().get().get(column.getHandle()).union(domain);
            }
            found = true;
            // remove from remaining constraints
            summary = summary.filter(
                    (columnHandle, tupleDomain) -> !columnHandle.equals(column.getHandle()));
        }
        if (!found) {
            return Optional.empty();
        }

        return Optional.of(new ConstraintApplicationResult<>(
                cloneWithConstraint(currentConstraint),
                summary,
                constraint.getExpression(),
                true));
    }

    private TupleDomain<ColumnHandle> normalizeConstraint(OpenApiColumnHandle column, TupleDomain<ColumnHandle> constraint, int domainExpansionLimit)
    {
        //noinspection OptionalGetWithoutIsPresent
        Domain domain = constraint.getDomains().get().get(column);
        if (domain == null) {
            return null;
        }
        TupleDomain<ColumnHandle> newConstraint = constraint.filter(
                (columnHandle, tupleDomain) -> columnHandle.equals(column));
        if (domain.getValues().isDiscreteSet()) {
            return newConstraint;
        }
        return domain.getValues().tryExpandRanges(domainExpansionLimit)
                .map(ranges -> TupleDomain.withColumnDomains(Map.of(
                        (ColumnHandle) column,
                        Domain.multipleValues(domain.getType(), ranges.stream().collect(toImmutableList())))))
                .orElse(null);
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
}
