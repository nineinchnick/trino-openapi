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
import com.google.common.collect.ImmutableList;
import io.airlift.slice.SizeOf;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.Constraint;
import io.trino.spi.connector.ConstraintApplicationResult;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.connector.SortItem;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.TupleDomain;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.spi.StandardErrorCode.INVALID_ROW_FILTER;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static java.util.Objects.requireNonNull;

public class OpenApiTableHandle
        implements ConnectorTableHandle
{
    private static final int INSTANCE_SIZE = SizeOf.instanceSize(OpenApiTableHandle.class);

    private final SchemaTableName schemaTableName;
    private final List<HttpPath> selectPaths;
    private final List<HttpPath> insertPaths;
    private final List<HttpPath> updatePaths;
    private final List<HttpPath> deletePaths;
    private final TupleDomain<ColumnHandle> constraint;
    private final OptionalLong limit;
    private final Optional<SortItem> sortItem;

    private OpenApiTableHandle(
            SchemaTableName schemaTableName,
            List<HttpPath> selectPaths,
            List<HttpPath> insertPaths,
            List<HttpPath> updatePaths,
            List<HttpPath> deletePaths,
            TupleDomain<ColumnHandle> constraint,
            OptionalLong limit,
            Optional<SortItem> sortItem)
    {
        this.schemaTableName = requireNonNull(schemaTableName, "schemaTableName is null");
        this.selectPaths = ImmutableList.copyOf(requireNonNull(selectPaths, "selectPaths is null"));
        this.insertPaths = ImmutableList.copyOf(requireNonNull(insertPaths, "insertPaths is null"));
        this.updatePaths = ImmutableList.copyOf(requireNonNull(updatePaths, "updatePaths is null"));
        this.deletePaths = ImmutableList.copyOf(requireNonNull(deletePaths, "deletePaths is null"));
        this.constraint = requireNonNull(constraint, "constraint is null");
        this.limit = requireNonNull(limit, "limit is null");
        this.sortItem = requireNonNull(sortItem, "sortItem is null");
    }

    // Trino only shares jackson-annotations with plugins, so handles can't be deserialized through a builder;
    // use the builder to create handles in code
    @JsonCreator
    public static OpenApiTableHandle fromJson(
            @JsonProperty("schemaTableName") SchemaTableName schemaTableName,
            @JsonProperty("selectPaths") List<HttpPath> selectPaths,
            @JsonProperty("insertPaths") List<HttpPath> insertPaths,
            @JsonProperty("updatePaths") List<HttpPath> updatePaths,
            @JsonProperty("deletePaths") List<HttpPath> deletePaths,
            @JsonProperty("constraint") TupleDomain<ColumnHandle> constraint,
            @JsonProperty("limit") OptionalLong limit,
            @JsonProperty("sortItem") Optional<SortItem> sortItem)
    {
        return new OpenApiTableHandle(schemaTableName, selectPaths, insertPaths, updatePaths, deletePaths, constraint, limit, sortItem);
    }

    public static Builder builder(SchemaTableName schemaTableName)
    {
        return new Builder(schemaTableName);
    }

    public Builder toBuilder()
    {
        return new Builder(schemaTableName)
                .setSelectPaths(selectPaths)
                .setInsertPaths(insertPaths)
                .setUpdatePaths(updatePaths)
                .setDeletePaths(deletePaths)
                .setConstraint(constraint)
                .setLimit(limit)
                .setSortItem(sortItem);
    }

    @JsonProperty
    public SchemaTableName getSchemaTableName()
    {
        return schemaTableName;
    }

    @JsonProperty
    public List<HttpPath> getSelectPaths()
    {
        return selectPaths;
    }

    @JsonProperty
    public List<HttpPath> getInsertPaths()
    {
        return insertPaths;
    }

    @JsonProperty
    public List<HttpPath> getUpdatePaths()
    {
        return updatePaths;
    }

    @JsonProperty
    public List<HttpPath> getDeletePaths()
    {
        return deletePaths;
    }

    @JsonProperty("constraint")
    public TupleDomain<ColumnHandle> getConstraint()
    {
        return constraint;
    }

    @JsonProperty
    public OptionalLong getLimit()
    {
        return limit;
    }

    public void checkHasPaths(String statement, List<HttpPath> paths)
    {
        if (!paths.isEmpty()) {
            return;
        }
        throw new TrinoException(NOT_SUPPORTED, "Table %s has no operation for %s. Set sqlOperation in the x-trino extension of an operation to use it for %s".formatted(
                schemaTableName.getTableName(),
                statement,
                statement));
    }

    /**
     * The order of rows requested from the server, with the name of the column instead of the name of a symbol in the query plan.
     */
    @JsonProperty
    public Optional<SortItem> getSortItem()
    {
        return sortItem;
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
                + SizeOf.estimatedSizeOf(selectPaths, HttpPath::getRetainedSizeInBytes)
                + SizeOf.estimatedSizeOf(insertPaths, HttpPath::getRetainedSizeInBytes)
                + SizeOf.estimatedSizeOf(updatePaths, HttpPath::getRetainedSizeInBytes)
                + SizeOf.estimatedSizeOf(deletePaths, HttpPath::getRetainedSizeInBytes)
                + constraint.getRetainedSizeInBytes(column -> ((OpenApiColumnHandle) column).getRetainedSizeInBytes())
                + SizeOf.sizeOf(limit)
                + SizeOf.sizeOf(sortItem, item -> SizeOf.estimatedSizeOf(item.getName()));
    }

    public OpenApiTableHandle cloneWithConstraint(TupleDomain<ColumnHandle> constraint)
    {
        return toBuilder()
                .setConstraint(constraint)
                .build();
    }

    public OpenApiTableHandle cloneWithLimit(long limit)
    {
        return toBuilder()
                .setLimit(OptionalLong.of(limit))
                .build();
    }

    public OpenApiTableHandle cloneWithTopN(SortItem sortItem, long limit)
    {
        return toBuilder()
                .setLimit(OptionalLong.of(limit))
                .setSortItem(Optional.of(sortItem))
                .build();
    }

    public Optional<ConstraintApplicationResult<ConnectorTableHandle>> applyFilter(Constraint constraint, Map<String, OpenApiColumn> columns, int domainExpansionLimit)
    {
        TupleDomain<ColumnHandle> summary = constraint.getSummary();
        // the only reason not to use isNone is so the linter doesn't complain about not checking an Optional
        if (summary.isAll() || summary.getDomains().isEmpty()) {
            return Optional.empty();
        }

        TupleDomain<ColumnHandle> currentConstraint = getConstraint();
        Map<ColumnHandle, Domain> previouslyPushedDomains = currentConstraint.getDomains().orElse(Map.of());

        boolean found = false;
        for (OpenApiColumn column : columns.values()) {
            if (column.getRequiresPredicate().isEmpty() && column.getOptionalPredicate().isEmpty()) {
                continue;
            }

            TupleDomain<ColumnHandle> newConstraint = normalizeConstraint(column.getHandle(), summary, domainExpansionLimit);
            if (newConstraint == null || newConstraint.getDomains().isEmpty()) {
                continue;
            }
            Domain domain = newConstraint.getDomains().get().get(column.getHandle());
            boolean isAlwaysSent = isSentByAllSelectPaths(column);
            // splits are generated for every value of a pushed down domain, so it's only useful if some path can send it,
            // and splits for paths that ignore it are merged, so they don't return the same rows
            if (!domain.isSingleValue() && !isSentBySomeSelectPath(column)) {
                continue;
            }
            if (!validateConstraint(column.getHandle(), currentConstraint, newConstraint)) {
                continue;
            }
            // Trino no longer applies an enforced range, and this value would be sent instead of its bound
            if (RangePushdown.isBoundOfEnforcedRange(columns.values(), selectPaths, previouslyPushedDomains, column)) {
                throw new TrinoException(INVALID_ROW_FILTER, "Cannot push down a predicate for " + column.getName() + ", which is a bound of an already pushed down range");
            }
            // merge with other pushed down constraints
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
            if (!isAlwaysSent) {
                // the path selected for a split might not send this predicate, so Trino still has to apply it
                continue;
            }
            // remove from remaining constraints
            summary = summary.filter(
                    (columnHandle, tupleDomain) -> !columnHandle.equals(column.getHandle()));
        }

        Map<ColumnHandle, Domain> summaryDomains = constraint.getSummary().getDomains().get();
        Map<ColumnHandle, Domain> pushedDomains = currentConstraint.getDomains().orElse(Map.of());
        for (OpenApiColumn column : columns.values()) {
            Domain domain = summaryDomains.get(column.getHandle());
            // ranges of parameters are expanded into single values instead
            if (domain == null || column.getRangeFilters().isEmpty() || isParameter(column)) {
                continue;
            }
            Domain pushedDomain = pushedDomains.get(column.getHandle());
            Domain newDomain = pushedDomain == null ? domain : pushedDomain.intersect(domain);
            if (newDomain.equals(pushedDomain) || !RangePushdown.isSentBySomePath(columns.values(), selectPaths, pushedDomains, column, newDomain)) {
                continue;
            }
            Map<ColumnHandle, Domain> domains = new HashMap<>(currentConstraint.getDomains().orElse(Map.of()));
            domains.put(column.getHandle(), newDomain);
            currentConstraint = TupleDomain.withColumnDomains(domains);
            found = true;
            if (!RangePushdown.isEnforcedByAllPaths(columns.values(), selectPaths, pushedDomains, column, newDomain)) {
                continue;
            }
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

    private static boolean isParameter(OpenApiColumn column)
    {
        return !column.getRequiresPredicate().isEmpty() || !column.getOptionalPredicate().isEmpty();
    }

    private boolean isSentByAllSelectPaths(OpenApiColumn column)
    {
        return selectPaths.stream()
                .allMatch(httpPath -> column.getRequiresPredicate().containsKey(httpPath) || column.getOptionalPredicate().containsKey(httpPath));
    }

    private boolean isSentBySomeSelectPath(OpenApiColumn column)
    {
        return selectPaths.stream()
                .anyMatch(httpPath -> column.getRequiresPredicate().containsKey(httpPath) || column.getOptionalPredicate().containsKey(httpPath));
    }

    private TupleDomain<ColumnHandle> normalizeConstraint(OpenApiColumnHandle column, TupleDomain<ColumnHandle> constraint, int domainExpansionLimit)
    {
        //noinspection OptionalGetWithoutIsPresent
        Domain domain = constraint.getDomains().get().get(column);
        // predicates are sent as single values, which can't represent nulls
        if (domain == null || domain.isNullAllowed() || domain.getValues().isNone()) {
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

    public static class Builder
    {
        private final SchemaTableName schemaTableName;
        private List<HttpPath> selectPaths = ImmutableList.of();
        private List<HttpPath> insertPaths = ImmutableList.of();
        private List<HttpPath> updatePaths = ImmutableList.of();
        private List<HttpPath> deletePaths = ImmutableList.of();
        private TupleDomain<ColumnHandle> constraint = TupleDomain.none();
        private OptionalLong limit = OptionalLong.empty();
        private Optional<SortItem> sortItem = Optional.empty();

        private Builder(SchemaTableName schemaTableName)
        {
            this.schemaTableName = requireNonNull(schemaTableName, "schemaTableName is null");
        }

        public Builder setSelectPaths(List<HttpPath> selectPaths)
        {
            this.selectPaths = requireNonNull(selectPaths, "selectPaths is null");
            return this;
        }

        public Builder setInsertPaths(List<HttpPath> insertPaths)
        {
            this.insertPaths = requireNonNull(insertPaths, "insertPaths is null");
            return this;
        }

        public Builder setUpdatePaths(List<HttpPath> updatePaths)
        {
            this.updatePaths = requireNonNull(updatePaths, "updatePaths is null");
            return this;
        }

        public Builder setDeletePaths(List<HttpPath> deletePaths)
        {
            this.deletePaths = requireNonNull(deletePaths, "deletePaths is null");
            return this;
        }

        public Builder setConstraint(TupleDomain<ColumnHandle> constraint)
        {
            this.constraint = requireNonNull(constraint, "constraint is null");
            return this;
        }

        public Builder setLimit(OptionalLong limit)
        {
            this.limit = requireNonNull(limit, "limit is null");
            return this;
        }

        public Builder setSortItem(Optional<SortItem> sortItem)
        {
            this.sortItem = requireNonNull(sortItem, "sortItem is null");
            return this;
        }

        public OpenApiTableHandle build()
        {
            return new OpenApiTableHandle(schemaTableName, selectPaths, insertPaths, updatePaths, deletePaths, constraint, limit, sortItem);
        }
    }
}
