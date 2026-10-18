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
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.inject.Inject;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplit;
import io.trino.spi.connector.ConnectorSplitManager;
import io.trino.spi.connector.ConnectorSplitSource;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.connector.Constraint;
import io.trino.spi.connector.DynamicFilterSnapshot;
import io.trino.spi.connector.FixedSplitSource;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.Range;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.predicate.ValueSet;

import java.util.AbstractMap;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.function.Predicate;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static io.trino.spi.StandardErrorCode.INVALID_ROW_FILTER;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.Executors.newCachedThreadPool;
import static java.util.function.UnaryOperator.identity;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toMap;

public class OpenApiSplitManager
        implements ConnectorSplitManager
{
    private static final long TIMEOUT_MILLIS = 20000;

    private final OpenApiSpec spec;
    private final double maxSplitsPerSecond;
    private final int domainExpansionLimit;

    private final ExecutorService executor = newCachedThreadPool(daemonThreadsNamed(OpenApiSplitManager.class.getName() + "-%d"));

    @Inject
    public OpenApiSplitManager(OpenApiSpec spec, OpenApiConfig config)
    {
        this.spec = requireNonNull(spec, "spec is null");
        this.maxSplitsPerSecond = config.getMaxSplitsPerSecond();
        this.domainExpansionLimit = config.getDomainExpansionLimit();
    }

    @Override
    public ConnectorSplitSource getSplits(
            ConnectorTransactionHandle transaction,
            ConnectorSession session,
            ConnectorTableHandle table,
            Set<ColumnHandle> dynamicFilterColumns,
            Constraint constraint)
    {
        OpenApiTableHandle tableHandle = (OpenApiTableHandle) table;
        tableHandle.checkHasPaths("SELECT", tableHandle.getSelectPaths());
        return new DynamicFilteringSplitSource(table, dynamicFilterColumns);
    }

    private class DynamicFilteringSplitSource
            implements ConnectorSplitSource
    {
        private final ConnectorTableHandle table;
        private final Set<ColumnHandle> dynamicFilterColumns;
        private Optional<ConnectorSplitSource> delegate = Optional.empty();

        private DynamicFilteringSplitSource(ConnectorTableHandle table, Set<ColumnHandle> dynamicFilterColumns)
        {
            this.table = requireNonNull(table, "table is null");
            this.dynamicFilterColumns = ImmutableSet.copyOf(requireNonNull(dynamicFilterColumns, "dynamicFilterColumns is null"));
        }

        @Override
        public long getRequestedDynamicFilterWaitTimeoutMillis()
        {
            return TIMEOUT_MILLIS;
        }

        @Override
        public CompletableFuture<List<ConnectorSplit>> getNextBatch(int maxSize, DynamicFilterSnapshot dynamicFilterSnapshot)
        {
            return getDelegate(dynamicFilterSnapshot).getNextBatch(maxSize, dynamicFilterSnapshot);
        }

        @Override
        public void close()
        {
            getDelegate().ifPresent(ConnectorSplitSource::close);
        }

        @Override
        public boolean isFinished()
        {
            return getDelegate()
                    .map(ConnectorSplitSource::isFinished)
                    .orElse(false);
        }

        private synchronized ConnectorSplitSource getDelegate(DynamicFilterSnapshot dynamicFilterSnapshot)
        {
            if (delegate.isEmpty()) {
                delegate = Optional.of(getSplitSource(table, dynamicFilterSnapshot, dynamicFilterColumns));
            }
            return delegate.get();
        }

        private synchronized Optional<ConnectorSplitSource> getDelegate()
        {
            return delegate;
        }
    }

    private ConnectorSplitSource getSplitSource(
            ConnectorTableHandle tableHandle,
            DynamicFilterSnapshot dynamicFilterSnapshot,
            Set<ColumnHandle> dynamicFilterColumns)
    {
        OpenApiTableHandle originalTable = (OpenApiTableHandle) tableHandle;
        List<OpenApiColumn> columnList = spec.getTables().get(originalTable.getSchemaTableName().getTableName());
        Map<String, OpenApiColumn> columns = columnList.stream()
                .collect(toMap(OpenApiColumn::getName, identity()));
        TupleDomain<ColumnHandle> pushedDownPredicate = originalTable.getConstraint();
        // a value of a bound parameter would be sent instead of a range bound, which might be enforced,
        // and dynamic filters can be ignored, because the join still applies them
        Set<ColumnHandle> boundParameters = RangePushdown.getBoundParameters(columnList, pushedDownPredicate);
        TupleDomain<ColumnHandle> dynamicPredicate = dynamicFilterSnapshot.currentPredicate()
                .filter((column, _) -> !boundParameters.contains(column));
        TupleDomain<ColumnHandle> predicate = pushedDownPredicate.isNone()
                ? dynamicPredicate
                : pushedDownPredicate.intersect(dynamicPredicate);
        if (predicate.isNone()) {
            // the join has no matching rows
            return getSplitSource(List.of());
        }
        // merge in constraints from dynamicFilter, which may contain multivalued domains,
        // and must be intersected with pushed down constraints, so the handle starts without them
        OpenApiTableHandle unfilteredTable = originalTable.cloneWithConstraint(TupleDomain.none());
        OpenApiTableHandle table = unfilteredTable.applyFilter(new Constraint(predicate), columns, domainExpansionLimit)
                .map(result -> (OpenApiTableHandle) result.getHandle())
                .orElse(unfilteredTable);

        Map<ColumnHandle, Domain> originalDomains = table.getConstraint().getDomains().orElse(Map.of());

        /*
        Generate splits based on the cartesian product of all multi valued domains.
        Example, given predicates such as: `job_id IN (12, 34) AND conclusion IN ('canceled', 'failure')`
        the cartesian product will yield:
        * job_id:12, conclusion:canceled
        * job_id:34, conclusion:canceled
        * job_id:12, conclusion:failure
        * job_id:34, conclusion:failure
         */
        // first build a list of lists of tuples with the column and single-valued domain, for every value of a multi valued domain
        ImmutableList.Builder<List<Map.Entry<ColumnHandle, Domain>>> singleDomains = new ImmutableList.Builder<>();
        for (Map.Entry<ColumnHandle, Domain> entry : originalDomains.entrySet()) {
            OpenApiColumnHandle column = (OpenApiColumnHandle) entry.getKey();
            Domain domain = entry.getValue();
            if (!isSentBySomePath(columns.get(column.getName()), table)) {
                continue;
            }

            List<Object> values;
            if (domain.getValues().isDiscreteSet()) {
                values = domain.getValues().getDiscreteSet();
            }
            else {
                values = domain.getValues().getRanges().getOrderedRanges()
                        .stream()
                        .map(Range::getSingleValue)
                        .collect(toList());
            }
            ImmutableList.Builder<Map.Entry<ColumnHandle, Domain>> splitDomains = new ImmutableList.Builder<>();
            for (Object value : values) {
                splitDomains.add(new AbstractMap.SimpleImmutableEntry<>(column, Domain.create(
                        ValueSet.of(domain.getType(), value),
                        domain.isNullAllowed())));
            }
            singleDomains.add(splitDomains.build());
        }
        // then create constraints with every multivalued domain replaced with single-value sets
        Map<TupleDomain<ColumnHandle>, TupleDomain<ColumnHandle>> splitRowFilters = new LinkedHashMap<>();
        for (List<Map.Entry<ColumnHandle, Domain>> splitDomains : Lists.cartesianProduct(singleDomains.build())) {
            Map<ColumnHandle, Domain> newDomains = new HashMap<>();
            for (Map.Entry<ColumnHandle, Domain> entry : splitDomains) {
                newDomains.put(entry.getKey(), entry.getValue());
            }
            HttpPath httpPath = selectPath(table, columnList, newDomains, dynamicFilterSnapshot, dynamicFilterColumns);
            // the connector applies filters on parameters the selected path doesn't send to the returned rows,
            // so splits that differ only in these filters would return the same rows;
            // dynamic filters are left out, because the join still applies them
            TupleDomain<ColumnHandle> rowFilter = pushedDownPredicate.isNone() ? TupleDomain.all() : pushedDownPredicate.filter((column, _) -> {
                OpenApiColumn openApiColumn = columns.get(((OpenApiColumnHandle) column).getName());
                return isParameter(openApiColumn) && !isSentByPath(openApiColumn, httpPath);
            });
            newDomains.keySet().removeIf(column -> !isSentByPath(columns.get(((OpenApiColumnHandle) column).getName()), httpPath));
            // ranges are the same for every split, and are sent as bounds, so they don't make more splits
            originalDomains.forEach((column, domain) -> {
                if (columns.get(((OpenApiColumnHandle) column).getName()).getRangeFilters().containsKey(httpPath)) {
                    newDomains.put(column, domain);
                }
            });
            splitRowFilters.put(TupleDomain.withColumnDomains(newDomains), rowFilter);
        }
        List<OpenApiSplit> splits = splitRowFilters.entrySet().stream()
                .map(entry -> new OpenApiSplit(table.toBuilder()
                        .setConstraint(entry.getKey())
                        .setRowFilter(entry.getValue())
                        .build()))
                .collect(toImmutableList());
        // TODO when implementing limits, this is where we'd break down splits more
        return getSplitSource(splits);
    }

    private HttpPath selectPath(
            OpenApiTableHandle table,
            List<OpenApiColumn> columns,
            Map<ColumnHandle, Domain> domains,
            DynamicFilterSnapshot dynamicFilterSnapshot,
            Set<ColumnHandle> dynamicFilterColumns)
    {
        Predicate<OpenApiColumn> hasValue = column -> domains.containsKey(column.getHandle());
        Optional<HttpPath> path = PathSelector.selectPath(columns, table.getSelectPaths(), hasValue);
        if (path.isPresent()) {
            return path.get();
        }
        String message = PathSelector.describeMissingPredicates(columns, table.getSelectPaths(), hasValue);
        List<String> missingDynamicFilters = table.getSelectPaths().stream()
                .flatMap(selectPath -> PathSelector.getMissingRequiredPredicates(columns, selectPath, hasValue).stream())
                .filter(column -> dynamicFilterColumns.contains(column.getHandle()))
                .map(OpenApiColumn::getName)
                .distinct()
                .toList();
        if (missingDynamicFilters.isEmpty()) {
            throw new TrinoException(INVALID_ROW_FILTER, message);
        }
        if (!dynamicFilterSnapshot.isComplete()) {
            throw new TrinoException(INVALID_ROW_FILTER, "%s. Dynamic filters from joins for %s were not ready after %d ms".formatted(
                    message,
                    String.join(", ", missingDynamicFilters),
                    TIMEOUT_MILLIS));
        }
        throw new TrinoException(INVALID_ROW_FILTER, "%s. Dynamic filters from joins for %s have values that can't be sent, like ranges with more than %d values (domain-expansion-limit)".formatted(
                message,
                String.join(", ", missingDynamicFilters),
                domainExpansionLimit));
    }

    private static boolean isSentBySomePath(OpenApiColumn column, OpenApiTableHandle table)
    {
        return table.getSelectPaths().stream()
                .anyMatch(path -> isSentByPath(column, path));
    }

    private static boolean isParameter(OpenApiColumn column)
    {
        return !column.getRequiresPredicate().isEmpty() || !column.getOptionalPredicate().isEmpty();
    }

    private static boolean isSentByPath(OpenApiColumn column, HttpPath httpPath)
    {
        return column.getRequiresPredicate().containsKey(httpPath) || column.getOptionalPredicate().containsKey(httpPath);
    }

    private ConnectorSplitSource getSplitSource(List<OpenApiSplit> splits)
    {
        if (maxSplitsPerSecond == Double.MAX_VALUE) {
            return new FixedSplitSource(splits);
        }
        return new RateLimitedSplitSource(executor, splits, maxSplitsPerSecond);
    }
}
