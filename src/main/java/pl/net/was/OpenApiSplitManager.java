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
import com.google.common.math.LongMath;
import com.google.inject.Inject;
import io.airlift.units.Duration;
import io.trino.spi.NodeManager;
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
import jakarta.annotation.PreDestroy;

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
import java.util.function.Supplier;
import java.util.stream.IntStream;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static io.trino.spi.StandardErrorCode.INVALID_ROW_FILTER;
import static java.lang.Math.min;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.Executors.newCachedThreadPool;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.function.UnaryOperator.identity;
import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toMap;
import static pl.net.was.OpenApiSessionProperties.getDynamicFilteringWaitTimeout;
import static pl.net.was.OpenApiSessionProperties.getProbeLimit;
import static pl.net.was.OpenApiSessionProperties.getSplitStrategy;
import static pl.net.was.OpenApiSessionProperties.isHttpCacheEnabled;

public class OpenApiSplitManager
        implements ConnectorSplitManager
{
    private final OpenApiSpec spec;
    private final OpenApiClient client;
    private final NodeManager nodeManager;
    private final Optional<Double> maxSplitsPerSecond;
    private final Optional<Double> maxScanRequestsPerSecond;
    private final int domainExpansionLimit;
    private final int maxFilterCombinations;
    private final SplitStrategy splitStrategy;
    private final ProbeLimit probeLimit;

    private final ExecutorService executor = newCachedThreadPool(daemonThreadsNamed(OpenApiSplitManager.class.getName() + "-%d"));

    @Inject
    public OpenApiSplitManager(OpenApiSpec spec, OpenApiClient client, NodeManager nodeManager, OpenApiConfig config)
    {
        this.spec = requireNonNull(spec, "spec is null");
        this.client = requireNonNull(client, "client is null");
        this.nodeManager = requireNonNull(nodeManager, "nodeManager is null");
        this.maxSplitsPerSecond = config.getMaxSplitsPerSecond();
        this.maxScanRequestsPerSecond = config.getMaxScanRequestsPerSecond();
        this.domainExpansionLimit = config.getDomainExpansionLimit();
        this.maxFilterCombinations = config.getMaxFilterCombinations();
        this.splitStrategy = config.getSplitStrategy();
        this.probeLimit = config.getProbeLimit();
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
        Duration dynamicFilteringWaitTimeout = canSendDynamicFilters(tableHandle, dynamicFilterColumns)
                ? getDynamicFilteringWaitTimeout(session)
                : new Duration(0, MILLISECONDS);
        return new DynamicFilteringSplitSource(session, table, dynamicFilterColumns, dynamicFilteringWaitTimeout);
    }

    @PreDestroy
    public void shutdown()
    {
        executor.shutdownNow();
    }

    // the join applies dynamic filters anyway, so waiting for them only delays reading a table if no request can send their values, or bounds of their ranges
    private boolean canSendDynamicFilters(OpenApiTableHandle table, Set<ColumnHandle> dynamicFilterColumns)
    {
        List<OpenApiColumn> columns = spec.getTables().get(table.getSchemaTableName().getTableName());
        Set<ColumnHandle> boundParameters = RangePushdown.getBoundParameters(columns, table.getConstraint());
        return columns.stream()
                .filter(column -> dynamicFilterColumns.contains(column.getHandle()) && !boundParameters.contains(column.getHandle()))
                .anyMatch(column -> isSentBySomePath(column, table) || table.getSelectPaths().stream().anyMatch(column.getRangeFilters()::containsKey));
    }

    private class DynamicFilteringSplitSource
            implements ConnectorSplitSource
    {
        private final ConnectorSession session;
        private final ConnectorTableHandle table;
        private final Set<ColumnHandle> dynamicFilterColumns;
        private final Duration dynamicFilteringWaitTimeout;
        private Optional<ConnectorSplitSource> delegate = Optional.empty();

        private DynamicFilteringSplitSource(ConnectorSession session, ConnectorTableHandle table, Set<ColumnHandle> dynamicFilterColumns, Duration dynamicFilteringWaitTimeout)
        {
            this.session = requireNonNull(session, "session is null");
            this.table = requireNonNull(table, "table is null");
            this.dynamicFilterColumns = ImmutableSet.copyOf(requireNonNull(dynamicFilterColumns, "dynamicFilterColumns is null"));
            this.dynamicFilteringWaitTimeout = requireNonNull(dynamicFilteringWaitTimeout, "dynamicFilteringWaitTimeout is null");
        }

        @Override
        public long getRequestedDynamicFilterWaitTimeoutMillis()
        {
            return dynamicFilteringWaitTimeout.toMillis();
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
                delegate = Optional.of(getSplitSource(session, table, dynamicFilterSnapshot, dynamicFilterColumns, dynamicFilteringWaitTimeout));
            }
            return delegate.get();
        }

        private synchronized Optional<ConnectorSplitSource> getDelegate()
        {
            return delegate;
        }
    }

    private ConnectorSplitSource getSplitSource(
            ConnectorSession session,
            ConnectorTableHandle tableHandle,
            DynamicFilterSnapshot dynamicFilterSnapshot,
            Set<ColumnHandle> dynamicFilterColumns,
            Duration dynamicFilteringWaitTimeout)
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
            return getSplitSource(List.of(), List.of());
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
        ImmutableList.Builder<List<Map.Entry<ColumnHandle, Domain>>> singleDomains = ImmutableList.builder();
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
            ImmutableList.Builder<Map.Entry<ColumnHandle, Domain>> splitDomains = ImmutableList.builder();
            for (Object value : values) {
                splitDomains.add(new AbstractMap.SimpleImmutableEntry<>(column, Domain.create(
                        ValueSet.of(domain.getType(), value),
                        domain.isNullAllowed())));
            }
            singleDomains.add(splitDomains.build());
        }
        List<List<Map.Entry<ColumnHandle, Domain>>> valuesToCombine = singleDomains.build();
        checkFilterCombinations(valuesToCombine);
        // then create constraints with every multivalued domain replaced with single-value sets
        Map<TupleDomain<ColumnHandle>, SplitTable> splitTables = new LinkedHashMap<>();
        for (List<Map.Entry<ColumnHandle, Domain>> splitDomains : Lists.cartesianProduct(valuesToCombine)) {
            Map<ColumnHandle, Domain> newDomains = new HashMap<>();
            for (Map.Entry<ColumnHandle, Domain> entry : splitDomains) {
                newDomains.put(entry.getKey(), entry.getValue());
            }
            HttpPath httpPath = selectPath(table, columnList, newDomains, dynamicFilterSnapshot, dynamicFilterColumns, dynamicFilteringWaitTimeout);
            // the connector applies filters on parameters the selected path doesn't send to the returned rows,
            // so splits that differ only in these filters would return the same rows;
            // dynamic filters are left out, because the join still applies them
            TupleDomain<ColumnHandle> rowFilter = pushedDownPredicate.isNone() ? TupleDomain.all() : pushedDownPredicate.filter((column, _) -> {
                OpenApiColumn openApiColumn = columns.get(((OpenApiColumnHandle) column).getName());
                return isParameter(openApiColumn) && !isSentByPath(openApiColumn, httpPath);
            });
            // servers can match sent values loosely, like by a substring or ignoring case,
            // and rows get values of parameters that are also response fields from the response, so they must still match the sent value
            TupleDomain<ColumnHandle> sentResponseFieldFilter = TupleDomain.withColumnDomains(newDomains).filter((column, _) -> {
                OpenApiColumn openApiColumn = columns.get(((OpenApiColumnHandle) column).getName());
                return !openApiColumn.isRequestOnly() && isSentByPath(openApiColumn, httpPath);
            });
            newDomains.keySet().removeIf(column -> !isSentByPath(columns.get(((OpenApiColumnHandle) column).getName()), httpPath));
            // ranges are the same for every split, and are sent as bounds, so they don't make more splits
            originalDomains.forEach((column, domain) -> {
                if (columns.get(((OpenApiColumnHandle) column).getName()).getRangeFilters().containsKey(httpPath)) {
                    newDomains.put(column, domain);
                }
            });
            TupleDomain<ColumnHandle> splitConstraint = TupleDomain.withColumnDomains(newDomains);
            splitTables.put(splitConstraint, new SplitTable(
                    table.toBuilder()
                            .setConstraint(splitConstraint)
                            .setRowFilter(rowFilter.intersect(sentResponseFieldFilter))
                            .build(),
                    httpPath));
        }
        ImmutableList.Builder<OpenApiSplit> splits = ImmutableList.builder();
        ImmutableList.Builder<Supplier<List<ConnectorSplit>>> splitsAfterFirstPages = ImmutableList.builder();
        // splits for multiple values of predicates are already read concurrently,
        // and probing for the number of pages of each would only add requests;
        // with a limit, reading pages one after another stops once it's reached
        boolean canProbe = splitTables.size() == 1 && table.getLimit().isEmpty();
        for (SplitTable splitTable : splitTables.values()) {
            Pagination pagination = spec.getPaginations(table.getSchemaTableName()).getOrDefault(splitTable.httpPath(), Pagination.none());
            SplitStrategy splitStrategy = getSplitStrategy(session)
                    .or(pagination::splitStrategy)
                    .orElse(this.splitStrategy);
            if (splitStrategy == SplitStrategy.STRIDE && table.getLimit().isEmpty() && client.canReadPagesWithStride(splitTable.table())) {
                int strideSplits = getStrideSplits(pagination);
                for (int firstPage = 1; firstPage <= strideSplits; firstPage++) {
                    splits.add(new OpenApiSplit(splitTable.table(), firstPage, strideSplits));
                }
                continue;
            }
            // without the page size, stride splits can't skip pages past the last one, but the probe strategy can read the totals from the first page
            Optional<OpenApiTableHandle> firstPage = canProbe && splitStrategy != SplitStrategy.OFF
                    ? client.getFirstPageWithTotals(splitTable.table())
                    : Optional.empty();
            if (firstPage.isEmpty()) {
                splits.add(new OpenApiSplit(splitTable.table()));
                continue;
            }
            // the first page tells how many pages there are, but its split doesn't wait for it to be requested
            splits.add(new OpenApiSplit(firstPage.get()));
            ProbeLimit probeLimit = getProbeLimit(session)
                    .or(pagination::probeLimit)
                    .orElse(this.probeLimit);
            // the number of the remaining pages is known only after reading the first one, so their splits share the rate separately
            splitsAfterFirstPages.add(() -> withRequestRates(client.getSplitsAfterFirstPage(session, splitTable.table(), probeLimit, !isHttpCacheEnabled(session))));
        }
        // TODO when implementing limits, this is where we'd break down splits more
        return getSplitSource(withRequestRates(splits.build()), splitsAfterFirstPages.build());
    }

    private List<ConnectorSplit> withRequestRates(List<OpenApiSplit> splits)
    {
        if (maxScanRequestsPerSecond.isEmpty() || splits.isEmpty()) {
            return ImmutableList.copyOf(splits);
        }
        double scanRequestsPerSecond = maxScanRequestsPerSecond.get();
        double splitRequestsPerSecond = scanRequestsPerSecond / splits.size();
        return IntStream.range(0, splits.size())
                .mapToObj(index -> splits.get(index).withRequestRate(new SplitRequestRate(
                        splitRequestsPerSecond,
                        new Duration(index * 1000.0 / scanRequestsPerSecond, MILLISECONDS))))
                .collect(toImmutableList());
    }

    private record SplitTable(OpenApiTableHandle table, HttpPath httpPath) {}

    // domain-expansion-limit applies to every filter separately, but every combination of their values is read in a separate split, with at least one request
    private void checkFilterCombinations(List<List<Map.Entry<ColumnHandle, Domain>>> valuesToCombine)
    {
        long combinations = 1;
        for (List<Map.Entry<ColumnHandle, Domain>> values : valuesToCombine) {
            combinations = LongMath.saturatedMultiply(combinations, values.size());
        }
        if (combinations <= maxFilterCombinations) {
            return;
        }
        String filters = valuesToCombine.stream()
                .filter(values -> values.size() > 1)
                .map(values -> "%s (%d values)".formatted(((OpenApiColumnHandle) values.getFirst().getKey()).getName(), values.size()))
                .collect(joining(", "));
        throw new TrinoException(INVALID_ROW_FILTER, "Filters on %s would read %d combinations of their values in separate splits, more than max-filter-combinations (%d). Filter on fewer values, or increase max-filter-combinations".formatted(
                filters,
                combinations,
                maxFilterCombinations));
    }

    private int getStrideSplits(Pagination pagination)
    {
        int strideSplits = pagination.strideSplits().orElseGet(() -> nodeManager.getRequiredWorkerNodes().size());
        if (pagination.maxPages().isPresent()) {
            return min(strideSplits, pagination.maxPages().orElseThrow());
        }
        return strideSplits;
    }

    private HttpPath selectPath(
            OpenApiTableHandle table,
            List<OpenApiColumn> columns,
            Map<ColumnHandle, Domain> domains,
            DynamicFilterSnapshot dynamicFilterSnapshot,
            Set<ColumnHandle> dynamicFilterColumns,
            Duration dynamicFilteringWaitTimeout)
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
            throw new TrinoException(INVALID_ROW_FILTER, "%s. Dynamic filters from joins for %s were not ready after %s (dynamic_filtering_wait_timeout)".formatted(
                    message,
                    String.join(", ", missingDynamicFilters),
                    dynamicFilteringWaitTimeout));
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

    private ConnectorSplitSource getSplitSource(List<ConnectorSplit> splits, List<Supplier<List<ConnectorSplit>>> laterSplits)
    {
        if (maxSplitsPerSecond.isEmpty() && laterSplits.isEmpty()) {
            return new FixedSplitSource(splits);
        }
        return new RateLimitedSplitSource(
                executor,
                ImmutableList.<Supplier<List<ConnectorSplit>>>builder()
                        .add(() -> splits)
                        .addAll(laterSplits)
                        .build(),
                // later splits are generated in the background, which this split source does even without a limit
                maxSplitsPerSecond.orElse(Double.MAX_VALUE));
    }
}
