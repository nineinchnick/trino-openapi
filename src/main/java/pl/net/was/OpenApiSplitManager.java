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
import com.google.common.collect.Lists;
import com.google.inject.Inject;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplitManager;
import io.trino.spi.connector.ConnectorSplitSource;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.connector.Constraint;
import io.trino.spi.connector.ConstraintApplicationResult;
import io.trino.spi.connector.DynamicFilter;
import io.trino.spi.connector.FixedSplitSource;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.Range;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.predicate.ValueSet;

import java.util.AbstractMap;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;

import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static io.trino.spi.connector.DynamicFilter.NOT_BLOCKED;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.Executors.newCachedThreadPool;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.function.UnaryOperator.identity;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toMap;

public class OpenApiSplitManager
        implements ConnectorSplitManager
{
    private static final long TIMEOUT_MILLIS = 20000;

    private final OpenApiSpec spec;
    private static double maxSplitsPerSecond;
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
            DynamicFilter dynamicFilter,
            Constraint constraint)
    {
        if (!dynamicFilter.isAwaitable()) {
            return getSplitSource(table, dynamicFilter);
        }
        CompletableFuture<?> dynamicFilterFuture = whenCompleted(dynamicFilter)
                .completeOnTimeout(null, TIMEOUT_MILLIS, MILLISECONDS);
        CompletableFuture<ConnectorSplitSource> splitSourceFuture = dynamicFilterFuture.thenApply(
                ignored -> getSplitSource(table, dynamicFilter));
        return new DynamicFilteringSplitSource(dynamicFilterFuture, splitSourceFuture);
    }

    private static CompletableFuture<?> whenCompleted(DynamicFilter dynamicFilter)
    {
        if (dynamicFilter.isAwaitable()) {
            return dynamicFilter.isBlocked().thenCompose(ignored -> whenCompleted(dynamicFilter));
        }
        return NOT_BLOCKED;
    }

    private static class DynamicFilteringSplitSource
            implements ConnectorSplitSource
    {
        private final CompletableFuture<?> dynamicFilterFuture;
        private final CompletableFuture<ConnectorSplitSource> splitSourceFuture;

        private DynamicFilteringSplitSource(
                CompletableFuture<?> dynamicFilterFuture,
                CompletableFuture<ConnectorSplitSource> splitSourceFuture)
        {
            this.dynamicFilterFuture = requireNonNull(dynamicFilterFuture, "dynamicFilterFuture is null");
            this.splitSourceFuture = requireNonNull(splitSourceFuture, "splitSourceFuture is null");
        }

        @Override
        public CompletableFuture<ConnectorSplitBatch> getNextBatch(int maxSize)
        {
            return splitSourceFuture.thenCompose(splitSource -> splitSource.getNextBatch(maxSize));
        }

        @Override
        @SuppressWarnings("FutureReturnValueIgnored")
        public void close()
        {
            if (!dynamicFilterFuture.cancel(true)) {
                splitSourceFuture.thenAccept(ConnectorSplitSource::close);
            }
        }

        @Override
        public boolean isFinished()
        {
            if (!splitSourceFuture.isDone()) {
                return false;
            }
            if (splitSourceFuture.isCompletedExceptionally()) {
                return false;
            }
            try {
                return splitSourceFuture.get().isFinished();
            }
            catch (InterruptedException | ExecutionException e) {
                throw new RuntimeException(e);
            }
        }
    }

    private ConnectorSplitSource getSplitSource(
            ConnectorTableHandle tableHandle,
            DynamicFilter dynamicFilter)
    {
        OpenApiTableHandle table = (OpenApiTableHandle) tableHandle;
        Map<String, OpenApiColumn> columns = spec.getTables().get(table.getSchemaTableName().getTableName()).stream()
                .collect(toMap(OpenApiColumn::getName, identity()));
        // merge in constraints from dynamicFilter, which may contain multivalued domains
        Optional<ConstraintApplicationResult<ConnectorTableHandle>> result = table.applyFilter(new Constraint(dynamicFilter.getCurrentPredicate()), columns, domainExpansionLimit);
        if (result.isPresent()) {
            table = (OpenApiTableHandle) result.get().getHandle();
        }

        TupleDomain<ColumnHandle> constraint = table.getConstraint();
        if (constraint.getDomains().isEmpty()) {
            List<OpenApiSplit> splits = List.of(new OpenApiSplit(table));
            return getSplitSource(splits);
        }

        /*
        Generate splits based on the cartesian product of all multi valued domains.
        Example, given predicates such as: `job_id IN (12, 34) AND conclusion IN ('canceled', 'failure')`
        the cartesian product will yield:
        * job_id:12, conclusion:canceled
        * job_id:34, conclusion:canceled
        * job_id:12, conclusion:failure
        * job_id:34, conclusion:failure
         */
        Map<ColumnHandle, Domain> originalDomains = constraint.getDomains().get();
        // first build a list of lists of tuples with the column and single-valued domain, for every value of a multi valued domain
        ImmutableList.Builder<List<Map.Entry<ColumnHandle, Domain>>> singleDomains = new ImmutableList.Builder<>();
        for (Map.Entry<ColumnHandle, Domain> entry : originalDomains.entrySet()) {
            OpenApiColumnHandle column = (OpenApiColumnHandle) entry.getKey();
            Domain domain = entry.getValue();

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
        // then create copies of the original constraints, with every multivalued domain replaced with single-value sets
        ImmutableList.Builder<OpenApiSplit> splits = new ImmutableList.Builder<>();
        for (List<Map.Entry<ColumnHandle, Domain>> splitDomains : Lists.cartesianProduct(singleDomains.build())) {
            Map<ColumnHandle, Domain> newDomains = new HashMap<>(originalDomains);
            for (Map.Entry<ColumnHandle, Domain> entry : splitDomains) {
                newDomains.put(entry.getKey(), entry.getValue());
            }
            splits.add(new OpenApiSplit(table.cloneWithConstraint(TupleDomain.withColumnDomains(newDomains))));
        }
        // TODO when implementing limits, this is where we'd break down splits more
        return getSplitSource(splits.build());
    }

    private ConnectorSplitSource getSplitSource(List<OpenApiSplit> splits)
    {
        if (maxSplitsPerSecond == Double.MAX_VALUE) {
            return new FixedSplitSource(splits);
        }
        return new RateLimitedSplitSource(executor, splits, maxSplitsPerSecond);
    }
}
