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
import io.trino.spi.connector.ConnectorSplit;
import io.trino.spi.connector.ConnectorSplitManager;
import io.trino.spi.connector.ConnectorSplitSource;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.connector.Constraint;
import io.trino.spi.connector.ConstraintApplicationResult;
import io.trino.spi.connector.DynamicFilterSnapshot;
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
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;

import static io.airlift.concurrent.Threads.daemonThreadsNamed;
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
            Set<ColumnHandle> dynamicFilterColumns,
            Constraint constraint)
    {
        return new DynamicFilteringSplitSource(table);
    }

    private class DynamicFilteringSplitSource
            implements ConnectorSplitSource
    {
        private final ConnectorTableHandle table;
        private Optional<ConnectorSplitSource> delegate = Optional.empty();

        private DynamicFilteringSplitSource(ConnectorTableHandle table)
        {
            this.table = requireNonNull(table, "table is null");
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
                delegate = Optional.of(getSplitSource(table, dynamicFilterSnapshot.currentPredicate()));
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
            TupleDomain<ColumnHandle> dynamicFilterPredicate)
    {
        OpenApiTableHandle table = (OpenApiTableHandle) tableHandle;
        Map<String, OpenApiColumn> columns = spec.getTables().get(table.getSchemaTableName().getTableName()).stream()
                .collect(toMap(OpenApiColumn::getName, identity()));
        // merge in constraints from dynamicFilter, which may contain multivalued domains
        Optional<ConstraintApplicationResult<ConnectorTableHandle>> result = table.applyFilter(new Constraint(dynamicFilterPredicate), columns, domainExpansionLimit);
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
