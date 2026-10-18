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

import io.swagger.v3.oas.models.media.Schema;
import io.swagger.v3.oas.models.media.StringSchema;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.Constraint;
import io.trino.spi.connector.ConstraintApplicationResult;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.Range;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.predicate.ValueSet;
import org.junit.jupiter.api.Test;

import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.google.common.collect.ImmutableMap.toImmutableMap;

import static io.swagger.v3.oas.models.PathItem.HttpMethod.GET;
import static io.trino.spi.connector.SchemaTableName.schemaTableName;
import static io.trino.spi.type.TimestampType.TIMESTAMP_MILLIS;
import static java.time.ZoneOffset.UTC;
import static java.util.function.Function.identity;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class TestRangePushdown
{
    private static final HttpPath CHANGES = new HttpPath(GET, "/changes");
    private static final HttpPath CHANGE = new HttpPath(GET, "/changes/{change_id}");
    private static final HttpPath SECONDS_CHANGES = new HttpPath(GET, "/seconds_changes");
    private static final RangeFilter RANGE_FILTER = new RangeFilter(
            Optional.of(new RangeFilter.Bound("since", true)),
            Optional.of(new RangeFilter.Bound("until", false)),
            true);

    private final OpenApiColumn updatedAt = OpenApiColumn.builder()
            .setName("updated_at")
            .setSourceName("updated_at")
            .setType(TIMESTAMP_MILLIS)
            .setSourceType(dateTimeSchema("date-time"))
            .setRangeFilters(Map.of(CHANGES, RANGE_FILTER, SECONDS_CHANGES, RANGE_FILTER))
            .build();
    private final OpenApiColumn since = queryParameter("since", "date-time", CHANGES);
    private final OpenApiColumn until = queryParameter("until", "date-time", CHANGES);
    // sent without fractions of seconds
    private final OpenApiColumn secondsSince = queryParameter("since", "yyyy-MM-dd'T'HH:mm:ssX", SECONDS_CHANGES);
    private final OpenApiColumn secondsUntil = queryParameter("until", "yyyy-MM-dd'T'HH:mm:ssX", SECONDS_CHANGES);
    private final List<OpenApiColumn> columns = List.of(updatedAt, since, until, secondsSince, secondsUntil);

    @Test
    void sendBoundsMatchingRange()
    {
        Domain domain = domain(Range.range(TIMESTAMP_MILLIS, micros("2024-01-02T00:00"), true, micros("2024-01-04T00:00"), false));

        assertThat(getBoundValues(CHANGES, domain))
                .containsExactlyInAnyOrderEntriesOf(Map.of(since, micros("2024-01-02T00:00"), until, micros("2024-01-04T00:00")));
        assertThat(isEnforced(List.of(CHANGES), domain)).isTrue();
    }

    @Test
    void sendInclusiveBoundForExclusiveComparison()
    {
        Domain domain = domain(Range.greaterThan(TIMESTAMP_MILLIS, micros("2024-01-02T00:00")));

        assertThat(getBoundValues(CHANGES, domain)).containsExactly(Map.entry(since, micros("2024-01-02T00:00")));
        assertThat(isEnforced(List.of(CHANGES), domain)).isFalse();
    }

    @Test
    void skipExclusiveBoundForInclusiveComparison()
    {
        Domain domain = domain(Range.lessThanOrEqual(TIMESTAMP_MILLIS, micros("2024-01-02T00:00")));

        assertThat(getBoundValues(CHANGES, domain)).isEmpty();
        assertThat(RangePushdown.isSentBySomePath(columns, List.of(CHANGES), Map.of(), updatedAt, domain)).isFalse();
        assertThat(isEnforced(List.of(CHANGES), domain)).isFalse();
    }

    @Test
    void sendSpanOfDisjointRanges()
    {
        Domain values = domain(
                Range.equal(TIMESTAMP_MILLIS, micros("2024-01-02T00:00")),
                Range.equal(TIMESTAMP_MILLIS, micros("2024-01-04T00:00")));
        assertThat(getBoundValues(CHANGES, values)).containsExactly(Map.entry(since, micros("2024-01-02T00:00")));
        assertThat(isEnforced(List.of(CHANGES), values)).isFalse();

        Domain exactSpan = domain(
                Range.range(TIMESTAMP_MILLIS, micros("2024-01-02T00:00"), true, micros("2024-01-03T00:00"), false),
                Range.range(TIMESTAMP_MILLIS, micros("2024-01-04T00:00"), true, micros("2024-01-05T00:00"), false));
        assertThat(getBoundValues(CHANGES, exactSpan))
                .containsExactlyInAnyOrderEntriesOf(Map.of(since, micros("2024-01-02T00:00"), until, micros("2024-01-05T00:00")));
        // rows between the ranges are returned too
        assertThat(isEnforced(List.of(CHANGES), exactSpan)).isFalse();

        Domain unbounded = domain(
                Range.lessThan(TIMESTAMP_MILLIS, micros("2024-01-02T00:00")),
                Range.greaterThanOrEqual(TIMESTAMP_MILLIS, micros("2024-01-04T00:00")));
        assertThat(getBoundValues(CHANGES, unbounded)).isEmpty();
    }

    @Test
    void skipRangesWithNulls()
    {
        Domain domain = Domain.create(ValueSet.ofRanges(Range.greaterThanOrEqual(TIMESTAMP_MILLIS, micros("2024-01-02T00:00"))), true);

        assertThat(getBoundValues(CHANGES, domain)).isEmpty();
    }

    @Test
    void preferFiltersOnParameters()
    {
        Domain domain = domain(Range.range(TIMESTAMP_MILLIS, micros("2024-01-02T00:00"), true, micros("2024-01-04T00:00"), false));
        Map<ColumnHandle, Domain> domains = Map.of(
                updatedAt.getHandle(), domain,
                since.getHandle(), Domain.singleValue(TIMESTAMP_MILLIS, micros("2024-01-03T00:00")));

        assertThat(RangePushdown.getBoundValues(columns, CHANGES, TupleDomain.withColumnDomains(domains)))
                .containsExactly(Map.entry(until, micros("2024-01-04T00:00")));
        assertThat(RangePushdown.isEnforcedByAllPaths(columns, List.of(CHANGES), domains, updatedAt, domain)).isFalse();
    }

    @Test
    void sendBoundsThatLosePrecision()
    {
        // the lower bound is sent without the fraction, so more rows are returned
        Domain lower = domain(Range.greaterThanOrEqual(TIMESTAMP_MILLIS, micros("2024-01-02T12:00:00.500")));
        assertThat(getBoundValues(SECONDS_CHANGES, lower)).containsExactly(Map.entry(secondsSince, micros("2024-01-02T12:00:00.500")));
        assertThat(isEnforced(List.of(SECONDS_CHANGES), lower)).isFalse();

        // the upper bound would be sent without the fraction, which would skip some rows
        Domain upper = domain(Range.lessThan(TIMESTAMP_MILLIS, micros("2024-01-02T12:00:00.500")));
        assertThat(getBoundValues(SECONDS_CHANGES, upper)).isEmpty();

        Domain wholeSeconds = domain(Range.range(TIMESTAMP_MILLIS, micros("2024-01-02T12:00"), true, micros("2024-01-02T13:00"), false));
        assertThat(getBoundValues(SECONDS_CHANGES, wholeSeconds))
                .containsExactlyInAnyOrderEntriesOf(Map.of(secondsSince, micros("2024-01-02T12:00"), secondsUntil, micros("2024-01-02T13:00")));
        assertThat(isEnforced(List.of(SECONDS_CHANGES), wholeSeconds)).isTrue();
    }

    @Test
    void enforceOnlyIfEveryPathSendsBounds()
    {
        Domain domain = domain(Range.greaterThanOrEqual(TIMESTAMP_MILLIS, micros("2024-01-02T00:00")));

        assertThat(getBoundValues(CHANGE, domain)).isEmpty();
        assertThat(RangePushdown.isSentBySomePath(columns, List.of(CHANGES, CHANGE), Map.of(), updatedAt, domain)).isTrue();
        assertThat(isEnforced(List.of(CHANGES, SECONDS_CHANGES), domain)).isTrue();
        assertThat(isEnforced(List.of(CHANGES, CHANGE), domain)).isFalse();
    }

    @Test
    void enforceOnlyExactFilters()
    {
        OpenApiColumn inexactUpdatedAt = OpenApiColumn.builderFrom(updatedAt)
                .setRangeFilters(Map.of(CHANGES, new RangeFilter(RANGE_FILTER.lower(), RANGE_FILTER.upper(), false)))
                .build();
        Domain domain = domain(Range.greaterThanOrEqual(TIMESTAMP_MILLIS, micros("2024-01-02T00:00")));

        assertThat(RangePushdown.isSentBySomePath(columns, List.of(CHANGES), Map.of(), inexactUpdatedAt, domain)).isTrue();
        assertThat(RangePushdown.isEnforcedByAllPaths(columns, List.of(CHANGES), Map.of(), inexactUpdatedAt, domain)).isFalse();
    }

    @Test
    void findBoundParametersOfConstrainedColumns()
    {
        Domain domain = domain(Range.greaterThanOrEqual(TIMESTAMP_MILLIS, micros("2024-01-02T00:00")));

        assertThat(RangePushdown.getBoundParameters(columns, TupleDomain.withColumnDomains(Map.of(updatedAt.getHandle(), domain))))
                .containsExactlyInAnyOrder(since.getHandle(), until.getHandle(), secondsSince.getHandle(), secondsUntil.getHandle());
        assertThat(RangePushdown.getBoundParameters(columns, TupleDomain.all())).isEmpty();
    }

    @Test
    void refuseFiltersReplacingBoundsOfEnforcedRanges()
    {
        Domain domain = domain(Range.greaterThanOrEqual(TIMESTAMP_MILLIS, micros("2024-01-02T00:00")));
        Map<ColumnHandle, Domain> enforced = Map.of(updatedAt.getHandle(), domain);
        Map<ColumnHandle, Domain> notEnforced = Map.of(
                updatedAt.getHandle(), domain(Range.greaterThan(TIMESTAMP_MILLIS, micros("2024-01-02T00:00"))));

        assertThat(RangePushdown.isBoundOfEnforcedRange(columns, List.of(CHANGES), enforced, since)).isTrue();
        assertThat(RangePushdown.isBoundOfEnforcedRange(columns, List.of(CHANGES), notEnforced, since)).isFalse();
        assertThat(RangePushdown.isBoundOfEnforcedRange(columns, List.of(CHANGES), Map.of(), since)).isFalse();

        Map<String, OpenApiColumn> columnsByName = columns.stream()
                .collect(toImmutableMap(OpenApiColumn::getName, identity()));
        OpenApiTableHandle table = OpenApiTableHandle.builder(schemaTableName("default", "changes"))
                .setSelectPaths(List.of(CHANGES))
                .build();
        Optional<ConstraintApplicationResult<ConnectorTableHandle>> rangeResult = table.applyFilter(
                new Constraint(TupleDomain.withColumnDomains(enforced)),
                columnsByName,
                256);
        assertThat(rangeResult).isPresent();
        assertThat(rangeResult.get().getRemainingFilter().isAll()).isTrue();

        // the range was removed from the filters Trino applies, so sending the parameter instead of its bound would return more rows
        OpenApiTableHandle rangeTable = (OpenApiTableHandle) rangeResult.get().getHandle();
        Constraint sinceConstraint = new Constraint(TupleDomain.withColumnDomains(Map.of(since.getHandle(), Domain.singleValue(TIMESTAMP_MILLIS, micros("2024-01-01T00:00")))));
        assertThatThrownBy(() -> rangeTable.applyFilter(sinceConstraint, columnsByName, 256))
                .isInstanceOf(TrinoException.class)
                .hasMessage("Cannot push down a predicate for since, which is a bound of an already pushed down range");
    }

    private Map<OpenApiColumn, Object> getBoundValues(HttpPath httpPath, Domain domain)
    {
        return RangePushdown.getBoundValues(columns, httpPath, TupleDomain.withColumnDomains(Map.of(updatedAt.getHandle(), domain)));
    }

    private boolean isEnforced(List<HttpPath> paths, Domain domain)
    {
        return RangePushdown.isEnforcedByAllPaths(columns, paths, Map.of(), updatedAt, domain);
    }

    private static Domain domain(Range first, Range... rest)
    {
        return Domain.create(ValueSet.ofRanges(first, rest), false);
    }

    private static long micros(String dateTime)
    {
        LocalDateTime value = LocalDateTime.parse(dateTime);
        return value.toEpochSecond(UTC) * 1_000_000 + value.getNano() / 1_000;
    }

    private static OpenApiColumn queryParameter(String name, String format, HttpPath httpPath)
    {
        return OpenApiColumn.builder()
                .setName(httpPath.equals(CHANGES) ? name : "seconds_" + name)
                .setSourceName(name)
                .setType(TIMESTAMP_MILLIS)
                .setSourceType(dateTimeSchema(format))
                .setOptionalPredicate(Map.of(httpPath, ParameterLocation.QUERY))
                .build();
    }

    private static Schema<?> dateTimeSchema(String format)
    {
        Schema<?> schema = new StringSchema();
        schema.setFormat(format);
        return schema;
    }
}
