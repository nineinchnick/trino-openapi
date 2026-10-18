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

import com.google.common.collect.ImmutableMap;
import io.swagger.v3.oas.models.media.Schema;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.Range;
import io.trino.spi.predicate.Ranges;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.type.DateType;
import io.trino.spi.type.TimestampType;

import java.time.DateTimeException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.Set;
import java.util.stream.Stream;

import static com.google.common.collect.ImmutableSet.toImmutableSet;

/**
 * Translates ranges of response columns into values of the bound parameters declared in their {@link RangeFilter}s.
 * A bound is only sent if the API returns all rows matching the range with it, so it never removes rows Trino would return.
 */
public final class RangePushdown
{
    private RangePushdown() {}

    /**
     * Returns native values of the parameters of the given path, which limit the rows to the ranges in the constraint.
     */
    public static Map<OpenApiColumn, Object> getBoundValues(Collection<OpenApiColumn> columns, HttpPath httpPath, TupleDomain<ColumnHandle> constraint)
    {
        Map<ColumnHandle, Domain> domains = constraint.getDomains().orElse(Map.of());
        ImmutableMap.Builder<OpenApiColumn, Object> values = ImmutableMap.builder();
        for (OpenApiColumn column : columns) {
            Domain domain = domains.get(column.getHandle());
            if (domain == null) {
                continue;
            }
            getSentRange(columns, httpPath, domains, column, domain)
                    .ifPresent(range -> range.bounds().forEach(bound -> values.put(bound.parameter(), bound.value())));
        }
        return values.buildOrThrow();
    }

    public static boolean isSentBySomePath(Collection<OpenApiColumn> columns, List<HttpPath> paths, Map<ColumnHandle, Domain> pushedDomains, OpenApiColumn column, Domain domain)
    {
        return paths.stream()
                .anyMatch(path -> getSentRange(columns, path, pushedDomains, column, domain)
                        .map(range -> !range.bounds().isEmpty())
                        .orElse(false));
    }

    /**
     * Returns true if every path sends bounds that make the API return only rows matching the domain.
     */
    public static boolean isEnforcedByAllPaths(Collection<OpenApiColumn> columns, List<HttpPath> paths, Map<ColumnHandle, Domain> pushedDomains, OpenApiColumn column, Domain domain)
    {
        return paths.stream()
                .allMatch(path -> getSentRange(columns, path, pushedDomains, column, domain)
                        .map(SentRange::exact)
                        .orElse(false));
    }

    /**
     * Returns true if a filter on the parameter would be sent instead of a bound of a pushed down range that is enforced.
     */
    public static boolean isBoundOfEnforcedRange(Collection<OpenApiColumn> columns, List<HttpPath> paths, Map<ColumnHandle, Domain> pushedDomains, OpenApiColumn parameter)
    {
        return columns.stream()
                .filter(column -> pushedDomains.containsKey(column.getHandle()))
                .filter(column -> column.getRangeFilters().values().stream()
                        .flatMap(filter -> Stream.of(filter.lower(), filter.upper()).flatMap(Optional::stream))
                        .anyMatch(bound -> bound.parameter().equals(parameter.getSourceName())))
                .anyMatch(column -> isEnforcedByAllPaths(columns, paths, pushedDomains, column, pushedDomains.get(column.getHandle())));
    }

    /**
     * Returns parameters that can be bound by ranges in the constraint, on any path.
     */
    public static Set<ColumnHandle> getBoundParameters(Collection<OpenApiColumn> columns, TupleDomain<ColumnHandle> constraint)
    {
        Map<ColumnHandle, Domain> domains = constraint.getDomains().orElse(Map.of());
        Set<String> names = columns.stream()
                .filter(column -> domains.containsKey(column.getHandle()))
                .flatMap(column -> column.getRangeFilters().values().stream())
                .flatMap(filter -> Stream.of(filter.lower(), filter.upper()).flatMap(Optional::stream))
                .map(RangeFilter.Bound::parameter)
                .collect(toImmutableSet());
        return columns.stream()
                .filter(column -> names.contains(column.getSourceName()))
                .filter(column -> !column.getRequiresPredicate().isEmpty() || !column.getOptionalPredicate().isEmpty())
                .map(OpenApiColumn::getHandle)
                .collect(toImmutableSet());
    }

    public static boolean isNonPathParameter(OpenApiColumn column, HttpPath httpPath)
    {
        ParameterLocation location = column.getRequiresPredicate().getOrDefault(httpPath, column.getOptionalPredicate().get(httpPath));
        return location != null && location != ParameterLocation.PATH;
    }

    private record SentBound(OpenApiColumn parameter, Object value, boolean exact) {}

    private record SentRange(List<SentBound> bounds, boolean exact) {}

    private static Optional<SentRange> getSentRange(Collection<OpenApiColumn> columns, HttpPath httpPath, Map<ColumnHandle, Domain> domains, OpenApiColumn column, Domain domain)
    {
        RangeFilter filter = column.getRangeFilters().get(httpPath);
        // the API can't be asked for rows without a value
        if (filter == null || domain.isNullAllowed() || !domain.getType().isOrderable() || domain.getValues().isNone()) {
            return Optional.empty();
        }
        Ranges ranges = domain.getValues().getRanges();
        // disjoint ranges are sent as their span, which includes the values between them
        Range span = ranges.getSpan();
        Optional<SentBound> lower = span.isLowUnbounded()
                ? Optional.empty()
                : filter.lower().flatMap(bound -> getLowerBound(columns, httpPath, domains, bound, span));
        Optional<SentBound> upper = span.isHighUnbounded()
                ? Optional.empty()
                : filter.upper().flatMap(bound -> getUpperBound(columns, httpPath, domains, bound, span));
        boolean isLowerExact = span.isLowUnbounded() || lower.map(SentBound::exact).orElse(false);
        boolean isUpperExact = span.isHighUnbounded() || upper.map(SentBound::exact).orElse(false);
        return Optional.of(new SentRange(
                Stream.of(lower, upper).flatMap(Optional::stream).toList(),
                filter.exact() && ranges.getRangeCount() == 1 && isLowerExact && isUpperExact));
    }

    private static Optional<SentBound> getLowerBound(Collection<OpenApiColumn> columns, HttpPath httpPath, Map<ColumnHandle, Domain> domains, RangeFilter.Bound bound, Range span)
    {
        // an exclusive parameter would skip rows equal to the bound
        if (span.isLowInclusive() && !bound.inclusive()) {
            return Optional.empty();
        }
        Optional<OpenApiColumn> parameter = getParameter(columns, httpPath, domains, bound.parameter());
        if (parameter.isEmpty()) {
            return Optional.empty();
        }
        Object value = span.getLowBoundedValue();
        OptionalInt sentComparison = compareSentValue(parameter.get(), value);
        // a lower bound that loses precision when sent must not become greater
        if (sentComparison.isEmpty() || sentComparison.getAsInt() > 0) {
            return Optional.empty();
        }
        return Optional.of(new SentBound(parameter.get(), value, sentComparison.getAsInt() == 0 && span.isLowInclusive() == bound.inclusive()));
    }

    private static Optional<SentBound> getUpperBound(Collection<OpenApiColumn> columns, HttpPath httpPath, Map<ColumnHandle, Domain> domains, RangeFilter.Bound bound, Range span)
    {
        if (span.isHighInclusive() && !bound.inclusive()) {
            return Optional.empty();
        }
        Optional<OpenApiColumn> parameter = getParameter(columns, httpPath, domains, bound.parameter());
        if (parameter.isEmpty()) {
            return Optional.empty();
        }
        Object value = span.getHighBoundedValue();
        OptionalInt sentComparison = compareSentValue(parameter.get(), value);
        if (sentComparison.isEmpty() || sentComparison.getAsInt() < 0) {
            return Optional.empty();
        }
        return Optional.of(new SentBound(parameter.get(), value, sentComparison.getAsInt() == 0 && span.isHighInclusive() == bound.inclusive()));
    }

    private static Optional<OpenApiColumn> getParameter(Collection<OpenApiColumn> columns, HttpPath httpPath, Map<ColumnHandle, Domain> domains, String name)
    {
        return columns.stream()
                .filter(column -> column.getSourceName().equals(name))
                .filter(column -> isNonPathParameter(column, httpPath))
                // a filter on the parameter itself is sent instead
                .filter(column -> !domains.containsKey(column.getHandle()))
                .findFirst();
    }

    /**
     * Compares the value the API receives, after formatting it as the parameter declares, with the original value.
     */
    private static OptionalInt compareSentValue(OpenApiColumn parameter, Object value)
    {
        Schema<?> schema = parameter.getSourceType();
        try {
            return switch (parameter.getType()) {
                case DateType _ -> OptionalInt.of(Long.compare(DateTimeConverter.parseDate(DateTimeConverter.formatDate((long) value, schema), schema), (long) value));
                case TimestampType type when value instanceof Long epochMicros ->
                        OptionalInt.of(Long.compare((long) DateTimeConverter.parseTimestamp(DateTimeConverter.formatTimestamp(epochMicros, type, schema), type, schema), epochMicros));
                case TimestampType _ -> OptionalInt.empty();
                default -> OptionalInt.of(0);
            };
        }
        catch (TrinoException | DateTimeException | IllegalArgumentException _) {
            // a custom format might not be readable, so it's unknown which value the API receives
            return OptionalInt.empty();
        }
    }
}
