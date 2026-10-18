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

import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.function.Predicate;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Comparator.comparingInt;
import static java.util.Comparator.comparingLong;
import static java.util.function.Predicate.not;
import static java.util.stream.Collectors.joining;

public final class PathSelector
{
    private PathSelector() {}

    /**
     * Returns the path that has all required predicates, and uses the most of the available path parameters,
     * then the most of other available predicates.
     */
    public static Optional<HttpPath> selectPath(List<OpenApiColumn> columns, List<HttpPath> paths, Predicate<OpenApiColumn> hasValue)
    {
        checkArgument(!paths.isEmpty(), "paths are empty");
        // path parameters are counted first, because rows read using a more generic path don't have values of the other path parameters,
        // so filters on them would remove all rows; paths of equal length are ordered only to make the choice deterministic
        Comparator<HttpPath> comparator = comparingLong((HttpPath path) -> countAvailablePredicates(columns, path, hasValue, true))
                .thenComparingLong(path -> countAvailablePredicates(columns, path, hasValue, false))
                .thenComparing(comparingInt((HttpPath path) -> path.path().length()).reversed())
                .thenComparing(Comparator.<HttpPath>naturalOrder().reversed());
        return paths.stream()
                .filter(path -> getMissingRequiredPredicates(columns, path, hasValue).isEmpty())
                .max(comparator);
    }

    public static String describeMissingPredicates(List<OpenApiColumn> columns, List<HttpPath> paths, Predicate<OpenApiColumn> hasValue)
    {
        if (paths.size() == 1) {
            return "Missing required constraint for " + describeColumns(getMissingRequiredPredicates(columns, paths.getFirst(), hasValue));
        }
        return "Missing required constraints for every path: " + paths.stream()
                .map(path -> "%s requires %s".formatted(path, describeColumns(getMissingRequiredPredicates(columns, path, hasValue))))
                .collect(joining("; "));
    }

    public static List<OpenApiColumn> getMissingRequiredPredicates(List<OpenApiColumn> columns, HttpPath httpPath, Predicate<OpenApiColumn> hasValue)
    {
        return columns.stream()
                .filter(column -> column.getRequiresPredicate().containsKey(httpPath))
                // page numbers are set when reading every page
                .filter(column -> !column.isPageNumber())
                .filter(not(hasValue))
                .toList();
    }

    private static long countAvailablePredicates(List<OpenApiColumn> columns, HttpPath httpPath, Predicate<OpenApiColumn> hasValue, boolean pathParameters)
    {
        return columns.stream()
                .filter(column -> column.getRequiresPredicate().containsKey(httpPath) || column.getOptionalPredicate().containsKey(httpPath))
                .filter(column -> isPathParameter(column, httpPath) == pathParameters)
                .filter(hasValue)
                .count();
    }

    private static boolean isPathParameter(OpenApiColumn column, HttpPath httpPath)
    {
        return column.getRequiresPredicate().get(httpPath) == ParameterLocation.PATH
                || column.getOptionalPredicate().get(httpPath) == ParameterLocation.PATH;
    }

    private static String describeColumns(List<OpenApiColumn> columns)
    {
        return columns.stream()
                .map(OpenApiColumn::getName)
                .collect(joining(", "));
    }
}
