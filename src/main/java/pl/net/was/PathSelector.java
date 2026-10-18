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

import io.swagger.v3.oas.models.PathItem;

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
     * Returns the path that has all required predicates, and uses the most of the available predicates.
     */
    public static Optional<String> selectPath(List<OpenApiColumn> columns, PathItem.HttpMethod method, List<String> paths, Predicate<OpenApiColumn> hasValue)
    {
        checkArgument(!paths.isEmpty(), "paths are empty");
        // the shortest path is the most generic one, and paths of equal length are ordered only to make the choice deterministic
        Comparator<String> comparator = comparingLong((String path) -> countAvailablePredicates(columns, new HttpPath(method, path), hasValue))
                .thenComparing(comparingInt(String::length).reversed())
                .thenComparing(Comparator.<String>naturalOrder().reversed());
        return paths.stream()
                .filter(path -> getMissingRequiredPredicates(columns, new HttpPath(method, path), hasValue).isEmpty())
                .max(comparator);
    }

    public static String describeMissingPredicates(List<OpenApiColumn> columns, PathItem.HttpMethod method, List<String> paths, Predicate<OpenApiColumn> hasValue)
    {
        if (paths.size() == 1) {
            return "Missing required constraint for " + describeColumns(getMissingRequiredPredicates(columns, new HttpPath(method, paths.getFirst()), hasValue));
        }
        return "Missing required constraints for every path: " + paths.stream()
                .map(path -> "%s %s requires %s".formatted(method, path, describeColumns(getMissingRequiredPredicates(columns, new HttpPath(method, path), hasValue))))
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

    private static long countAvailablePredicates(List<OpenApiColumn> columns, HttpPath httpPath, Predicate<OpenApiColumn> hasValue)
    {
        return columns.stream()
                .filter(column -> column.getRequiresPredicate().containsKey(httpPath) || column.getOptionalPredicate().containsKey(httpPath))
                .filter(hasValue)
                .count();
    }

    private static String describeColumns(List<OpenApiColumn> columns)
    {
        return columns.stream()
                .map(OpenApiColumn::getName)
                .collect(joining(", "));
    }
}
