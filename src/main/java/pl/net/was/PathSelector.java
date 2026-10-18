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
import java.util.function.Predicate;

import static java.util.Comparator.comparingInt;
import static java.util.Comparator.comparingLong;

public final class PathSelector
{
    private PathSelector() {}

    public static String selectPath(List<OpenApiColumn> columns, PathItem.HttpMethod method, List<String> paths, Predicate<OpenApiColumn> hasValue)
    {
        // pick the path that has the highest number of params present in query predicates, and the shortest if there are ties
        Comparator<String> comparator = comparingLong((String path) -> countPathParams(columns, new HttpPath(method, path), hasValue))
                .thenComparing(comparingInt(String::length).reversed());
        return paths.stream()
                // ignore paths where required params are not present
                .filter(path -> hasRequiredPathParams(columns, new HttpPath(method, path), hasValue))
                .max(comparator)
                .orElseThrow();
    }

    private static long countPathParams(List<OpenApiColumn> columns, HttpPath httpPath, Predicate<OpenApiColumn> hasValue)
    {
        return columns.stream()
                .filter(column -> column.getRequiresPredicate().get(httpPath) == ParameterLocation.PATH
                        || column.getOptionalPredicate().get(httpPath) == ParameterLocation.PATH)
                .filter(hasValue)
                .count();
    }

    private static boolean hasRequiredPathParams(List<OpenApiColumn> columns, HttpPath httpPath, Predicate<OpenApiColumn> hasValue)
    {
        return columns.stream()
                .filter(column -> column.getRequiresPredicate().get(httpPath) == ParameterLocation.PATH)
                .allMatch(hasValue);
    }
}
