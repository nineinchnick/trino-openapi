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
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Set;
import java.util.stream.Stream;

import static io.swagger.v3.oas.models.PathItem.HttpMethod.GET;
import static io.trino.spi.type.BigintType.BIGINT;
import static java.util.stream.Collectors.toMap;
import static org.assertj.core.api.Assertions.assertThat;

class TestPathSelector
{
    private static final String PROJECT_COLUMNS = "/projects/{project_id}/columns";
    private static final String COLUMNS = "/projects/columns/{column_id}";
    private static final String CARDS = "/projects/cards/{column_id}";

    private final OpenApiColumn projectId = pathParameter("project_id", PROJECT_COLUMNS);
    private final OpenApiColumn columnId = pathParameter("column_id", COLUMNS, CARDS);
    private final List<OpenApiColumn> columns = List.of(projectId, columnId);

    @Test
    void selectPathWithAvailablePredicates()
    {
        List<String> paths = List.of(PROJECT_COLUMNS, COLUMNS);
        assertThat(PathSelector.selectPath(columns, GET, paths, Set.of(projectId)::contains)).hasValue(PROJECT_COLUMNS);
        assertThat(PathSelector.selectPath(columns, GET, paths, Set.of(columnId)::contains)).hasValue(COLUMNS);
        assertThat(PathSelector.selectPath(columns, GET, paths, Set.of()::contains)).isEmpty();
    }

    @Test
    void selectPathDeterministically()
    {
        Set<OpenApiColumn> allColumns = Set.of(projectId, columnId);
        // both paths use one predicate, so the shorter one is selected
        assertThat(PathSelector.selectPath(columns, GET, List.of(PROJECT_COLUMNS, COLUMNS), allColumns::contains)).hasValue(COLUMNS);
        assertThat(PathSelector.selectPath(columns, GET, List.of(COLUMNS, PROJECT_COLUMNS), allColumns::contains)).hasValue(COLUMNS);
        // both paths have the same length, so the first one in alphabetical order is selected
        assertThat(PathSelector.selectPath(columns, GET, List.of(COLUMNS, CARDS), allColumns::contains)).hasValue(CARDS);
        assertThat(PathSelector.selectPath(columns, GET, List.of(CARDS, COLUMNS), allColumns::contains)).hasValue(CARDS);
    }

    @Test
    void describeMissingPredicates()
    {
        assertThat(PathSelector.describeMissingPredicates(columns, GET, List.of(COLUMNS), Set.of()::contains))
                .isEqualTo("Missing required constraint for column_id");
        assertThat(PathSelector.describeMissingPredicates(columns, GET, List.of(PROJECT_COLUMNS, COLUMNS), Set.of()::contains))
                .isEqualTo("Missing required constraints for every path: GET /projects/{project_id}/columns requires project_id; GET /projects/columns/{column_id} requires column_id");
    }

    private static OpenApiColumn pathParameter(String name, String... paths)
    {
        Schema<?> schema = new Schema<>();
        schema.setType("integer");
        return OpenApiColumn.builder()
                .setName(name)
                .setSourceName(name)
                .setType(BIGINT)
                .setSourceType(schema)
                .setRequiresPredicate(Stream.of(paths)
                        .collect(toMap(path -> new HttpPath(GET, path), _ -> ParameterLocation.PATH)))
                .build();
    }
}
