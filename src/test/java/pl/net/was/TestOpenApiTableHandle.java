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

import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.SortItem;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.TupleDomain;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;

import static io.swagger.v3.oas.models.PathItem.HttpMethod.DELETE;
import static io.swagger.v3.oas.models.PathItem.HttpMethod.GET;
import static io.swagger.v3.oas.models.PathItem.HttpMethod.PATCH;
import static io.swagger.v3.oas.models.PathItem.HttpMethod.POST;
import static io.trino.spi.connector.SchemaTableName.schemaTableName;
import static io.trino.spi.connector.SortOrder.ASC_NULLS_LAST;
import static io.trino.spi.type.BigintType.BIGINT;
import static org.assertj.core.api.Assertions.assertThat;

class TestOpenApiTableHandle
{
    private static final OpenApiColumnHandle ID = new OpenApiColumnHandle("id", BIGINT);
    private static final TupleDomain<ColumnHandle> ID_EQUALS_ONE = TupleDomain.withColumnDomains(Map.of(ID, Domain.singleValue(BIGINT, 1L)));

    @Test
    void testEqualsAndHashCode()
    {
        OpenApiTableHandle handle = fullHandle();

        assertThat(handle)
                .isEqualTo(fullHandle())
                .hasSameHashCodeAs(fullHandle());
        assertThat(List.of(
                OpenApiTableHandle.builder(schemaTableName("default", "other")).build(),
                handle.toBuilder().setSelectPaths(List.of()).build(),
                handle.toBuilder().setInsertPaths(List.of()).build(),
                handle.toBuilder().setUpdatePaths(List.of()).build(),
                handle.toBuilder().setDeletePaths(List.of()).build(),
                handle.toBuilder().setConstraint(TupleDomain.none()).build(),
                handle.toBuilder().setLimit(OptionalLong.empty()).build(),
                handle.toBuilder().setSortItem(Optional.empty()).build(),
                handle.toBuilder().setRowFilter(TupleDomain.all()).build(),
                handle.toBuilder().setSpecHash("other").build()))
                .allSatisfy(different -> assertThat(different).isNotEqualTo(handle));
    }

    private static OpenApiTableHandle fullHandle()
    {
        return OpenApiTableHandle.builder(schemaTableName("default", "items"))
                .setSelectPaths(List.of(new HttpPath(GET, "/items")))
                .setInsertPaths(List.of(new HttpPath(POST, "/items")))
                .setUpdatePaths(List.of(new HttpPath(PATCH, "/items/{id}")))
                .setDeletePaths(List.of(new HttpPath(DELETE, "/items/{id}")))
                .setConstraint(ID_EQUALS_ONE)
                .setLimit(OptionalLong.of(10))
                .setSortItem(Optional.of(new SortItem("id", ASC_NULLS_LAST)))
                .setRowFilter(ID_EQUALS_ONE)
                .setSpecHash("hash")
                .build();
    }
}
