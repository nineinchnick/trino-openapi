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

import io.airlift.slice.SizeOf;
import io.swagger.v3.oas.models.media.IntegerSchema;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.Constraint;
import io.trino.spi.connector.ConstraintApplicationResult;
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
    private static final HttpPath ITEMS = new HttpPath(GET, "/items");
    private static final HttpPath ITEM = new HttpPath(GET, "/items/{id}");
    private static final OpenApiColumn ID_COLUMN = OpenApiColumn.builder()
            .setName("id")
            .setSourceName("id")
            .setType(BIGINT)
            .setSourceType(new IntegerSchema())
            .setRequiresPredicate(Map.of(ITEM, ParameterLocation.PATH))
            .build();
    private static final Map<String, OpenApiColumn> COLUMNS = Map.of("id", ID_COLUMN);

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

    @Test
    void testRetainedSizeCountsEveryField()
    {
        OpenApiTableHandle handle = fullHandle();

        assertThat(List.of(
                handle.toBuilder().setSelectPaths(List.of()).build(),
                handle.toBuilder().setInsertPaths(List.of()).build(),
                handle.toBuilder().setUpdatePaths(List.of()).build(),
                handle.toBuilder().setDeletePaths(List.of()).build(),
                handle.toBuilder().setConstraint(TupleDomain.none()).build(),
                handle.toBuilder().setLimit(OptionalLong.empty()).build(),
                handle.toBuilder().setSortItem(Optional.empty()).build(),
                handle.toBuilder().setRowFilter(TupleDomain.all()).build(),
                handle.toBuilder().setSpecHash("").build()))
                .allSatisfy(smaller -> assertThat(smaller.getRetainedSizeInBytes()).isLessThan(handle.getRetainedSizeInBytes()));
        assertThat(handle.getRetainedSizeInBytes() - handle.toBuilder().setSortItem(Optional.empty()).build().getRetainedSizeInBytes())
                .isEqualTo(SizeOf.OPTIONAL_INSTANCE_SIZE + SizeOf.instanceSize(SortItem.class) + SizeOf.estimatedSizeOf("id"));
    }

    @Test
    void testIntersectPredicatePushedDownAgain()
    {
        OpenApiTableHandle filtered = pushDown(itemsHandle(), idIn(1L, 2L, 3L));

        Optional<ConstraintApplicationResult<ConnectorTableHandle>> result = filtered.applyFilter(new Constraint(idIn(1L, 3L)), COLUMNS, 256);

        assertThat(result).hasValueSatisfying(narrowed -> {
            assertThat(((OpenApiTableHandle) narrowed.getHandle()).getConstraint()).isEqualTo(idIn(1L, 3L));
            assertThat(narrowed.getRemainingFilter().isAll()).isTrue();
        });
    }

    @Test
    void testSamePredicatePushedDownAgainChangesNothing()
    {
        OpenApiTableHandle filtered = pushDown(itemsHandle(), idIn(1L, 2L));

        // a broader predicate doesn't change the intersection either
        assertThat(List.of(idIn(1L, 2L), idIn(1L, 2L, 3L)))
                .allSatisfy(constraint -> assertThat(filtered.applyFilter(new Constraint(constraint), COLUMNS, 256)).isEmpty());
    }

    @Test
    void testDisjointPredicateIsNotPushedDown()
    {
        OpenApiTableHandle filtered = pushDown(itemsHandle(), idIn(1L, 2L));

        // the handle can't represent an empty intersection, so Trino applies the filter and returns no rows
        assertThat(filtered.applyFilter(new Constraint(idIn(3L)), COLUMNS, 256)).isEmpty();
    }

    private static OpenApiTableHandle itemsHandle()
    {
        return OpenApiTableHandle.builder(schemaTableName("default", "items"))
                .setSelectPaths(List.of(ITEMS, ITEM))
                .build();
    }

    private static OpenApiTableHandle pushDown(OpenApiTableHandle handle, TupleDomain<ColumnHandle> constraint)
    {
        return (OpenApiTableHandle) handle.applyFilter(new Constraint(constraint), COLUMNS, 256)
                .orElseThrow()
                .getHandle();
    }

    private static TupleDomain<ColumnHandle> idIn(Long... ids)
    {
        return TupleDomain.withColumnDomains(Map.of(ID_COLUMN.getHandle(), Domain.multipleValues(BIGINT, List.of(ids))));
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
