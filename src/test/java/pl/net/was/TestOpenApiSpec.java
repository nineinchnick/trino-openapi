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
import io.swagger.v3.oas.models.media.Schema;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.RowType;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import java.net.URL;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static io.trino.spi.connector.SchemaTableName.schemaTableName;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static java.util.Objects.requireNonNull;
import static pl.net.was.OpenApiSpec.SCHEMA_NAME;

class TestOpenApiSpec
{
    @Test
    public void getGithubTables()
    {
        OpenApiSpec spec = loadSpec("github.json");
        Map<String, List<OpenApiColumn>> tables = spec.getTables();

        Set<String> expected = Set.of(
                "repos_contributors",
                "repos_pages",
                "projects");
        Assertions.assertThat(tables.keySet()).containsAll(expected);
    }

    @Test
    public void getJiraTables()
    {
        OpenApiSpec spec = loadSpec("jira.json");
        Map<String, List<OpenApiColumn>> tables = spec.getTables();

        Set<String> expected = Set.of(
                "rest_api_3_issue",
                "rest_api_3_project");
        Assertions.assertThat(tables.keySet()).containsAll(expected);
    }

    @Test
    public void getGalaxyTables()
    {
        OpenApiSpec spec = loadSpec("galaxy.json");
        Map<String, List<OpenApiColumn>> tables = spec.getTables();

        Set<String> expected = Set.of(
                "public_api_v1_catalog_schema_table",
                "public_api_v1_policy",
                "public_api_v1_catalog_schema",
                "public_api_v1_service_account",
                "public_api_v1_catalog_schema_discovery",
                "public_api_v1_schema_discovery:apply",
                "public_api_v1_role_rolegrant",
                "public_api_v1_cluster",
                "public_api_v1_user",
                "public_api_v1_catalog",
                "public_api_v1_tag",
                "public_api_v1_service_account_service_account_password",
                "public_api_v1_catalog_catalog_metadata",
                "public_api_v1_column_mask",
                "public_api_v1_catalog_schema_table_column",
                "public_api_v1_row_filter",
                "public_api_v1_role",
                "public_api_v1_schema_discovery",
                "public_api_v1_role_privilege");
        Assertions.assertThat(tables.keySet()).containsAll(expected);
    }

    @Test
    public void getPetstoreTables()
    {
        OpenApiSpec spec = loadSpec("petstore.yaml");
        Map<String, List<OpenApiColumn>> tables = spec.getTables();

        Set<String> expected = Set.of(
                "pet_upload_image",
                "pet_find_by_status",
                "user_login",
                "store_order",
                "store_inventory",
                "user",
                "pet");
        Assertions.assertThat(tables.keySet()).containsAll(expected);
        OpenApiTableHandle tableHandle = spec.getTableHandle(schemaTableName(SCHEMA_NAME, "pet"));
        Assertions.assertThat(tableHandle.getSelectPath()).isEqualTo("/pet/{petId}");
        Assertions.assertThat(tableHandle.getInsertPath()).isEqualTo("/pet");
        Assertions.assertThat(tableHandle.getUpdatePath()).isEqualTo("/pet");
        Assertions.assertThat(tableHandle.getDeletePath()).isEqualTo("/pet/{petId}");
        List<OpenApiColumn> petColumns = tables.get("pet").stream()
                .map(column -> {
                    // compare only source types, so rebuild it without any other attribute
                    Schema<?> sourceType = new Schema<>();
                    sourceType.setType(column.getSourceType().getType());
                    return OpenApiColumn.builderFrom(column)
                            .setSourceType(sourceType)
                            .build();
                })
                .toList();
        Schema<?> intSchema = new Schema<>();
        intSchema.setType("integer");
        Schema<?> stringSchema = new Schema<>();
        stringSchema.setType("string");
        Schema<?> arraySchema = new Schema<>();
        arraySchema.setType("array");
        Schema<?> objectSchema = new Schema<>();
        objectSchema.setType("object");
        RowType categoryType = RowType.from(List.of(
                new RowType.Field(Optional.of("id"), INTEGER),
                new RowType.Field(Optional.of("name"), VARCHAR)));
        ArrayType photosType = new ArrayType(VARCHAR);
        ArrayType tagsType = new ArrayType(categoryType);
        Assertions.assertThat(petColumns)
                .containsExactly(
                        OpenApiColumn.builder()
                                .setName("name").setSourceName("name")
                                .setType(VARCHAR).setSourceType(stringSchema)
                                .build(),
                        OpenApiColumn.builder()
                                .setName("__trino_row_id")
                                .setType(VARCHAR).setSourceType(stringSchema)
                                .build(),
                        OpenApiColumn.builder()
                                .setName("id").setSourceName("id")
                                .setType(INTEGER).setSourceType(intSchema)
                                .setIsNullable(true)
                                .build(),
                        OpenApiColumn.builder()
                                .setName("category").setSourceName("category")
                                .setType(categoryType).setSourceType(objectSchema)
                                .setIsNullable(true)
                                .setComment("A category for a pet")
                                .build(),
                        OpenApiColumn.builder()
                                .setName("photo_urls").setSourceName("photoUrls")
                                .setType(photosType).setSourceType(arraySchema)
                                .build(),
                        OpenApiColumn.builder()
                                .setName("status").setSourceName("status")
                                .setType(VARCHAR).setSourceType(stringSchema)
                                .setIsNullable(true)
                                .setComment("pet status in the store")
                                .build(),
                        OpenApiColumn.builder()
                                .setName("pet_id").setSourceName("petId")
                                .setType(INTEGER).setSourceType(intSchema)
                                .setRequiresPredicate(Map.of(PathItem.HttpMethod.GET, "path", PathItem.HttpMethod.DELETE, "path"))
                                .setIsNullable(true)
                                .build(),
                        OpenApiColumn.builder()
                                .setName("tags").setSourceName("tags")
                                .setType(tagsType).setSourceType(arraySchema)
                                .setIsNullable(true)
                                .build());
    }

    private OpenApiSpec loadSpec(String name)
    {
        URL specResource = requireNonNull(getClass().getClassLoader().getResource(name));
        OpenApiConfig config = new OpenApiConfig();
        config.setSpecLocation(specResource.getFile());
        return new OpenApiSpec(config);
    }
}
