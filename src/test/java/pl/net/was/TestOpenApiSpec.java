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

import com.fasterxml.jackson.core.JsonPointer;
import io.swagger.v3.oas.models.PathItem;
import io.swagger.v3.oas.models.media.Schema;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.RowType;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import java.net.URL;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static io.trino.spi.connector.SchemaTableName.schemaTableName;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.DecimalType.createDecimalType;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.TimestampType.TIMESTAMP_MILLIS;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static java.util.Objects.requireNonNull;
import static pl.net.was.OpenApiSpec.SCHEMA_NAME;

class TestOpenApiSpec
{
    private final Schema<?> intSchema = newSchema("integer");
    private final Schema<?> numberSchema = newSchema("number");
    private final Schema<?> stringSchema = newSchema("string");
    private final Schema<?> booleanSchema = newSchema("boolean");
    private final Schema<?> arraySchema = newSchema("array");
    private final Schema<?> objectSchema = newSchema("object");
    private final Schema<?> nullSchema = new Schema<>();

    private static Schema<?> newSchema(String type)
    {
        Schema<?> schema = new Schema<>();
        schema.setType(type);
        return schema;
    }

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
    public void getGithubExtensionTables()
    {
        OpenApiSpec spec = loadSpec("github-patched.json");
        Map<String, List<OpenApiColumn>> tables = spec.getTables();

        Set<String> expected = Set.of(
                "repos",
                "repos_actions_workflows");
        Assertions.assertThat(tables.keySet()).containsAll(expected);
        // select a.* from repos_actions_workflows cross join unnest(workflows) a where owner = 'nineinchnick' and repo = 'trino-openapi' and workflow_id = 'a' and per_page=1
        OpenApiTableHandle tableHandle = spec.getTableHandle(schemaTableName(SCHEMA_NAME, "repos_actions_workflows"));
        Assertions.assertThat(tableHandle.getSelectPath()).isEqualTo("/repos/{owner}/{repo}/actions/workflows");
        Assertions.assertThat(tableHandle.getInsertPath()).isNull();
        Assertions.assertThat(tableHandle.getUpdatePath()).isNull();
        Assertions.assertThat(tableHandle.getDeletePath()).isNull();
        List<OpenApiColumn> columns = tables.get("repos_actions_workflows").stream()
                .map(column -> {
                    // compare only source types, so rebuild it without any other attribute
                    Schema<?> sourceType = new Schema<>();
                    sourceType.setType(column.getSourceType().getType());
                    return OpenApiColumn.builderFrom(column)
                            .setSourceType(sourceType)
                            .build();
                })
                .toList();
        Assertions.assertThat(columns)
                .containsExactly(
                        OpenApiColumn.builder()
                                .setName("owner").setSourceName("owner")
                                .setType(VARCHAR).setSourceType(stringSchema)
                                .setRequiresPredicate(Map.of(PathItem.HttpMethod.GET, "path"))
                                .setIsNullable(true)
                                .build(),
                        OpenApiColumn.builder()
                                .setName("badge_url").setSourceName("badge_url")
                                .setResultsPointer(JsonPointer.valueOf("/workflows"))
                                .setType(VARCHAR).setSourceType(stringSchema)
                                .setIsNullable(true)
                                .build(),
                        OpenApiColumn.builder()
                                .setName("per_page").setSourceName("per_page")
                                .setType(INTEGER).setSourceType(intSchema)
                                .setOptionalPredicate(Map.of(PathItem.HttpMethod.GET, "query"))
                                .setIsNullable(true)
                                .setIsHidden(true)
                                .build(),
                        OpenApiColumn.builder()
                                .setName("workflow_id").setSourceName("workflow_id")
                                .setType(VARCHAR).setSourceType(nullSchema)
                                .setRequiresPredicate(Map.of(PathItem.HttpMethod.GET, "path"))
                                .setIsNullable(true)
                                .build(),
                        OpenApiColumn.builder()
                                .setName("total_count").setSourceName("total_count")
                                .setType(INTEGER).setSourceType(intSchema)
                                .build(),
                        OpenApiColumn.builder()
                                .setName("repo").setSourceName("repo")
                                .setType(VARCHAR).setSourceType(stringSchema)
                                .setRequiresPredicate(Map.of(PathItem.HttpMethod.GET, "path"))
                                .setIsNullable(true)
                                .build(),
                        OpenApiColumn.builder()
                                .setName("created_at").setSourceName("created_at")
                                .setResultsPointer(JsonPointer.valueOf("/workflows"))
                                .setType(TIMESTAMP_MILLIS).setSourceType(stringSchema)
                                .setIsNullable(true)
                                .build(),
                        OpenApiColumn.builder()
                                .setName("deleted_at").setSourceName("deleted_at")
                                .setResultsPointer(JsonPointer.valueOf("/workflows"))
                                .setType(TIMESTAMP_MILLIS).setSourceType(stringSchema)
                                .setIsNullable(true)
                                .build(),
                        OpenApiColumn.builder()
                                .setName("url").setSourceName("url")
                                .setResultsPointer(JsonPointer.valueOf("/workflows"))
                                .setType(VARCHAR).setSourceType(stringSchema)
                                .setIsNullable(true)
                                .build(),
                        OpenApiColumn.builder()
                                .setName("path").setSourceName("path")
                                .setResultsPointer(JsonPointer.valueOf("/workflows"))
                                .setType(VARCHAR).setSourceType(stringSchema)
                                .setIsNullable(true)
                                .build(),
                        OpenApiColumn.builder()
                                .setName("updated_at").setSourceName("updated_at")
                                .setResultsPointer(JsonPointer.valueOf("/workflows"))
                                .setType(TIMESTAMP_MILLIS).setSourceType(stringSchema)
                                .setIsNullable(true)
                                .build(),
                        OpenApiColumn.builder()
                                .setName("html_url").setSourceName("html_url")
                                .setResultsPointer(JsonPointer.valueOf("/workflows"))
                                .setType(VARCHAR).setSourceType(stringSchema)
                                .setIsNullable(true)
                                .build(),
                        OpenApiColumn.builder()
                                .setName("name").setSourceName("name")
                                .setResultsPointer(JsonPointer.valueOf("/workflows"))
                                .setType(VARCHAR).setSourceType(stringSchema)
                                .setIsNullable(true)
                                .build(),
                        OpenApiColumn.builder()
                                .setName("id").setSourceName("id")
                                .setResultsPointer(JsonPointer.valueOf("/workflows"))
                                .setType(INTEGER).setSourceType(intSchema)
                                .setIsNullable(true)
                                .build(),
                        OpenApiColumn.builder()
                                .setName("state").setSourceName("state")
                                .setResultsPointer(JsonPointer.valueOf("/workflows"))
                                .setType(VARCHAR).setSourceType(stringSchema)
                                .setIsNullable(true)
                                .build(),
                        OpenApiColumn.builder()
                                .setName("page").setSourceName("page")
                                .setType(INTEGER).setSourceType(intSchema)
                                .setOptionalPredicate(Map.of(PathItem.HttpMethod.GET, "query"))
                                .setIsNullable(true)
                                .setIsHidden(true)
                                .setIsPageNumber(true)
                                .build(),
                        OpenApiColumn.builder()
                                .setName("node_id").setSourceName("node_id")
                                .setResultsPointer(JsonPointer.valueOf("/workflows"))
                                .setType(VARCHAR).setSourceType(stringSchema)
                                .setIsNullable(true)
                                .build());
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
        RowType categoryType = RowType.from(List.of(
                RowType.field("id", BIGINT),
                RowType.field("name", VARCHAR)));
        ArrayType photosType = new ArrayType(VARCHAR);
        ArrayType tagsType = new ArrayType(categoryType);
        Assertions.assertThat(petColumns)
                .containsExactly(
                        OpenApiColumn.builder()
                                .setName("api_key").setSourceName("api_key")
                                .setType(VARCHAR).setSourceType(stringSchema)
                                .setOptionalPredicate(Map.of(PathItem.HttpMethod.DELETE, "header"))
                                .setIsNullable(true)
                                .build(),
                        OpenApiColumn.builder()
                                .setName("name").setSourceName("name")
                                .setType(VARCHAR).setSourceType(stringSchema)
                                .build(),
                        OpenApiColumn.builder()
                                .setName("__trino_row_id")
                                .setType(VARCHAR).setSourceType(stringSchema)
                                .setIsHidden(true)
                                .build(),
                        OpenApiColumn.builder()
                                .setName("id").setSourceName("id")
                                .setType(BIGINT).setSourceType(intSchema)
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
                                .setName("tags").setSourceName("tags")
                                .setType(tagsType).setSourceType(arraySchema)
                                .setIsNullable(true)
                                .build(),
                        OpenApiColumn.builder()
                                .setName("status").setSourceName("status")
                                .setType(VARCHAR).setSourceType(stringSchema)
                                .setIsNullable(true)
                                .setComment("pet status in the store")
                                .build(),
                        OpenApiColumn.builder()
                                .setName("pet_id").setSourceName("petId")
                                .setType(BIGINT).setSourceType(intSchema)
                                .setRequiresPredicate(Map.of(PathItem.HttpMethod.GET, "path", PathItem.HttpMethod.DELETE, "path"))
                                .setIsNullable(true)
                                .build());
    }

    @Test
    public void getDatadogTables()
    {
        OpenApiSpec spec = loadSpec("datadog.yaml");
        Map<String, List<OpenApiColumn>> tables = spec.getTables();

        Set<String> expected = Set.of(
                "api_v2_events",
                "api_v2_team",
                "api_v2_metrics",
                "api_v2_query_timeseries");
        Assertions.assertThat(tables.keySet()).containsAll(expected);
        OpenApiTableHandle tableHandle = spec.getTableHandle(schemaTableName(SCHEMA_NAME, "api_v2_query_timeseries"));
        Assertions.assertThat(tableHandle.getSelectPath()).isEqualTo("/api/v2/query/timeseries");
        Assertions.assertThat(tableHandle.getInsertPath()).isEqualTo("/api/v2/query/timeseries");
        Assertions.assertThat(tableHandle.getUpdatePath()).isEqualTo("/api/v2/query/timeseries");
        Assertions.assertThat(tableHandle.getDeletePath()).isNull();
        List<OpenApiColumn> columns = tables.get("api_v2_query_timeseries").stream()
                .map(column -> {
                    // compare only source types, so rebuild it without any other attribute
                    Schema<?> sourceType = new Schema<>();
                    sourceType.setType(column.getSourceType().getType());
                    return OpenApiColumn.builderFrom(column)
                            .setSourceType(sourceType)
                            .build();
                })
                .toList();
        RowType dataType = RowType.from(List.of(
                RowType.field("attributes", RowType.from(List.of(
                        RowType.field("series", new ArrayType(RowType.from(List.of(
                                RowType.field("group_tags", new ArrayType(VARCHAR)),
                                RowType.field("query_index", INTEGER),
                                RowType.field("unit", new ArrayType(RowType.from(List.of(
                                        RowType.field("family", VARCHAR),
                                        RowType.field("name", VARCHAR),
                                        RowType.field("plural", VARCHAR),
                                        RowType.field("scale_factor", DOUBLE),
                                        RowType.field("short_name", VARCHAR))))))))),
                        RowType.field("times", new ArrayType(BIGINT)),
                        RowType.field("values", new ArrayType(new ArrayType(DOUBLE)))))),
                RowType.field("type", VARCHAR)));
        RowType dataReqType = RowType.from(List.of(
                RowType.field("attributes", RowType.from(List.of(
                        RowType.field("formulas", new ArrayType(RowType.from(List.of(
                                RowType.field("formula", VARCHAR),
                                RowType.field("limit", RowType.from(List.of(
                                        RowType.field("count", INTEGER),
                                        RowType.field("order", VARCHAR)))))))),
                        RowType.field("from", BIGINT),
                        RowType.field("interval", BIGINT),
                        RowType.field("queries", new ArrayType(VARCHAR)),
                        RowType.field("to", BIGINT)))),
                RowType.field("type", VARCHAR)));
        Assertions.assertThat(columns)
                .containsExactly(
                        OpenApiColumn.builder()
                                .setName("data").setSourceName("data")
                                .setType(dataType).setSourceType(objectSchema)
                                .setIsNullable(true)
                                .setComment("A message containing the response to a timeseries query.")
                                .build(),
                        OpenApiColumn.builder()
                                .setName("__trino_row_id")
                                .setType(VARCHAR).setSourceType(stringSchema)
                                .setIsHidden(true)
                                .build(),
                        OpenApiColumn.builder()
                                .setName("data_req").setSourceName("data")
                                .setType(dataReqType).setSourceType(objectSchema)
                                .setRequiresPredicate(Map.of(PathItem.HttpMethod.POST, "body"))
                                .setComment("A single timeseries query to be executed.")
                                .build(),
                        OpenApiColumn.builder()
                                .setName("errors").setSourceName("errors")
                                .setType(VARCHAR).setSourceType(stringSchema)
                                .setIsNullable(true)
                                .setComment("The error generated by the request.")
                                .build());
    }

    @Test
    public void getCloudflareTables()
    {
        OpenApiSpec spec = loadSpec("cloudflare.json");
        Map<String, List<OpenApiColumn>> tables = spec.getTables();

        Set<String> expected = Set.of(
                "accounts",
                "certificates",
                "user",
                "zones");
        Assertions.assertThat(tables.keySet()).containsAll(expected);
        OpenApiTableHandle tableHandle = spec.getTableHandle(schemaTableName(SCHEMA_NAME, "zones"));
        Assertions.assertThat(tableHandle.getSelectPath()).isEqualTo("/zones");
        Assertions.assertThat(tableHandle.getInsertPath()).isEqualTo("/zones");
        Assertions.assertThat(tableHandle.getUpdatePath()).isEqualTo("/zones");
        Assertions.assertThat(tableHandle.getDeletePath()).isEqualTo("/zones/{identifier}");
        List<OpenApiColumn> columns = tables.get("zones").stream()
                .map(column -> {
                    // compare only source types, so rebuild it without any other attribute
                    Schema<?> sourceType = new Schema<>();
                    sourceType.setType(column.getSourceType().getType());
                    return OpenApiColumn.builderFrom(column)
                            .setSourceType(sourceType)
                            .build();
                })
                .toList();
        RowType resultType = RowType.from(List.of(
                RowType.field("account", RowType.from(List.of(
                        RowType.field("id", VARCHAR),
                        RowType.field("name", VARCHAR)))),
                RowType.field("activated_on", TIMESTAMP_MILLIS),
                RowType.field("created_on", TIMESTAMP_MILLIS),
                RowType.field("development_mode", createDecimalType(18, 8)),
                RowType.field("id", VARCHAR),
                RowType.field("meta", RowType.from(List.of(
                        RowType.field("cdn_only", BOOLEAN),
                        RowType.field("custom_certificate_quota", INTEGER),
                        RowType.field("dns_only", BOOLEAN),
                        RowType.field("foundation_dns", BOOLEAN),
                        RowType.field("page_rule_quota", INTEGER),
                        RowType.field("phishing_detected", BOOLEAN),
                        RowType.field("step", INTEGER)))),
                RowType.field("modified_on", TIMESTAMP_MILLIS),
                RowType.field("name", VARCHAR),
                RowType.field("original_dnshost", VARCHAR),
                RowType.field("original_name_servers", new ArrayType(VARCHAR)),
                RowType.field("original_registrar", VARCHAR),
                RowType.field("owner", RowType.from(List.of(
                        RowType.field("id", VARCHAR),
                        RowType.field("name", VARCHAR),
                        RowType.field("type", VARCHAR)))),
                RowType.field("vanity_name_servers", new ArrayType(VARCHAR))));
        RowType result2Type = RowType.from(List.of(RowType.field("id", VARCHAR)));
        RowType resultInfoType = RowType.from(List.of(
                RowType.field("count", createDecimalType(18, 8)),
                RowType.field("page", createDecimalType(18, 8)),
                RowType.field("per_page", createDecimalType(18, 8)),
                RowType.field("total_count", createDecimalType(18, 8))));
        Assertions.assertThat(columns)
                .containsExactly(
                        OpenApiColumn.builder()
                                .setName("identifier").setSourceName("identifier")
                                .setType(VARCHAR).setSourceType(stringSchema)
                                // TODO we've merged /zones and /zones/{identifier} and now we require identifier for /zones, which is wrong
                                .setRequiresPredicate(Map.of(PathItem.HttpMethod.GET, "path", PathItem.HttpMethod.DELETE, "path", PathItem.HttpMethod.PATCH, "path"))
                                .setIsNullable(true)
                                .setComment("Identifier")
                                .build(),
                        OpenApiColumn.builder()
                                .setName("per_page").setSourceName("per_page")
                                .setType(createDecimalType(18, 8)).setSourceType(numberSchema)
                                .setOptionalPredicate(Map.of(PathItem.HttpMethod.GET, "query"))
                                .setIsNullable(true)
                                .setComment("Number of zones per page.")
                                .build(),
                        OpenApiColumn.builder()
                                .setName("account.id").setSourceName("account.id")
                                .setType(VARCHAR).setSourceType(stringSchema)
                                .setOptionalPredicate(Map.of(PathItem.HttpMethod.GET, "query"))
                                .setIsNullable(true)
                                .setComment("An account ID")
                                .build(),
                        OpenApiColumn.builder()
                                .setName("paused").setSourceName("paused")
                                .setType(BOOLEAN).setSourceType(booleanSchema)
                                .setOptionalPredicate(Map.of(PathItem.HttpMethod.PATCH, "body"))
                                .setIsNullable(true)
                                .setComment("Indicates whether the zone is only using Cloudflare DNS services. A\n" +
                                        "true value means the zone will not receive security or performance\n" +
                                        "benefits.\n")
                                .build(),
                        OpenApiColumn.builder()
                                .setName("vanity_name_servers").setSourceName("vanity_name_servers")
                                .setType(new ArrayType(VARCHAR)).setSourceType(arraySchema)
                                .setOptionalPredicate(Map.of(PathItem.HttpMethod.PATCH, "body"))
                                .setIsNullable(true)
                                .setComment("An array of domains used for custom name servers. This is only\n" +
                                        "available for Business and Enterprise plans.")
                                .build(),
                        OpenApiColumn.builder()
                                .setName("result_info").setSourceName("result_info")
                                .setType(resultInfoType).setSourceType(objectSchema)
                                .setIsNullable(true)
                                .build(),
                        OpenApiColumn.builder()
                                .setName("match").setSourceName("match")
                                .setType(VARCHAR).setSourceType(stringSchema)
                                .setOptionalPredicate(Map.of(PathItem.HttpMethod.GET, "query"))
                                .setIsNullable(true)
                                .setComment("Whether to match all search requirements or at least one (any).")
                                .build(),
                        OpenApiColumn.builder()
                                .setName("__trino_row_id")
                                .setType(VARCHAR).setSourceType(stringSchema)
                                .setIsHidden(true)
                                .build(),
                        OpenApiColumn.builder()
                                .setName("type").setSourceName("type")
                                .setType(VARCHAR).setSourceType(stringSchema)
                                .setOptionalPredicate(Map.of(PathItem.HttpMethod.POST, "body", PathItem.HttpMethod.PATCH, "body"))
                                .setIsNullable(true)
                                .setComment("A full zone implies that DNS is hosted with Cloudflare. A partial zone is \n" +
                                        "typically a partner-hosted zone or a CNAME setup.\n")
                                .build(),
                        OpenApiColumn.builder()
                                .setName("result").setSourceName("result")
                                .setType(new ArrayType(resultType)).setSourceType(arraySchema)
                                .setIsNullable(true)
                                .build(),
                        OpenApiColumn.builder()
                                .setName("result_2").setSourceName("result")
                                .setType(result2Type).setSourceType(objectSchema)
                                .setIsNullable(true)
                                .build(),
                        OpenApiColumn.builder()
                                .setName("result_3").setSourceName("result")
                                .setType(resultType).setSourceType(objectSchema)
                                .setIsNullable(true)
                                .build(),
                        OpenApiColumn.builder()
                                .setName("account.name").setSourceName("account.name")
                                .setType(VARCHAR).setSourceType(stringSchema)
                                .setOptionalPredicate(Map.of(PathItem.HttpMethod.GET, "query"))
                                .setIsNullable(true)
                                .setComment("An account Name. Optional filter operators can be provided to extend refine the search:\n" +
                                        "  * `equal` (default)\n" +
                                        "  * `not_equal`\n" +
                                        "  * `starts_with`\n" +
                                        "  * `ends_with`\n" +
                                        "  * `contains`\n" +
                                        "  * `starts_with_case_sensitive`\n" +
                                        "  * `ends_with_case_sensitive`\n" +
                                        "  * `contains_case_sensitive`\n")
                                .build(),
                        OpenApiColumn.builder()
                                .setName("success").setSourceName("success")
                                .setType(BOOLEAN).setSourceType(booleanSchema)
                                .setComment("Whether the API call was successful")
                                .build(),
                        OpenApiColumn.builder()
                                .setName("name").setSourceName("name")
                                .setType(VARCHAR).setSourceType(stringSchema)
                                .setRequiresPredicate(Map.of(PathItem.HttpMethod.POST, "body"))
                                .setOptionalPredicate(Map.of(PathItem.HttpMethod.GET, "query"))
                                .setComment("A domain name. Optional filter operators can be provided to extend refine the search:\n" +
                                        "  * `equal` (default)\n" +
                                        "  * `not_equal`\n" +
                                        "  * `starts_with`\n" +
                                        "  * `ends_with`\n" +
                                        "  * `contains`\n" +
                                        "  * `starts_with_case_sensitive`\n" +
                                        "  * `ends_with_case_sensitive`\n" +
                                        "  * `contains_case_sensitive`\n")
                                .setIsNullable(true)
                                .build(),
                        OpenApiColumn.builder()
                                .setName("messages").setSourceName("messages")
                                .setType(new ArrayType(RowType.from(List.of(
                                        RowType.field("code", INTEGER),
                                        RowType.field("message", VARCHAR))))).setSourceType(arraySchema)
                                .build(),
                        OpenApiColumn.builder()
                                .setName("page").setSourceName("page")
                                .setType(createDecimalType(18, 8)).setSourceType(numberSchema)
                                .setOptionalPredicate(Map.of(PathItem.HttpMethod.GET, "query"))
                                .setIsNullable(true)
                                .setComment("Page number of paginated results.")
                                .build(),
                        OpenApiColumn.builder()
                                .setName("plan").setSourceName("plan")
                                .setType(RowType.from(List.of(RowType.field("id", VARCHAR)))).setSourceType(objectSchema)
                                .setOptionalPredicate(Map.of(PathItem.HttpMethod.PATCH, "body"))
                                .setIsNullable(true)
                                .setComment("(Deprecated) Please use the `/zones/{identifier}/subscription` API\n" +
                                        "to update a zone's plan. Changing this value will create/cancel\n" +
                                        "associated subscriptions. To view available plans for this zone,\n" +
                                        "see Zone Plans.\n")
                                .build(),
                        OpenApiColumn.builder()
                                .setName("account").setSourceName("account")
                                .setType(RowType.from(List.of(RowType.field("id", VARCHAR)))).setSourceType(objectSchema)
                                .setRequiresPredicate(Map.of(PathItem.HttpMethod.POST, "body"))
                                .build(),
                        OpenApiColumn.builder()
                                .setName("errors").setSourceName("errors")
                                .setType(new ArrayType(RowType.from(List.of(
                                        RowType.field("code", INTEGER),
                                        RowType.field("message", VARCHAR))))).setSourceType(arraySchema)
                                .build(),
                        OpenApiColumn.builder()
                                .setName("order").setSourceName("order")
                                .setType(VARCHAR).setSourceType(stringSchema)
                                .setOptionalPredicate(Map.of(PathItem.HttpMethod.GET, "query"))
                                .setIsNullable(true)
                                .setComment("Field to order zones by.")
                                .build(),
                        OpenApiColumn.builder()
                                .setName("status").setSourceName("status")
                                .setType(VARCHAR).setSourceType(stringSchema)
                                .setOptionalPredicate(Map.of(PathItem.HttpMethod.GET, "query"))
                                .setIsNullable(true)
                                .setComment("A zone status")
                                .build(),
                        OpenApiColumn.builder()
                                .setName("direction").setSourceName("direction")
                                .setType(VARCHAR).setSourceType(stringSchema)
                                .setOptionalPredicate(Map.of(PathItem.HttpMethod.GET, "query"))
                                .setIsNullable(true)
                                .setComment("Direction to order zones.")
                                .build());
    }

    private OpenApiSpec loadSpec(String name)
    {
        OpenApiConfig config = new OpenApiConfig();
        URL specResource = requireNonNull(getClass().getClassLoader().getResource(name));
        config.setSpecLocation(specResource.getFile());
        return new OpenApiSpec(config);
    }
}
