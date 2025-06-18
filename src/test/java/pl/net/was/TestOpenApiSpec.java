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
import io.swagger.v3.parser.OpenAPIV3Parser;
import io.swagger.v3.parser.core.models.ParseOptions;
import io.swagger.v3.parser.core.models.SwaggerParseResult;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.MapType;
import io.trino.spi.type.RowType;
import io.trino.spi.type.TypeOperators;
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
import static io.trino.spi.type.RealType.REAL;
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
                "orgs",
                "repos",
                "repos_actions_workflows");
        Assertions.assertThat(tables.keySet()).containsAll(expected);

        OpenApiTableHandle orgsTableHandle = spec.getTableHandle(schemaTableName(SCHEMA_NAME, "orgs"));
        Assertions.assertThat(orgsTableHandle.getSelectPaths()).containsExactly("/orgs/{org}");
        Assertions.assertThat(orgsTableHandle.getInsertPaths()).isEmpty();
        Assertions.assertThat(orgsTableHandle.getUpdatePaths()).isEmpty();
        Assertions.assertThat(orgsTableHandle.getDeletePaths()).containsExactly("/orgs/{org}");
        List<OpenApiColumn> orgColumns = tables.get("orgs").stream()
                .map(column -> {
                    // compare only source types, so rebuild it without any other attribute
                    Schema<?> sourceType = new Schema<>();
                    sourceType.setType(column.getSourceType().getType());
                    return OpenApiColumn.builderFrom(column)
                            .setSourceType(sourceType)
                            .build();
                })
                .toList();
        Assertions.assertThat(orgColumns)
                .containsExactly(
                        OpenApiColumn.builder()
                                .setName("__trino_row_id")
                                .setType(VARCHAR).setSourceType(stringSchema)
                                .setIsHidden(true)
                                .build(),
                        OpenApiColumn.builder()
                                .setName("login").setSourceName("login")
                                .setType(VARCHAR).setSourceType(stringSchema)
                                .build(),
                        OpenApiColumn.builder()
                                .setName("id").setSourceName("id")
                                .setType(BIGINT).setSourceType(intSchema)
                                .build(),
                        OpenApiColumn.builder()
                                .setName("node_id").setSourceName("node_id")
                                .setType(VARCHAR).setSourceType(stringSchema)
                                .build(),
                        OpenApiColumn.builder()
                                .setName("url").setSourceName("url")
                                .setType(VARCHAR).setSourceType(stringSchema)
                                .build(),
                        OpenApiColumn.builder()
                                .setName("repos_url").setSourceName("repos_url")
                                .setType(VARCHAR).setSourceType(stringSchema)
                                .build(),
                        OpenApiColumn.builder()
                                .setName("events_url").setSourceName("events_url")
                                .setType(VARCHAR).setSourceType(stringSchema)
                                .build(),
                        OpenApiColumn.builder()
                                .setName("hooks_url").setSourceName("hooks_url")
                                .setType(VARCHAR).setSourceType(stringSchema)
                                .build(),
                        OpenApiColumn.builder()
                                .setName("issues_url").setSourceName("issues_url")
                                .setType(VARCHAR).setSourceType(stringSchema)
                                .build(),
                        OpenApiColumn.builder()
                                .setName("members_url").setSourceName("members_url")
                                .setType(VARCHAR).setSourceType(stringSchema)
                                .build(),
                        OpenApiColumn.builder()
                                .setName("public_members_url").setSourceName("public_members_url")
                                .setType(VARCHAR).setSourceType(stringSchema)
                                .build(),
                        OpenApiColumn.builder()
                                .setName("avatar_url").setSourceName("avatar_url")
                                .setType(VARCHAR).setSourceType(stringSchema)
                                .build(),
                        OpenApiColumn.builder()
                                .setName("description").setSourceName("description")
                                .setType(VARCHAR).setSourceType(stringSchema)
                                .setOptionalPredicate(Map.of(PathItem.HttpMethod.PATCH, ParameterLocation.BODY))
                                .setIsNullable(true)
                                .build(),
                        OpenApiColumn.builder()
                                .setName("name").setSourceName("name")
                                .setType(VARCHAR).setSourceType(stringSchema)
                                .setOptionalPredicate(Map.of(PathItem.HttpMethod.PATCH, ParameterLocation.BODY))
                                .setIsNullable(true)
                                .build(),
                        OpenApiColumn.builder()
                                .setName("company").setSourceName("company")
                                .setType(VARCHAR).setSourceType(stringSchema)
                                .setOptionalPredicate(Map.of(PathItem.HttpMethod.PATCH, ParameterLocation.BODY))
                                .setIsNullable(true)
                                .build(),
                        OpenApiColumn.builder()
                                .setName("blog").setSourceName("blog")
                                .setType(VARCHAR).setSourceType(stringSchema)
                                .setOptionalPredicate(Map.of(PathItem.HttpMethod.PATCH, ParameterLocation.BODY))
                                .setIsNullable(true)
                                .build(),
                        OpenApiColumn.builder()
                                .setName("location").setSourceName("location")
                                .setType(VARCHAR).setSourceType(stringSchema)
                                .setOptionalPredicate(Map.of(PathItem.HttpMethod.PATCH, ParameterLocation.BODY))
                                .setIsNullable(true)
                                .build(),
                        OpenApiColumn.builder()
                                .setName("email").setSourceName("email")
                                .setType(VARCHAR).setSourceType(stringSchema)
                                .setOptionalPredicate(Map.of(PathItem.HttpMethod.PATCH, ParameterLocation.BODY))
                                .setIsNullable(true)
                                .build(),
                        OpenApiColumn.builder()
                                .setName("twitter_username").setSourceName("twitter_username")
                                .setType(VARCHAR).setSourceType(stringSchema)
                                .setOptionalPredicate(Map.of(PathItem.HttpMethod.PATCH, ParameterLocation.BODY))
                                .setIsNullable(true)
                                .build(),
                        OpenApiColumn.builder()
                                .setName("is_verified").setSourceName("is_verified")
                                .setType(BOOLEAN).setSourceType(booleanSchema)
                                .setIsNullable(true)
                                .build(),
                        OpenApiColumn.builder()
                                .setName("has_organization_projects").setSourceName("has_organization_projects")
                                .setType(BOOLEAN).setSourceType(booleanSchema)
                                .setOptionalPredicate(Map.of(PathItem.HttpMethod.PATCH, ParameterLocation.BODY))
                                .setIsNullable(true)
                                .build(),
                        OpenApiColumn.builder()
                                .setName("has_repository_projects").setSourceName("has_repository_projects")
                                .setType(BOOLEAN).setSourceType(booleanSchema)
                                .setOptionalPredicate(Map.of(PathItem.HttpMethod.PATCH, ParameterLocation.BODY))
                                .setIsNullable(true)
                                .build(),
                        OpenApiColumn.builder()
                                .setName("public_repos").setSourceName("public_repos")
                                .setType(BIGINT).setSourceType(intSchema)
                                .build(),
                        OpenApiColumn.builder()
                                .setName("public_gists").setSourceName("public_gists")
                                .setType(BIGINT).setSourceType(intSchema)
                                .build(),
                        OpenApiColumn.builder()
                                .setName("followers").setSourceName("followers")
                                .setType(BIGINT).setSourceType(intSchema)
                                .build(),
                        OpenApiColumn.builder()
                                .setName("following").setSourceName("following")
                                .setType(BIGINT).setSourceType(intSchema)
                                .build(),
                        OpenApiColumn.builder()
                                .setName("html_url").setSourceName("html_url")
                                .setType(VARCHAR).setSourceType(stringSchema)
                                .build(),
                        OpenApiColumn.builder()
                                .setName("type").setSourceName("type")
                                .setType(VARCHAR).setSourceType(stringSchema)
                                .build(),
                        OpenApiColumn.builder()
                                .setName("total_private_repos").setSourceName("total_private_repos")
                                .setType(BIGINT).setSourceType(intSchema)
                                .setIsNullable(true)
                                .build(),
                        OpenApiColumn.builder()
                                .setName("owned_private_repos").setSourceName("owned_private_repos")
                                .setType(BIGINT).setSourceType(intSchema)
                                .setIsNullable(true)
                                .build(),
                        OpenApiColumn.builder()
                                .setName("private_gists").setSourceName("private_gists")
                                .setType(BIGINT).setSourceType(intSchema)
                                .setIsNullable(true)
                                .build(),
                        OpenApiColumn.builder()
                                .setName("disk_usage").setSourceName("disk_usage")
                                .setType(BIGINT).setSourceType(intSchema)
                                .setIsNullable(true)
                                .build(),
                        OpenApiColumn.builder()
                                .setName("collaborators").setSourceName("collaborators")
                                .setType(BIGINT).setSourceType(intSchema)
                                .setIsNullable(true)
                                .build(),
                        OpenApiColumn.builder()
                                .setName("billing_email").setSourceName("billing_email")
                                .setType(VARCHAR).setSourceType(stringSchema)
                                .setOptionalPredicate(Map.of(PathItem.HttpMethod.PATCH, ParameterLocation.BODY))
                                .setIsNullable(true)
                                .build(),
                        OpenApiColumn.builder()
                                .setName("plan").setSourceName("plan")
                                .setType(RowType.from(List.of(
                                        RowType.field("name", VARCHAR),
                                        RowType.field("space", BIGINT),
                                        RowType.field("private_repos", BIGINT),
                                        RowType.field("filled_seats", BIGINT),
                                        RowType.field("seats", BIGINT)))).setSourceType(objectSchema)
                                .setIsNullable(true)
                                .build(),
                        OpenApiColumn.builder()
                                .setName("default_repository_permission").setSourceName("default_repository_permission")
                                .setType(VARCHAR).setSourceType(stringSchema)
                                .setOptionalPredicate(Map.of(PathItem.HttpMethod.PATCH, ParameterLocation.BODY))
                                .setIsNullable(true)
                                .build(),
                        OpenApiColumn.builder()
                                .setName("members_can_create_repositories").setSourceName("members_can_create_repositories")
                                .setType(BOOLEAN).setSourceType(booleanSchema)
                                .setOptionalPredicate(Map.of(PathItem.HttpMethod.PATCH, ParameterLocation.BODY))
                                .setIsNullable(true)
                                .build(),
                        OpenApiColumn.builder()
                                .setName("two_factor_requirement_enabled").setSourceName("two_factor_requirement_enabled")
                                .setType(BOOLEAN).setSourceType(booleanSchema)
                                .setIsNullable(true)
                                .build(),
                        OpenApiColumn.builder()
                                .setName("members_allowed_repository_creation_type").setSourceName("members_allowed_repository_creation_type")
                                .setType(VARCHAR).setSourceType(stringSchema)
                                .setOptionalPredicate(Map.of(PathItem.HttpMethod.PATCH, ParameterLocation.BODY))
                                .setIsNullable(true)
                                .build(),
                        OpenApiColumn.builder()
                                .setName("members_can_create_public_repositories").setSourceName("members_can_create_public_repositories")
                                .setType(BOOLEAN).setSourceType(booleanSchema)
                                .setOptionalPredicate(Map.of(PathItem.HttpMethod.PATCH, ParameterLocation.BODY))
                                .setIsNullable(true)
                                .build(),
                        OpenApiColumn.builder()
                                .setName("members_can_create_private_repositories").setSourceName("members_can_create_private_repositories")
                                .setType(BOOLEAN).setSourceType(booleanSchema)
                                .setOptionalPredicate(Map.of(PathItem.HttpMethod.PATCH, ParameterLocation.BODY))
                                .setIsNullable(true)
                                .build(),
                        OpenApiColumn.builder()
                                .setName("members_can_create_internal_repositories").setSourceName("members_can_create_internal_repositories")
                                .setType(BOOLEAN).setSourceType(booleanSchema)
                                .setOptionalPredicate(Map.of(PathItem.HttpMethod.PATCH, ParameterLocation.BODY))
                                .setIsNullable(true)
                                .build(),
                        OpenApiColumn.builder()
                                .setName("members_can_create_pages").setSourceName("members_can_create_pages")
                                .setType(BOOLEAN).setSourceType(booleanSchema)
                                .setOptionalPredicate(Map.of(PathItem.HttpMethod.PATCH, ParameterLocation.BODY))
                                .setIsNullable(true)
                                .build(),
                        OpenApiColumn.builder()
                                .setName("members_can_create_public_pages").setSourceName("members_can_create_public_pages")
                                .setType(BOOLEAN).setSourceType(booleanSchema)
                                .setOptionalPredicate(Map.of(PathItem.HttpMethod.PATCH, ParameterLocation.BODY))
                                .setIsNullable(true)
                                .build(),
                        OpenApiColumn.builder()
                                .setName("members_can_create_private_pages").setSourceName("members_can_create_private_pages")
                                .setType(BOOLEAN).setSourceType(booleanSchema)
                                .setOptionalPredicate(Map.of(PathItem.HttpMethod.PATCH, ParameterLocation.BODY))
                                .setIsNullable(true)
                                .build(),
                        OpenApiColumn.builder()
                                .setName("members_can_fork_private_repositories").setSourceName("members_can_fork_private_repositories")
                                .setType(BOOLEAN).setSourceType(booleanSchema)
                                .setOptionalPredicate(Map.of(PathItem.HttpMethod.PATCH, ParameterLocation.BODY))
                                .setIsNullable(true)
                                .build(),
                        OpenApiColumn.builder()
                                .setName("web_commit_signoff_required").setSourceName("web_commit_signoff_required")
                                .setType(BOOLEAN).setSourceType(booleanSchema)
                                .setOptionalPredicate(Map.of(PathItem.HttpMethod.PATCH, ParameterLocation.BODY))
                                .setIsNullable(true)
                                .build(),
                        OpenApiColumn.builder()
                                .setName("advanced_security_enabled_for_new_repositories").setSourceName("advanced_security_enabled_for_new_repositories")
                                .setType(BOOLEAN).setSourceType(booleanSchema)
                                .setOptionalPredicate(Map.of(PathItem.HttpMethod.PATCH, ParameterLocation.BODY))
                                .setIsNullable(true)
                                .setComment("""
Whether GitHub Advanced Security is enabled for new repositories and repositories transferred to this organization.

This field is only visible to organization owners or members of a team with the security manager role.""")
                                .build(),
                        OpenApiColumn.builder()
                                .setName("dependabot_alerts_enabled_for_new_repositories").setSourceName("dependabot_alerts_enabled_for_new_repositories")
                                .setType(BOOLEAN).setSourceType(booleanSchema)
                                .setOptionalPredicate(Map.of(PathItem.HttpMethod.PATCH, ParameterLocation.BODY))
                                .setIsNullable(true)
                                .setComment("""
Whether GitHub Advanced Security is automatically enabled for new repositories and repositories transferred to
this organization.

This field is only visible to organization owners or members of a team with the security manager role.""")
                                .build(),
                        OpenApiColumn.builder()
                                .setName("dependabot_security_updates_enabled_for_new_repositories").setSourceName("dependabot_security_updates_enabled_for_new_repositories")
                                .setType(BOOLEAN).setSourceType(booleanSchema)
                                .setOptionalPredicate(Map.of(PathItem.HttpMethod.PATCH, ParameterLocation.BODY))
                                .setIsNullable(true)
                                .setComment("""
Whether dependabot security updates are automatically enabled for new repositories and repositories transferred
to this organization.

This field is only visible to organization owners or members of a team with the security manager role.""")
                                .build(),
                        OpenApiColumn.builder()
                                .setName("dependency_graph_enabled_for_new_repositories").setSourceName("dependency_graph_enabled_for_new_repositories")
                                .setType(BOOLEAN).setSourceType(booleanSchema)
                                .setOptionalPredicate(Map.of(PathItem.HttpMethod.PATCH, ParameterLocation.BODY))
                                .setIsNullable(true)
                                .setComment("""
Whether dependency graph is automatically enabled for new repositories and repositories transferred to this
organization.

This field is only visible to organization owners or members of a team with the security manager role.""")
                                .build(),
                        OpenApiColumn.builder()
                                .setName("secret_scanning_enabled_for_new_repositories").setSourceName("secret_scanning_enabled_for_new_repositories")
                                .setType(BOOLEAN).setSourceType(booleanSchema)
                                .setOptionalPredicate(Map.of(PathItem.HttpMethod.PATCH, ParameterLocation.BODY))
                                .setIsNullable(true)
                                .setComment("""
Whether secret scanning is automatically enabled for new repositories and repositories transferred to this
organization.

This field is only visible to organization owners or members of a team with the security manager role.""")
                                .build(),
                        OpenApiColumn.builder()
                                .setName("secret_scanning_push_protection_enabled_for_new_repositories").setSourceName("secret_scanning_push_protection_enabled_for_new_repositories")
                                .setType(BOOLEAN).setSourceType(booleanSchema)
                                .setOptionalPredicate(Map.of(PathItem.HttpMethod.PATCH, ParameterLocation.BODY))
                                .setIsNullable(true)
                                .setComment("""
Whether secret scanning push protection is automatically enabled for new repositories and repositories
transferred to this organization.

This field is only visible to organization owners or members of a team with the security manager role.""")
                                .build(),
                        OpenApiColumn.builder()
                                .setName("secret_scanning_push_protection_custom_link_enabled").setSourceName("secret_scanning_push_protection_custom_link_enabled")
                                .setType(BOOLEAN).setSourceType(booleanSchema)
                                .setOptionalPredicate(Map.of(PathItem.HttpMethod.PATCH, ParameterLocation.BODY))
                                .setIsNullable(true)
                                .setComment("Whether a custom link is shown to contributors who are blocked from pushing a secret by push protection.")
                                .build(),
                        OpenApiColumn.builder()
                                .setName("secret_scanning_push_protection_custom_link").setSourceName("secret_scanning_push_protection_custom_link")
                                .setType(VARCHAR).setSourceType(stringSchema)
                                .setOptionalPredicate(Map.of(PathItem.HttpMethod.PATCH, ParameterLocation.BODY))
                                .setIsNullable(true)
                                .setComment("An optional URL string to display to contributors who are blocked from pushing a secret.")
                                .build(),
                        OpenApiColumn.builder()
                                .setName("created_at").setSourceName("created_at")
                                .setType(TIMESTAMP_MILLIS).setSourceType(stringSchema)
                                .build(),
                        OpenApiColumn.builder()
                                .setName("updated_at").setSourceName("updated_at")
                                .setType(TIMESTAMP_MILLIS).setSourceType(stringSchema)
                                .build(),
                        OpenApiColumn.builder()
                                .setName("archived_at").setSourceName("archived_at")
                                .setType(TIMESTAMP_MILLIS).setSourceType(stringSchema)
                                .setIsNullable(true)
                                .build(),
                        OpenApiColumn.builder()
                                .setName("org").setSourceName("org")
                                .setType(VARCHAR).setSourceType(stringSchema)
                                .setRequiresPredicate(Map.of(
                                        PathItem.HttpMethod.GET, ParameterLocation.PATH,
                                        PathItem.HttpMethod.PATCH, ParameterLocation.PATH,
                                        PathItem.HttpMethod.DELETE, ParameterLocation.PATH))
                                .setIsNullable(true)
                                .build());

        OpenApiTableHandle workflowTableHandle = spec.getTableHandle(schemaTableName(SCHEMA_NAME, "repos_actions_workflows"));
        Assertions.assertThat(workflowTableHandle.getSelectPaths()).containsExactly("/repos/{owner}/{repo}/actions/workflows", "/repos/{owner}/{repo}/actions/workflows/{workflow_id}");
        Assertions.assertThat(workflowTableHandle.getInsertPaths()).isEmpty();
        Assertions.assertThat(workflowTableHandle.getUpdatePaths()).isEmpty();
        Assertions.assertThat(workflowTableHandle.getDeletePaths()).isEmpty();
        List<OpenApiColumn> workflowColumns = tables.get("repos_actions_workflows").stream()
                .map(column -> {
                    // compare only source types, so rebuild it without any other attribute
                    Schema<?> sourceType = new Schema<>();
                    sourceType.setType(column.getSourceType().getType());
                    return OpenApiColumn.builderFrom(column)
                            .setSourceType(sourceType)
                            .build();
                })
                .toList();
        Assertions.assertThat(workflowColumns)
                .containsExactly(
                        OpenApiColumn.builder()
                                .setName("total_count").setSourceName("total_count")
                                .setType(BIGINT).setSourceType(intSchema)
                                .build(),
                        OpenApiColumn.builder()
                                .setName("id").setSourceName("id")
                                .setResultsPointer(JsonPointer.valueOf("/workflows"))
                                .setType(BIGINT).setSourceType(intSchema)
                                .setIsNullable(true)
                                .build(),
                        OpenApiColumn.builder()
                                .setName("node_id").setSourceName("node_id")
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
                                .setName("path").setSourceName("path")
                                .setResultsPointer(JsonPointer.valueOf("/workflows"))
                                .setType(VARCHAR).setSourceType(stringSchema)
                                .setIsNullable(true)
                                .build(),
                        OpenApiColumn.builder()
                                .setName("state").setSourceName("state")
                                .setResultsPointer(JsonPointer.valueOf("/workflows"))
                                .setType(VARCHAR).setSourceType(stringSchema)
                                .setIsNullable(true)
                                .build(),
                        OpenApiColumn.builder()
                                .setName("created_at").setSourceName("created_at")
                                .setResultsPointer(JsonPointer.valueOf("/workflows"))
                                .setType(TIMESTAMP_MILLIS).setSourceType(stringSchema)
                                .setIsNullable(true)
                                .build(),
                        OpenApiColumn.builder()
                                .setName("updated_at").setSourceName("updated_at")
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
                                .setName("html_url").setSourceName("html_url")
                                .setResultsPointer(JsonPointer.valueOf("/workflows"))
                                .setType(VARCHAR).setSourceType(stringSchema)
                                .setIsNullable(true)
                                .build(),
                        OpenApiColumn.builder()
                                .setName("badge_url").setSourceName("badge_url")
                                .setResultsPointer(JsonPointer.valueOf("/workflows"))
                                .setType(VARCHAR).setSourceType(stringSchema)
                                .setIsNullable(true)
                                .build(),
                        OpenApiColumn.builder()
                                .setName("deleted_at").setSourceName("deleted_at")
                                .setResultsPointer(JsonPointer.valueOf("/workflows"))
                                .setType(TIMESTAMP_MILLIS).setSourceType(stringSchema)
                                .setIsNullable(true)
                                .build(),
                        OpenApiColumn.builder()
                                .setName("owner").setSourceName("owner")
                                .setType(VARCHAR).setSourceType(stringSchema)
                                .setRequiresPredicate(Map.of(PathItem.HttpMethod.GET, ParameterLocation.PATH))
                                .setIsNullable(true)
                                .build(),
                        OpenApiColumn.builder()
                                .setName("repo").setSourceName("repo")
                                .setType(VARCHAR).setSourceType(stringSchema)
                                .setRequiresPredicate(Map.of(PathItem.HttpMethod.GET, ParameterLocation.PATH))
                                .setIsNullable(true)
                                .build(),
                        OpenApiColumn.builder()
                                .setName("per_page").setSourceName("per_page")
                                .setType(BIGINT).setSourceType(intSchema)
                                .setOptionalPredicate(Map.of(PathItem.HttpMethod.GET, ParameterLocation.QUERY))
                                .setIsNullable(true)
                                .setIsHidden(true)
                                .build(),
                        OpenApiColumn.builder()
                                .setName("page").setSourceName("page")
                                .setType(BIGINT).setSourceType(intSchema)
                                .setOptionalPredicate(Map.of(PathItem.HttpMethod.GET, ParameterLocation.QUERY))
                                .setIsNullable(true)
                                .setIsHidden(true)
                                .setIsPageNumber(true)
                                .build(),
                        OpenApiColumn.builder()
                                .setName("workflow_id").setSourceName("workflow_id")
                                .setType(VARCHAR).setSourceType(nullSchema)
                                .setRequiresPredicate(Map.of(PathItem.HttpMethod.GET, ParameterLocation.PATH))
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
                "rest_api_3_project",
                "rest_api_3_search");
        Assertions.assertThat(tables.keySet()).containsAll(expected);

        OpenApiTableHandle tableHandle = spec.getTableHandle(schemaTableName(SCHEMA_NAME, "rest_api_3_search"));
        Assertions.assertThat(tableHandle.getSelectPaths()).containsExactly("/rest/api/3/search");
        Assertions.assertThat(tableHandle.getInsertPaths()).containsExactly("/rest/api/3/search");
        Assertions.assertThat(tableHandle.getUpdatePaths()).containsExactly("/rest/api/3/search");
        Assertions.assertThat(tableHandle.getDeletePaths()).isEmpty();
        List<OpenApiColumn> columns = tables.get("rest_api_3_search").stream()
                .map(column -> {
                    // compare only source types, so rebuild it without any other attribute
                    Schema<?> sourceType = new Schema<>();
                    sourceType.setType(column.getSourceType().getType());
                    return OpenApiColumn.builderFrom(column)
                            .setSourceType(sourceType)
                            .build();
                })
                .toList();

        MapType schemaType = new MapType(VARCHAR, RowType.from(List.of(
                RowType.field("configuration", new MapType(VARCHAR, VARCHAR, new TypeOperators())),
                RowType.field("custom", VARCHAR),
                RowType.field("customId", BIGINT),
                RowType.field("items", VARCHAR),
                RowType.field("system", VARCHAR),
                RowType.field("type", VARCHAR))), new TypeOperators());
        ArrayType issuesType = new ArrayType(RowType.from(List.of(
                RowType.field("changelog", VARCHAR),
                RowType.field("editmeta", VARCHAR),
                RowType.field("expand", VARCHAR),
                RowType.field("fields", new MapType(VARCHAR, VARCHAR, new TypeOperators())),
                RowType.field("fieldsToInclude", RowType.from(List.of(
                        RowType.field("actuallyIncluded", new ArrayType(VARCHAR)),
                        RowType.field("excluded", new ArrayType(VARCHAR)),
                        RowType.field("included", new ArrayType(VARCHAR))))),
                RowType.field("id", VARCHAR),
                RowType.field("key", VARCHAR),
                RowType.field("names", new MapType(VARCHAR, VARCHAR, new TypeOperators())),
                RowType.field("operations", VARCHAR),
                RowType.field("properties", new MapType(VARCHAR, VARCHAR, new TypeOperators())),
                RowType.field("renderedFields", new MapType(VARCHAR, VARCHAR, new TypeOperators())),
                RowType.field("schema", schemaType),
                RowType.field("self", VARCHAR),
                RowType.field("transitions", new ArrayType(VARCHAR)),
                RowType.field("versionedRepresentations", new MapType(VARCHAR, new MapType(VARCHAR, VARCHAR, new TypeOperators()), new TypeOperators())))));
        Assertions.assertThat(columns)
                .containsExactly(
                        OpenApiColumn.builder()
                                .setName("__trino_row_id")
                                .setType(VARCHAR).setSourceType(stringSchema)
                                .setIsHidden(true)
                                .build(),
                        OpenApiColumn.builder()
                                .setName("expand").setSourceName("expand")
                                .setType(VARCHAR).setSourceType(stringSchema)
                                .setOptionalPredicate(Map.of(PathItem.HttpMethod.GET, ParameterLocation.QUERY))
                                .setIsNullable(true)
                                .setComment("Expand options that include additional search result details in the response.")
                                .build(),
                        OpenApiColumn.builder()
                                .setName("issues").setSourceName("issues")
                                .setType(issuesType).setSourceType(arraySchema)
                                .setIsNullable(true)
                                .setComment("The list of issues found by the search.")
                                .build(),
                        OpenApiColumn.builder()
                                .setName("max_results").setSourceName("maxResults")
                                .setType(INTEGER).setSourceType(intSchema)
                                .setOptionalPredicate(Map.of(PathItem.HttpMethod.POST, ParameterLocation.BODY, PathItem.HttpMethod.GET, ParameterLocation.QUERY))
                                .setIsNullable(true)
                                .setComment("The maximum number of results that could be on the page.")
                                .build(),
                        OpenApiColumn.builder()
                                .setName("names").setSourceName("names")
                                .setType(new MapType(VARCHAR, VARCHAR, new TypeOperators())).setSourceType(objectSchema)
                                .setIsNullable(true)
                                .setComment("The ID and name of each field in the search results.")
                                .build(),
                        OpenApiColumn.builder()
                                .setName("schema").setSourceName("schema")
                                .setType(schemaType).setSourceType(objectSchema)
                                .setIsNullable(true)
                                .setComment("The schema describing the field types in the search results.")
                                .build(),
                        OpenApiColumn.builder()
                                .setName("start_at").setSourceName("startAt")
                                .setType(INTEGER).setSourceType(intSchema)
                                .setOptionalPredicate(Map.of(PathItem.HttpMethod.POST, ParameterLocation.BODY, PathItem.HttpMethod.GET, ParameterLocation.QUERY))
                                .setIsNullable(true)
                                .setComment("The index of the first item returned on the page.")
                                .build(),
                        OpenApiColumn.builder()
                                .setName("total").setSourceName("total")
                                .setType(INTEGER).setSourceType(intSchema)
                                .setIsNullable(true)
                                .setComment("The number of results on the page.")
                                .build(),
                        OpenApiColumn.builder()
                                .setName("warning_messages").setSourceName("warningMessages")
                                .setType(new ArrayType(VARCHAR)).setSourceType(arraySchema)
                                .setIsNullable(true)
                                .setComment("Any warnings related to the JQL query.")
                                .build(),
                        OpenApiColumn.builder()
                                .setName("jql").setSourceName("jql")
                                .setType(VARCHAR).setSourceType(stringSchema)
                                .setOptionalPredicate(Map.of(PathItem.HttpMethod.POST, ParameterLocation.BODY, PathItem.HttpMethod.GET, ParameterLocation.QUERY))
                                .setIsNullable(true)
                                .build(),
                        OpenApiColumn.builder()
                                .setName("validate_query").setSourceName("validateQuery")
                                .setType(VARCHAR).setSourceType(stringSchema)
                                .setOptionalPredicate(Map.of(PathItem.HttpMethod.POST, ParameterLocation.BODY, PathItem.HttpMethod.GET, ParameterLocation.QUERY))
                                .setIsNullable(true)
                                .build(),
                        OpenApiColumn.builder()
                                .setName("fields").setSourceName("fields")
                                .setType(new ArrayType(VARCHAR)).setSourceType(arraySchema)
                                .setOptionalPredicate(Map.of(PathItem.HttpMethod.POST, ParameterLocation.BODY, PathItem.HttpMethod.GET, ParameterLocation.QUERY))
                                .setIsNullable(true)
                                .build(),
                        OpenApiColumn.builder()
                                .setName("properties").setSourceName("properties")
                                .setType(new ArrayType(VARCHAR)).setSourceType(arraySchema)
                                .setOptionalPredicate(Map.of(PathItem.HttpMethod.POST, ParameterLocation.BODY, PathItem.HttpMethod.GET, ParameterLocation.QUERY))
                                .setIsNullable(true)
                                .build(),
                        OpenApiColumn.builder()
                                .setName("fields_by_keys").setSourceName("fieldsByKeys")
                                .setType(BOOLEAN).setSourceType(booleanSchema)
                                .setOptionalPredicate(Map.of(PathItem.HttpMethod.POST, ParameterLocation.BODY, PathItem.HttpMethod.GET, ParameterLocation.QUERY))
                                .setIsNullable(true)
                                .build(),
                        OpenApiColumn.builder()
                                .setName("expand_req").setSourceName("expand")
                                .setType(new ArrayType(VARCHAR)).setSourceType(arraySchema)
                                .setOptionalPredicate(Map.of(PathItem.HttpMethod.POST, ParameterLocation.BODY))
                                .setIsNullable(true)
                                .setComment("Use [expand](em>#expansion) to include additional information about issues in the response. Note that, unlike the majority of instances where `expand` is specified, `expand` is defined as a list of values. The expand options are:\n" +
                                        "\n" +
                                        " *  `renderedFields` Returns field values rendered in HTML format.\n" +
                                        " *  `names` Returns the display name of each field.\n" +
                                        " *  `schema` Returns the schema describing a field type.\n" +
                                        " *  `transitions` Returns all possible transitions for the issue.\n" +
                                        " *  `operations` Returns all possible operations for the issue.\n" +
                                        " *  `editmeta` Returns information about how each field can be edited.\n" +
                                        " *  `changelog` Returns a list of recent updates to an issue, sorted by date, starting from the most recent.\n" +
                                        " *  `versionedRepresentations` Instead of `fields`, returns `versionedRepresentations` a JSON array containing each version of a field's value, with the highest numbered item representing the most recent version.")
                                .build());
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
                "pet_find_by_tags",
                "user_login",
                "store_order",
                "store_inventory",
                "user",
                "user_create_with_list",
                "pet");
        Assertions.assertThat(tables.keySet()).containsAll(expected);
        OpenApiTableHandle tableHandle = spec.getTableHandle(schemaTableName(SCHEMA_NAME, "pet"));
        Assertions.assertThat(tableHandle.getSelectPaths()).containsExactly("/pet/{petId}");
        Assertions.assertThat(tableHandle.getInsertPaths()).containsExactly("/pet", "/pet/{petId}");
        Assertions.assertThat(tableHandle.getUpdatePaths()).containsExactly("/pet", "/pet/{petId}");
        Assertions.assertThat(tableHandle.getDeletePaths()).containsExactly("/pet/{petId}");
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
                                .setName("__trino_row_id")
                                .setType(VARCHAR).setSourceType(stringSchema)
                                .setIsHidden(true)
                                .build(),
                        OpenApiColumn.builder()
                                .setName("id").setSourceName("id")
                                .setType(BIGINT).setSourceType(intSchema)
                                .setOptionalPredicate(Map.of(
                                        PathItem.HttpMethod.POST, ParameterLocation.BODY,
                                        PathItem.HttpMethod.PUT, ParameterLocation.BODY))
                                .setIsNullable(true)
                                .build(),
                        OpenApiColumn.builder()
                                .setName("name").setSourceName("name")
                                .setType(VARCHAR).setSourceType(stringSchema)
                                .setRequiresPredicate(Map.of(
                                        PathItem.HttpMethod.POST, ParameterLocation.BODY,
                                        PathItem.HttpMethod.PUT, ParameterLocation.BODY))
                                .setOptionalPredicate(Map.of(PathItem.HttpMethod.POST, ParameterLocation.QUERY))
                                .setIsNullable(true)
                                .build(),
                        OpenApiColumn.builder()
                                .setName("category").setSourceName("category")
                                .setType(categoryType).setSourceType(objectSchema)
                                .setOptionalPredicate(Map.of(
                                        PathItem.HttpMethod.POST, ParameterLocation.BODY,
                                        PathItem.HttpMethod.PUT, ParameterLocation.BODY))
                                .setIsNullable(true)
                                .build(),
                        OpenApiColumn.builder()
                                .setName("photo_urls").setSourceName("photoUrls")
                                .setType(photosType).setSourceType(arraySchema)
                                .setRequiresPredicate(Map.of(
                                        PathItem.HttpMethod.POST, ParameterLocation.BODY,
                                        PathItem.HttpMethod.PUT, ParameterLocation.BODY))
                                .build(),
                        OpenApiColumn.builder()
                                .setName("tags").setSourceName("tags")
                                .setType(tagsType).setSourceType(arraySchema)
                                .setOptionalPredicate(Map.of(
                                        PathItem.HttpMethod.POST, ParameterLocation.BODY,
                                        PathItem.HttpMethod.PUT, ParameterLocation.BODY))
                                .setIsNullable(true)
                                .build(),
                        OpenApiColumn.builder()
                                .setName("status").setSourceName("status")
                                .setType(VARCHAR).setSourceType(stringSchema)
                                .setOptionalPredicate(Map.of(
                                        PathItem.HttpMethod.POST, ParameterLocation.BODY,
                                        PathItem.HttpMethod.PUT, ParameterLocation.BODY))
                                .setIsNullable(true)
                                .setComment("pet status in the store")
                                .build(),
                        OpenApiColumn.builder()
                                .setName("pet_id").setSourceName("petId")
                                .setType(BIGINT).setSourceType(intSchema)
                                .setRequiresPredicate(Map.of(
                                        PathItem.HttpMethod.POST, ParameterLocation.PATH,
                                        PathItem.HttpMethod.GET, ParameterLocation.PATH,
                                        PathItem.HttpMethod.DELETE, ParameterLocation.PATH))
                                .setIsNullable(true)
                                .build(),
                        OpenApiColumn.builder()
                                .setName("api_key").setSourceName("api_key")
                                .setType(VARCHAR).setSourceType(stringSchema)
                                .setOptionalPredicate(Map.of(PathItem.HttpMethod.DELETE, ParameterLocation.HEADER))
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
        Assertions.assertThat(tableHandle.getSelectPaths()).containsExactly("/api/v2/query/timeseries");
        Assertions.assertThat(tableHandle.getInsertPaths()).containsExactly("/api/v2/query/timeseries");
        Assertions.assertThat(tableHandle.getUpdatePaths()).containsExactly("/api/v2/query/timeseries");
        Assertions.assertThat(tableHandle.getDeletePaths()).isEmpty();
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
                                .setName("__trino_row_id")
                                .setType(VARCHAR).setSourceType(stringSchema)
                                .setIsHidden(true)
                                .build(),
                        OpenApiColumn.builder()
                                .setName("data").setSourceName("data")
                                .setType(dataType).setSourceType(objectSchema)
                                .setIsNullable(true)
                                .setComment("A message containing the response to a timeseries query.")
                                .build(),
                        OpenApiColumn.builder()
                                .setName("errors").setSourceName("errors")
                                .setType(VARCHAR).setSourceType(stringSchema)
                                .setIsNullable(true)
                                .setComment("The error generated by the request.")
                                .build(),
                        OpenApiColumn.builder()
                                .setName("data_req").setSourceName("data")
                                .setType(dataReqType).setSourceType(objectSchema)
                                .setRequiresPredicate(Map.of(PathItem.HttpMethod.POST, ParameterLocation.BODY))
                                .setComment("A single timeseries query to be executed.")
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
        Assertions.assertThat(tableHandle.getSelectPaths()).containsExactly("/zones", "/zones/{identifier}");
        Assertions.assertThat(tableHandle.getInsertPaths()).containsExactly("/zones");
        Assertions.assertThat(tableHandle.getUpdatePaths()).containsExactly("/zones");
        Assertions.assertThat(tableHandle.getDeletePaths()).containsExactly("/zones/{identifier}");
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
                        RowType.field("custom_certificate_quota", BIGINT),
                        RowType.field("dns_only", BOOLEAN),
                        RowType.field("foundation_dns", BOOLEAN),
                        RowType.field("page_rule_quota", BIGINT),
                        RowType.field("phishing_detected", BOOLEAN),
                        RowType.field("step", BIGINT)))),
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
        RowType result3Type = RowType.from(List.of(RowType.field("id", VARCHAR)));
        RowType resultInfoType = RowType.from(List.of(
                RowType.field("count", createDecimalType(18, 8)),
                RowType.field("page", createDecimalType(18, 8)),
                RowType.field("per_page", createDecimalType(18, 8)),
                RowType.field("total_count", createDecimalType(18, 8))));
        Assertions.assertThat(columns)
                .containsExactly(
                        OpenApiColumn.builder()
                                .setName("__trino_row_id")
                                .setType(VARCHAR).setSourceType(stringSchema)
                                .setIsHidden(true)
                                .build(),
                        OpenApiColumn.builder()
                                .setName("errors").setSourceName("errors")
                                .setType(new ArrayType(RowType.from(List.of(
                                        RowType.field("code", BIGINT),
                                        RowType.field("message", VARCHAR))))).setSourceType(arraySchema)
                                .build(),
                        OpenApiColumn.builder()
                                .setName("messages").setSourceName("messages")
                                .setType(new ArrayType(RowType.from(List.of(
                                        RowType.field("code", BIGINT),
                                        RowType.field("message", VARCHAR))))).setSourceType(arraySchema)
                                .build(),
                        OpenApiColumn.builder()
                                .setName("success").setSourceName("success")
                                .setType(BOOLEAN).setSourceType(booleanSchema)
                                .setComment("Whether the API call was successful")
                                .build(),
                        OpenApiColumn.builder()
                                .setName("result_info").setSourceName("result_info")
                                .setType(resultInfoType).setSourceType(objectSchema)
                                .setIsNullable(true)
                                .build(),
                        OpenApiColumn.builder()
                                .setName("result").setSourceName("result")
                                .setType(new ArrayType(resultType)).setSourceType(arraySchema)
                                .setIsNullable(true)
                                .build(),
                        OpenApiColumn.builder()
                                .setName("result_2").setSourceName("result")
                                .setType(resultType).setSourceType(objectSchema)
                                .setIsNullable(true)
                                .build(),
                        OpenApiColumn.builder()
                                .setName("result_3").setSourceName("result")
                                .setType(result3Type).setSourceType(objectSchema)
                                .setIsNullable(true)
                                .build(),
                        OpenApiColumn.builder()
                                .setName("name").setSourceName("name")
                                .setType(VARCHAR).setSourceType(stringSchema)
                                .setRequiresPredicate(Map.of(PathItem.HttpMethod.POST, ParameterLocation.BODY))
                                .setOptionalPredicate(Map.of(PathItem.HttpMethod.GET, ParameterLocation.QUERY))
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
                                .setName("status").setSourceName("status")
                                .setType(VARCHAR).setSourceType(stringSchema)
                                .setOptionalPredicate(Map.of(PathItem.HttpMethod.GET, ParameterLocation.QUERY))
                                .setIsNullable(true)
                                .setComment("A zone status")
                                .build(),
                        OpenApiColumn.builder()
                                .setName("account.id").setSourceName("account.id")
                                .setType(VARCHAR).setSourceType(stringSchema)
                                .setOptionalPredicate(Map.of(PathItem.HttpMethod.GET, ParameterLocation.QUERY))
                                .setIsNullable(true)
                                .setComment("An account ID")
                                .build(),
                        OpenApiColumn.builder()
                                .setName("account.name").setSourceName("account.name")
                                .setType(VARCHAR).setSourceType(stringSchema)
                                .setOptionalPredicate(Map.of(PathItem.HttpMethod.GET, ParameterLocation.QUERY))
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
                                .setName("page").setSourceName("page")
                                .setType(createDecimalType(18, 8)).setSourceType(numberSchema)
                                .setOptionalPredicate(Map.of(PathItem.HttpMethod.GET, ParameterLocation.QUERY))
                                .setIsNullable(true)
                                .setComment("Page number of paginated results.")
                                .build(),
                        OpenApiColumn.builder()
                                .setName("per_page").setSourceName("per_page")
                                .setType(createDecimalType(18, 8)).setSourceType(numberSchema)
                                .setOptionalPredicate(Map.of(PathItem.HttpMethod.GET, ParameterLocation.QUERY))
                                .setIsNullable(true)
                                .setComment("Number of zones per page.")
                                .build(),
                        OpenApiColumn.builder()
                                .setName("order").setSourceName("order")
                                .setType(VARCHAR).setSourceType(stringSchema)
                                .setOptionalPredicate(Map.of(PathItem.HttpMethod.GET, ParameterLocation.QUERY))
                                .setIsNullable(true)
                                .setComment("Field to order zones by.")
                                .build(),
                        OpenApiColumn.builder()
                                .setName("direction").setSourceName("direction")
                                .setType(VARCHAR).setSourceType(stringSchema)
                                .setOptionalPredicate(Map.of(PathItem.HttpMethod.GET, ParameterLocation.QUERY))
                                .setIsNullable(true)
                                .setComment("Direction to order zones.")
                                .build(),
                        OpenApiColumn.builder()
                                .setName("match").setSourceName("match")
                                .setType(VARCHAR).setSourceType(stringSchema)
                                .setOptionalPredicate(Map.of(PathItem.HttpMethod.GET, ParameterLocation.QUERY))
                                .setIsNullable(true)
                                .setComment("Whether to match all search requirements or at least one (any).")
                                .build(),
                        OpenApiColumn.builder()
                                .setName("account").setSourceName("account")
                                .setType(RowType.from(List.of(RowType.field("id", VARCHAR)))).setSourceType(objectSchema)
                                .setRequiresPredicate(Map.of(PathItem.HttpMethod.POST, ParameterLocation.BODY))
                                .build(),
                        OpenApiColumn.builder()
                                .setName("type").setSourceName("type")
                                .setType(VARCHAR).setSourceType(stringSchema)
                                .setOptionalPredicate(Map.of(PathItem.HttpMethod.POST, ParameterLocation.BODY, PathItem.HttpMethod.PATCH, ParameterLocation.BODY))
                                .setIsNullable(true)
                                .setComment("A full zone implies that DNS is hosted with Cloudflare. A partial zone is \n" +
                                        "typically a partner-hosted zone or a CNAME setup.\n")
                                .build(),
                        OpenApiColumn.builder()
                                .setName("identifier").setSourceName("identifier")
                                .setType(VARCHAR).setSourceType(stringSchema)
                                // TODO we've merged /zones and /zones/{identifier} and now we require identifier for /zones, which is wrong
                                .setRequiresPredicate(Map.of(PathItem.HttpMethod.GET, ParameterLocation.PATH, PathItem.HttpMethod.DELETE, ParameterLocation.PATH, PathItem.HttpMethod.PATCH, ParameterLocation.PATH))
                                .setIsNullable(true)
                                .setComment("Identifier")
                                .build(),
                        OpenApiColumn.builder()
                                .setName("paused").setSourceName("paused")
                                .setType(BOOLEAN).setSourceType(booleanSchema)
                                .setOptionalPredicate(Map.of(PathItem.HttpMethod.PATCH, ParameterLocation.BODY))
                                .setIsNullable(true)
                                .setComment("Indicates whether the zone is only using Cloudflare DNS services. A\n" +
                                        "true value means the zone will not receive security or performance\n" +
                                        "benefits.\n")
                                .build(),
                        OpenApiColumn.builder()
                                .setName("plan").setSourceName("plan")
                                .setType(RowType.from(List.of(RowType.field("id", VARCHAR)))).setSourceType(objectSchema)
                                .setOptionalPredicate(Map.of(PathItem.HttpMethod.PATCH, ParameterLocation.BODY))
                                .setIsNullable(true)
                                .setComment("(Deprecated) Please use the `/zones/{identifier}/subscription` API\n" +
                                        "to update a zone's plan. Changing this value will create/cancel\n" +
                                        "associated subscriptions. To view available plans for this zone,\n" +
                                        "see Zone Plans.\n")
                                .build(),
                        OpenApiColumn.builder()
                                .setName("vanity_name_servers").setSourceName("vanity_name_servers")
                                .setType(new ArrayType(VARCHAR)).setSourceType(arraySchema)
                                .setOptionalPredicate(Map.of(PathItem.HttpMethod.PATCH, ParameterLocation.BODY))
                                .setIsNullable(true)
                                .setComment("An array of domains used for custom name servers. This is only\n" +
                                        "available for Business and Enterprise plans.")
                                .build());
    }

    @Test
    public void getOpenMeteoTables()
    {
        OpenApiSpec spec = loadSpec("openmeteo.yml");
        Map<String, List<OpenApiColumn>> tables = spec.getTables();

        Set<String> expected = Set.of(
                "v1_forecast");
        Assertions.assertThat(tables.keySet()).containsAll(expected);
        OpenApiTableHandle tableHandle = spec.getTableHandle(schemaTableName(SCHEMA_NAME, "v1_forecast"));
        Assertions.assertThat(tableHandle.getSelectPaths()).containsExactly("/v1/forecast");
        Assertions.assertThat(tableHandle.getInsertPaths()).isEmpty();
        Assertions.assertThat(tableHandle.getUpdatePaths()).isEmpty();
        Assertions.assertThat(tableHandle.getDeletePaths()).isEmpty();
        List<OpenApiColumn> columns = tables.get("v1_forecast").stream()
                .map(column -> {
                    // compare only source types, so rebuild it without any other attribute
                    Schema<?> sourceType = new Schema<>();
                    sourceType.setType(column.getSourceType().getType());
                    return OpenApiColumn.builderFrom(column)
                            .setSourceType(sourceType)
                            .build();
                })
                .toList();
        RowType dailyType = RowType.from(List.of(
                RowType.field("time", new ArrayType(VARCHAR)),
                RowType.field("temperature_2m_max", new ArrayType(REAL)),
                RowType.field("temperature_2m_min", new ArrayType(REAL)),
                RowType.field("apparent_temperature_max", new ArrayType(REAL)),
                RowType.field("apparent_temperature_min", new ArrayType(REAL)),
                RowType.field("precipitation_sum", new ArrayType(REAL)),
                RowType.field("precipitation_hours", new ArrayType(REAL)),
                RowType.field("weather_code", new ArrayType(REAL)),
                RowType.field("sunrise", new ArrayType(REAL)),
                RowType.field("sunset", new ArrayType(REAL)),
                RowType.field("wind_speed_10m_max", new ArrayType(REAL)),
                RowType.field("wind_gusts_10m_max", new ArrayType(REAL)),
                RowType.field("wind_direction_10m_dominant", new ArrayType(REAL)),
                RowType.field("shortwave_radiation_sum", new ArrayType(REAL)),
                RowType.field("uv_index_max", new ArrayType(REAL)),
                RowType.field("uv_index_clear_sky_max", new ArrayType(REAL)),
                RowType.field("et0_fao_evapotranspiration", new ArrayType(REAL))));
        RowType hourlyType = RowType.from(List.of(
                RowType.field("time", new ArrayType(VARCHAR)),
                RowType.field("temperature_2m", new ArrayType(REAL)),
                RowType.field("relative_humidity_2m", new ArrayType(REAL)),
                RowType.field("dew_point_2m", new ArrayType(REAL)),
                RowType.field("apparent_temperature", new ArrayType(REAL)),
                RowType.field("pressure_msl", new ArrayType(REAL)),
                RowType.field("cloud_cover", new ArrayType(REAL)),
                RowType.field("cloud_cover_low", new ArrayType(REAL)),
                RowType.field("cloud_cover_mid", new ArrayType(REAL)),
                RowType.field("cloud_cover_high", new ArrayType(REAL)),
                RowType.field("wind_speed_10m", new ArrayType(REAL)),
                RowType.field("wind_speed_80m", new ArrayType(REAL)),
                RowType.field("wind_speed_120m", new ArrayType(REAL)),
                RowType.field("wind_speed_180m", new ArrayType(REAL)),
                RowType.field("wind_direction_10m", new ArrayType(REAL)),
                RowType.field("wind_direction_80m", new ArrayType(REAL)),
                RowType.field("wind_direction_120m", new ArrayType(REAL)),
                RowType.field("wind_direction_180m", new ArrayType(REAL)),
                RowType.field("wind_gusts_10m", new ArrayType(REAL)),
                RowType.field("shortwave_radiation", new ArrayType(REAL)),
                RowType.field("direct_radiation", new ArrayType(REAL)),
                RowType.field("direct_normal_irradiance", new ArrayType(REAL)),
                RowType.field("diffuse_radiation", new ArrayType(REAL)),
                RowType.field("vapour_pressure_deficit", new ArrayType(REAL)),
                RowType.field("evapotranspiration", new ArrayType(REAL)),
                RowType.field("precipitation", new ArrayType(REAL)),
                RowType.field("weather_code", new ArrayType(REAL)),
                RowType.field("snow_height", new ArrayType(REAL)),
                RowType.field("freezing_level_height", new ArrayType(REAL)),
                RowType.field("soil_temperature_0cm", new ArrayType(REAL)),
                RowType.field("soil_temperature_6cm", new ArrayType(REAL)),
                RowType.field("soil_temperature_18cm", new ArrayType(REAL)),
                RowType.field("soil_temperature_54cm", new ArrayType(REAL)),
                RowType.field("soil_moisture_0_1cm", new ArrayType(REAL)),
                RowType.field("soil_moisture_1_3cm", new ArrayType(REAL)),
                RowType.field("soil_moisture_3_9cm", new ArrayType(REAL)),
                RowType.field("soil_moisture_9_27cm", new ArrayType(REAL)),
                RowType.field("soil_moisture_27_81cm", new ArrayType(REAL))));
        RowType currentWeatherType = RowType.from(List.of(
                RowType.field("time", VARCHAR),
                RowType.field("temperature", REAL),
                RowType.field("wind_speed", REAL),
                RowType.field("wind_direction", REAL),
                RowType.field("weather_code", INTEGER)));
        Assertions.assertThat(columns)
                .containsExactly(
                        OpenApiColumn.builder()
                                .setName("latitude").setSourceName("latitude")
                                .setType(createDecimalType(18, 8)).setSourceType(numberSchema)
                                .setIsNullable(true)
                                .setComment("WGS84 of the center of the weather grid-cell which was used to generate this forecast. This coordinate might be up to 5 km away.")
                                .build(),
                        OpenApiColumn.builder()
                                .setName("longitude").setSourceName("longitude")
                                .setType(createDecimalType(18, 8)).setSourceType(numberSchema)
                                .setIsNullable(true)
                                .setComment("WGS84 of the center of the weather grid-cell which was used to generate this forecast. This coordinate might be up to 5 km away.")
                                .build(),
                        OpenApiColumn.builder()
                                .setName("elevation").setSourceName("elevation")
                                .setType(createDecimalType(18, 8)).setSourceType(numberSchema)
                                .setIsNullable(true)
                                .setComment("The elevation in meters of the selected weather grid-cell. In mountain terrain it might differ from the location you would expect.")
                                .build(),
                        OpenApiColumn.builder()
                                .setName("generationtime_ms").setSourceName("generationtime_ms")
                                .setType(createDecimalType(18, 8)).setSourceType(numberSchema)
                                .setIsNullable(true)
                                .setComment("Generation time of the weather forecast in milli seconds. This is mainly used for performance monitoring and improvements.")
                                .build(),
                        OpenApiColumn.builder()
                                .setName("utc_offset_seconds").setSourceName("utc_offset_seconds")
                                .setType(BIGINT).setSourceType(intSchema)
                                .setIsNullable(true)
                                .setComment("Applied timezone offset from the &timezone= parameter.")
                                .build(),
                        OpenApiColumn.builder()
                                .setName("hourly").setSourceName("hourly")
                                .setType(hourlyType).setSourceType(objectSchema)
                                .setIsNullable(true)
                                .setComment("For each selected weather variable, data will be returned as a floating point array. Additionally a `time` array will be returned with ISO8601 timestamps.")
                                .build(),
                        OpenApiColumn.builder()
                                .setName("hourly_units").setSourceName("hourly_units")
                                .setType(new MapType(VARCHAR, VARCHAR, new TypeOperators())).setSourceType(objectSchema)
                                .setIsNullable(true)
                                .setComment("For each selected weather variable, the unit will be listed here.")
                                .build(),
                        OpenApiColumn.builder()
                                .setName("daily").setSourceName("daily")
                                .setType(dailyType).setSourceType(objectSchema)
                                .setIsNullable(true)
                                .setComment("For each selected daily weather variable, data will be returned as a floating point array. Additionally a `time` array will be returned with ISO8601 timestamps.")
                                .build(),
                        OpenApiColumn.builder()
                                .setName("daily_units").setSourceName("daily_units")
                                .setType(new MapType(VARCHAR, VARCHAR, new TypeOperators())).setSourceType(objectSchema)
                                .setIsNullable(true)
                                .setComment("For each selected daily weather variable, the unit will be listed here.")
                                .build(),
                        OpenApiColumn.builder()
                                .setName("current_weather").setSourceName("current_weather")
                                .setType(currentWeatherType).setSourceType(objectSchema)
                                .setIsNullable(true)
                                .setComment("Current weather conditions with the attributes: time, temperature, wind_speed, wind_direction and weather_code")
                                .build(),
                        OpenApiColumn.builder()
                                .setName("hourly_req").setSourceName("hourly")
                                .setType(new ArrayType(VARCHAR)).setSourceType(arraySchema)
                                .setOptionalPredicate(Map.of(PathItem.HttpMethod.GET, ParameterLocation.QUERY))
                                .setIsNullable(true)
                                .build(),
                        OpenApiColumn.builder()
                                .setName("daily_req").setSourceName("daily")
                                .setType(new ArrayType(VARCHAR)).setSourceType(arraySchema)
                                .setOptionalPredicate(Map.of(PathItem.HttpMethod.GET, ParameterLocation.QUERY))
                                .setIsNullable(true)
                                .build(),
                        OpenApiColumn.builder()
                                .setName("latitude_req").setSourceName("latitude")
                                .setType(REAL).setSourceType(numberSchema)
                                .setRequiresPredicate(Map.of(PathItem.HttpMethod.GET, ParameterLocation.QUERY))
                                .setIsNullable(true)
                                .build(),
                        OpenApiColumn.builder()
                                .setName("longitude_req").setSourceName("longitude")
                                .setType(REAL).setSourceType(numberSchema)
                                .setRequiresPredicate(Map.of(PathItem.HttpMethod.GET, ParameterLocation.QUERY))
                                .setIsNullable(true)
                                .build(),
                        OpenApiColumn.builder()
                                .setName("current_weather_req").setSourceName("current_weather")
                                .setType(BOOLEAN).setSourceType(booleanSchema)
                                .setOptionalPredicate(Map.of(PathItem.HttpMethod.GET, ParameterLocation.QUERY))
                                .setIsNullable(true)
                                .build(),
                        OpenApiColumn.builder()
                                .setName("temperature_unit").setSourceName("temperature_unit")
                                .setType(VARCHAR).setSourceType(stringSchema)
                                .setOptionalPredicate(Map.of(PathItem.HttpMethod.GET, ParameterLocation.QUERY))
                                .setIsNullable(true)
                                .build(),
                        OpenApiColumn.builder()
                                .setName("wind_speed_unit").setSourceName("wind_speed_unit")
                                .setType(VARCHAR).setSourceType(stringSchema)
                                .setOptionalPredicate(Map.of(PathItem.HttpMethod.GET, ParameterLocation.QUERY))
                                .setIsNullable(true)
                                .build(),
                        OpenApiColumn.builder()
                                .setName("timeformat").setSourceName("timeformat")
                                .setType(VARCHAR).setSourceType(stringSchema)
                                .setOptionalPredicate(Map.of(PathItem.HttpMethod.GET, ParameterLocation.QUERY))
                                .setIsNullable(true)
                                .build(),
                        OpenApiColumn.builder()
                                .setName("timezone").setSourceName("timezone")
                                .setType(VARCHAR).setSourceType(stringSchema)
                                .setOptionalPredicate(Map.of(PathItem.HttpMethod.GET, ParameterLocation.QUERY))
                                .setIsNullable(true)
                                .build(),
                        OpenApiColumn.builder()
                                .setName("past_days").setSourceName("past_days")
                                .setType(BIGINT).setSourceType(intSchema)
                                .setOptionalPredicate(Map.of(PathItem.HttpMethod.GET, ParameterLocation.QUERY))
                                .setIsNullable(true)
                                .build());
    }

    @Test
    public void testPathParams()
    {
        String specContents = """
                openapi: 3.0.0
                info:
                  title: test
                  version: 1.0
                paths:
                  /namespaces/{namespace}/indexes/{index}/search:
                    post:
                      operationId: index_search
                      parameters:
                      - explode: false
                        in: path
                        name: namespace
                        required: true
                        schema:
                          type: string
                        style: simple
                      - explode: false
                        in: path
                        name: index
                        required: true
                        schema:
                          type: string
                        style: simple
                      requestBody:
                        content:
                          application/json:
                            schema:
                              $ref: '#/components/schemas/SearchRequest'
                        description: ""
                        required: true
                      responses:
                        "200":
                          content:
                            application/json:
                              schema:
                                $ref: '#/components/schemas/IndexSearchResponse'
                          description: Index search results
                        "500":
                          description: Unable to search index
                      summary: Search a vector index in a namespace
                components:
                  schemas:
                    SearchRequest:
                      type: object
                      properties:
                        id:
                          type: integer
                          format: int64
                    IndexSearchResponse:
                      type: object
                      properties:
                        id:
                          type: integer
                          format: int64
                """;
        OpenApiSpec spec = parseSpec(specContents);
        Map<String, List<OpenApiColumn>> tables = spec.getTables();

        Set<String> expected = Set.of("namespaces_indexes_search");
        Assertions.assertThat(tables.keySet()).containsAll(expected);
        List<OpenApiColumn> columns = tables.get("namespaces_indexes_search").stream()
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
                                .setName("__trino_row_id")
                                .setType(VARCHAR).setSourceType(stringSchema)
                                .setIsHidden(true)
                                .build(),
                        OpenApiColumn.builder()
                                .setName("id").setSourceName("id")
                                .setType(BIGINT).setSourceType(intSchema)
                                .setOptionalPredicate(Map.of(PathItem.HttpMethod.POST, ParameterLocation.BODY))
                                .setIsNullable(true)
                                .build(),
                        OpenApiColumn.builder()
                                .setName("namespace").setSourceName("namespace")
                                .setType(VARCHAR).setSourceType(stringSchema)
                                .setRequiresPredicate(Map.of(PathItem.HttpMethod.POST, ParameterLocation.PATH))
                                .setIsNullable(true)
                                .build(),
                        OpenApiColumn.builder()
                                .setName("index").setSourceName("index")
                                .setType(VARCHAR).setSourceType(stringSchema)
                                .setRequiresPredicate(Map.of(PathItem.HttpMethod.POST, ParameterLocation.PATH))
                                .setIsNullable(true)
                                .build());
    }

    private OpenApiSpec loadSpec(String name)
    {
        OpenApiConfig config = new OpenApiConfig();
        URL specResource = requireNonNull(getClass().getClassLoader().getResource(name));
        config.setSpecLocation(specResource.getFile());
        return new OpenApiSpec(config);
    }

    private OpenApiSpec parseSpec(String spec)
    {
        ParseOptions parseOptions = new ParseOptions();
        parseOptions.setResolveFully(true);
        SwaggerParseResult result = new OpenAPIV3Parser().readContents(spec, null, parseOptions);
        if (result.getMessages() != null && !result.getMessages().isEmpty()) {
            throw new IllegalArgumentException("Failed to parse the OpenAPI spec: " + String.join(", ", result.getMessages()));
        }
        return new OpenApiSpec(result.getOpenAPI());
    }
}
