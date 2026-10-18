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
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.swagger.v3.oas.models.PathItem;
import io.swagger.v3.oas.models.media.Schema;
import io.swagger.v3.oas.models.parameters.Parameter;
import io.swagger.v3.parser.OpenAPIV3Parser;
import io.swagger.v3.parser.core.models.ParseOptions;
import io.swagger.v3.parser.core.models.SwaggerParseResult;
import io.trino.spi.block.SqlRow;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.MapType;
import io.trino.spi.type.RowType;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeOperators;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import java.net.URL;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.Set;

import static io.trino.spi.connector.SchemaTableName.schemaTableName;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.DecimalType.createDecimalType;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.spi.type.TimestampType.TIMESTAMP_MILLIS;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.type.InternalTypeManager.TESTING_TYPE_MANAGER;
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
    public void getGithubUnionTables()
    {
        OpenApiSpec spec = loadSpec("github.json");
        Map<String, List<OpenApiColumn>> tables = spec.getTables();

        // GET /repos/{owner}/{repo}/contents/{path} returns a file, a symlink, a submodule, or an array of directory entries,
        // and the content of PUT responses is a different type than the file content
        RowType linksType = RowType.from(List.of(
                RowType.field("git", VARCHAR),
                RowType.field("html", VARCHAR),
                RowType.field("self", VARCHAR)));
        Assertions.assertThat(tables.get("repos_contents"))
                // the commit is only included in responses of writes
                .filteredOn(column -> column.getRequiresPredicate().isEmpty() && column.getOptionalPredicate().isEmpty() && !column.getMetadata().isHidden() && !column.getName().equals("commit"))
                .extracting(OpenApiColumn::getName, OpenApiColumn::getType, column -> column.getMetadata().isNullable())
                .containsExactly(
                        Assertions.tuple("type", VARCHAR, true),
                        Assertions.tuple("size", BIGINT, true),
                        Assertions.tuple("name", VARCHAR, true),
                        Assertions.tuple("content", VARCHAR, true),
                        Assertions.tuple("content_2", RowType.from(List.of(
                                RowType.field("name", VARCHAR),
                                RowType.field("path", VARCHAR),
                                RowType.field("sha", VARCHAR),
                                RowType.field("size", BIGINT),
                                RowType.field("url", VARCHAR),
                                RowType.field("html_url", VARCHAR),
                                RowType.field("git_url", VARCHAR),
                                RowType.field("download_url", VARCHAR),
                                RowType.field("type", VARCHAR),
                                RowType.field("_links", RowType.from(List.of(
                                        RowType.field("self", VARCHAR),
                                        RowType.field("git", VARCHAR),
                                        RowType.field("html", VARCHAR)))))), true),
                        Assertions.tuple("url", VARCHAR, true),
                        Assertions.tuple("git_url", VARCHAR, true),
                        Assertions.tuple("html_url", VARCHAR, true),
                        Assertions.tuple("download_url", VARCHAR, true),
                        Assertions.tuple("_links", linksType, true),
                        Assertions.tuple("encoding", VARCHAR, true),
                        Assertions.tuple("target", VARCHAR, true),
                        Assertions.tuple("submodule_git_url", VARCHAR, true));

        // GET /user returns either a private or a public user, so all of their fields are nullable
        Assertions.assertThat(tables.get("user"))
                .filteredOn(column -> column.getOptionalPredicate().isEmpty())
                .extracting(OpenApiColumn::getName, OpenApiColumn::getType, column -> column.getMetadata().isNullable())
                .contains(
                        Assertions.tuple("login", VARCHAR, true),
                        Assertions.tuple("id", BIGINT, true),
                        Assertions.tuple("public_repos", BIGINT, true),
                        Assertions.tuple("private_gists", BIGINT, true),
                        Assertions.tuple("two_factor_authentication", BOOLEAN, true),
                        Assertions.tuple("plan", RowType.from(List.of(
                                RowType.field("collaborators", BIGINT),
                                RowType.field("name", VARCHAR),
                                RowType.field("space", BIGINT),
                                RowType.field("private_repos", BIGINT))), true),
                        Assertions.tuple("ldap_dn", VARCHAR, true));

        // timeline events are a oneOf of many event types, which define actors with different fields, so they're merged into a single row
        Assertions.assertThat(tables.get("repos_issues_timeline"))
                .filteredOn(column -> column.getName().equals("actor"))
                .singleElement()
                .extracting(OpenApiColumn::getType)
                .isInstanceOf(RowType.class);
    }

    @Test
    public void getGithubExtensionTables()
    {
        OpenApiSpec spec = loadSpecFile("specs/github.json");
        Map<String, List<OpenApiColumn>> tables = spec.getTables();

        Set<String> expected = Set.of(
                "orgs",
                "repos",
                "repos_actions_workflows");
        Assertions.assertThat(tables.keySet()).containsAll(expected);

        OpenApiTableHandle orgsTableHandle = spec.getTableHandle(schemaTableName(SCHEMA_NAME, "orgs"));
        String paramsPath = "/orgs/{org}";
        Assertions.assertThat(orgsTableHandle.getSelectPaths()).containsExactly(new HttpPath(PathItem.HttpMethod.GET, "/orgs/{org}"));
        Assertions.assertThat(orgsTableHandle.getInsertPaths()).isEmpty();
        Assertions.assertThat(orgsTableHandle.getUpdatePaths()).isEmpty();
        Assertions.assertThat(orgsTableHandle.getDeletePaths()).containsExactly(new HttpPath(PathItem.HttpMethod.DELETE, "/orgs/{org}"));
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
                                .setOptionalPredicate(Map.of(new HttpPath(PathItem.HttpMethod.PATCH, paramsPath), ParameterLocation.BODY))
                                .setIsNullable(true)
                                .build(),
                        OpenApiColumn.builder()
                                .setName("name").setSourceName("name")
                                .setType(VARCHAR).setSourceType(stringSchema)
                                .setOptionalPredicate(Map.of(new HttpPath(PathItem.HttpMethod.PATCH, paramsPath), ParameterLocation.BODY))
                                .setIsNullable(true)
                                .build(),
                        OpenApiColumn.builder()
                                .setName("company").setSourceName("company")
                                .setType(VARCHAR).setSourceType(stringSchema)
                                .setOptionalPredicate(Map.of(new HttpPath(PathItem.HttpMethod.PATCH, paramsPath), ParameterLocation.BODY))
                                .setIsNullable(true)
                                .build(),
                        OpenApiColumn.builder()
                                .setName("blog").setSourceName("blog")
                                .setType(VARCHAR).setSourceType(stringSchema)
                                .setOptionalPredicate(Map.of(new HttpPath(PathItem.HttpMethod.PATCH, paramsPath), ParameterLocation.BODY))
                                .setIsNullable(true)
                                .build(),
                        OpenApiColumn.builder()
                                .setName("location").setSourceName("location")
                                .setType(VARCHAR).setSourceType(stringSchema)
                                .setOptionalPredicate(Map.of(new HttpPath(PathItem.HttpMethod.PATCH, paramsPath), ParameterLocation.BODY))
                                .setIsNullable(true)
                                .build(),
                        OpenApiColumn.builder()
                                .setName("email").setSourceName("email")
                                .setType(VARCHAR).setSourceType(stringSchema)
                                .setOptionalPredicate(Map.of(new HttpPath(PathItem.HttpMethod.PATCH, paramsPath), ParameterLocation.BODY))
                                .setIsNullable(true)
                                .build(),
                        OpenApiColumn.builder()
                                .setName("twitter_username").setSourceName("twitter_username")
                                .setType(VARCHAR).setSourceType(stringSchema)
                                .setOptionalPredicate(Map.of(new HttpPath(PathItem.HttpMethod.PATCH, paramsPath), ParameterLocation.BODY))
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
                                .setOptionalPredicate(Map.of(new HttpPath(PathItem.HttpMethod.PATCH, paramsPath), ParameterLocation.BODY))
                                .setIsNullable(true)
                                .build(),
                        OpenApiColumn.builder()
                                .setName("has_repository_projects").setSourceName("has_repository_projects")
                                .setType(BOOLEAN).setSourceType(booleanSchema)
                                .setOptionalPredicate(Map.of(new HttpPath(PathItem.HttpMethod.PATCH, paramsPath), ParameterLocation.BODY))
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
                                .setOptionalPredicate(Map.of(new HttpPath(PathItem.HttpMethod.PATCH, paramsPath), ParameterLocation.BODY))
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
                                .setOptionalPredicate(Map.of(new HttpPath(PathItem.HttpMethod.PATCH, paramsPath), ParameterLocation.BODY))
                                .setIsNullable(true)
                                .build(),
                        OpenApiColumn.builder()
                                .setName("members_can_create_repositories").setSourceName("members_can_create_repositories")
                                .setType(BOOLEAN).setSourceType(booleanSchema)
                                .setOptionalPredicate(Map.of(new HttpPath(PathItem.HttpMethod.PATCH, paramsPath), ParameterLocation.BODY))
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
                                .setOptionalPredicate(Map.of(new HttpPath(PathItem.HttpMethod.PATCH, paramsPath), ParameterLocation.BODY))
                                .setIsNullable(true)
                                .build(),
                        OpenApiColumn.builder()
                                .setName("members_can_create_public_repositories").setSourceName("members_can_create_public_repositories")
                                .setType(BOOLEAN).setSourceType(booleanSchema)
                                .setOptionalPredicate(Map.of(new HttpPath(PathItem.HttpMethod.PATCH, paramsPath), ParameterLocation.BODY))
                                .setIsNullable(true)
                                .build(),
                        OpenApiColumn.builder()
                                .setName("members_can_create_private_repositories").setSourceName("members_can_create_private_repositories")
                                .setType(BOOLEAN).setSourceType(booleanSchema)
                                .setOptionalPredicate(Map.of(new HttpPath(PathItem.HttpMethod.PATCH, paramsPath), ParameterLocation.BODY))
                                .setIsNullable(true)
                                .build(),
                        OpenApiColumn.builder()
                                .setName("members_can_create_internal_repositories").setSourceName("members_can_create_internal_repositories")
                                .setType(BOOLEAN).setSourceType(booleanSchema)
                                .setOptionalPredicate(Map.of(new HttpPath(PathItem.HttpMethod.PATCH, paramsPath), ParameterLocation.BODY))
                                .setIsNullable(true)
                                .build(),
                        OpenApiColumn.builder()
                                .setName("members_can_create_pages").setSourceName("members_can_create_pages")
                                .setType(BOOLEAN).setSourceType(booleanSchema)
                                .setOptionalPredicate(Map.of(new HttpPath(PathItem.HttpMethod.PATCH, paramsPath), ParameterLocation.BODY))
                                .setIsNullable(true)
                                .build(),
                        OpenApiColumn.builder()
                                .setName("members_can_create_public_pages").setSourceName("members_can_create_public_pages")
                                .setType(BOOLEAN).setSourceType(booleanSchema)
                                .setOptionalPredicate(Map.of(new HttpPath(PathItem.HttpMethod.PATCH, paramsPath), ParameterLocation.BODY))
                                .setIsNullable(true)
                                .build(),
                        OpenApiColumn.builder()
                                .setName("members_can_create_private_pages").setSourceName("members_can_create_private_pages")
                                .setType(BOOLEAN).setSourceType(booleanSchema)
                                .setOptionalPredicate(Map.of(new HttpPath(PathItem.HttpMethod.PATCH, paramsPath), ParameterLocation.BODY))
                                .setIsNullable(true)
                                .build(),
                        OpenApiColumn.builder()
                                .setName("members_can_fork_private_repositories").setSourceName("members_can_fork_private_repositories")
                                .setType(BOOLEAN).setSourceType(booleanSchema)
                                .setOptionalPredicate(Map.of(new HttpPath(PathItem.HttpMethod.PATCH, paramsPath), ParameterLocation.BODY))
                                .setIsNullable(true)
                                .build(),
                        OpenApiColumn.builder()
                                .setName("web_commit_signoff_required").setSourceName("web_commit_signoff_required")
                                .setType(BOOLEAN).setSourceType(booleanSchema)
                                .setOptionalPredicate(Map.of(new HttpPath(PathItem.HttpMethod.PATCH, paramsPath), ParameterLocation.BODY))
                                .setIsNullable(true)
                                .build(),
                        OpenApiColumn.builder()
                                .setName("advanced_security_enabled_for_new_repositories").setSourceName("advanced_security_enabled_for_new_repositories")
                                .setType(BOOLEAN).setSourceType(booleanSchema)
                                .setOptionalPredicate(Map.of(new HttpPath(PathItem.HttpMethod.PATCH, paramsPath), ParameterLocation.BODY))
                                .setIsNullable(true)
                                .setComment("""
                                            Whether GitHub Advanced Security is enabled for new repositories and repositories transferred to this organization.
                                            
                                            This field is only visible to organization owners or members of a team with the security manager role.""")
                                .build(),
                        OpenApiColumn.builder()
                                .setName("dependabot_alerts_enabled_for_new_repositories").setSourceName("dependabot_alerts_enabled_for_new_repositories")
                                .setType(BOOLEAN).setSourceType(booleanSchema)
                                .setOptionalPredicate(Map.of(new HttpPath(PathItem.HttpMethod.PATCH, paramsPath), ParameterLocation.BODY))
                                .setIsNullable(true)
                                .setComment("""
                                            Whether GitHub Advanced Security is automatically enabled for new repositories and repositories transferred to
                                            this organization.
                                            
                                            This field is only visible to organization owners or members of a team with the security manager role.""")
                                .build(),
                        OpenApiColumn.builder()
                                .setName("dependabot_security_updates_enabled_for_new_repositories").setSourceName("dependabot_security_updates_enabled_for_new_repositories")
                                .setType(BOOLEAN).setSourceType(booleanSchema)
                                .setOptionalPredicate(Map.of(new HttpPath(PathItem.HttpMethod.PATCH, paramsPath), ParameterLocation.BODY))
                                .setIsNullable(true)
                                .setComment("""
                                            Whether dependabot security updates are automatically enabled for new repositories and repositories transferred
                                            to this organization.
                                            
                                            This field is only visible to organization owners or members of a team with the security manager role.""")
                                .build(),
                        OpenApiColumn.builder()
                                .setName("dependency_graph_enabled_for_new_repositories").setSourceName("dependency_graph_enabled_for_new_repositories")
                                .setType(BOOLEAN).setSourceType(booleanSchema)
                                .setOptionalPredicate(Map.of(new HttpPath(PathItem.HttpMethod.PATCH, paramsPath), ParameterLocation.BODY))
                                .setIsNullable(true)
                                .setComment("""
                                            Whether dependency graph is automatically enabled for new repositories and repositories transferred to this
                                            organization.
                                            
                                            This field is only visible to organization owners or members of a team with the security manager role.""")
                                .build(),
                        OpenApiColumn.builder()
                                .setName("secret_scanning_enabled_for_new_repositories").setSourceName("secret_scanning_enabled_for_new_repositories")
                                .setType(BOOLEAN).setSourceType(booleanSchema)
                                .setOptionalPredicate(Map.of(new HttpPath(PathItem.HttpMethod.PATCH, paramsPath), ParameterLocation.BODY))
                                .setIsNullable(true)
                                .setComment("""
                                            Whether secret scanning is automatically enabled for new repositories and repositories transferred to this
                                            organization.
                                            
                                            This field is only visible to organization owners or members of a team with the security manager role.""")
                                .build(),
                        OpenApiColumn.builder()
                                .setName("secret_scanning_push_protection_enabled_for_new_repositories").setSourceName("secret_scanning_push_protection_enabled_for_new_repositories")
                                .setType(BOOLEAN).setSourceType(booleanSchema)
                                .setOptionalPredicate(Map.of(new HttpPath(PathItem.HttpMethod.PATCH, paramsPath), ParameterLocation.BODY))
                                .setIsNullable(true)
                                .setComment("""
                                            Whether secret scanning push protection is automatically enabled for new repositories and repositories
                                            transferred to this organization.
                                            
                                            This field is only visible to organization owners or members of a team with the security manager role.""")
                                .build(),
                        OpenApiColumn.builder()
                                .setName("secret_scanning_push_protection_custom_link_enabled").setSourceName("secret_scanning_push_protection_custom_link_enabled")
                                .setType(BOOLEAN).setSourceType(booleanSchema)
                                .setOptionalPredicate(Map.of(new HttpPath(PathItem.HttpMethod.PATCH, paramsPath), ParameterLocation.BODY))
                                .setIsNullable(true)
                                .setComment("Whether a custom link is shown to contributors who are blocked from pushing a secret by push protection.")
                                .build(),
                        OpenApiColumn.builder()
                                .setName("secret_scanning_push_protection_custom_link").setSourceName("secret_scanning_push_protection_custom_link")
                                .setType(VARCHAR).setSourceType(stringSchema)
                                .setOptionalPredicate(Map.of(new HttpPath(PathItem.HttpMethod.PATCH, paramsPath), ParameterLocation.BODY))
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
                                        new HttpPath(PathItem.HttpMethod.GET, paramsPath), ParameterLocation.PATH,
                                        new HttpPath(PathItem.HttpMethod.PATCH, paramsPath), ParameterLocation.PATH,
                                        new HttpPath(PathItem.HttpMethod.DELETE, paramsPath), ParameterLocation.PATH))
                                .setIsNullable(true)
                                .build());

        OpenApiTableHandle workflowTableHandle = spec.getTableHandle(schemaTableName(SCHEMA_NAME, "repos_actions_workflows"));
        String listPath = "/repos/{owner}/{repo}/actions/workflows";
        String onePath = "/repos/{owner}/{repo}/actions/workflows/{workflow_id}";
        Assertions.assertThat(workflowTableHandle.getSelectPaths()).containsExactly(new HttpPath(PathItem.HttpMethod.GET, listPath), new HttpPath(PathItem.HttpMethod.GET, onePath));
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
                                .setType(BIGINT).setSourceType(intSchema)
                                .build(),
                        OpenApiColumn.builder()
                                .setName("node_id").setSourceName("node_id")
                                .setType(VARCHAR).setSourceType(stringSchema)
                                .build(),
                        OpenApiColumn.builder()
                                .setName("name").setSourceName("name")
                                .setType(VARCHAR).setSourceType(stringSchema)
                                .build(),
                        OpenApiColumn.builder()
                                .setName("path").setSourceName("path")
                                .setType(VARCHAR).setSourceType(stringSchema)
                                .build(),
                        OpenApiColumn.builder()
                                .setName("state").setSourceName("state")
                                .setType(VARCHAR).setSourceType(stringSchema)
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
                                .setName("url").setSourceName("url")
                                .setType(VARCHAR).setSourceType(stringSchema)
                                .build(),
                        OpenApiColumn.builder()
                                .setName("html_url").setSourceName("html_url")
                                .setType(VARCHAR).setSourceType(stringSchema)
                                .build(),
                        OpenApiColumn.builder()
                                .setName("badge_url").setSourceName("badge_url")
                                .setType(VARCHAR).setSourceType(stringSchema)
                                .build(),
                        OpenApiColumn.builder()
                                .setName("deleted_at").setSourceName("deleted_at")
                                .setType(TIMESTAMP_MILLIS).setSourceType(stringSchema)
                                .setIsNullable(true)
                                .build(),
                        OpenApiColumn.builder()
                                .setName("owner").setSourceName("owner")
                                .setType(VARCHAR).setSourceType(stringSchema)
                                .setRequiresPredicate(Map.of(
                                        new HttpPath(PathItem.HttpMethod.GET, listPath), ParameterLocation.PATH,
                                        new HttpPath(PathItem.HttpMethod.GET, onePath), ParameterLocation.PATH))
                                .setIsNullable(true)
                                .build(),
                        OpenApiColumn.builder()
                                .setName("repo").setSourceName("repo")
                                .setType(VARCHAR).setSourceType(stringSchema)
                                .setRequiresPredicate(Map.of(
                                        new HttpPath(PathItem.HttpMethod.GET, listPath), ParameterLocation.PATH,
                                        new HttpPath(PathItem.HttpMethod.GET, onePath), ParameterLocation.PATH))
                                .setIsNullable(true)
                                .build(),
                        OpenApiColumn.builder()
                                .setName("per_page").setSourceName("per_page")
                                .setType(BIGINT).setSourceType(intSchema)
                                .setOptionalPredicate(Map.of(new HttpPath(PathItem.HttpMethod.GET, listPath), ParameterLocation.QUERY))
                                .setIsNullable(true)
                                .setIsHidden(true)
                                .build(),
                        OpenApiColumn.builder()
                                .setName("page").setSourceName("page")
                                .setType(BIGINT).setSourceType(intSchema)
                                .setOptionalPredicate(Map.of(new HttpPath(PathItem.HttpMethod.GET, listPath), ParameterLocation.QUERY))
                                .setIsNullable(true)
                                .setIsHidden(true)
                                .setIsPageNumber(true)
                                .build(),
                        OpenApiColumn.builder()
                                .setName("workflow_id").setSourceName("workflow_id")
                                .setType(VARCHAR).setSourceType(nullSchema)
                                .setRequiresPredicate(Map.of(new HttpPath(PathItem.HttpMethod.GET, onePath), ParameterLocation.PATH))
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
        String path = "/rest/api/3/search";
        Assertions.assertThat(tableHandle.getSelectPaths()).containsExactly(new HttpPath(PathItem.HttpMethod.GET, path));
        Assertions.assertThat(tableHandle.getInsertPaths()).containsExactly(new HttpPath(PathItem.HttpMethod.POST, path));
        Assertions.assertThat(tableHandle.getUpdatePaths()).containsExactly(new HttpPath(PathItem.HttpMethod.POST, path));
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
        // these fields are objects wrapped in allOf, which the parser merges into object schemas
        String linkType = "row(href varchar, \"iconClass\" varchar, id varchar, label varchar, \"styleClass\" varchar, title varchar, weight integer)";
        String userInterfaceType = "row(\"avatarUrl\" varchar, \"displayName\" varchar, \"displayNameKey\" varchar, id varchar, type varchar, url varchar)";
        Type changelogType = TESTING_TYPE_MANAGER.fromSqlType("row(histories array(row(" +
                "author row(\"accountId\" varchar, \"accountType\" varchar, active boolean, " +
                "\"avatarUrls\" row(\"16x16\" varchar, \"24x24\" varchar, \"32x32\" varchar, \"48x48\" varchar), " +
                "\"displayName\" varchar, \"emailAddress\" varchar, key varchar, name varchar, self varchar, \"timeZone\" varchar), " +
                "created timestamp(3), " +
                "\"historyMetadata\" row(\"activityDescription\" varchar, \"activityDescriptionKey\" varchar, actor " + userInterfaceType + ", cause " + userInterfaceType + ", " +
                "description varchar, \"descriptionKey\" varchar, \"emailDescription\" varchar, \"emailDescriptionKey\" varchar, \"extraData\" map(varchar, varchar), " +
                "generator " + userInterfaceType + ", type varchar), " +
                "id varchar, " +
                "items array(row(field varchar, \"fieldId\" varchar, fieldtype varchar, \"from\" varchar, \"fromString\" varchar, \"to\" varchar, \"toString\" varchar)))), " +
                "\"maxResults\" integer, \"startAt\" integer, total integer)");
        Type editmetaType = TESTING_TYPE_MANAGER.fromSqlType("row(fields map(varchar, row(" +
                "\"allowedValues\" array(varchar), \"autoCompleteUrl\" varchar, configuration map(varchar, varchar), \"defaultValue\" varchar, \"hasDefaultValue\" boolean, " +
                "key varchar, name varchar, operations array(varchar), required boolean, " +
                "schema row(configuration map(varchar, varchar), custom varchar, \"customId\" bigint, items varchar, system varchar, type varchar))))");
        Type operationsType = TESTING_TYPE_MANAGER.fromSqlType("row(\"linkGroups\" array(row(" +
                "groups array(varchar), header " + linkType + ", id varchar, links array(" + linkType + "), \"styleClass\" varchar, weight integer)))");
        ArrayType issuesType = new ArrayType(RowType.from(List.of(
                RowType.field("changelog", changelogType),
                RowType.field("editmeta", editmetaType),
                RowType.field("expand", VARCHAR),
                RowType.field("fields", new MapType(VARCHAR, VARCHAR, new TypeOperators())),
                RowType.field("fieldsToInclude", RowType.from(List.of(
                        RowType.field("actuallyIncluded", new ArrayType(VARCHAR)),
                        RowType.field("excluded", new ArrayType(VARCHAR)),
                        RowType.field("included", new ArrayType(VARCHAR))))),
                RowType.field("id", VARCHAR),
                RowType.field("key", VARCHAR),
                RowType.field("names", new MapType(VARCHAR, VARCHAR, new TypeOperators())),
                RowType.field("operations", operationsType),
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
                                .setOptionalPredicate(Map.of(new HttpPath(PathItem.HttpMethod.GET, path), ParameterLocation.QUERY))
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
                                .setOptionalPredicate(Map.of(
                                        new HttpPath(PathItem.HttpMethod.POST, path), ParameterLocation.BODY,
                                        new HttpPath(PathItem.HttpMethod.GET, path), ParameterLocation.QUERY))
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
                                .setOptionalPredicate(Map.of(
                                        new HttpPath(PathItem.HttpMethod.POST, path), ParameterLocation.BODY,
                                        new HttpPath(PathItem.HttpMethod.GET, path), ParameterLocation.QUERY))
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
                                .setOptionalPredicate(Map.of(new HttpPath(PathItem.HttpMethod.POST, path), ParameterLocation.BODY, new HttpPath(PathItem.HttpMethod.GET, path), ParameterLocation.QUERY))
                                .setIsNullable(true)
                                .build(),
                        OpenApiColumn.builder()
                                .setName("validate_query").setSourceName("validateQuery")
                                .setType(VARCHAR).setSourceType(stringSchema)
                                .setOptionalPredicate(Map.of(new HttpPath(PathItem.HttpMethod.POST, path), ParameterLocation.BODY, new HttpPath(PathItem.HttpMethod.GET, path), ParameterLocation.QUERY))
                                .setIsNullable(true)
                                .build(),
                        OpenApiColumn.builder()
                                .setName("fields").setSourceName("fields")
                                .setType(new ArrayType(VARCHAR)).setSourceType(arraySchema)
                                .setOptionalPredicate(Map.of(new HttpPath(PathItem.HttpMethod.POST, path), ParameterLocation.BODY, new HttpPath(PathItem.HttpMethod.GET, path), ParameterLocation.QUERY))
                                .setIsNullable(true)
                                .build(),
                        OpenApiColumn.builder()
                                .setName("properties").setSourceName("properties")
                                .setType(new ArrayType(VARCHAR)).setSourceType(arraySchema)
                                .setOptionalPredicate(Map.of(new HttpPath(PathItem.HttpMethod.POST, path), ParameterLocation.BODY, new HttpPath(PathItem.HttpMethod.GET, path), ParameterLocation.QUERY))
                                .setIsNullable(true)
                                .build(),
                        OpenApiColumn.builder()
                                .setName("fields_by_keys").setSourceName("fieldsByKeys")
                                .setType(BOOLEAN).setSourceType(booleanSchema)
                                .setOptionalPredicate(Map.of(new HttpPath(PathItem.HttpMethod.POST, path), ParameterLocation.BODY, new HttpPath(PathItem.HttpMethod.GET, path), ParameterLocation.QUERY))
                                .setIsNullable(true)
                                .build(),
                        OpenApiColumn.builder()
                                .setName("expand_req").setSourceName("expand")
                                .setType(new ArrayType(VARCHAR)).setSourceType(arraySchema)
                                .setOptionalPredicate(Map.of(new HttpPath(PathItem.HttpMethod.POST, path), ParameterLocation.BODY))
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
        String listPath = "/pet";
        String onePath = "/pet/{petId}";
        Assertions.assertThat(tableHandle.getSelectPaths()).containsExactly(new HttpPath(PathItem.HttpMethod.GET, onePath));
        Assertions.assertThat(tableHandle.getInsertPaths()).containsExactly(new HttpPath(PathItem.HttpMethod.POST, listPath), new HttpPath(PathItem.HttpMethod.POST, onePath));
        Assertions.assertThat(tableHandle.getUpdatePaths()).containsExactly(new HttpPath(PathItem.HttpMethod.POST, listPath), new HttpPath(PathItem.HttpMethod.POST, onePath));
        Assertions.assertThat(tableHandle.getDeletePaths()).containsExactly(new HttpPath(PathItem.HttpMethod.DELETE, onePath));
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
                                        new HttpPath(PathItem.HttpMethod.POST, listPath), ParameterLocation.BODY,
                                        new HttpPath(PathItem.HttpMethod.PUT, listPath), ParameterLocation.BODY))
                                .setIsNullable(true)
                                .build(),
                        OpenApiColumn.builder()
                                .setName("name").setSourceName("name")
                                .setType(VARCHAR).setSourceType(stringSchema)
                                .setRequiresPredicate(Map.of(
                                        new HttpPath(PathItem.HttpMethod.POST, listPath), ParameterLocation.BODY,
                                        new HttpPath(PathItem.HttpMethod.PUT, listPath), ParameterLocation.BODY))
                                .setOptionalPredicate(Map.of(new HttpPath(PathItem.HttpMethod.POST, onePath), ParameterLocation.QUERY))
                                .setIsNullable(true)
                                .build(),
                        OpenApiColumn.builder()
                                .setName("category").setSourceName("category")
                                .setType(categoryType).setSourceType(objectSchema)
                                .setOptionalPredicate(Map.of(
                                        new HttpPath(PathItem.HttpMethod.POST, listPath), ParameterLocation.BODY,
                                        new HttpPath(PathItem.HttpMethod.PUT, listPath), ParameterLocation.BODY))
                                .setIsNullable(true)
                                .build(),
                        OpenApiColumn.builder()
                                .setName("photo_urls").setSourceName("photoUrls")
                                .setType(photosType).setSourceType(arraySchema)
                                .setRequiresPredicate(Map.of(
                                        new HttpPath(PathItem.HttpMethod.POST, listPath), ParameterLocation.BODY,
                                        new HttpPath(PathItem.HttpMethod.PUT, listPath), ParameterLocation.BODY))
                                .build(),
                        OpenApiColumn.builder()
                                .setName("tags").setSourceName("tags")
                                .setType(tagsType).setSourceType(arraySchema)
                                .setOptionalPredicate(Map.of(
                                        new HttpPath(PathItem.HttpMethod.POST, listPath), ParameterLocation.BODY,
                                        new HttpPath(PathItem.HttpMethod.PUT, listPath), ParameterLocation.BODY))
                                .setIsNullable(true)
                                .build(),
                        OpenApiColumn.builder()
                                .setName("status").setSourceName("status")
                                .setType(VARCHAR).setSourceType(stringSchema)
                                .setOptionalPredicate(Map.of(
                                        new HttpPath(PathItem.HttpMethod.POST, listPath), ParameterLocation.BODY,
                                        new HttpPath(PathItem.HttpMethod.POST, onePath), ParameterLocation.QUERY,
                                        new HttpPath(PathItem.HttpMethod.PUT, listPath), ParameterLocation.BODY))
                                .setIsNullable(true)
                                .setComment("pet status in the store")
                                .build(),
                        OpenApiColumn.builder()
                                .setName("pet_id").setSourceName("petId")
                                .setType(BIGINT).setSourceType(intSchema)
                                .setRequiresPredicate(Map.of(
                                        new HttpPath(PathItem.HttpMethod.POST, onePath), ParameterLocation.PATH,
                                        new HttpPath(PathItem.HttpMethod.GET, onePath), ParameterLocation.PATH,
                                        new HttpPath(PathItem.HttpMethod.DELETE, onePath), ParameterLocation.PATH))
                                .setIsNullable(true)
                                .build(),
                        OpenApiColumn.builder()
                                .setName("api_key").setSourceName("api_key")
                                .setType(VARCHAR).setSourceType(stringSchema)
                                .setOptionalPredicate(Map.of(new HttpPath(PathItem.HttpMethod.DELETE, onePath), ParameterLocation.HEADER))
                                .setIsNullable(true)
                                .build());
    }

    @Test
    public void getDatadogUnionTables()
    {
        OpenApiSpec spec = loadSpec("datadog.yaml");

        // rules are a oneOf of standard and signal rules, which define queries as arrays of different objects;
        // request bodies don't include the default rule ID, so they're a different type
        RowType queryReqType = RowType.from(List.of(
                RowType.field("aggregation", VARCHAR),
                RowType.field("distinctFields", new ArrayType(VARCHAR)),
                RowType.field("groupByFields", new ArrayType(VARCHAR)),
                RowType.field("hasOptionalGroupByFields", BOOLEAN),
                RowType.field("metric", VARCHAR),
                RowType.field("metrics", new ArrayType(VARCHAR)),
                RowType.field("name", VARCHAR),
                RowType.field("query", VARCHAR),
                RowType.field("correlatedByFields", new ArrayType(VARCHAR)),
                RowType.field("correlatedQueryIndex", INTEGER),
                RowType.field("ruleId", VARCHAR)));
        RowType queryType = RowType.from(List.of(
                RowType.field("aggregation", VARCHAR),
                RowType.field("distinctFields", new ArrayType(VARCHAR)),
                RowType.field("groupByFields", new ArrayType(VARCHAR)),
                RowType.field("hasOptionalGroupByFields", BOOLEAN),
                RowType.field("metric", VARCHAR),
                RowType.field("metrics", new ArrayType(VARCHAR)),
                RowType.field("name", VARCHAR),
                RowType.field("query", VARCHAR),
                RowType.field("correlatedByFields", new ArrayType(VARCHAR)),
                RowType.field("correlatedQueryIndex", INTEGER),
                RowType.field("defaultRuleId", VARCHAR),
                RowType.field("ruleId", VARCHAR)));
        Assertions.assertThat(spec.getTables().get("api_v2_security_monitoring_rules"))
                .filteredOn(column -> !column.getMetadata().isHidden())
                .extracting(OpenApiColumn::getName, column -> column.getMetadata().isNullable())
                .containsExactly(
                        Assertions.tuple("data", true),
                        Assertions.tuple("meta", true),
                        Assertions.tuple("page[size]", true),
                        Assertions.tuple("page[number]", true),
                        Assertions.tuple("cases", true),
                        Assertions.tuple("compliance_signal_options", true),
                        Assertions.tuple("created_at", true),
                        Assertions.tuple("creation_author_id", true),
                        Assertions.tuple("deprecation_date", true),
                        Assertions.tuple("filters", true),
                        Assertions.tuple("has_extended_title", true),
                        Assertions.tuple("id", true),
                        Assertions.tuple("is_default", true),
                        Assertions.tuple("is_deleted", true),
                        Assertions.tuple("is_enabled", true),
                        Assertions.tuple("message", true),
                        Assertions.tuple("name", true),
                        Assertions.tuple("options", true),
                        Assertions.tuple("queries", true),
                        Assertions.tuple("tags", true),
                        Assertions.tuple("type", true),
                        Assertions.tuple("update_author_id", true),
                        Assertions.tuple("version", true),
                        Assertions.tuple("rule_id", true),
                        Assertions.tuple("queries_req", true),
                        Assertions.tuple("version_req", true));
        Assertions.assertThat(spec.getTables().get("api_v2_security_monitoring_rules"))
                .filteredOn(column -> column.getName().startsWith("queries") || column.getName().startsWith("version"))
                .extracting(OpenApiColumn::getName, OpenApiColumn::getType)
                .containsExactly(
                        Assertions.tuple("queries", new ArrayType(queryType)),
                        Assertions.tuple("version", BIGINT),
                        Assertions.tuple("queries_req", new ArrayType(queryReqType)),
                        Assertions.tuple("version_req", INTEGER));
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
        String path = "/api/v2/query/timeseries";
        Assertions.assertThat(tableHandle.getSelectPaths()).containsExactly(new HttpPath(PathItem.HttpMethod.POST, path));
        Assertions.assertThat(tableHandle.getInsertPaths()).containsExactly(new HttpPath(PathItem.HttpMethod.POST, path));
        Assertions.assertThat(tableHandle.getUpdatePaths()).containsExactly(new HttpPath(PathItem.HttpMethod.POST, path));
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
                        // queries are a oneOf of metrics and events queries, which are merged into a single row
                        RowType.field("queries", new ArrayType(RowType.from(List.of(
                                RowType.field("data_source", VARCHAR),
                                RowType.field("name", VARCHAR),
                                RowType.field("query", VARCHAR),
                                RowType.field("compute", RowType.from(List.of(
                                        RowType.field("aggregation", VARCHAR),
                                        RowType.field("interval", BIGINT),
                                        RowType.field("metric", VARCHAR)))),
                                RowType.field("group_by", new ArrayType(RowType.from(List.of(
                                        RowType.field("facet", VARCHAR),
                                        RowType.field("limit", INTEGER),
                                        RowType.field("sort", RowType.from(List.of(
                                                RowType.field("aggregation", VARCHAR),
                                                RowType.field("metric", VARCHAR),
                                                RowType.field("order", VARCHAR),
                                                RowType.field("type", VARCHAR)))))))),
                                RowType.field("indexes", new ArrayType(VARCHAR)),
                                RowType.field("search", RowType.from(List.of(
                                        RowType.field("query", VARCHAR)))))))),
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
                                .setRequiresPredicate(Map.of(new HttpPath(PathItem.HttpMethod.POST, path), ParameterLocation.BODY))
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
        String listPath = "/zones";
        String onePath = "/zones/{identifier}";
        Assertions.assertThat(tableHandle.getSelectPaths()).containsExactly(new HttpPath(PathItem.HttpMethod.GET, listPath), new HttpPath(PathItem.HttpMethod.GET, onePath));
        Assertions.assertThat(tableHandle.getInsertPaths()).containsExactly(new HttpPath(PathItem.HttpMethod.POST, listPath));
        Assertions.assertThat(tableHandle.getUpdatePaths()).containsExactly(new HttpPath(PathItem.HttpMethod.POST, listPath));
        Assertions.assertThat(tableHandle.getDeletePaths()).containsExactly(new HttpPath(PathItem.HttpMethod.DELETE, onePath));
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
                                .setIsNullable(true)
                                .build(),
                        OpenApiColumn.builder()
                                .setName("messages").setSourceName("messages")
                                .setType(new ArrayType(RowType.from(List.of(
                                        RowType.field("code", BIGINT),
                                        RowType.field("message", VARCHAR))))).setSourceType(arraySchema)
                                .setIsNullable(true)
                                .build(),
                        OpenApiColumn.builder()
                                .setName("success").setSourceName("success")
                                .setType(BOOLEAN).setSourceType(booleanSchema)
                                .setComment("Whether the API call was successful")
                                .setIsNullable(true)
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
                                .setRequiresPredicate(Map.of(new HttpPath(PathItem.HttpMethod.POST, listPath), ParameterLocation.BODY))
                                .setOptionalPredicate(Map.of(new HttpPath(PathItem.HttpMethod.GET, listPath), ParameterLocation.QUERY))
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
                                .setOptionalPredicate(Map.of(new HttpPath(PathItem.HttpMethod.GET, listPath), ParameterLocation.QUERY))
                                .setIsNullable(true)
                                .setComment("A zone status")
                                .build(),
                        OpenApiColumn.builder()
                                .setName("account.id").setSourceName("account.id")
                                .setType(VARCHAR).setSourceType(stringSchema)
                                .setOptionalPredicate(Map.of(new HttpPath(PathItem.HttpMethod.GET, listPath), ParameterLocation.QUERY))
                                .setIsNullable(true)
                                .setComment("An account ID")
                                .build(),
                        OpenApiColumn.builder()
                                .setName("account.name").setSourceName("account.name")
                                .setType(VARCHAR).setSourceType(stringSchema)
                                .setOptionalPredicate(Map.of(new HttpPath(PathItem.HttpMethod.GET, listPath), ParameterLocation.QUERY))
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
                                .setOptionalPredicate(Map.of(new HttpPath(PathItem.HttpMethod.GET, listPath), ParameterLocation.QUERY))
                                .setIsNullable(true)
                                .setComment("Page number of paginated results.")
                                .build(),
                        OpenApiColumn.builder()
                                .setName("per_page").setSourceName("per_page")
                                .setType(createDecimalType(18, 8)).setSourceType(numberSchema)
                                .setOptionalPredicate(Map.of(new HttpPath(PathItem.HttpMethod.GET, listPath), ParameterLocation.QUERY))
                                .setIsNullable(true)
                                .setComment("Number of zones per page.")
                                .build(),
                        OpenApiColumn.builder()
                                .setName("order").setSourceName("order")
                                .setType(VARCHAR).setSourceType(stringSchema)
                                .setOptionalPredicate(Map.of(new HttpPath(PathItem.HttpMethod.GET, listPath), ParameterLocation.QUERY))
                                .setIsNullable(true)
                                .setComment("Field to order zones by.")
                                .build(),
                        OpenApiColumn.builder()
                                .setName("direction").setSourceName("direction")
                                .setType(VARCHAR).setSourceType(stringSchema)
                                .setOptionalPredicate(Map.of(new HttpPath(PathItem.HttpMethod.GET, listPath), ParameterLocation.QUERY))
                                .setIsNullable(true)
                                .setComment("Direction to order zones.")
                                .build(),
                        OpenApiColumn.builder()
                                .setName("match").setSourceName("match")
                                .setType(VARCHAR).setSourceType(stringSchema)
                                .setOptionalPredicate(Map.of(new HttpPath(PathItem.HttpMethod.GET, listPath), ParameterLocation.QUERY))
                                .setIsNullable(true)
                                .setComment("Whether to match all search requirements or at least one (any).")
                                .build(),
                        OpenApiColumn.builder()
                                .setName("account").setSourceName("account")
                                .setType(RowType.from(List.of(RowType.field("id", VARCHAR)))).setSourceType(objectSchema)
                                .setRequiresPredicate(Map.of(new HttpPath(PathItem.HttpMethod.POST, listPath), ParameterLocation.BODY))
                                .build(),
                        OpenApiColumn.builder()
                                .setName("type").setSourceName("type")
                                .setType(VARCHAR).setSourceType(stringSchema)
                                .setOptionalPredicate(Map.of(
                                        new HttpPath(PathItem.HttpMethod.POST, listPath), ParameterLocation.BODY,
                                        new HttpPath(PathItem.HttpMethod.PATCH, onePath), ParameterLocation.BODY))
                                .setIsNullable(true)
                                .setComment("A full zone implies that DNS is hosted with Cloudflare. A partial zone is \n" +
                                        "typically a partner-hosted zone or a CNAME setup.\n")
                                .build(),
                        OpenApiColumn.builder()
                                .setName("identifier").setSourceName("identifier")
                                .setType(VARCHAR).setSourceType(stringSchema)
                                // TODO we've merged /zones and /zones/{identifier} and now we require identifier for /zones, which is wrong
                                .setRequiresPredicate(Map.of(
                                        new HttpPath(PathItem.HttpMethod.GET, onePath), ParameterLocation.PATH,
                                        new HttpPath(PathItem.HttpMethod.DELETE, onePath), ParameterLocation.PATH,
                                        new HttpPath(PathItem.HttpMethod.PATCH, onePath), ParameterLocation.PATH))
                                .setIsNullable(true)
                                .setComment("Identifier")
                                .build(),
                        OpenApiColumn.builder()
                                .setName("paused").setSourceName("paused")
                                .setType(BOOLEAN).setSourceType(booleanSchema)
                                .setOptionalPredicate(Map.of(new HttpPath(PathItem.HttpMethod.PATCH, onePath), ParameterLocation.BODY))
                                .setIsNullable(true)
                                .setComment("Indicates whether the zone is only using Cloudflare DNS services. A\n" +
                                        "true value means the zone will not receive security or performance\n" +
                                        "benefits.\n")
                                .build(),
                        OpenApiColumn.builder()
                                .setName("plan").setSourceName("plan")
                                .setType(RowType.from(List.of(RowType.field("id", VARCHAR)))).setSourceType(objectSchema)
                                .setOptionalPredicate(Map.of(new HttpPath(PathItem.HttpMethod.PATCH, onePath), ParameterLocation.BODY))
                                .setIsNullable(true)
                                .setComment("(Deprecated) Please use the `/zones/{identifier}/subscription` API\n" +
                                        "to update a zone's plan. Changing this value will create/cancel\n" +
                                        "associated subscriptions. To view available plans for this zone,\n" +
                                        "see Zone Plans.\n")
                                .build(),
                        OpenApiColumn.builder()
                                .setName("vanity_name_servers").setSourceName("vanity_name_servers")
                                .setType(new ArrayType(VARCHAR)).setSourceType(arraySchema)
                                .setOptionalPredicate(Map.of(new HttpPath(PathItem.HttpMethod.PATCH, onePath), ParameterLocation.BODY))
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
        String path = "/v1/forecast";
        Assertions.assertThat(tableHandle.getSelectPaths()).containsExactly(new HttpPath(PathItem.HttpMethod.GET, path));
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
                                .setOptionalPredicate(Map.of(new HttpPath(PathItem.HttpMethod.GET, path), ParameterLocation.QUERY))
                                .setParameterStyles(Map.of(new HttpPath(PathItem.HttpMethod.GET, path), new ParameterStyle(Parameter.StyleEnum.FORM, false)))
                                .setIsNullable(true)
                                .build(),
                        OpenApiColumn.builder()
                                .setName("daily_req").setSourceName("daily")
                                .setType(new ArrayType(VARCHAR)).setSourceType(arraySchema)
                                .setOptionalPredicate(Map.of(new HttpPath(PathItem.HttpMethod.GET, path), ParameterLocation.QUERY))
                                .setIsNullable(true)
                                .build(),
                        OpenApiColumn.builder()
                                .setName("latitude_req").setSourceName("latitude")
                                .setType(REAL).setSourceType(numberSchema)
                                .setRequiresPredicate(Map.of(new HttpPath(PathItem.HttpMethod.GET, path), ParameterLocation.QUERY))
                                .setIsNullable(true)
                                .build(),
                        OpenApiColumn.builder()
                                .setName("longitude_req").setSourceName("longitude")
                                .setType(REAL).setSourceType(numberSchema)
                                .setRequiresPredicate(Map.of(new HttpPath(PathItem.HttpMethod.GET, path), ParameterLocation.QUERY))
                                .setIsNullable(true)
                                .build(),
                        OpenApiColumn.builder()
                                .setName("current_weather_req").setSourceName("current_weather")
                                .setType(BOOLEAN).setSourceType(booleanSchema)
                                .setOptionalPredicate(Map.of(new HttpPath(PathItem.HttpMethod.GET, path), ParameterLocation.QUERY))
                                .setIsNullable(true)
                                .build(),
                        OpenApiColumn.builder()
                                .setName("temperature_unit").setSourceName("temperature_unit")
                                .setType(VARCHAR).setSourceType(stringSchema)
                                .setOptionalPredicate(Map.of(new HttpPath(PathItem.HttpMethod.GET, path), ParameterLocation.QUERY))
                                .setIsNullable(true)
                                .build(),
                        OpenApiColumn.builder()
                                .setName("wind_speed_unit").setSourceName("wind_speed_unit")
                                .setType(VARCHAR).setSourceType(stringSchema)
                                .setOptionalPredicate(Map.of(new HttpPath(PathItem.HttpMethod.GET, path), ParameterLocation.QUERY))
                                .setIsNullable(true)
                                .build(),
                        OpenApiColumn.builder()
                                .setName("timeformat").setSourceName("timeformat")
                                .setType(VARCHAR).setSourceType(stringSchema)
                                .setOptionalPredicate(Map.of(new HttpPath(PathItem.HttpMethod.GET, path), ParameterLocation.QUERY))
                                .setIsNullable(true)
                                .build(),
                        OpenApiColumn.builder()
                                .setName("timezone").setSourceName("timezone")
                                .setType(VARCHAR).setSourceType(stringSchema)
                                .setOptionalPredicate(Map.of(new HttpPath(PathItem.HttpMethod.GET, path), ParameterLocation.QUERY))
                                .setIsNullable(true)
                                .build(),
                        OpenApiColumn.builder()
                                .setName("past_days").setSourceName("past_days")
                                .setType(BIGINT).setSourceType(intSchema)
                                .setOptionalPredicate(Map.of(new HttpPath(PathItem.HttpMethod.GET, path), ParameterLocation.QUERY))
                                .setIsNullable(true)
                                .build());
    }

    @Test
    public void testPathParams()
    {
        String specContents =
                """
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

        String postPath = "/namespaces/{namespace}/indexes/{index}/search";
        OpenApiTableHandle tableHandle = spec.getTableHandle(schemaTableName(SCHEMA_NAME, "namespaces_indexes_search"));
        Assertions.assertThat(tableHandle.getSelectPaths()).containsExactly(new HttpPath(PathItem.HttpMethod.POST, postPath));
        Assertions.assertThat(tableHandle.getInsertPaths()).containsExactly(new HttpPath(PathItem.HttpMethod.POST, postPath));
        Assertions.assertThat(tableHandle.getUpdatePaths()).containsExactly(new HttpPath(PathItem.HttpMethod.POST, postPath));
        Assertions.assertThat(tableHandle.getDeletePaths()).isEmpty();

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
                                .setOptionalPredicate(Map.of(new HttpPath(PathItem.HttpMethod.POST, postPath), ParameterLocation.BODY))
                                .setIsNullable(true)
                                .build(),
                        OpenApiColumn.builder()
                                .setName("namespace").setSourceName("namespace")
                                .setType(VARCHAR).setSourceType(stringSchema)
                                .setRequiresPredicate(Map.of(new HttpPath(PathItem.HttpMethod.POST, postPath), ParameterLocation.PATH))
                                .setIsNullable(true)
                                .build(),
                        OpenApiColumn.builder()
                                .setName("index").setSourceName("index")
                                .setType(VARCHAR).setSourceType(stringSchema)
                                .setRequiresPredicate(Map.of(new HttpPath(PathItem.HttpMethod.POST, postPath), ParameterLocation.PATH))
                                .setIsNullable(true)
                                .build());
    }

    @Test
    public void testNormalizedNameCollisions()
    {
        String specContents =
                """
                openapi: 3.0.0
                info:
                  title: test
                  version: 1.0
                paths:
                  /items:
                    get:
                      parameters:
                      - in: query
                        name: fooBar
                        schema:
                          type: string
                      responses:
                        "200":
                          description: Items
                          content:
                            application/json:
                              schema:
                                type: array
                                items:
                                  type: object
                                  properties:
                                    foo-bar:
                                      type: string
                                    foo_bar:
                                      type: string
                """;
        OpenApiSpec spec = parseSpec(specContents);

        Assertions.assertThat(spec.getTables().get("items"))
                .extracting(OpenApiColumn::getName, OpenApiColumn::getSourceName)
                .containsExactly(
                        Assertions.tuple("foo_bar", "foo-bar"),
                        Assertions.tuple("foo_bar_2", "foo_bar"),
                        Assertions.tuple("foo_bar_3", "fooBar"));
    }

    @Test
    public void testSpecWithoutComponents()
    {
        OpenApiSpec spec = parseSpec(itemsSpec(""));

        assertItemsTable(spec);
        Assertions.assertThat(spec.getSecuritySchemas()).isEmpty();
    }

    @Test
    public void testComponentsWithoutSchemas()
    {
        OpenApiSpec spec = parseSpec(itemsSpec("""
                components:
                  parameters:
                    limit:
                      in: query
                      name: limit
                      schema:
                        type: integer
                """));

        assertItemsTable(spec);
        Assertions.assertThat(spec.getSecuritySchemas()).isEmpty();
    }

    @Test
    public void testSpecWithoutPaths()
    {
        OpenApiSpec spec = parseSpec("""
                openapi: 3.1.0
                info:
                  title: test
                  version: 1.0
                components:
                  securitySchemes:
                    basicAuth:
                      type: http
                      scheme: basic
                """);

        Assertions.assertThat(spec.getTables()).isEmpty();
        Assertions.assertThat(spec.getPathSecurityRequirements()).isEmpty();
        Assertions.assertThat(spec.getSecuritySchemas()).containsOnlyKeys("basicAuth");
    }

    @Test
    public void testOpenApi31TypesMatchOpenApi30()
    {
        RowType objectType = RowType.from(List.of(
                RowType.field("int64", BIGINT),
                RowType.field("boolean", BOOLEAN),
                RowType.field("float", REAL)));
        RowType dimensionsType = RowType.from(List.of(
                RowType.field("width", DOUBLE),
                RowType.field("height", DOUBLE)));
        List<OpenApiColumn> columns30 = parseSpec(typesSpec("3.0.3")).getTables().get("types");
        List<OpenApiColumn> columns31 = parseSpec(typesSpec("3.1.0")).getTables().get("types");

        Assertions.assertThat(columns30)
                .extracting(OpenApiColumn::getName, OpenApiColumn::getType)
                .containsExactly(
                        Assertions.tuple("integer", BIGINT),
                        Assertions.tuple("int32", INTEGER),
                        Assertions.tuple("int64", BIGINT),
                        Assertions.tuple("number", createDecimalType(18, 8)),
                        Assertions.tuple("float", REAL),
                        Assertions.tuple("double", DOUBLE),
                        Assertions.tuple("boolean", BOOLEAN),
                        Assertions.tuple("string", VARCHAR),
                        Assertions.tuple("date", DATE),
                        Assertions.tuple("date_time", TIMESTAMP_MILLIS),
                        Assertions.tuple("integers", new ArrayType(BIGINT)),
                        Assertions.tuple("object", objectType),
                        Assertions.tuple("objects", new ArrayType(objectType)),
                        Assertions.tuple("map", new MapType(VARCHAR, DOUBLE, new TypeOperators())),
                        Assertions.tuple("empty_object", VARCHAR),
                        Assertions.tuple("open_object", VARCHAR),
                        Assertions.tuple("closed_object", RowType.from(List.of(RowType.field("name", VARCHAR)))),
                        Assertions.tuple("dimensions", dimensionsType));
        Assertions.assertThat(columns31)
                .extracting(OpenApiColumn::getName, OpenApiColumn::getType)
                .containsExactlyElementsOf(columns30.stream()
                        .map(column -> Assertions.tuple(column.getName(), column.getType()))
                        .toList());
        // rows are decoded and serialized using the properties of the source schema, in the order of the row fields
        Assertions.assertThat(columns31)
                .filteredOn(column -> column.getType() instanceof RowType)
                .allSatisfy(column -> Assertions.assertThat(column.getSourceType().getProperties().keySet())
                        .containsExactlyElementsOf(((RowType) column.getType()).getFields().stream()
                                .map(field -> field.getName().orElseThrow())
                                .toList()));
    }

    @Test
    public void testOpenApi31NullableTypesMatchOpenApi30()
    {
        List<OpenApiColumn> columns30 = parseSpec(nullableTypesSpec("3.0.3", "type: integer, nullable: true", "type: object, nullable: true")).getTables().get("points");
        List<OpenApiColumn> columns31 = parseSpec(nullableTypesSpec("3.1.0", "type: [integer, \"null\"]", "type: [object, \"null\"]")).getTables().get("points");

        Assertions.assertThat(columns30)
                .extracting(OpenApiColumn::getName, OpenApiColumn::getType, column -> column.getMetadata().isNullable())
                .containsExactly(
                        Assertions.tuple("id", BIGINT, false),
                        Assertions.tuple("count", BIGINT, true),
                        Assertions.tuple("point", RowType.from(List.of(RowType.field("x", DOUBLE))), true));
        Assertions.assertThat(columns31)
                .extracting(OpenApiColumn::getName, OpenApiColumn::getType, column -> column.getMetadata().isNullable())
                .containsExactlyElementsOf(columns30.stream()
                        .map(column -> Assertions.tuple(column.getName(), column.getType(), column.getMetadata().isNullable()))
                        .toList());
    }

    private static String nullableTypesSpec(String version, String nullableInteger, String nullableObject)
    {
        // all properties are required, so only the nullable ones map to nullable columns
        return """
                openapi: %s
                info:
                  title: test
                  version: 1.0
                paths:
                  /points:
                    get:
                      responses:
                        "200":
                          description: Points
                          content:
                            application/json:
                              schema:
                                type: object
                                required:
                                  - id
                                  - count
                                  - point
                                properties:
                                  id:
                                    type: integer
                                  count: {%s}
                                  point: {%s, properties: {x: {type: number, format: double}}}
                """.formatted(version, nullableInteger, nullableObject);
    }

    private static String typesSpec(String version)
    {
        return """
                openapi: %s
                info:
                  title: test
                  version: 1.0
                paths:
                  /types:
                    get:
                      responses:
                        "200":
                          description: Types
                          content:
                            application/json:
                              schema:
                                type: array
                                items:
                                  type: object
                                  properties:
                                    integer:
                                      type: integer
                                    int32:
                                      type: integer
                                      format: int32
                                    int64:
                                      type: integer
                                      format: int64
                                    number:
                                      type: number
                                    float:
                                      type: number
                                      format: float
                                    double:
                                      type: number
                                      format: double
                                    boolean:
                                      type: boolean
                                    string:
                                      type: string
                                    date:
                                      type: string
                                      format: date
                                    date_time:
                                      type: string
                                      format: date-time
                                    integers:
                                      type: array
                                      items:
                                        type: integer
                                        format: int64
                                    object:
                                      type: object
                                      properties:
                                        int64:
                                          type: integer
                                          format: int64
                                        boolean:
                                          type: boolean
                                        float:
                                          type: number
                                          format: float
                                    objects:
                                      type: array
                                      items:
                                        type: object
                                        properties:
                                          int64:
                                            type: integer
                                            format: int64
                                          boolean:
                                            type: boolean
                                          float:
                                            type: number
                                            format: float
                                    map:
                                      type: object
                                      additionalProperties:
                                        type: number
                                        format: double
                                    empty_object:
                                      type: object
                                    open_object:
                                      type: object
                                      properties:
                                        name:
                                          type: string
                                      additionalProperties: true
                                    closed_object:
                                      type: object
                                      properties:
                                        name:
                                          type: string
                                      additionalProperties: false
                                    dimensions:
                                      $ref: '#/components/schemas/Dimensions'
                components:
                  schemas:
                    Dimensions:
                      type: object
                      properties:
                        width:
                          type: number
                          format: double
                        height:
                          type: number
                          format: double
                """.formatted(version);
    }

    private static String itemsSpec(String components)
    {
        // a non-standard type makes the spec look it up as a named schema in components
        return """
                openapi: 3.0.0
                info:
                  title: test
                  version: 1.0
                paths:
                  /items:
                    get:
                      responses:
                        "200":
                          description: Items
                          content:
                            application/json:
                              schema:
                                type: array
                                items:
                                  type: object
                                  properties:
                                    name:
                                      type: string
                                    custom:
                                      type: custom
                %s
                """.formatted(components);
    }

    private static void assertItemsTable(OpenApiSpec spec)
    {
        Assertions.assertThat(spec.getTables().get("items"))
                .extracting(OpenApiColumn::getName, OpenApiColumn::getType)
                .containsExactly(
                        Assertions.tuple("name", VARCHAR),
                        Assertions.tuple("custom", VARCHAR));
    }

    @Test
    public void testPathItemParameters()
    {
        String specContents =
                """
                openapi: 3.0.0
                info:
                  title: test
                  version: 1.0
                paths:
                  /items/{id}:
                    parameters:
                    - in: path
                      name: id
                      required: true
                      schema:
                        type: string
                    - in: query
                      name: filter
                      schema:
                        type: string
                    get:
                      parameters:
                      - in: query
                        name: filter
                        required: true
                        schema:
                          type: string
                      - in: cookie
                        name: session
                        schema:
                          type: string
                      - in: query
                        name: tags
                        style: pipeDelimited
                        explode: false
                        schema:
                          type: array
                          items:
                            type: string
                      responses:
                        "200":
                          description: Item
                          content:
                            application/json:
                              schema:
                                $ref: '#/components/schemas/Item'
                components:
                  schemas:
                    Item:
                      type: object
                      properties:
                        name:
                          type: string
                """;
        OpenApiSpec spec = parseSpec(specContents);

        HttpPath httpPath = new HttpPath(PathItem.HttpMethod.GET, "/items/{id}");
        Assertions.assertThat(spec.getTables().get("items"))
                .extracting(OpenApiColumn::getName, OpenApiColumn::getRequiresPredicate, OpenApiColumn::getOptionalPredicate, OpenApiColumn::getParameterStyles)
                .containsExactly(
                        Assertions.tuple("name", Map.of(), Map.of(), Map.of()),
                        Assertions.tuple("id", Map.of(httpPath, ParameterLocation.PATH), Map.of(), Map.of()),
                        Assertions.tuple("filter", Map.of(httpPath, ParameterLocation.QUERY), Map.of(), Map.of()),
                        Assertions.tuple("session", Map.of(), Map.of(httpPath, ParameterLocation.COOKIE), Map.of()),
                        Assertions.tuple("tags", Map.of(), Map.of(httpPath, ParameterLocation.QUERY), Map.of(httpPath, new ParameterStyle(Parameter.StyleEnum.PIPEDELIMITED, false))));
    }

    @Test
    public void testParameterStyles()
    {
        String specContents =
                """
                openapi: 3.0.0
                info:
                  title: test
                  version: 1.0
                paths:
                  /items/{id}:
                    get:
                      parameters:
                      - in: path
                        name: id
                        required: true
                        style: matrix
                        explode: true
                        schema:
                          type: array
                          items:
                            type: string
                      - in: query
                        name: filter
                        allowReserved: true
                        schema:
                          type: string
                      - in: query
                        name: color
                        style: deepObject
                        explode: true
                        schema:
                          type: object
                          properties:
                            r:
                              type: integer
                      - in: header
                        name: tag
                        allowReserved: true
                        schema:
                          type: string
                      responses:
                        "200":
                          description: Item
                          content:
                            application/json:
                              schema:
                                $ref: '#/components/schemas/Item'
                components:
                  schemas:
                    Item:
                      type: object
                      properties:
                        name:
                          type: string
                """;
        OpenApiSpec spec = parseSpec(specContents);

        HttpPath httpPath = new HttpPath(PathItem.HttpMethod.GET, "/items/{id}");
        Assertions.assertThat(spec.getTables().get("items"))
                .extracting(OpenApiColumn::getName, OpenApiColumn::getParameterStyles)
                .containsExactly(
                        Assertions.tuple("name", Map.of()),
                        Assertions.tuple("id", Map.of(httpPath, new ParameterStyle(Parameter.StyleEnum.MATRIX, true))),
                        Assertions.tuple("filter", Map.of(httpPath, new ParameterStyle(Parameter.StyleEnum.FORM, true, true))),
                        Assertions.tuple("color", Map.of(httpPath, new ParameterStyle(Parameter.StyleEnum.DEEPOBJECT, true))),
                        Assertions.tuple("tag", Map.of()));
    }

    @Test
    public void testContentParametersAreIgnored()
    {
        String specContents =
                """
                openapi: 3.0.0
                info:
                  title: test
                  version: 1.0
                paths:
                  /items:
                    get:
                      parameters:
                      - in: query
                        name: filter
                        content:
                          application/json:
                            schema:
                              type: object
                              properties:
                                name:
                                  type: string
                      - in: query
                        name: limit
                        schema:
                          type: integer
                      responses:
                        "200":
                          description: Item
                          content:
                            application/json:
                              schema:
                                $ref: '#/components/schemas/Item'
                components:
                  schemas:
                    Item:
                      type: object
                      properties:
                        name:
                          type: string
                """;
        OpenApiSpec spec = parseSpec(specContents);

        Assertions.assertThat(spec.getTables().get("items"))
                .extracting(OpenApiColumn::getName)
                .containsExactly("name", "limit");
    }

    @Test
    public void testPaginationExtensions()
    {
        String specContents =
                """
                openapi: 3.0.0
                info:
                  title: test
                  version: 1.0
                paths:
                  /items:
                    get:
                      parameters:
                      - in: query
                        name: page
                        schema:
                          type: integer
                      - in: query
                        name: size
                        schema:
                          type: integer
                      x-trino:
                        pageParam: page
                        limitParam: size
                        resultsPath: $response.body#/results
                        totalResultsPath: $response.body#/total
                        totalPagesPath: $response.body#/pages
                        hasMorePath: $response.body#/has_more
                        maxPages: 10
                      responses:
                        "200":
                          description: Items
                          content:
                            application/json:
                              schema:
                                type: object
                                properties:
                                  results:
                                    type: array
                                    items:
                                      type: object
                                      properties:
                                        name:
                                          type: string
                """;
        OpenApiSpec spec = parseSpec(specContents);

        Assertions.assertThat(spec.getPaginations(schemaTableName(SCHEMA_NAME, "items")))
                .containsExactly(Map.entry(
                        new HttpPath(PathItem.HttpMethod.GET, "/items"),
                        Pagination.builder()
                                .setLimitParam(Optional.of("size"))
                                .setTotalResultsPointer(JsonPointer.compile("/total"))
                                .setTotalPagesPointer(JsonPointer.compile("/pages"))
                                .setHasMorePointer(JsonPointer.compile("/has_more"))
                                .setMaxPages(OptionalInt.of(10))
                                .build()));
        Assertions.assertThatThrownBy(() -> parseSpec(specContents.replace("maxPages: 10", "maxPages: many")))
                .hasMessage("Invalid value of maxPages: many");
        Assertions.assertThatThrownBy(() -> parseSpec(specContents.replace("maxPages: 10", "maxPages: 0")))
                .hasMessage("maxPages must be positive");
    }

    @Test
    public void testParserDropsEmptyGlobalSecurity()
    {
        // OpenApiSpec.parse() restores the empty list, so if this fails after upgrading the parser, that workaround can be removed
        SwaggerParseResult result = new OpenAPIV3Parser().readContents("""
                openapi: 3.0.0
                info:
                  title: test
                  version: '1'
                security: []
                paths: {}
                """, null, new ParseOptions());

        Assertions.assertThat(result.getOpenAPI().getSecurity()).isNull();
    }

    @Test
    public void testResponseOnlyColumnsAreNullableInInsertableTables()
    {
        String specContents =
                """
                openapi: 3.0.0
                info:
                  title: test
                  version: 1.0
                paths:
                  /notes/{note_id}:
                    parameters:
                    - in: path
                      name: note_id
                      required: true
                      schema:
                        type: integer
                    get:
                      responses:
                        "200":
                          description: Note
                          content:
                            application/json:
                              schema:
                                $ref: '#/components/schemas/Note'
                    post:
                      requestBody:
                        content:
                          application/json:
                            schema:
                              type: object
                              required:
                              - text
                              properties:
                                text:
                                  type: string
                      responses:
                        "201":
                          description: Note
                          content:
                            application/json:
                              schema:
                                $ref: '#/components/schemas/Note'
                  /archived_notes/{note_id}:
                    parameters:
                    - in: path
                      name: note_id
                      required: true
                      schema:
                        type: integer
                    get:
                      responses:
                        "200":
                          description: Note
                          content:
                            application/json:
                              schema:
                                $ref: '#/components/schemas/Note'
                components:
                  schemas:
                    Note:
                      type: object
                      required:
                      - id
                      - text
                      properties:
                        id:
                          type: integer
                        text:
                          type: string
                """;
        OpenApiSpec spec = parseSpec(specContents);

        Assertions.assertThat(spec.getTables().get("notes"))
                .filteredOn(column -> !column.getMetadata().isHidden())
                .extracting(OpenApiColumn::getName, column -> column.getMetadata().isNullable())
                .containsExactly(
                        Assertions.tuple("id", true),
                        Assertions.tuple("text", false),
                        Assertions.tuple("note_id", true));
        Assertions.assertThat(spec.getTables().get("archived_notes"))
                .extracting(OpenApiColumn::getName, column -> column.getMetadata().isNullable())
                .containsExactly(
                        Assertions.tuple("id", false),
                        Assertions.tuple("text", false),
                        Assertions.tuple("note_id", true));
    }

    @Test
    public void testPaginatedResultsNullability()
    {
        String specContents =
                """
                openapi: 3.0.0
                info:
                  title: test
                  version: 1.0
                paths:
                  /runs:
                    get:
                      parameters:
                      - in: query
                        name: page
                        schema:
                          type: integer
                      x-trino:
                        pageParam: page
                        resultsPath: $response.body#/results
                      responses:
                        "200":
                          description: Runs
                          content:
                            application/json:
                              schema:
                                $ref: '#/components/schemas/RunPage'
                  /nested_runs:
                    get:
                      x-trino:
                        resultsPath: $response.body#/data/results
                      responses:
                        "200":
                          description: Runs
                          content:
                            application/json:
                              schema:
                                type: object
                                properties:
                                  data:
                                    $ref: '#/components/schemas/RunPage'
                  /all_runs:
                    get:
                      responses:
                        "200":
                          description: Runs
                          content:
                            application/json:
                              schema:
                                type: array
                                items:
                                  $ref: '#/components/schemas/Run'
                components:
                  schemas:
                    RunPage:
                      type: object
                      required:
                      - results
                      - total
                      properties:
                        results:
                          type: array
                          items:
                            $ref: '#/components/schemas/Run'
                        total:
                          type: integer
                        next:
                          type: string
                    Run:
                      type: object
                      required:
                      - runId
                      - name
                      - note
                      properties:
                        runId:
                          type: integer
                        name:
                          type: string
                        note:
                          type: string
                          nullable: true
                        comment:
                          type: string
                        label:
                          type: string
                          nullable: false
                """;
        OpenApiSpec spec = parseSpec(specContents);

        Assertions.assertThat(spec.getTables().get("runs"))
                .extracting(OpenApiColumn::getName, column -> column.getMetadata().isNullable())
                .containsExactly(
                        Assertions.tuple("total", false),
                        Assertions.tuple("next", true),
                        Assertions.tuple("run_id", false),
                        Assertions.tuple("name", false),
                        Assertions.tuple("note", true),
                        Assertions.tuple("comment", true),
                        Assertions.tuple("label", true),
                        Assertions.tuple("page", true));
        Assertions.assertThat(spec.getTables().get("nested_runs"))
                .extracting(OpenApiColumn::getName, column -> column.getMetadata().isNullable())
                .containsExactly(
                        Assertions.tuple("run_id", false),
                        Assertions.tuple("name", false),
                        Assertions.tuple("note", true),
                        Assertions.tuple("comment", true),
                        Assertions.tuple("label", true));
        Assertions.assertThat(spec.getTables().get("all_runs"))
                .extracting(OpenApiColumn::getName, column -> column.getMetadata().isNullable())
                .containsExactly(
                        Assertions.tuple("run_id", false),
                        Assertions.tuple("name", false),
                        Assertions.tuple("note", true),
                        Assertions.tuple("comment", true),
                        Assertions.tuple("label", true));
    }

    @Test
    public void testOpenApi31NullableResults()
    {
        String specContents =
                """
                openapi: 3.1.0
                info:
                  title: test
                  version: 1.0
                paths:
                  /runs:
                    get:
                      x-trino:
                        resultsPath: $response.body#/results
                      responses:
                        "200":
                          description: Runs
                          content:
                            application/json:
                              schema:
                                type: object
                                required:
                                - results
                                properties:
                                  results:
                                    type: array
                                    items:
                                      type: object
                                      required:
                                      - name
                                      - note
                                      properties:
                                        name:
                                          type: string
                                        note:
                                          type:
                                          - string
                                          - "null"
                                        comment:
                                          type: string
                """;
        OpenApiSpec spec = parseSpec(specContents);

        Assertions.assertThat(spec.getTables().get("runs"))
                .extracting(OpenApiColumn::getName, column -> column.getMetadata().isNullable())
                .containsExactly(
                        Assertions.tuple("name", false),
                        Assertions.tuple("note", true),
                        Assertions.tuple("comment", true));
    }

    @Test
    public void testArrayRequestBodyRequiredProperties()
    {
        String specContents =
                """
                openapi: 3.0.0
                info:
                  title: test
                  version: 1.0
                paths:
                  /bulk_items:
                    post:
                      requestBody:
                        content:
                          application/json:
                            schema:
                              type: array
                              items:
                                type: object
                                required:
                                - name
                                properties:
                                  name:
                                    type: string
                                  note:
                                    type: string
                      responses:
                        "200":
                          description: OK
                          content:
                            application/json:
                              schema:
                                type: object
                                properties:
                                  count:
                                    type: integer
                  /bulk_values:
                    post:
                      requestBody:
                        content:
                          application/json:
                            schema:
                              type: array
                              # some specs list required item properties in the array schema
                              required:
                              - key
                              items:
                                type: object
                                properties:
                                  key:
                                    type: string
                                  value:
                                    type: string
                      responses:
                        "200":
                          description: OK
                          content:
                            application/json:
                              schema:
                                type: object
                                properties:
                                  count:
                                    type: integer
                """;
        OpenApiSpec spec = parseSpec(specContents);

        Map<HttpPath, ParameterLocation> bulkItemsBody = Map.of(new HttpPath(PathItem.HttpMethod.POST, "/bulk_items"), ParameterLocation.BODY);
        Assertions.assertThat(spec.getTables().get("bulk_items"))
                .filteredOn(column -> !column.getMetadata().isHidden())
                .extracting(OpenApiColumn::getName, column -> column.getMetadata().isNullable(), OpenApiColumn::getRequiresPredicate, OpenApiColumn::getOptionalPredicate)
                .containsExactly(
                        Assertions.tuple("count", true, Map.of(), Map.of()),
                        Assertions.tuple("name", false, bulkItemsBody, Map.of()),
                        Assertions.tuple("note", true, Map.of(), bulkItemsBody));
        Map<HttpPath, ParameterLocation> bulkValuesBody = Map.of(new HttpPath(PathItem.HttpMethod.POST, "/bulk_values"), ParameterLocation.BODY);
        Assertions.assertThat(spec.getTables().get("bulk_values"))
                .filteredOn(column -> !column.getMetadata().isHidden())
                .extracting(OpenApiColumn::getName, column -> column.getMetadata().isNullable(), OpenApiColumn::getRequiresPredicate, OpenApiColumn::getOptionalPredicate)
                .containsExactly(
                        Assertions.tuple("count", true, Map.of(), Map.of()),
                        Assertions.tuple("key", false, bulkValuesBody, Map.of()),
                        Assertions.tuple("value", true, Map.of(), bulkValuesBody));
    }

    @Test
    public void testUnionResponses()
    {
        String specContents =
                """
                openapi: 3.0.0
                info:
                  title: test
                  version: 1.0
                paths:
                  /one_of:
                    get:
                      responses:
                        "200":
                          description: Views or an error
                          content:
                            application/json:
                              schema:
                                oneOf:
                                - $ref: "#/components/schemas/Views"
                                - $ref: "#/components/schemas/Error"
                  /one_of_list:
                    get:
                      responses:
                        "200":
                          description: Views or errors
                          content:
                            application/json:
                              schema:
                                type: array
                                items:
                                  oneOf:
                                  - $ref: "#/components/schemas/Views"
                                  - $ref: "#/components/schemas/Error"
                  /any_of:
                    get:
                      responses:
                        "200":
                          description: Views or an error
                          content:
                            application/json:
                              schema:
                                anyOf:
                                - $ref: "#/components/schemas/Views"
                                - $ref: "#/components/schemas/Error"
                  /any_of_list:
                    get:
                      responses:
                        "200":
                          description: Views or errors
                          content:
                            application/json:
                              schema:
                                type: array
                                items:
                                  anyOf:
                                  - $ref: "#/components/schemas/Views"
                                  - $ref: "#/components/schemas/Error"
                  /all_of:
                    get:
                      responses:
                        "200":
                          description: Views with an error
                          content:
                            application/json:
                              schema:
                                allOf:
                                - $ref: "#/components/schemas/Views"
                                - $ref: "#/components/schemas/Error"
                  /all_of_list:
                    get:
                      responses:
                        "200":
                          description: Views with errors
                          content:
                            application/json:
                              schema:
                                type: array
                                items:
                                  allOf:
                                  - $ref: "#/components/schemas/Views"
                                  - $ref: "#/components/schemas/Error"
                components:
                  schemas:
                    Views:
                      type: object
                      required:
                      - views
                      properties:
                        views:
                          type: array
                          items:
                            type: string
                        total:
                          type: integer
                          description: Number of views
                    Error:
                      type: object
                      required:
                      - code
                      properties:
                        code:
                          type: integer
                        message:
                          type: string
                """;
        OpenApiSpec spec = parseSpec(specContents);

        for (String table : List.of("one_of", "one_of_list", "any_of", "any_of_list")) {
            Assertions.assertThat(spec.getTables().get(table))
                    .as(table)
                    .extracting(OpenApiColumn::getName, OpenApiColumn::getType, column -> column.getMetadata().isNullable(), column -> column.getMetadata().getComment())
                    .containsExactly(
                            Assertions.tuple("views", new ArrayType(VARCHAR), true, Optional.empty()),
                            Assertions.tuple("total", BIGINT, true, Optional.of("Number of views")),
                            Assertions.tuple("code", BIGINT, true, Optional.empty()),
                            Assertions.tuple("message", VARCHAR, true, Optional.empty()));
        }
        // properties of all allOf parts are always present, so they keep being required
        for (String table : List.of("all_of", "all_of_list")) {
            Assertions.assertThat(spec.getTables().get(table))
                    .as(table)
                    .extracting(OpenApiColumn::getName, OpenApiColumn::getType, column -> column.getMetadata().isNullable())
                    .containsExactly(
                            Assertions.tuple("views", new ArrayType(VARCHAR), false),
                            Assertions.tuple("total", BIGINT, true),
                            Assertions.tuple("code", BIGINT, false),
                            Assertions.tuple("message", VARCHAR, true));
        }
    }

    @Test
    public void testUnionResponseWithResultsPath()
    {
        String specContents =
                """
                openapi: 3.0.0
                info:
                  title: test
                  version: 1.0
                paths:
                  /items:
                    get:
                      x-trino:
                        resultsPath: $response.body#/results
                        errorPath: $response.body#/message
                      responses:
                        "200":
                          description: Items or an error
                          content:
                            application/json:
                              schema:
                                oneOf:
                                - type: object
                                  properties:
                                    results:
                                      type: array
                                      items:
                                        oneOf:
                                        - type: object
                                          properties:
                                            name:
                                              type: string
                                        - type: object
                                          properties:
                                            id:
                                              type: integer
                                - type: object
                                  properties:
                                    message:
                                      type: string
                """;
        OpenApiSpec spec = parseSpec(specContents);

        Assertions.assertThat(spec.getTables().get("items"))
                .extracting(OpenApiColumn::getName, OpenApiColumn::getType)
                .containsExactly(
                        Assertions.tuple("message", VARCHAR),
                        Assertions.tuple("name", VARCHAR),
                        Assertions.tuple("id", BIGINT));
    }

    @Test
    public void testUnionResponseWithConflictingPropertyTypes()
    {
        String specContents =
                """
                openapi: 3.0.0
                info:
                  title: test
                  version: 1.0
                paths:
                  /status:
                    get:
                      responses:
                        "200":
                          description: Status or an error
                          content:
                            application/json:
                              schema:
                                oneOf:
                                - type: object
                                  properties:
                                    code:
                                      type: string
                                      description: Status code
                                    state:
                                      type: string
                                - type: object
                                  properties:
                                    code:
                                      type: integer
                                      description: Error code
                                    state:
                                      type: string
                                      description: Error state
                """;
        OpenApiSpec spec = parseSpec(specContents);

        List<OpenApiColumn> columns = spec.getTables().get("status");
        Assertions.assertThat(columns)
                .extracting(OpenApiColumn::getName, OpenApiColumn::getType, column -> column.getMetadata().getComment())
                .containsExactly(
                        Assertions.tuple("code", VARCHAR, Optional.of("Status code")),
                        Assertions.tuple("state", VARCHAR, Optional.empty()));
        Assertions.assertThat(columns.getFirst().getSourceType())
                .matches(JsonTrinoConverter::isUnion);
        Assertions.assertThat(columns.getLast().getSourceType())
                .matches(schema -> !JsonTrinoConverter.isUnion(schema));
    }

    @Test
    public void testUnionOfObjectAndArrayResponses()
    {
        String specContents =
                """
                openapi: 3.0.0
                info:
                  title: test
                  version: 1.0
                paths:
                  /contents:
                    get:
                      responses:
                        "200":
                          description: A file or a directory listing
                          content:
                            application/json:
                              schema:
                                $ref: "#/components/schemas/Contents"
                  /contents_results:
                    get:
                      x-trino:
                        resultsPath: $response.body#/results
                      responses:
                        "200":
                          description: A file or a directory listing
                          content:
                            application/json:
                              schema:
                                type: object
                                properties:
                                  results:
                                    $ref: "#/components/schemas/Contents"
                components:
                  schemas:
                    Contents:
                      oneOf:
                      - $ref: "#/components/schemas/File"
                      - type: array
                        items:
                          $ref: "#/components/schemas/Entry"
                      - type: array
                        items:
                          type: array
                          items:
                            $ref: "#/components/schemas/Entry"
                    File:
                      type: object
                      properties:
                        name:
                          type: string
                        content:
                          type: string
                    Entry:
                      type: object
                      properties:
                        name:
                          type: string
                        size:
                          type: integer
                """;
        OpenApiSpec spec = parseSpec(specContents);

        for (String table : List.of("contents", "contents_results")) {
            Assertions.assertThat(spec.getTables().get(table))
                    .as(table)
                    .extracting(OpenApiColumn::getName, OpenApiColumn::getType, column -> column.getMetadata().isNullable())
                    .containsExactly(
                            Assertions.tuple("name", VARCHAR, true),
                            Assertions.tuple("content", VARCHAR, true),
                            Assertions.tuple("size", BIGINT, true));
        }
    }

    @Test
    public void testNestedObjectUnions()
            throws JsonProcessingException
    {
        String specContents =
                """
                openapi: 3.0.0
                info:
                  title: test
                  version: 1.0
                paths:
                  /events:
                    get:
                      responses:
                        "200":
                          description: Events
                          content:
                            application/json:
                              schema:
                                type: array
                                items:
                                  type: object
                                  properties:
                                    actor:
                                      oneOf:
                                      - $ref: "#/components/schemas/Bot"
                                      - $ref: "#/components/schemas/Person"
                                    value:
                                      oneOf:
                                      - type: string
                                      - type: object
                                        properties:
                                          name:
                                            type: string
                components:
                  schemas:
                    Bot:
                      type: object
                      properties:
                        id:
                          type: string
                        bot_name:
                          type: string
                        meta:
                          type: object
                          properties:
                            a:
                              type: string
                    Person:
                      type: object
                      properties:
                        id:
                          type: integer
                        email:
                          type: string
                        meta:
                          type: object
                          properties:
                            b:
                              type: integer
                """;
        OpenApiSpec spec = parseSpec(specContents);

        List<OpenApiColumn> columns = spec.getTables().get("events");
        RowType actorType = RowType.from(List.of(
                RowType.field("id", VARCHAR),
                RowType.field("bot_name", VARCHAR),
                RowType.field("meta", RowType.from(List.of(
                        RowType.field("a", VARCHAR),
                        RowType.field("b", BIGINT)))),
                RowType.field("email", VARCHAR)));
        Assertions.assertThat(columns)
                .extracting(OpenApiColumn::getName, OpenApiColumn::getType)
                .containsExactly(
                        Assertions.tuple("actor", actorType),
                        Assertions.tuple("value", VARCHAR));

        // fields missing from the value are NULL, and they're not sent back, so the value still matches one alternative
        OpenApiColumn actor = columns.getFirst();
        ObjectMapper objectMapper = new ObjectMapper();
        SqlRow row = (SqlRow) JsonTrinoConverter.convert(objectMapper.readTree("{\"id\": 7, \"email\": \"a@b.c\", \"meta\": {\"b\": 1}}"), actor.getType(), actor.getSourceType());
        Assertions.assertThat(row.getRawFieldBlock(1).isNull(row.getRawIndex())).isTrue();
        ObjectNode node = objectMapper.createObjectNode();
        JsonTrinoConverter.convertRow(node, row, actorType, actor.getSourceType(), objectMapper);
        Assertions.assertThat(objectMapper.writeValueAsString(node)).isEqualTo("{\"id\":7,\"meta\":{\"b\":1},\"email\":\"a@b.c\"}");
    }

    @Test
    public void testNestedUnionOfObjectArrays()
    {
        String specContents =
                """
                openapi: 3.0.0
                info:
                  title: test
                  version: 1.0
                paths:
                  /rules:
                    get:
                      responses:
                        "200":
                          description: Rules
                          content:
                            application/json:
                              schema:
                                type: object
                                properties:
                                  queries:
                                    oneOf:
                                    - type: array
                                      items:
                                        type: object
                                        properties:
                                          name:
                                            type: string
                                    - type: array
                                      items:
                                        type: object
                                        properties:
                                          id:
                                            type: integer
                                  values:
                                    oneOf:
                                    - type: array
                                      items:
                                        type: string
                                    - type: array
                                      items:
                                        type: object
                                        properties:
                                          id:
                                            type: integer
                """;
        OpenApiSpec spec = parseSpec(specContents);

        Assertions.assertThat(spec.getTables().get("rules"))
                .extracting(OpenApiColumn::getName, OpenApiColumn::getType)
                .containsExactly(
                        Assertions.tuple("queries", new ArrayType(RowType.from(List.of(
                                RowType.field("name", VARCHAR),
                                RowType.field("id", BIGINT))))),
                        Assertions.tuple("values", VARCHAR));
    }

    private OpenApiSpec loadSpecFile(String path)
    {
        OpenApiConfig config = new OpenApiConfig();
        config.setSpecLocation(path);
        return new OpenApiSpec(config);
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
