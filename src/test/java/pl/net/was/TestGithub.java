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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.QueryRunner;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;

import java.util.Map;

import static java.lang.String.format;
import static java.util.Objects.requireNonNullElse;
import static org.assertj.core.api.Assertions.assertThat;

@EnabledIfEnvironmentVariable(named = "GITHUB_TOKEN", matches = ".*")
public class TestGithub
        extends AbstractTestQueryFramework
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        ImmutableMap.Builder<String, String> properties = ImmutableMap.builder();
        properties.putAll(Map.of(
                // a copy of https://github.com/github/rest-api-description/raw/main/descriptions/api.github.com/api.github.com.json with the pagination extension added to some endpoints
                "spec-location", "src/test/resources/github-patched.json",
                "base-uri", "https://api.github.com",
                "authentication.type", "http",
                "authentication.scheme", "bearer",
                "authentication.bearer-token", requireNonNullElse(System.getenv("GITHUB_TOKEN"), "")));
        return OpenApiQueryRunner.createQueryRunner(Map.of("github", properties.buildOrThrow()));
    }

    @Test
    public void showTables()
    {
        assertQuery("SHOW SCHEMAS FROM github", "VALUES 'default', 'information_schema'");
        ImmutableList.Builder<String> expectedTables = ImmutableList.builder();
        expectedTables
                .add("advisories")
                .add("app")
                .add("app_hook_config")
                .add("app_hook_deliveries")
                .add("app_installation_requests")
                .add("app_installations")
                .add("applications_token")
                .add("applications_token_scoped")
                .add("apps")
                .add("codes_of_conduct")
                .add("emojis")
                .add("enterprises_dependabot_alerts")
                .add("enterprises_secret_scanning_alerts")
                .add("events")
                .add("feeds")
                .add("gists_comments")
                .add("gists_commits")
                .add("gists_forks")
                .add("gists_gist_id")
                .add("gists_gist_id_sha")
                .add("gists_public")
                .add("gists_starred")
                .add("gitignore_templates")
                .add("installation_repositories")
                .add("licenses")
                .add("marketplace_listing_accounts")
                .add("marketplace_listing_plans")
                .add("marketplace_listing_plans_accounts")
                .add("marketplace_listing_stubbed_accounts")
                .add("marketplace_listing_stubbed_plans")
                .add("marketplace_listing_stubbed_plans_accounts")
                .add("meta")
                .add("networks_events")
                .add("notifications")
                .add("notifications_threads")
                .add("notifications_threads_subscription")
                .add("organizations")
                .add("orgs_actions_cache_usage")
                .add("orgs_actions_cache_usage_by_repository")
                .add("orgs_actions_oidc_customization_sub")
                .add("orgs_actions_permissions")
                .add("orgs_actions_permissions_repositories")
                .add("orgs_actions_permissions_selected_actions")
                .add("orgs_actions_permissions_workflow")
                .add("orgs_actions_runners")
                .add("orgs_actions_runners_downloads")
                .add("orgs_actions_runners_labels")
                .add("orgs_actions_secrets")
                .add("orgs_actions_secrets_public_key")
                .add("orgs_actions_secrets_repositories")
                .add("orgs_actions_variables")
                .add("orgs_actions_variables_repositories")
                .add("orgs_blocks")
                .add("orgs_code_scanning_alerts")
                .add("orgs_codespaces")
                .add("orgs_codespaces_secrets")
                .add("orgs_codespaces_secrets_public_key")
                .add("orgs_codespaces_secrets_repositories")
                .add("orgs_copilot_billing")
                .add("orgs_copilot_billing_seats")
                .add("orgs_copilot_billing_selected_teams")
                .add("orgs_copilot_billing_selected_users")
                .add("orgs_dependabot_alerts")
                .add("orgs_dependabot_secrets")
                .add("orgs_dependabot_secrets_public_key")
                .add("orgs_dependabot_secrets_repositories")
                .add("orgs_docker_conflicts")
                .add("orgs_events")
                .add("orgs_failed_invitations")
                .add("orgs_hooks")
                .add("orgs_hooks_config")
                .add("orgs_hooks_deliveries")
                .add("issues")
                .add("orgs")
                .add("orgs_installation")
                .add("orgs_installations")
                .add("orgs_interaction_limits")
                .add("orgs_invitations")
                .add("orgs_invitations_teams")
                .add("orgs_issues")
                .add("orgs_members")
                .add("orgs_members_codespaces")
                .add("orgs_members_codespaces_stop")
                .add("orgs_members_copilot")
                .add("orgs_memberships")
                .add("orgs_migrations")
                .add("orgs_migrations_repositories")
                .add("orgs_outside_collaborators")
                .add("orgs_packages")
                .add("orgs_packages_versions")
                .add("orgs_personal_access_token_requests")
                .add("orgs_personal_access_token_requests_repositories")
                .add("orgs_personal_access_tokens")
                .add("orgs_personal_access_tokens_repositories")
                .add("orgs_projects")
                .add("orgs_public_members")
                .add("orgs_repos")
                .add("orgs_rulesets")
                .add("orgs_secret_scanning_alerts")
                .add("orgs_security_advisories")
                .add("orgs_security_managers")
                .add("orgs_settings_billing_actions")
                .add("orgs_settings_billing_packages")
                .add("orgs_settings_billing_shared_storage")
                .add("orgs_teams")
                .add("orgs_teams_discussions")
                .add("orgs_teams_discussions_comments")
                .add("orgs_teams_discussions_comments_reactions")
                .add("orgs_teams_discussions_reactions")
                .add("orgs_teams_invitations")
                .add("orgs_teams_members")
                .add("orgs_teams_memberships")
                .add("orgs_teams_projects")
                .add("orgs_teams_repos")
                .add("orgs_teams_teams")
                .add("projects")
                .add("projects_collaborators")
                .add("projects_collaborators_permission")
                .add("projects_columns")
                .add("projects_columns_cards")
                .add("rate_limit")
                .add("repos")
                .add("repos_actions_artifacts")
                .add("repos_actions_cache_usage")
                .add("repos_actions_caches")
                .add("repos_actions_jobs")
                .add("repos_actions_oidc_customization_sub")
                .add("repos_actions_organization_secrets")
                .add("repos_actions_organization_variables")
                .add("repos_actions_permissions")
                .add("repos_actions_permissions_access")
                .add("repos_actions_permissions_selected_actions")
                .add("repos_actions_permissions_workflow")
                .add("repos_actions_runners")
                .add("repos_actions_runners_downloads")
                .add("repos_actions_runners_labels")
                .add("repos_actions_runs")
                .add("repos_actions_runs_approvals")
                .add("repos_actions_runs_artifacts")
                .add("repos_actions_runs_attempts")
                .add("repos_actions_runs_attempts_jobs")
                .add("repos_actions_runs_jobs")
                .add("repos_actions_runs_pending_deployments")
                .add("repos_actions_runs_timing")
                .add("repos_actions_secrets")
                .add("repos_actions_secrets_public_key")
                .add("repos_actions_variables")
                .add("repos_actions_workflows")
                .add("repos_actions_workflows_runs")
                .add("repos_actions_workflows_timing")
                .add("repos_activity")
                .add("repos_assignees")
                .add("repos_autolinks")
                .add("repos_automated_security_fixes")
                .add("repos_branches")
                .add("repos_branches_protection")
                .add("repos_branches_protection_enforce_admins")
                .add("repos_branches_protection_required_pull_request_reviews")
                .add("repos_branches_protection_required_signatures")
                .add("repos_branches_protection_required_status_checks")
                .add("repos_branches_protection_required_status_checks_contexts")
                .add("repos_branches_protection_restrictions")
                .add("repos_branches_protection_restrictions_apps")
                .add("repos_branches_protection_restrictions_teams")
                .add("repos_branches_protection_restrictions_users")
                .add("repos_check_runs")
                .add("repos_check_runs_annotations")
                .add("repos_check_suites")
                .add("repos_check_suites_check_runs")
                .add("repos_check_suites_preferences")
                .add("repos_code_scanning_alerts")
                .add("repos_code_scanning_alerts_instances")
                .add("repos_code_scanning_analyses")
                .add("repos_code_scanning_codeql_databases")
                .add("repos_code_scanning_default_setup")
                .add("repos_code_scanning_sarifs")
                .add("repos_codeowners_errors")
                .add("repos_codespaces")
                .add("repos_codespaces_devcontainers")
                .add("repos_codespaces_machines")
                .add("repos_codespaces_new")
                .add("repos_codespaces_secrets")
                .add("repos_codespaces_secrets_public_key")
                .add("repos_collaborators")
                .add("repos_collaborators_permission")
                .add("repos_comments")
                .add("repos_comments_reactions")
                .add("repos_commits")
                .add("repos_commits_branches_where_head")
                .add("repos_commits_check_runs")
                .add("repos_commits_check_suites")
                .add("repos_commits_comments")
                .add("repos_commits_pulls")
                .add("repos_commits_status")
                .add("repos_commits_statuses")
                .add("repos_community_profile")
                .add("repos_compare")
                .add("repos_contents")
                .add("repos_contributors")
                .add("repos_dependabot_alerts")
                .add("repos_dependabot_secrets")
                .add("repos_dependabot_secrets_public_key")
                .add("repos_dependency_graph_compare")
                .add("repos_dependency_graph_sbom")
                .add("repos_deployments")
                .add("repos_deployments_statuses")
                .add("repos_environments")
                .add("repos_environments_deployment_branch_policies")
                .add("repos_environments_deployment_protection_rules")
                .add("repos_environments_deployment_protection_rules_apps")
                .add("repos_events")
                .add("repos_forks")
                .add("repos_git_blobs")
                .add("repos_git_commits")
                .add("repos_git_matching_refs")
                .add("repos_git_ref")
                .add("repos_git_refs")
                .add("repos_git_tags")
                .add("repos_git_trees")
                .add("repos_hooks")
                .add("repos_hooks_config")
                .add("repos_hooks_deliveries")
                .add("repos_import")
                .add("repos_import_authors")
                .add("repos_import_large_files")
                .add("repos_import_lfs")
                .add("repos_installation")
                .add("repos_interaction_limits")
                .add("repos_invitations")
                .add("repos_issues")
                .add("repos_issues_assignees")
                .add("repos_issues_comments_reactions")
                .add("repos_issues_labels")
                .add("repos_issues_reactions")
                .add("repos_issues_timeline")
                .add("repos_keys")
                .add("repos_labels")
                .add("repos_languages")
                .add("repos_license")
                .add("repos_merge_upstream")
                .add("repos_milestones")
                .add("repos_milestones_labels")
                .add("repos_notifications")
                .add("repos_owner_repo_issues_comments_comment_id")
                .add("repos_owner_repo_issues_events_event_id")
                .add("repos_owner_repo_issues_issue_number_comments")
                .add("repos_owner_repo_issues_issue_number_events")
                .add("repos_owner_repo_pulls_comments_comment_id")
                .add("repos_owner_repo_pulls_pull_number_comments")
                .add("repos_pages")
                .add("repos_pages_builds")
                .add("repos_pages_builds_latest")
                .add("repos_pages_deployment")
                .add("repos_pages_health")
                .add("repos_projects")
                .add("repos_pulls")
                .add("repos_pulls_comments_reactions")
                .add("repos_pulls_commits")
                .add("repos_pulls_files")
                .add("repos_pulls_merge")
                .add("repos_pulls_requested_reviewers")
                .add("repos_pulls_reviews")
                .add("repos_pulls_reviews_comments")
                .add("repos_pulls_reviews_dismissals")
                .add("repos_pulls_reviews_events")
                .add("repos_readme")
                .add("repos_releases")
                .add("repos_releases_assets")
                .add("repos_releases_generate_notes")
                .add("repos_releases_latest")
                .add("repos_releases_reactions")
                .add("repos_releases_tags")
                .add("repos_rules_branches")
                .add("repos_rulesets")
                .add("repos_secret_scanning_alerts")
                .add("repos_secret_scanning_alerts_locations")
                .add("repos_security_advisories")
                .add("repos_stargazers")
                .add("repos_stats_code_frequency")
                .add("repos_stats_commit_activity")
                .add("repos_stats_contributors")
                .add("repos_stats_participation")
                .add("repos_stats_punch_card")
                .add("repos_subscribers")
                .add("repos_subscription")
                .add("repos_tags")
                .add("repos_tags_protection")
                .add("repos_teams")
                .add("repos_topics")
                .add("repos_traffic_clones")
                .add("repos_traffic_popular_paths")
                .add("repos_traffic_popular_referrers")
                .add("repos_traffic_views")
                .add("repositories")
                .add("repositories_environments_secrets")
                .add("repositories_environments_secrets_public_key")
                .add("repositories_environments_variables")
                .add("search_code")
                .add("search_commits")
                .add("search_labels")
                .add("search_issues")
                .add("search_repositories")
                .add("search_topics")
                .add("search_users")
                .add("user")
                .add("user_blocks")
                .add("user_codespaces")
                .add("user_codespaces_exports")
                .add("user_codespaces_machines")
                .add("user_codespaces_secrets")
                .add("user_codespaces_secrets_public_key")
                .add("user_codespaces_secrets_repositories")
                .add("user_codespaces_start")
                .add("user_codespaces_stop")
                .add("user_docker_conflicts")
                .add("user_email_visibility")
                .add("user_emails")
                .add("user_followers")
                .add("user_following")
                .add("user_gpg_keys")
                .add("user_installations")
                .add("user_installations_repositories")
                .add("user_interaction_limits")
                .add("user_issues")
                .add("user_keys")
                .add("user_marketplace_purchases")
                .add("user_marketplace_purchases_stubbed")
                .add("user_memberships_orgs")
                .add("user_migrations")
                .add("user_migrations_repositories")
                .add("user_orgs")
                .add("user_packages")
                .add("user_packages_versions")
                .add("user_public_emails")
                .add("user_repos")
                .add("user_repository_invitations")
                .add("user_social_accounts")
                .add("user_ssh_signing_keys")
                .add("user_starred")
                .add("user_subscriptions")
                .add("user_teams")
                .add("users")
                .add("users_docker_conflicts")
                .add("users_events")
                .add("users_events_orgs")
                .add("users_events_public")
                .add("users_followers")
                .add("users_following")
                .add("users_gists")
                .add("users_gpg_keys")
                .add("users_hovercard")
                .add("users_installation")
                .add("users_keys")
                .add("users_orgs")
                .add("users_packages")
                .add("users_packages_versions")
                .add("users_projects")
                .add("users_received_events")
                .add("users_received_events_public")
                .add("users_repos")
                .add("users_settings_billing_actions")
                .add("users_settings_billing_packages")
                .add("users_settings_billing_shared_storage")
                .add("users_social_accounts")
                .add("users_ssh_signing_keys")
                .add("users_starred")
                .add("users_subscriptions")
                .add("versions")
                .add("zen");
        assertQuery("SHOW TABLES FROM github.default", "VALUES '" + String.join("', '", expectedTables.build()) + "'");
    }

    @Test
    public void selectFromTable()
    {
        assertQuery("SELECT login FROM orgs WHERE org = 'trinodb'",
                "VALUES ('trinodb')");
        // TODO when merging endpoints into one table, the required parameters should be marked optional, but should be used to make a decision which endpoint to call
        /*
        assertQuery("SELECT login FROM users WHERE username = 'nineinchnick'",
                "VALUES ('nineinchnick')");
        assertQuery("SELECT name FROM repos WHERE owner_req = 'nineinchnick' and name = 'trino-openapi'",
                "VALUES ('trino-openapi')");
         */
        assertQuery("SELECT login FROM orgs_members WHERE org = 'trinodb' AND login = 'martint'",
                "VALUES ('martint')");
        assertQuery("SELECT login FROM repos_collaborators WHERE owner = 'nineinchnick' AND repo = 'trino-openapi' AND login = 'nineinchnick'",
                "VALUES ('nineinchnick')");
        assertQuery("SELECT message FROM repos_git_commits WHERE owner = 'nineinchnick' AND repo = 'trino-openapi' AND commit_sha = '293cca50a6e7b508f61b7270988d8444d3b736c3'",
                "VALUES ('Initial commit')");
        assertQuery("SELECT title FROM repos_issues WHERE owner = 'nineinchnick' AND repo = 'trino-openapi' AND issue_number = 5 AND number = 5",
                "VALUES ('Improve predicate pushdown operator support')");
        assertQuery("SELECT user.login FROM repos_owner_repo_issues_issue_number_comments WHERE owner = 'nineinchnick' AND repo = 'trino-openapi' AND issue_number = 5 AND id = 2970234636",
                "VALUES ('nineinchnick')");
        /*
        assertQuery("SELECT title FROM repos_pulls WHERE owner = 'nineinchnick' AND repo = 'trino-openapi' AND pull_number = 1",
                "VALUES ('GitHub runs')");
        // H2 doesn't support TIMESTAMP WITH TIME ZONE type, so it requires a cast
        assertQuery("SELECT commits, additions, deletions, changed_files, CAST(merged_at AS VARCHAR) FROM pull_stats WHERE owner = 'nineinchnick' AND repo = 'trino-openapi' AND pull_number = 1",
                "VALUES (5, 1736, 75, 27, '2021-04-09 12:08:15.000 UTC')");
        assertQuery("SELECT commit_message FROM pull_commits WHERE owner = 'nineinchnick' AND repo = 'trino-openapi' AND pull_number = 1 AND sha = 'e43f63027cae851f3a02c2816b2f234991b2d139'",
                "VALUES ('Add Github Action runs')");
        assertQuery("SELECT user_login FROM reviews WHERE owner = 'nineinchnick' AND repo = 'trino-openapi' AND pull_number = 66",
                "VALUES ('nineinchnick')");
        assertQuery("SELECT user_login FROM review_comments WHERE owner = 'nineinchnick' AND repo = 'trino-openapi' AND id = 660141310",
                "VALUES ('nineinchnick')");
         */
    }

    @Test
    public void selectFromGithubActionsTable()
    {
        assertQuery("SELECT name FROM repos_actions_workflows WHERE owner = 'nineinchnick' AND repo = 'trino-openapi' AND name = 'Release with Maven'",
                "VALUES ('Release with Maven')");
        assertQuery("SELECT name FROM repos_actions_runs WHERE owner = 'nineinchnick' AND repo = 'trino-openapi' AND name = 'Release with Maven' LIMIT 1",
                "VALUES ('Release with Maven')");

        QueryRunner runner = getQueryRunner();
        long runId = (long) runner.execute("SELECT id FROM repos_actions_runs WHERE owner = 'nineinchnick' AND repo = 'trino-openapi' AND name = 'Release with Maven' ORDER BY created_at DESC LIMIT 1").getOnlyValue();
        assertThat(runId).isGreaterThan(0);
        long jobId = (long) runner.execute(format("SELECT id FROM repos_actions_runs_jobs WHERE owner = 'nineinchnick' AND repo = 'trino-openapi' AND run_id = %d LIMIT 1", runId)).getOnlyValue();
        assertThat(jobId).isGreaterThan(0);
        // can't check results, since currently no jobs produce artifacts
        assertQuerySucceeds(format("SELECT owner FROM repos_actions_runs_artifacts WHERE owner = 'nineinchnick' AND repo = 'trino-openapi' AND run_id = %d", runId));
    }

    @Test
    public void selectJoinDynamicFilter()
    {
        /*
        assertQuery("WITH " +
                        "r AS (SELECT * FROM repos WHERE owner_login = 'nineinchnick' AND name IN ('trino-git', 'trino-openapi')) " +
                        "SELECT count(*) > 0 " +
                        "FROM r " +
                        "JOIN workflows w ON w.owner = r.owner_login AND w.repo = r.name",
                "VALUES (true)");
         */
        assertQuery("SELECT count(*) > 0 " +
                        "FROM repos_actions_workflows w " +
                        "JOIN repos_actions_runs r ON r.workflow_id = w.id " +
                        "WHERE w.owner = 'nineinchnick' AND w.repo = 'trino-openapi' " +
                        "AND r.owner = 'nineinchnick' AND r.repo = 'trino-openapi'",
                "VALUES (true)");
    }
}
