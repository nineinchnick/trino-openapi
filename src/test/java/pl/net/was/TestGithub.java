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
import io.trino.sql.planner.plan.TableScanNode;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.QueryRunner;
import io.trino.testing.QueryRunner.MaterializedResultWithPlan;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;

import java.time.LocalDateTime;
import java.util.Map;
import java.util.Set;

import static io.trino.sql.planner.optimizations.PlanNodeSearcher.searchFrom;
import static java.lang.String.format;
import static java.time.format.DateTimeFormatter.ofPattern;
import static java.util.Objects.requireNonNullElse;
import static org.assertj.core.api.Assertions.assertThat;

@EnabledIfEnvironmentVariable(named = "GITHUB_TOKEN", matches = ".+")
public class TestGithub
        extends AbstractTestQueryFramework
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        ImmutableMap.Builder<String, String> properties = ImmutableMap.builder();
        properties.putAll(Map.of(
                // a copy of https://github.com/github/rest-api-description/raw/main/descriptions/api.github.com/api.github.com.json with the pagination extension added to list endpoints by dev/specs/github.patch.json
                "spec-location", "specs/github.json",
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
                .add("agents_automations")
                .add("agents_automations_available_tools")
                .add("agents_automations_available_triggers")
                .add("agents_automations_tasks")
                .add("agents_repos_automations")
                .add("agents_repos_tasks")
                .add("agents_tasks")
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
                .add("enterprises_actions_cache_retention_limit")
                .add("enterprises_actions_cache_storage_limit")
                .add("enterprises_actions_oidc_customization_properties_repo")
                .add("enterprises_code_security_configurations")
                .add("enterprises_code_security_configurations_defaults")
                .add("enterprises_code_security_configurations_repositories")
                .add("enterprises_copilot_metrics_reports_enterprise_1_day")
                .add("enterprises_copilot_metrics_reports_enterprise_28_day_latest")
                .add("enterprises_copilot_metrics_reports_repos_1_day")
                .add("enterprises_copilot_metrics_reports_user_teams_1_day")
                .add("enterprises_copilot_metrics_reports_users_1_day")
                .add("enterprises_copilot_metrics_reports_users_28_day_latest")
                .add("enterprises_dependabot_alerts")
                .add("enterprises_dependabot_repository_access")
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
                .add("organizations_actions_cache_retention_limit")
                .add("organizations_actions_cache_storage_limit")
                .add("organizations_settings_billing_ai_credit_usage")
                .add("organizations_settings_billing_budgets")
                .add("organizations_settings_billing_premium_request_usage")
                .add("organizations_settings_billing_usage")
                .add("organizations_settings_billing_usage_summary")
                .add("orgs_actions_cache_usage")
                .add("orgs_actions_cache_usage_by_repository")
                .add("orgs_actions_hosted_runners")
                .add("orgs_actions_hosted_runners_images_custom")
                .add("orgs_actions_hosted_runners_images_custom_versions")
                .add("orgs_actions_hosted_runners_images_github_owned")
                .add("orgs_actions_hosted_runners_images_partner")
                .add("orgs_actions_hosted_runners_limits")
                .add("orgs_actions_hosted_runners_machine_sizes")
                .add("orgs_actions_hosted_runners_platforms")
                .add("orgs_actions_oidc_customization_properties_repo")
                .add("orgs_actions_oidc_customization_sub")
                .add("orgs_actions_permissions")
                .add("orgs_actions_permissions_artifact_and_log_retention")
                .add("orgs_actions_permissions_fork_pr_contributor_approval")
                .add("orgs_actions_permissions_fork_pr_workflows_private_repos")
                .add("orgs_actions_permissions_repositories")
                .add("orgs_actions_permissions_selected_actions")
                .add("orgs_actions_permissions_self_hosted_runners")
                .add("orgs_actions_permissions_self_hosted_runners_repositories")
                .add("orgs_actions_permissions_workflow")
                .add("orgs_actions_policies")
                .add("orgs_actions_runner_groups")
                .add("orgs_actions_runner_groups_hosted_runners")
                .add("orgs_actions_runner_groups_repositories")
                .add("orgs_actions_runner_groups_runners")
                .add("orgs_actions_runners")
                .add("orgs_actions_runners_deprecations")
                .add("orgs_actions_runners_downloads")
                .add("orgs_actions_runners_labels")
                .add("orgs_actions_secrets")
                .add("orgs_actions_secrets_public_key")
                .add("orgs_actions_secrets_repositories")
                .add("orgs_actions_variables")
                .add("orgs_actions_variables_repositories")
                .add("orgs_agents_secrets")
                .add("orgs_agents_secrets_public_key")
                .add("orgs_agents_secrets_repositories")
                .add("orgs_agents_variables")
                .add("orgs_agents_variables_repositories")
                .add("orgs_artifacts_metadata_deployment_record")
                .add("orgs_artifacts_metadata_deployment_record_cluster")
                .add("orgs_artifacts_metadata_deployment_record_cluster_jobs")
                .add("orgs_artifacts_metadata_deployment_records")
                .add("orgs_artifacts_metadata_storage_record")
                .add("orgs_artifacts_metadata_storage_records")
                .add("orgs_attestations")
                .add("orgs_attestations_bulk_list")
                .add("orgs_attestations_repositories")
                .add("orgs_blocks")
                .add("orgs_campaigns")
                .add("orgs_code_scanning_ai_scan")
                .add("orgs_code_scanning_alerts")
                .add("orgs_code_security_configurations")
                .add("orgs_code_security_configurations_defaults")
                .add("orgs_code_security_configurations_repositories")
                .add("orgs_codespaces")
                .add("orgs_codespaces_secrets")
                .add("orgs_codespaces_secrets_public_key")
                .add("orgs_codespaces_secrets_repositories")
                .add("orgs_copilot_billing")
                .add("orgs_copilot_billing_seats")
                .add("orgs_copilot_billing_selected_teams")
                .add("orgs_copilot_billing_selected_users")
                .add("orgs_copilot_coding_agent_permissions")
                .add("orgs_copilot_coding_agent_permissions_repositories")
                .add("orgs_copilot_content_exclusion")
                .add("orgs_copilot_metrics_reports_organization_1_day")
                .add("orgs_copilot_metrics_reports_organization_28_day_latest")
                .add("orgs_copilot_metrics_reports_repos_1_day")
                .add("orgs_copilot_metrics_reports_user_teams_1_day")
                .add("orgs_copilot_metrics_reports_users_1_day")
                .add("orgs_copilot_metrics_reports_users_28_day_latest")
                .add("orgs_copilot_spaces")
                .add("orgs_copilot_spaces_collaborators")
                .add("orgs_copilot_spaces_resources")
                .add("orgs_dependabot_alerts")
                .add("orgs_dependabot_repository_access")
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
                .add("orgs_insights_api_route_stats")
                .add("orgs_insights_api_subject_stats")
                .add("orgs_insights_api_summary_stats")
                .add("orgs_insights_api_summary_stats_users")
                .add("orgs_insights_api_time_stats")
                .add("orgs_insights_api_time_stats_users")
                .add("orgs_insights_api_user_stats")
                .add("orgs_installation")
                .add("orgs_installations")
                .add("orgs_interaction_limits")
                .add("orgs_interaction_limits_pulls_creation_cap")
                .add("orgs_invitations")
                .add("orgs_invitations_teams")
                .add("orgs_issue_fields")
                .add("orgs_issue_types")
                .add("orgs_issues")
                .add("orgs_members")
                .add("orgs_members_codespaces")
                .add("orgs_members_codespaces_stop")
                .add("orgs_members_copilot")
                .add("orgs_memberships")
                .add("orgs_migrations")
                .add("orgs_migrations_repositories")
                .add("orgs_organization_roles")
                .add("orgs_organization_roles_teams")
                .add("orgs_organization_roles_users")
                .add("orgs_outside_collaborators")
                .add("orgs_packages")
                .add("orgs_packages_versions")
                .add("orgs_personal_access_token_requests")
                .add("orgs_personal_access_token_requests_repositories")
                .add("orgs_personal_access_tokens")
                .add("orgs_personal_access_tokens_repositories")
                .add("orgs_private_registries")
                .add("orgs_private_registries_public_key")
                .add("orgs_projects_v2")
                .add("orgs_projects_v2_fields")
                .add("orgs_projects_v2_items")
                .add("orgs_projects_v2_views_items")
                .add("orgs_properties_installations")
                .add("orgs_properties_installations_schema")
                .add("orgs_properties_schema")
                .add("orgs_properties_values")
                .add("orgs_public_members")
                .add("orgs_repos")
                .add("orgs_rulesets")
                .add("orgs_rulesets_history")
                .add("orgs_rulesets_rule_suites")
                .add("orgs_secret_scanning_alerts")
                .add("orgs_secret_scanning_custom_patterns")
                .add("orgs_secret_scanning_pattern_configurations")
                .add("orgs_security_advisories")
                .add("orgs_settings_immutable_releases")
                .add("orgs_settings_immutable_releases_repositories")
                .add("orgs_settings_network_configurations")
                .add("orgs_settings_network_settings")
                .add("orgs_teams")
                .add("orgs_teams_invitations")
                .add("orgs_teams_members")
                .add("orgs_teams_memberships")
                .add("orgs_teams_repos")
                .add("orgs_teams_teams")
                .add("rate_limit")
                .add("repos")
                .add("repos_actions_artifacts")
                .add("repos_actions_cache_retention_limit")
                .add("repos_actions_cache_storage_limit")
                .add("repos_actions_cache_usage")
                .add("repos_actions_caches")
                .add("repos_actions_concurrency_groups")
                .add("repos_actions_jobs")
                .add("repos_actions_oidc_customization_sub")
                .add("repos_actions_organization_secrets")
                .add("repos_actions_organization_variables")
                .add("repos_actions_permissions")
                .add("repos_actions_permissions_access")
                .add("repos_actions_permissions_artifact_and_log_retention")
                .add("repos_actions_permissions_fork_pr_contributor_approval")
                .add("repos_actions_permissions_fork_pr_workflows_private_repos")
                .add("repos_actions_permissions_selected_actions")
                .add("repos_actions_permissions_workflow")
                .add("repos_actions_policies")
                .add("repos_actions_runners")
                .add("repos_actions_runners_deprecations")
                .add("repos_actions_runners_downloads")
                .add("repos_actions_runners_labels")
                .add("repos_actions_runs")
                .add("repos_actions_runs_approvals")
                .add("repos_actions_runs_artifacts")
                .add("repos_actions_runs_attempts")
                .add("repos_actions_runs_attempts_jobs")
                .add("repos_actions_runs_concurrency_groups")
                .add("repos_actions_runs_jobs")
                .add("repos_actions_runs_pending_deployments")
                .add("repos_actions_runs_timing")
                .add("repos_actions_secrets")
                .add("repos_actions_secrets_public_key")
                .add("repos_actions_variables")
                .add("repos_actions_workflows")
                .add("repos_actions_workflows_dispatches")
                .add("repos_actions_workflows_runs")
                .add("repos_actions_workflows_timing")
                .add("repos_activity")
                .add("repos_agents_organization_secrets")
                .add("repos_agents_organization_variables")
                .add("repos_agents_secrets")
                .add("repos_agents_secrets_public_key")
                .add("repos_agents_variables")
                .add("repos_assignees")
                .add("repos_attestations")
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
                .add("repos_code_quality_findings")
                .add("repos_code_quality_setup")
                .add("repos_code_scanning_ai_scan")
                .add("repos_code_scanning_alerts")
                .add("repos_code_scanning_alerts_autofix")
                .add("repos_code_scanning_alerts_instances")
                .add("repos_code_scanning_analyses")
                .add("repos_code_scanning_codeql_databases")
                .add("repos_code_scanning_codeql_variant_analyses")
                .add("repos_code_scanning_codeql_variant_analyses_repos")
                .add("repos_code_scanning_default_setup")
                .add("repos_code_scanning_sarifs")
                .add("repos_code_security_configuration")
                .add("repos_codeowners_errors")
                .add("repos_codespaces")
                .add("repos_codespaces_devcontainers")
                .add("repos_codespaces_machines")
                .add("repos_codespaces_new")
                .add("repos_codespaces_permissions_check")
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
                .add("repos_copilot_cloud_agent_configuration")
                .add("repos_dependabot_alerts")
                .add("repos_dependabot_secrets")
                .add("repos_dependabot_secrets_public_key")
                .add("repos_dependency_graph_compare")
                .add("repos_deployments")
                .add("repos_deployments_statuses")
                .add("repos_environments")
                .add("repos_environments_deployment_branch_policies")
                .add("repos_environments_deployment_protection_rules")
                .add("repos_environments_deployment_protection_rules_apps")
                .add("repos_environments_secrets")
                .add("repos_environments_secrets_public_key")
                .add("repos_environments_variables")
                .add("repos_events")
                .add("repos_forks")
                .add("repos_git_blobs")
                .add("repos_git_commits")
                .add("repos_git_matching_refs")
                .add("repos_git_ref")
                .add("repos_git_refs")
                .add("repos_git_tags")
                .add("repos_git_trees")
                .add("repos_hash_algorithm")
                .add("repos_hooks")
                .add("repos_hooks_config")
                .add("repos_hooks_deliveries")
                .add("repos_immutable_releases")
                .add("repos_installation")
                .add("repos_interaction_limits")
                .add("repos_interaction_limits_pulls_bypass_list")
                .add("repos_interaction_limits_pulls_creation_cap")
                .add("repos_invitations")
                .add("repos_issue_types")
                .add("repos_issues")
                .add("repos_issues_assignees")
                .add("repos_issues_comments_pin")
                .add("repos_issues_comments_reactions")
                .add("repos_issues_dependencies_blocked_by")
                .add("repos_issues_dependencies_blocking")
                .add("repos_issues_issue_field_values")
                .add("repos_issues_labels")
                .add("repos_issues_parent")
                .add("repos_issues_reactions")
                .add("repos_issues_relates_to")
                .add("repos_issues_sub_issue")
                .add("repos_issues_sub_issues")
                .add("repos_issues_sub_issues_priority")
                .add("repos_issues_suggestions")
                .add("repos_issues_suggestions_approve")
                .add("repos_issues_suggestions_dismiss")
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
                .add("repos_pages_deployments")
                .add("repos_pages_health")
                .add("repos_private_vulnerability_reporting")
                .add("repos_properties_values")
                .add("repos_pulls")
                .add("repos_pulls_comments_reactions")
                .add("repos_pulls_commits")
                .add("repos_pulls_files")
                .add("repos_pulls_merge")
                .add("repos_pulls_merge_async")
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
                .add("repos_rulesets_history")
                .add("repos_rulesets_rule_suites")
                .add("repos_secret_scanning_alerts")
                .add("repos_secret_scanning_alerts_locations")
                .add("repos_secret_scanning_custom_patterns")
                .add("repos_secret_scanning_push_protection_bypasses")
                .add("repos_secret_scanning_scan_history")
                .add("repos_security_advisories")
                .add("repos_stacks")
                .add("repos_stacks_add")
                .add("repos_stacks_unstack")
                .add("repos_stargazers")
                .add("repos_stargazers_count")
                .add("repos_stargazers_history")
                .add("repos_stats_code_frequency")
                .add("repos_stats_commit_activity")
                .add("repos_stats_contributors")
                .add("repos_stats_participation")
                .add("repos_stats_punch_card")
                .add("repos_subscribers")
                .add("repos_subscription")
                .add("repos_tags")
                .add("repos_teams")
                .add("repos_topics")
                .add("repos_traffic_clones")
                .add("repos_traffic_popular_paths")
                .add("repos_traffic_popular_referrers")
                .add("repos_traffic_views")
                .add("repositories")
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
                .add("users_attestations")
                .add("users_attestations_bulk_list")
                .add("users_copilot_spaces")
                .add("users_copilot_spaces_collaborators")
                .add("users_copilot_spaces_resources")
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
                .add("users_projects_v2")
                .add("users_projects_v2_fields")
                .add("users_projects_v2_items")
                .add("users_projects_v2_views_items")
                .add("users_received_events")
                .add("users_received_events_public")
                .add("users_repos")
                .add("users_settings_billing_ai_credit_usage")
                .add("users_settings_billing_premium_request_usage")
                .add("users_settings_billing_usage")
                .add("users_settings_billing_usage_summary")
                .add("users_social_accounts")
                .add("users_ssh_signing_keys")
                .add("users_starred")
                .add("users_subscriptions")
                .add("versions");
        assertQuery("SHOW TABLES FROM github.default", "VALUES '" + String.join("', '", expectedTables.build()) + "'");
    }

    @Test
    public void selectFromTable()
    {
        assertQuery(
                "SELECT login FROM orgs WHERE org = 'trinodb'",
                "VALUES ('trinodb')");
        // TODO when merging endpoints into one table, the required parameters should be marked optional, but should be used to make a decision which endpoint to call
        /*
        assertQuery("SELECT login FROM users WHERE username = 'nineinchnick'",
                "VALUES ('nineinchnick')");
        assertQuery("SELECT name FROM repos WHERE owner_req = 'nineinchnick' and name = 'trino-openapi'",
                "VALUES ('trino-openapi')");
         */
        assertQuery(
                "SELECT login FROM orgs_members WHERE org = 'trinodb' AND login = 'martint'",
                "VALUES ('martint')");
        assertQuery(
                "SELECT login FROM repos_collaborators WHERE owner = 'nineinchnick' AND repo = 'trino-openapi' AND login = 'nineinchnick'",
                "VALUES ('nineinchnick')");
        assertQuery(
                "SELECT message FROM repos_git_commits WHERE owner = 'nineinchnick' AND repo = 'trino-openapi' AND commit_sha = '293cca50a6e7b508f61b7270988d8444d3b736c3'",
                "VALUES ('Initial commit')");
        assertQuery(
                "SELECT title FROM repos_issues WHERE owner = 'nineinchnick' AND repo = 'trino-openapi' AND issue_number = 5 AND number = 5",
                "VALUES ('Improve predicate pushdown operator support')");
        assertQuery(
                "SELECT user.login FROM repos_owner_repo_issues_issue_number_comments WHERE owner = 'nineinchnick' AND repo = 'trino-openapi' AND issue_number = 5 AND id = 2970234636",
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
        assertQuery(
                "SELECT name FROM repos_actions_workflows WHERE owner = 'nineinchnick' AND repo = 'trino-openapi' AND name = 'Release with Maven'",
                "VALUES ('Release with Maven')");
        assertQuery(
                "SELECT name FROM repos_actions_runs WHERE owner = 'nineinchnick' AND repo = 'trino-openapi' AND name = 'Release with Maven' LIMIT 1",
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
    public void selectPaginatedTables()
    {
        // without pagination, only the first page of 30 results would be returned
        assertQuery("SELECT count(*) > 30 FROM orgs_repos WHERE org = 'trinodb'", "VALUES true");
        assertQuery("SELECT count(*) > 30 FROM repos_contributors WHERE owner = 'trinodb' AND repo = 'trino-python-client'", "VALUES true");
        assertQuery("SELECT count(*) > 30 FROM repos_issues WHERE owner = 'nineinchnick' AND repo = 'trino-openapi' AND state = 'closed'", "VALUES true");
        assertQuery("SELECT count(*) > 30 FROM repos_pulls WHERE owner = 'nineinchnick' AND repo = 'trino-openapi' AND state = 'closed'", "VALUES true");
        assertQuery("SELECT count(*) > 30 FROM repos_releases WHERE owner = 'nineinchnick' AND repo = 'trino-openapi'", "VALUES true");
        assertQuery("SELECT count(*) > 30 FROM repos_tags WHERE owner = 'nineinchnick' AND repo = 'trino-openapi'", "VALUES true");
        assertQuery("SELECT count(*) > 30 FROM users_repos WHERE username = 'nineinchnick'", "VALUES true");
    }

    @Test
    public void selectWithParameterDefaults()
    {
        // without the state=all default, GitHub would return only open pull requests, 30 on every page
        assertQuery("SELECT bool_or(state = 'closed'), min(per_page), max(per_page) FROM repos_pulls WHERE owner = 'nineinchnick' AND repo = 'trino-openapi'", "VALUES (true, 100, 100)");
    }

    @Test
    public void selectWithErrorMessage()
    {
        // the API responds with 404 to a missing repository, which returns no rows, but with 422 to a missing commit
        assertQueryFails(
                "SELECT sha FROM repos_commits WHERE owner = 'nineinchnick' AND repo = 'trino-openapi' AND ref = 'nonexistent-ref'",
                "Request GET https://api\\.github\\.com/repos/nineinchnick/trino-openapi/commits/nonexistent-ref for operation repos/get-commit failed with HTTP 422: \"No commit found for SHA: nonexistent-ref\"");
    }

    @Test
    public void selectTopNSortedByApi()
    {
        // the path to get a single pull request can't filter by state, but the connector would filter its rows, so the filter doesn't prevent the pushdown
        String pulls = "SELECT number FROM repos_pulls WHERE owner = 'nineinchnick' AND repo = 'trino-openapi' AND state = 'all'";
        MaterializedResultWithPlan result = getDistributedQueryRunner().executeWithPlan(getSession(), pulls + " ORDER BY updated_at DESC LIMIT 1");
        assertThat(result.result().getRowCount()).isEqualTo(1);
        // sorting by two columns is not pushed down
        assertThat(result.result().getOnlyColumnAsSet())
                .isEqualTo(computeActual(pulls + " ORDER BY updated_at DESC, number LIMIT 1").getOnlyColumnAsSet());
        // the table has an INSERT operation, so updated_at allows nulls, but every response includes it, so the API sorts the rows
        TableScanNode scan = (TableScanNode) searchFrom(result.queryPlan().orElseThrow().getRoot())
                .whereIsInstanceOfAny(TableScanNode.class)
                .findOnlyElement();
        assertThat(((OpenApiTableHandle) scan.getTable().connectorHandle()).getSortItem()).isPresent();
    }

    @Test
    public void selectRangeFilteredByApi()
    {
        String issues = "SELECT number FROM repos_issues WHERE owner = 'nineinchnick' AND repo = 'trino-openapi' AND state = 'closed'";
        LocalDateTime updatedAt = (LocalDateTime) computeScalar(issues.replace("SELECT number", "SELECT updated_at") + " ORDER BY updated_at DESC OFFSET 2 LIMIT 1");
        String since = updatedAt.format(ofPattern("yyyy-MM-dd HH:mm:ss.SSS"));

        // the bound is sent as the since parameter, and the API must return the issue updated exactly at that time
        Set<Object> filteredByApi = computeActual(format("%s AND updated_at >= TIMESTAMP '%s'", issues, since)).getOnlyColumnAsSet();
        // a function call is not pushed down, so Trino filters all issues
        Set<Object> filteredByTrino = computeActual(format("%s AND date_diff('millisecond', TIMESTAMP '%s', updated_at) >= 0", issues, since)).getOnlyColumnAsSet();
        assertThat(filteredByApi)
                .hasSizeGreaterThanOrEqualTo(3)
                .isEqualTo(filteredByTrino);
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
        assertQuery(
                "SELECT count(*) > 0 " +
                        "FROM repos_actions_workflows w " +
                        "JOIN repos_actions_runs r ON r.workflow_id = w.id " +
                        "WHERE w.owner = 'nineinchnick' AND w.repo = 'trino-openapi' " +
                        "AND r.owner = 'nineinchnick' AND r.repo = 'trino-openapi'",
                "VALUES (true)");
    }
}
