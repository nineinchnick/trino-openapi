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

import com.google.common.collect.ImmutableMap;
import io.trino.sql.planner.plan.TableScanNode;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.QueryRunner;
import io.trino.testing.QueryRunner.MaterializedResultWithPlan;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;

import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static io.trino.sql.planner.optimizations.PlanNodeSearcher.searchFrom;
import static java.lang.String.format;
import static java.time.format.DateTimeFormatter.ofPattern;
import static java.util.Objects.requireNonNullElse;
import static org.assertj.core.api.Assertions.assertThat;

@EnabledIfEnvironmentVariable(named = "GITHUB_TOKEN", matches = ".+")
@Tag("live-api")
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
        List<String> expectedTables = List.of(
                "advisories",
                "agents_automations",
                "agents_automations_available_tools",
                "agents_automations_available_triggers",
                "agents_automations_tasks",
                "agents_repos_automations",
                "agents_repos_tasks",
                "agents_tasks",
                "app",
                "app_hook_config",
                "app_hook_deliveries",
                "app_installation_requests",
                "app_installations",
                "applications_token",
                "applications_token_scoped",
                "apps",
                "codes_of_conduct",
                "emojis",
                "enterprises_actions_cache_retention_limit",
                "enterprises_actions_cache_storage_limit",
                "enterprises_actions_oidc_customization_properties_repo",
                "enterprises_code_security_configurations",
                "enterprises_code_security_configurations_defaults",
                "enterprises_code_security_configurations_repositories",
                "enterprises_copilot_metrics_reports_enterprise_1_day",
                "enterprises_copilot_metrics_reports_enterprise_28_day_latest",
                "enterprises_copilot_metrics_reports_repos_1_day",
                "enterprises_copilot_metrics_reports_user_teams_1_day",
                "enterprises_copilot_metrics_reports_users_1_day",
                "enterprises_copilot_metrics_reports_users_28_day_latest",
                "enterprises_dependabot_alerts",
                "enterprises_dependabot_repository_access",
                "events",
                "feeds",
                "gists_comments",
                "gists_commits",
                "gists_forks",
                "gists_gist_id",
                "gists_gist_id_sha",
                "gists_public",
                "gists_starred",
                "gitignore_templates",
                "installation_repositories",
                "licenses",
                "marketplace_listing_accounts",
                "marketplace_listing_plans",
                "marketplace_listing_plans_accounts",
                "marketplace_listing_stubbed_accounts",
                "marketplace_listing_stubbed_plans",
                "marketplace_listing_stubbed_plans_accounts",
                "meta",
                "networks_events",
                "notifications",
                "notifications_threads",
                "notifications_threads_subscription",
                "organizations",
                "organizations_actions_cache_retention_limit",
                "organizations_actions_cache_storage_limit",
                "organizations_settings_billing_ai_credit_usage",
                "organizations_settings_billing_budgets",
                "organizations_settings_billing_premium_request_usage",
                "organizations_settings_billing_usage",
                "organizations_settings_billing_usage_summary",
                "orgs_actions_cache_usage",
                "orgs_actions_cache_usage_by_repository",
                "orgs_actions_hosted_runners",
                "orgs_actions_hosted_runners_images_custom",
                "orgs_actions_hosted_runners_images_custom_versions",
                "orgs_actions_hosted_runners_images_github_owned",
                "orgs_actions_hosted_runners_images_partner",
                "orgs_actions_hosted_runners_limits",
                "orgs_actions_hosted_runners_machine_sizes",
                "orgs_actions_hosted_runners_platforms",
                "orgs_actions_oidc_customization_properties_repo",
                "orgs_actions_oidc_customization_sub",
                "orgs_actions_permissions",
                "orgs_actions_permissions_artifact_and_log_retention",
                "orgs_actions_permissions_fork_pr_contributor_approval",
                "orgs_actions_permissions_fork_pr_workflows_private_repos",
                "orgs_actions_permissions_repositories",
                "orgs_actions_permissions_selected_actions",
                "orgs_actions_permissions_self_hosted_runners",
                "orgs_actions_permissions_self_hosted_runners_repositories",
                "orgs_actions_permissions_workflow",
                "orgs_actions_policies",
                "orgs_actions_runner_groups",
                "orgs_actions_runner_groups_hosted_runners",
                "orgs_actions_runner_groups_repositories",
                "orgs_actions_runner_groups_runners",
                "orgs_actions_runners",
                "orgs_actions_runners_deprecations",
                "orgs_actions_runners_downloads",
                "orgs_actions_runners_labels",
                "orgs_actions_secrets",
                "orgs_actions_secrets_public_key",
                "orgs_actions_secrets_repositories",
                "orgs_actions_variables",
                "orgs_actions_variables_repositories",
                "orgs_agents_secrets",
                "orgs_agents_secrets_public_key",
                "orgs_agents_secrets_repositories",
                "orgs_agents_variables",
                "orgs_agents_variables_repositories",
                "orgs_artifacts_metadata_deployment_record",
                "orgs_artifacts_metadata_deployment_record_cluster",
                "orgs_artifacts_metadata_deployment_record_cluster_jobs",
                "orgs_artifacts_metadata_deployment_records",
                "orgs_artifacts_metadata_storage_record",
                "orgs_artifacts_metadata_storage_records",
                "orgs_attestations",
                "orgs_attestations_bulk_list",
                "orgs_attestations_repositories",
                "orgs_blocks",
                "orgs_campaigns",
                "orgs_code_scanning_ai_scan",
                "orgs_code_scanning_alerts",
                "orgs_code_security_configurations",
                "orgs_code_security_configurations_defaults",
                "orgs_code_security_configurations_repositories",
                "orgs_codespaces",
                "orgs_codespaces_secrets",
                "orgs_codespaces_secrets_public_key",
                "orgs_codespaces_secrets_repositories",
                "orgs_copilot_billing",
                "orgs_copilot_billing_seats",
                "orgs_copilot_billing_selected_teams",
                "orgs_copilot_billing_selected_users",
                "orgs_copilot_coding_agent_permissions",
                "orgs_copilot_coding_agent_permissions_repositories",
                "orgs_copilot_content_exclusion",
                "orgs_copilot_metrics_reports_organization_1_day",
                "orgs_copilot_metrics_reports_organization_28_day_latest",
                "orgs_copilot_metrics_reports_repos_1_day",
                "orgs_copilot_metrics_reports_user_teams_1_day",
                "orgs_copilot_metrics_reports_users_1_day",
                "orgs_copilot_metrics_reports_users_28_day_latest",
                "orgs_copilot_spaces",
                "orgs_copilot_spaces_collaborators",
                "orgs_copilot_spaces_resources",
                "orgs_dependabot_alerts",
                "orgs_dependabot_repository_access",
                "orgs_dependabot_secrets",
                "orgs_dependabot_secrets_public_key",
                "orgs_dependabot_secrets_repositories",
                "orgs_docker_conflicts",
                "orgs_events",
                "orgs_failed_invitations",
                "orgs_hooks",
                "orgs_hooks_config",
                "orgs_hooks_deliveries",
                "issues",
                "orgs",
                "orgs_insights_api_route_stats",
                "orgs_insights_api_subject_stats",
                "orgs_insights_api_summary_stats",
                "orgs_insights_api_summary_stats_users",
                "orgs_insights_api_time_stats",
                "orgs_insights_api_time_stats_users",
                "orgs_insights_api_user_stats",
                "orgs_installation",
                "orgs_installations",
                "orgs_interaction_limits",
                "orgs_interaction_limits_pulls_creation_cap",
                "orgs_invitations",
                "orgs_invitations_teams",
                "orgs_issue_fields",
                "orgs_issue_types",
                "orgs_issues",
                "orgs_members",
                "orgs_members_codespaces",
                "orgs_members_codespaces_stop",
                "orgs_members_copilot",
                "orgs_memberships",
                "orgs_migrations",
                "orgs_migrations_repositories",
                "orgs_organization_roles",
                "orgs_organization_roles_teams",
                "orgs_organization_roles_users",
                "orgs_outside_collaborators",
                "orgs_packages",
                "orgs_packages_versions",
                "orgs_personal_access_token_requests",
                "orgs_personal_access_token_requests_repositories",
                "orgs_personal_access_tokens",
                "orgs_personal_access_tokens_repositories",
                "orgs_private_registries",
                "orgs_private_registries_public_key",
                "orgs_projects_v2",
                "orgs_projects_v2_fields",
                "orgs_projects_v2_items",
                "orgs_projects_v2_views_items",
                "orgs_properties_installations",
                "orgs_properties_installations_schema",
                "orgs_properties_schema",
                "orgs_properties_values",
                "orgs_public_members",
                "orgs_repos",
                "orgs_rulesets",
                "orgs_rulesets_history",
                "orgs_rulesets_rule_suites",
                "orgs_secret_scanning_alerts",
                "orgs_secret_scanning_custom_patterns",
                "orgs_secret_scanning_pattern_configurations",
                "orgs_security_advisories",
                "orgs_settings_immutable_releases",
                "orgs_settings_immutable_releases_repositories",
                "orgs_settings_network_configurations",
                "orgs_settings_network_settings",
                "orgs_teams",
                "orgs_teams_invitations",
                "orgs_teams_members",
                "orgs_teams_memberships",
                "orgs_teams_repos",
                "orgs_teams_teams",
                "rate_limit",
                "repos",
                "repos_actions_artifacts",
                "repos_actions_cache_retention_limit",
                "repos_actions_cache_storage_limit",
                "repos_actions_cache_usage",
                "repos_actions_caches",
                "repos_actions_concurrency_groups",
                "repos_actions_jobs",
                "repos_actions_oidc_customization_sub",
                "repos_actions_organization_secrets",
                "repos_actions_organization_variables",
                "repos_actions_permissions",
                "repos_actions_permissions_access",
                "repos_actions_permissions_artifact_and_log_retention",
                "repos_actions_permissions_fork_pr_contributor_approval",
                "repos_actions_permissions_fork_pr_workflows_private_repos",
                "repos_actions_permissions_selected_actions",
                "repos_actions_permissions_workflow",
                "repos_actions_policies",
                "repos_actions_runners",
                "repos_actions_runners_deprecations",
                "repos_actions_runners_downloads",
                "repos_actions_runners_labels",
                "repos_actions_runs",
                "repos_actions_runs_approvals",
                "repos_actions_runs_artifacts",
                "repos_actions_runs_attempts",
                "repos_actions_runs_attempts_jobs",
                "repos_actions_runs_concurrency_groups",
                "repos_actions_runs_jobs",
                "repos_actions_runs_pending_deployments",
                "repos_actions_runs_timing",
                "repos_actions_secrets",
                "repos_actions_secrets_public_key",
                "repos_actions_variables",
                "repos_actions_workflows",
                "repos_actions_workflows_dispatches",
                "repos_actions_workflows_runs",
                "repos_actions_workflows_timing",
                "repos_activity",
                "repos_agents_organization_secrets",
                "repos_agents_organization_variables",
                "repos_agents_secrets",
                "repos_agents_secrets_public_key",
                "repos_agents_variables",
                "repos_assignees",
                "repos_attestations",
                "repos_autolinks",
                "repos_automated_security_fixes",
                "repos_branches",
                "repos_branches_protection",
                "repos_branches_protection_enforce_admins",
                "repos_branches_protection_required_pull_request_reviews",
                "repos_branches_protection_required_signatures",
                "repos_branches_protection_required_status_checks",
                "repos_branches_protection_required_status_checks_contexts",
                "repos_branches_protection_restrictions",
                "repos_branches_protection_restrictions_apps",
                "repos_branches_protection_restrictions_teams",
                "repos_branches_protection_restrictions_users",
                "repos_check_runs",
                "repos_check_runs_annotations",
                "repos_check_suites",
                "repos_check_suites_check_runs",
                "repos_check_suites_preferences",
                "repos_code_quality_findings",
                "repos_code_quality_setup",
                "repos_code_scanning_ai_scan",
                "repos_code_scanning_alerts",
                "repos_code_scanning_alerts_autofix",
                "repos_code_scanning_alerts_instances",
                "repos_code_scanning_analyses",
                "repos_code_scanning_codeql_databases",
                "repos_code_scanning_codeql_variant_analyses",
                "repos_code_scanning_codeql_variant_analyses_repos",
                "repos_code_scanning_default_setup",
                "repos_code_scanning_sarifs",
                "repos_code_security_configuration",
                "repos_codeowners_errors",
                "repos_codespaces",
                "repos_codespaces_devcontainers",
                "repos_codespaces_machines",
                "repos_codespaces_new",
                "repos_codespaces_permissions_check",
                "repos_codespaces_secrets",
                "repos_codespaces_secrets_public_key",
                "repos_collaborators",
                "repos_collaborators_permission",
                "repos_comments",
                "repos_comments_reactions",
                "repos_commits",
                "repos_commits_branches_where_head",
                "repos_commits_check_runs",
                "repos_commits_check_suites",
                "repos_commits_comments",
                "repos_commits_pulls",
                "repos_commits_status",
                "repos_commits_statuses",
                "repos_community_profile",
                "repos_compare",
                "repos_contents",
                "repos_contributors",
                "repos_copilot_cloud_agent_configuration",
                "repos_dependabot_alerts",
                "repos_dependabot_secrets",
                "repos_dependabot_secrets_public_key",
                "repos_dependency_graph_compare",
                "repos_deployments",
                "repos_deployments_statuses",
                "repos_environments",
                "repos_environments_deployment_branch_policies",
                "repos_environments_deployment_protection_rules",
                "repos_environments_deployment_protection_rules_apps",
                "repos_environments_secrets",
                "repos_environments_secrets_public_key",
                "repos_environments_variables",
                "repos_events",
                "repos_forks",
                "repos_git_blobs",
                "repos_git_commits",
                "repos_git_matching_refs",
                "repos_git_ref",
                "repos_git_refs",
                "repos_git_tags",
                "repos_git_trees",
                "repos_hash_algorithm",
                "repos_hooks",
                "repos_hooks_config",
                "repos_hooks_deliveries",
                "repos_immutable_releases",
                "repos_installation",
                "repos_interaction_limits",
                "repos_interaction_limits_pulls_bypass_list",
                "repos_interaction_limits_pulls_creation_cap",
                "repos_invitations",
                "repos_issue_types",
                "repos_issues",
                "repos_issues_assignees",
                "repos_issues_comments_pin",
                "repos_issues_comments_reactions",
                "repos_issues_dependencies_blocked_by",
                "repos_issues_dependencies_blocking",
                "repos_issues_issue_field_values",
                "repos_issues_labels",
                "repos_issues_parent",
                "repos_issues_reactions",
                "repos_issues_relates_to",
                "repos_issues_sub_issue",
                "repos_issues_sub_issues",
                "repos_issues_sub_issues_priority",
                "repos_issues_suggestions",
                "repos_issues_suggestions_approve",
                "repos_issues_suggestions_dismiss",
                "repos_issues_timeline",
                "repos_keys",
                "repos_labels",
                "repos_languages",
                "repos_license",
                "repos_merge_upstream",
                "repos_milestones",
                "repos_milestones_labels",
                "repos_notifications",
                "repos_owner_repo_issues_comments_comment_id",
                "repos_owner_repo_issues_events_event_id",
                "repos_owner_repo_issues_issue_number_comments",
                "repos_owner_repo_issues_issue_number_events",
                "repos_owner_repo_pulls_comments_comment_id",
                "repos_owner_repo_pulls_pull_number_comments",
                "repos_pages",
                "repos_pages_builds",
                "repos_pages_builds_latest",
                "repos_pages_deployments",
                "repos_pages_health",
                "repos_private_vulnerability_reporting",
                "repos_properties_values",
                "repos_pulls",
                "repos_pulls_comments_reactions",
                "repos_pulls_commits",
                "repos_pulls_files",
                "repos_pulls_merge",
                "repos_pulls_merge_async",
                "repos_pulls_requested_reviewers",
                "repos_pulls_reviews",
                "repos_pulls_reviews_comments",
                "repos_pulls_reviews_dismissals",
                "repos_pulls_reviews_events",
                "repos_readme",
                "repos_releases",
                "repos_releases_assets",
                "repos_releases_generate_notes",
                "repos_releases_latest",
                "repos_releases_reactions",
                "repos_releases_tags",
                "repos_rules_branches",
                "repos_rulesets",
                "repos_rulesets_history",
                "repos_rulesets_rule_suites",
                "repos_secret_scanning_alerts",
                "repos_secret_scanning_alerts_locations",
                "repos_secret_scanning_custom_patterns",
                "repos_secret_scanning_push_protection_bypasses",
                "repos_secret_scanning_scan_history",
                "repos_security_advisories",
                "repos_stacks",
                "repos_stacks_add",
                "repos_stacks_unstack",
                "repos_stargazers",
                "repos_stargazers_count",
                "repos_stargazers_history",
                "repos_stats_code_frequency",
                "repos_stats_commit_activity",
                "repos_stats_contributors",
                "repos_stats_participation",
                "repos_stats_punch_card",
                "repos_subscribers",
                "repos_subscription",
                "repos_tags",
                "repos_teams",
                "repos_topics",
                "repos_traffic_clones",
                "repos_traffic_popular_paths",
                "repos_traffic_popular_referrers",
                "repos_traffic_views",
                "repositories",
                "search_code",
                "search_commits",
                "search_labels",
                "search_issues",
                "search_repositories",
                "search_topics",
                "search_users",
                "user",
                "user_blocks",
                "user_codespaces",
                "user_codespaces_exports",
                "user_codespaces_machines",
                "user_codespaces_secrets",
                "user_codespaces_secrets_public_key",
                "user_codespaces_secrets_repositories",
                "user_codespaces_start",
                "user_codespaces_stop",
                "user_docker_conflicts",
                "user_email_visibility",
                "user_emails",
                "user_followers",
                "user_following",
                "user_gpg_keys",
                "user_installations",
                "user_installations_repositories",
                "user_interaction_limits",
                "user_issues",
                "user_keys",
                "user_marketplace_purchases",
                "user_marketplace_purchases_stubbed",
                "user_memberships_orgs",
                "user_migrations",
                "user_migrations_repositories",
                "user_orgs",
                "user_packages",
                "user_packages_versions",
                "user_public_emails",
                "user_repos",
                "user_repository_invitations",
                "user_social_accounts",
                "user_ssh_signing_keys",
                "user_starred",
                "user_subscriptions",
                "user_teams",
                "users",
                "users_attestations",
                "users_attestations_bulk_list",
                "users_copilot_spaces",
                "users_copilot_spaces_collaborators",
                "users_copilot_spaces_resources",
                "users_docker_conflicts",
                "users_events",
                "users_events_orgs",
                "users_events_public",
                "users_followers",
                "users_following",
                "users_gists",
                "users_gpg_keys",
                "users_hovercard",
                "users_installation",
                "users_keys",
                "users_orgs",
                "users_packages",
                "users_packages_versions",
                "users_projects_v2",
                "users_projects_v2_fields",
                "users_projects_v2_items",
                "users_projects_v2_views_items",
                "users_received_events",
                "users_received_events_public",
                "users_repos",
                "users_settings_billing_ai_credit_usage",
                "users_settings_billing_premium_request_usage",
                "users_settings_billing_usage",
                "users_settings_billing_usage_summary",
                "users_social_accounts",
                "users_ssh_signing_keys",
                "users_starred",
                "users_subscriptions",
                "versions");
        assertQuery("SHOW TABLES FROM github.default", "VALUES '" + String.join("', '", expectedTables) + "'");
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
        String pulls = "SELECT updated_at FROM repos_pulls WHERE owner = 'nineinchnick' AND repo = 'trino-openapi' AND state = 'closed'";
        // sorting by two columns is not pushed down
        LocalDateTime sortedByTrino = (LocalDateTime) computeScalar(pulls + " ORDER BY updated_at DESC, number LIMIT 1");
        MaterializedResultWithPlan result = getDistributedQueryRunner().executeWithPlan(getSession(), pulls + " ORDER BY updated_at DESC LIMIT 1");
        // pull requests opened or updated between the queries are more recent, so they can only move the result forward
        assertThat((LocalDateTime) result.result().getOnlyValue()).isAfterOrEqualTo(sortedByTrino);
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
