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

package pl.net.was.sync;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.trino.testing.QueryRunner;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.io.TempDir;

import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;
import static pl.net.was.sync.GithubFixtures.OWNER;
import static pl.net.was.sync.GithubFixtures.REPO;
import static pl.net.was.sync.GithubFixtures.REPO_PATH;
import static pl.net.was.sync.GithubFixtures.annotation;
import static pl.net.was.sync.GithubFixtures.artifact;
import static pl.net.was.sync.GithubFixtures.checkRun;
import static pl.net.was.sync.GithubFixtures.checkSuite;
import static pl.net.was.sync.GithubFixtures.collaborator;
import static pl.net.was.sync.GithubFixtures.commit;
import static pl.net.was.sync.GithubFixtures.hoursAgo;
import static pl.net.was.sync.GithubFixtures.issue;
import static pl.net.was.sync.GithubFixtures.issueComment;
import static pl.net.was.sync.GithubFixtures.job;
import static pl.net.was.sync.GithubFixtures.privateRepository;
import static pl.net.was.sync.GithubFixtures.pull;
import static pl.net.was.sync.GithubFixtures.pullDetails;
import static pl.net.was.sync.GithubFixtures.repository;
import static pl.net.was.sync.GithubFixtures.review;
import static pl.net.was.sync.GithubFixtures.reviewComment;
import static pl.net.was.sync.GithubFixtures.run;
import static pl.net.was.sync.GithubFixtures.team;
import static pl.net.was.sync.GithubFixtures.user;
import static pl.net.was.sync.GithubFixtures.workflow;
import static pl.net.was.sync.SyncQueryRunner.TARGET_CATALOG;

@TestInstance(PER_CLASS)
public class TestGithubSync
{
    private static final String TARGET = TARGET_CATALOG + ".github";
    private static final List<String> ALL_STEPS = ImmutableList.of(
            "repos",
            "org_repos",
            "commits",
            "issues",
            "issue_comments",
            "pulls",
            "pull_commits",
            "pull_stats",
            "review_comments",
            "reviews",
            "workflows",
            "runs",
            "jobs",
            "steps",
            "artifacts",
            "check_suites",
            "check_runs",
            "check_run_annotations",
            "teams",
            "members",
            "collaborators");

    private static final List<String> TABLES = ALL_STEPS.stream()
            .filter(step -> !step.equals("org_repos"))
            .collect(toImmutableList());

    private FakeGithubServer server;
    private QueryRunner queryRunner;

    @BeforeAll
    public void setUp()
            throws Exception
    {
        server = new FakeGithubServer();
        populate(server);
        queryRunner = SyncQueryRunner.createQueryRunner(ImmutableMap.of(
                "spec-location", Path.of("../specs/github.json").toAbsolutePath().toString(),
                "base-uri", server.getUrl(),
                "authentication.type", "none"));
    }

    @AfterAll
    public void tearDown()
    {
        queryRunner.close();
        server.close();
    }

    @Test
    public void testIncrementalSync()
            throws Exception
    {
        assertThat(sync().failedSteps()).isEmpty();

        assertThat(countRows()).containsExactlyInAnyOrderEntriesOf(ImmutableMap.<String, Long>builder()
                .put("repos", 3L)
                .put("commits", 2L)
                .put("issues", 2L)
                .put("issue_comments", 1L)
                .put("pulls", 2L)
                .put("pull_commits", 3L)
                .put("pull_stats", 2L)
                .put("review_comments", 1L)
                .put("reviews", 1L)
                .put("workflows", 1L)
                .put("runs", 2L)
                .put("jobs", 1L)
                .put("steps", 2L)
                .put("artifacts", 1L)
                .put("check_suites", 2L)
                .put("check_runs", 2L)
                .put("check_run_annotations", 2L)
                .put("teams", 1L)
                .put("members", 3L)
                .put("collaborators", 2L)
                .buildOrThrow());
        // the organization repositories include private ones
        assertThat(queryRunner.execute("SELECT name FROM %s.repos WHERE private".formatted(TARGET)).getOnlyValue())
                .isEqualTo("secret");
        assertThat(queryRunner.execute("SELECT label_names FROM %s.pulls WHERE number = 1".formatted(TARGET)).getOnlyValue())
                .isEqualTo(List.of("bug"));
        assertThat(queryRunner.execute("SELECT issue_number FROM %s.issue_comments".formatted(TARGET)).getOnlyValue())
                .isEqualTo(1L);
        assertThat(queryRunner.execute("SELECT additions FROM %s.pull_stats WHERE pull_number = 2".formatted(TARGET)).getOnlyValue())
                .isEqualTo(20L);

        Map<String, Long> rowsAfterFirstSync = countRows();
        server.clearRequests();
        SyncResult secondSync = sync();
        assertThat(secondSync.failedSteps()).isEmpty();

        assertThat(countRows()).isEqualTo(rowsAfterFirstSync);
        assertThat(secondSync.changedRows()).hasSize(ALL_STEPS.size()).allSatisfy((_, changedRows) -> assertThat(changedRows).isZero());
        // cursors are sent as the since parameter, so the second sync reads only the newest rows again
        assertThat(server.getRequests(REPO_PATH + "/issues")).singleElement()
                .satisfies(uri -> assertThat(uri.getQuery()).contains("since=" + hoursAgo(50).replace("Z", ".000Z")));
        assertThat(server.getRequests(REPO_PATH + "/commits")).singleElement()
                .satisfies(uri -> assertThat(uri.getQuery()).contains("since="));
        assertThat(server.getRequests(REPO_PATH + "/issues/comments")).singleElement()
                .satisfies(uri -> assertThat(uri.getQuery()).contains("since="));
        assertThat(server.getRequests(REPO_PATH + "/pulls/comments")).singleElement()
                .satisfies(uri -> assertThat(uri.getQuery()).contains("since="));
        // sorted endpoints stop at the first page without changes
        assertThat(server.getRequests("/users/" + OWNER + "/repos")).hasSize(1);
        assertThat(server.getRequests("/orgs/" + OWNER + "/repos")).singleElement()
                .satisfies(uri -> assertThat(uri.getQuery()).contains("type=all"));
        assertThat(server.getRequests(REPO_PATH + "/pulls")).hasSize(1);
        // runs stop after two pages without changes
        assertThat(server.getRequests(REPO_PATH + "/actions/runs")).hasSize(2);
        // children of checked parents are not read again
        assertThat(server.getRequests()).extracting(URI::getPath)
                .noneMatch(path -> path.matches(".*/(pulls/\\d+.*|actions/runs/\\d+/.*|check-suites/.*|check-runs/.*)"));

        server.upsert(REPO_PATH + "/issues", "id", issue(3, hoursAgo(1)));
        server.upsert(REPO_PATH + "/pulls", "id", pull(2, hoursAgo(1)));
        server.putObject(REPO_PATH + "/pulls/2", pullDetails(2, hoursAgo(1)));
        server.putList(REPO_PATH + "/pulls/2/reviews", List.of(review(32, "CHANGES_REQUESTED")));
        server.upsert(REPO_PATH + "/pulls/2/commits", "sha", commit("d", hoursAgo(1)));
        server.upsert(REPO_PATH + "/actions/runs", "id", run(4, 1, hoursAgo(3)));
        server.putList(REPO_PATH + "/actions/runs/4/jobs", Optional.of("jobs"), Optional.empty(), List.of(job(14, 4, 1, hoursAgo(3))));
        server.putList(REPO_PATH + "/actions/runs/4/artifacts", Optional.of("artifacts"), Optional.empty(), List.of());
        server.putObject(REPO_PATH + "/check-suites/5004", checkSuite(5004));
        server.putList(REPO_PATH + "/check-suites/5004/check-runs", Optional.of("check_runs"), Optional.empty(), List.of(checkRun(74, 5004, 0)));
        server.putList(REPO_PATH + "/collaborators", List.of(collaborator("jane", true)));
        server.clearRequests();
        assertThat(sync().failedSteps()).isEmpty();

        assertThat(queryRunner.execute("SELECT count(*) FROM %s.issues".formatted(TARGET)).getOnlyValue()).isEqualTo(3L);
        assertThat(queryRunner.execute("SELECT count(*) FROM %s.pull_stats WHERE pull_number = 2".formatted(TARGET)).getOnlyValue()).isEqualTo(2L);
        assertThat(queryRunner.execute("SELECT state FROM %s.reviews WHERE pull_number = 2".formatted(TARGET)).getOnlyValue()).isEqualTo("CHANGES_REQUESTED");
        assertThat(queryRunner.execute("SELECT count(*) FROM %s.pull_commits WHERE pull_number = 2".formatted(TARGET)).getOnlyValue()).isEqualTo(3L);
        assertThat(queryRunner.execute("SELECT count(*) FROM %s.jobs".formatted(TARGET)).getOnlyValue()).isEqualTo(2L);
        assertThat(queryRunner.execute("SELECT count(*) FROM %s.check_runs".formatted(TARGET)).getOnlyValue()).isEqualTo(3L);
        assertThat(queryRunner.execute("SELECT login FROM %s.collaborators WHERE removed_at IS NOT NULL".formatted(TARGET)).getOnlyValue()).isEqualTo("john");
        // only the children of the updated pull request and the new run are read
        assertThat(server.getRequests()).extracting(URI::getPath)
                .filteredOn(path -> path.matches(".*/(pulls/\\d+.*|actions/runs/\\d+/.*|check-suites/.*|check-runs/.*)"))
                .containsExactlyInAnyOrder(
                        REPO_PATH + "/pulls/2/commits",
                        REPO_PATH + "/pulls/2",
                        REPO_PATH + "/pulls/2/reviews",
                        REPO_PATH + "/actions/runs/4/jobs",
                        REPO_PATH + "/actions/runs/4/artifacts",
                        REPO_PATH + "/check-suites/5004",
                        REPO_PATH + "/check-suites/5004/check-runs");
    }

    @Test
    public void testFailedStepDoesNotStopOthers(@TempDir Path stepsDir)
            throws Exception
    {
        Files.writeString(stepsDir.resolve("broken.sql"), "CREATE TABLE ${target}.broken AS SELECT * FROM ${target}.missing");
        Files.writeString(stepsDir.resolve("working.sql"),
                """
                CREATE SCHEMA IF NOT EXISTS ${target};
                CREATE TABLE IF NOT EXISTS ${target}.working (id bigint);
                INSERT INTO ${target}.working VALUES (1);
                """);

        SyncResult result = sync(ImmutableMap.of(SyncConfig.STEPS_DIR, stepsDir.toString(), SyncConfig.STEPS, "broken,working", "target", TARGET_CATALOG + ".failures"));

        assertThat(result.failedSteps()).containsExactly("broken");
        assertThat(result.changedRows()).containsExactly(Map.entry("working", 1L));
    }

    private SyncResult sync()
            throws Exception
    {
        return sync(ImmutableMap.of(SyncConfig.STEPS, String.join(",", ALL_STEPS)));
    }

    private SyncResult sync(Map<String, String> overrides)
            throws Exception
    {
        Map<String, String> values = new HashMap<>(Map.of(
                SyncConfig.TRINO_URL, "jdbc:trino://%s:%s".formatted(queryRunner.getCoordinator().getBaseUrl().getHost(), queryRunner.getCoordinator().getBaseUrl().getPort()),
                SyncConfig.TRINO_USER, "test",
                SyncConfig.MAX_RETRIES, "0",
                "source", "github.default",
                "target", TARGET,
                "owner", OWNER,
                "repo", REPO));
        values.putAll(overrides);
        return new Sync(SyncConfig.of(values)).run();
    }

    private Map<String, Long> countRows()
    {
        ImmutableMap.Builder<String, Long> counts = ImmutableMap.builder();
        for (String table : TABLES) {
            counts.put(table, (Long) queryRunner.execute("SELECT count(*) FROM %s.%s".formatted(TARGET, table)).getOnlyValue());
        }
        return counts.buildOrThrow();
    }

    private static void populate(FakeGithubServer server)
    {
        server.putList("/users/" + OWNER + "/repos", List.of(repository(1, REPO, hoursAgo(10)), repository(2, "other", hoursAgo(20))));
        server.putList("/orgs/" + OWNER + "/repos", List.of(privateRepository(3, "secret", hoursAgo(5)), repository(1, REPO, hoursAgo(10)), repository(2, "other", hoursAgo(20))));
        server.putList(REPO_PATH + "/commits", Optional.empty(), Optional.of("commit.committer.date"), List.of(commit("b", hoursAgo(50)), commit("a", hoursAgo(100))));
        server.putList(REPO_PATH + "/issues", List.of(issue(2, hoursAgo(50)), issue(1, hoursAgo(100))));
        server.putList(REPO_PATH + "/issues/comments", List.of(issueComment(31, 1, hoursAgo(60))));
        server.putList(REPO_PATH + "/pulls", List.of(pull(2, hoursAgo(40)), pull(1, hoursAgo(100))));
        server.putObject(REPO_PATH + "/pulls/1", pullDetails(1, hoursAgo(100)));
        server.putObject(REPO_PATH + "/pulls/2", pullDetails(2, hoursAgo(40)));
        server.putList(REPO_PATH + "/pulls/1/commits", List.of(commit("a", hoursAgo(100))));
        server.putList(REPO_PATH + "/pulls/2/commits", List.of(commit("b", hoursAgo(50)), commit("c", hoursAgo(45))));
        server.putList(REPO_PATH + "/pulls/1/reviews", List.of(review(31, "APPROVED")));
        server.putList(REPO_PATH + "/pulls/2/reviews", List.of());
        server.putList(REPO_PATH + "/pulls/comments", List.of(reviewComment(41, 1, hoursAgo(90))));
        server.putList(REPO_PATH + "/actions/workflows", Optional.of("workflows"), Optional.empty(), List.of(workflow(1, hoursAgo(500))));
        server.putList(REPO_PATH + "/actions/runs", Optional.of("workflow_runs"), Optional.empty(), List.of(
                inProgress(run(3, 1, hoursAgo(1))),
                run(2, 1, hoursAgo(5)),
                run(1, 1, hoursAgo(10))));
        server.putList(REPO_PATH + "/actions/runs/1/jobs", Optional.of("jobs"), Optional.empty(), List.of(job(11, 1, 1, hoursAgo(10))));
        server.putList(REPO_PATH + "/actions/runs/2/jobs", Optional.of("jobs"), Optional.empty(), List.of());
        server.putList(REPO_PATH + "/actions/runs/1/artifacts", Optional.of("artifacts"), Optional.empty(), List.of(artifact(61, 1)));
        server.putList(REPO_PATH + "/actions/runs/2/artifacts", Optional.of("artifacts"), Optional.empty(), List.of());
        server.putObject(REPO_PATH + "/check-suites/5001", checkSuite(5001));
        server.putObject(REPO_PATH + "/check-suites/5002", checkSuite(5002));
        server.putList(REPO_PATH + "/check-suites/5001/check-runs", Optional.of("check_runs"), Optional.empty(), List.of(checkRun(71, 5001, 2)));
        server.putList(REPO_PATH + "/check-suites/5002/check-runs", Optional.of("check_runs"), Optional.empty(), List.of(checkRun(72, 5002, 0)));
        server.putList(REPO_PATH + "/check-runs/71/annotations", List.of(annotation("src/Main.java", 10), annotation("src/Main.java", 20)));
        server.putList("/orgs/" + OWNER + "/teams", List.of(team(81, "core")));
        server.putList("/orgs/" + OWNER + "/members", List.of(user("jane")));
        server.putList("/orgs/" + OWNER + "/teams/core/members", List.of(user("jane"), user("john")));
        server.putList(REPO_PATH + "/collaborators", List.of(collaborator("jane", true), collaborator("john", false)));
    }

    private static Map<String, Object> inProgress(Map<String, Object> run)
    {
        return ImmutableMap.<String, Object>builder()
                .putAll(run)
                .put("status", "in_progress")
                .buildKeepingLast();
    }
}
