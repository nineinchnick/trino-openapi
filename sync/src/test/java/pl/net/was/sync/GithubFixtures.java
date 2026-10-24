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

import com.google.common.collect.ImmutableMap;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Map;

/**
 * Minimal GitHub API objects, with the fields the sync steps read.
 */
final class GithubFixtures
{
    static final String OWNER = "acme";
    static final String REPO = "rocket";
    static final String REPO_PATH = "/repos/" + OWNER + "/" + REPO;

    private static final Instant NOW = Instant.now().truncatedTo(ChronoUnit.SECONDS);

    private GithubFixtures() {}

    static String hoursAgo(long hours)
    {
        return NOW.minus(hours, ChronoUnit.HOURS).toString();
    }

    static Map<String, Object> user(String login)
    {
        return object()
                .put("id", (long) login.hashCode() & 0xFFFF)
                .put("login", login)
                .put("type", "User")
                .put("site_admin", false)
                .buildOrThrow();
    }

    static Map<String, Object> repository(long id, String name, String updatedAt)
    {
        return object()
                .put("id", id)
                .put("name", name)
                .put("full_name", OWNER + "/" + name)
                .put("owner", user(OWNER))
                .put("private", false)
                .put("fork", false)
                .put("created_at", hoursAgo(1000))
                .put("updated_at", updatedAt)
                .put("pushed_at", updatedAt)
                .buildOrThrow();
    }

    static Map<String, Object> privateRepository(long id, String name, String updatedAt)
    {
        return object()
                .putAll(repository(id, name, updatedAt))
                .put("private", true)
                .put("visibility", "private")
                .buildKeepingLast();
    }

    static Map<String, Object> commit(String sha, String date)
    {
        Map<String, Object> person = object()
                .put("name", "Jane")
                .put("email", "jane@example.com")
                .put("date", date)
                .buildOrThrow();
        return object()
                .put("sha", sha)
                .put("commit", object()
                        .put("message", "Change " + sha)
                        .put("author", person)
                        .put("committer", person)
                        .put("comment_count", 0)
                        .buildOrThrow())
                .put("author", user("jane"))
                .put("committer", user("jane"))
                .put("parents", List.of())
                .buildOrThrow();
    }

    static Map<String, Object> issue(long number, String updatedAt)
    {
        return object()
                .put("id", 1000 + number)
                .put("number", number)
                .put("state", "open")
                .put("title", "Issue " + number)
                .put("user", user("jane"))
                .put("labels", List.of())
                .put("comments", 0)
                .put("created_at", hoursAgo(1000))
                .put("updated_at", updatedAt)
                .buildOrThrow();
    }

    static Map<String, Object> issueComment(long id, long issueNumber, String updatedAt)
    {
        return object()
                .put("id", id)
                .put("body", "Comment " + id)
                .put("user", user("jane"))
                .put("issue_url", "https://api.github.com" + REPO_PATH + "/issues/" + issueNumber)
                .put("created_at", hoursAgo(1000))
                .put("updated_at", updatedAt)
                .buildOrThrow();
    }

    static Map<String, Object> pull(long number, String updatedAt)
    {
        return object()
                .put("id", 2000 + number)
                .put("number", number)
                .put("state", "open")
                .put("title", "Pull " + number)
                .put("user", user("jane"))
                .put("labels", List.of(object()
                        .put("id", 1)
                        .put("name", "bug")
                        .buildOrThrow()))
                .put("head", object()
                        .put("ref", "feature-" + number)
                        .put("sha", "head" + number)
                        .buildOrThrow())
                .put("base", object()
                        .put("ref", "main")
                        .put("sha", "base" + number)
                        .buildOrThrow())
                .put("created_at", hoursAgo(1000))
                .put("updated_at", updatedAt)
                .buildOrThrow();
    }

    static Map<String, Object> pullDetails(long number, String updatedAt)
    {
        return ImmutableMap.<String, Object>builder()
                .putAll(pull(number, updatedAt))
                .put("additions", 10 * number)
                .put("deletions", number)
                .put("changed_files", 1)
                .put("commits", 1)
                .put("comments", 0)
                .put("review_comments", 0)
                .put("merged", false)
                .buildOrThrow();
    }

    static Map<String, Object> review(long id, String state)
    {
        return object()
                .put("id", id)
                .put("user", user("john"))
                .put("body", "Review " + id)
                .put("state", state)
                .put("commit_id", "abc")
                .put("submitted_at", hoursAgo(100))
                .buildOrThrow();
    }

    static Map<String, Object> reviewComment(long id, long pullNumber, String updatedAt)
    {
        return object()
                .put("id", id)
                .put("pull_request_review_id", 1)
                .put("path", "README.md")
                .put("body", "Review comment " + id)
                .put("user", user("john"))
                .put("pull_request_url", "https://api.github.com" + REPO_PATH + "/pulls/" + pullNumber)
                .put("created_at", hoursAgo(1000))
                .put("updated_at", updatedAt)
                .buildOrThrow();
    }

    static Map<String, Object> workflow(long id, String updatedAt)
    {
        return object()
                .put("id", id)
                .put("name", "CI " + id)
                .put("path", ".github/workflows/ci" + id + ".yaml")
                .put("state", "active")
                .put("created_at", hoursAgo(1000))
                .put("updated_at", updatedAt)
                .buildOrThrow();
    }

    static Map<String, Object> run(long id, long attempt, String createdAt)
    {
        return object()
                .put("id", id)
                .put("name", "CI")
                .put("check_suite_id", 5000 + id)
                .put("head_branch", "main")
                .put("head_sha", "sha" + id)
                .put("run_number", id)
                .put("run_attempt", attempt)
                .put("event", "push")
                .put("status", "completed")
                .put("conclusion", "success")
                .put("workflow_id", 1)
                .put("actor", user("jane"))
                .put("pull_requests", List.of())
                .put("created_at", createdAt)
                .put("updated_at", createdAt)
                .put("run_started_at", createdAt)
                .buildOrThrow();
    }

    static Map<String, Object> job(long id, long runId, long attempt, String startedAt)
    {
        return object()
                .put("id", id)
                .put("run_id", runId)
                .put("run_attempt", attempt)
                .put("head_sha", "sha" + runId)
                .put("status", "completed")
                .put("conclusion", "failure")
                .put("name", "build")
                .put("created_at", startedAt)
                .put("started_at", startedAt)
                .put("completed_at", startedAt)
                .put("labels", List.of("ubuntu-latest"))
                .put("steps", List.of(
                        object()
                                .put("name", "checkout")
                                .put("status", "completed")
                                .put("conclusion", "success")
                                .put("number", 1)
                                .put("started_at", startedAt)
                                .put("completed_at", startedAt)
                                .buildOrThrow(),
                        object()
                                .put("name", "test")
                                .put("status", "completed")
                                .put("conclusion", "failure")
                                .put("number", 2)
                                .put("started_at", startedAt)
                                .put("completed_at", startedAt)
                                .buildOrThrow()))
                .buildOrThrow();
    }

    static Map<String, Object> artifact(long id, long runId)
    {
        return object()
                .put("id", id)
                .put("name", "logs")
                .put("size_in_bytes", 100)
                .put("expired", false)
                .put("created_at", hoursAgo(10))
                .put("updated_at", hoursAgo(10))
                .put("workflow_run", object()
                        .put("id", runId)
                        .buildOrThrow())
                .buildOrThrow();
    }

    static Map<String, Object> checkSuite(long id)
    {
        return object()
                .put("id", id)
                .put("head_branch", "main")
                .put("head_sha", "sha")
                .put("status", "completed")
                .put("conclusion", "success")
                .put("app", object()
                        .put("id", 15368)
                        .put("slug", "github-actions")
                        .buildOrThrow())
                .put("pull_requests", List.of())
                .put("created_at", hoursAgo(10))
                .put("updated_at", hoursAgo(10))
                .buildOrThrow();
    }

    static Map<String, Object> checkRun(long id, long checkSuiteId, long annotationsCount)
    {
        return object()
                .put("id", id)
                .put("head_sha", "sha")
                .put("status", "completed")
                .put("conclusion", "failure")
                .put("name", "build")
                .put("started_at", hoursAgo(10))
                .put("completed_at", hoursAgo(10))
                .put("output", object()
                        .put("title", "Build")
                        .put("annotations_count", annotationsCount)
                        .buildOrThrow())
                .put("check_suite", object()
                        .put("id", checkSuiteId)
                        .buildOrThrow())
                .put("app", object()
                        .put("id", 15368)
                        .put("slug", "github-actions")
                        .buildOrThrow())
                .put("pull_requests", List.of())
                .buildOrThrow();
    }

    static Map<String, Object> annotation(String path, long line)
    {
        return object()
                .put("path", path)
                .put("start_line", line)
                .put("end_line", line)
                .put("annotation_level", "failure")
                .put("title", "Error")
                .put("message", "Failed at line " + line)
                .put("blob_href", "https://github.com/" + OWNER + "/" + REPO + "/blob/sha/" + path)
                .buildOrThrow();
    }

    static Map<String, Object> team(long id, String slug)
    {
        return object()
                .put("id", id)
                .put("name", slug)
                .put("slug", slug)
                .put("privacy", "closed")
                .put("permission", "pull")
                .buildOrThrow();
    }

    static Map<String, Object> collaborator(String login, boolean admin)
    {
        return ImmutableMap.<String, Object>builder()
                .putAll(user(login))
                .put("permissions", object()
                        .put("pull", true)
                        .put("triage", true)
                        .put("push", true)
                        .put("maintain", admin)
                        .put("admin", admin)
                        .buildOrThrow())
                .put("role_name", admin ? "admin" : "write")
                .buildOrThrow();
    }

    private static ImmutableMap.Builder<String, Object> object()
    {
        return ImmutableMap.builder();
    }
}
