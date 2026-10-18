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
import io.trino.testing.QueryRunner;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static pl.net.was.sync.SyncQueryRunner.TARGET_CATALOG;

/**
 * Syncs a few tables of this repository from the GitHub API twice, to check that the second sync reads only new data.
 */
@EnabledIfEnvironmentVariable(named = "GITHUB_TOKEN", matches = ".+")
public class TestLiveGithubSync
{
    private static final String TARGET = TARGET_CATALOG + ".live";
    private static final String REPO_PATH = "/repos/nineinchnick/trino-openapi/";

    @Test
    public void testIncrementalSync(@TempDir Path requestLogs, @TempDir Path httpCache)
            throws Exception
    {
        try (QueryRunner queryRunner = SyncQueryRunner.createQueryRunner(ImmutableMap.<String, String>builder()
                .put("spec-location", Path.of("../specs/github.json").toAbsolutePath().toString())
                .put("base-uri", "https://api.github.com")
                .put("authentication.type", "http")
                .put("authentication.scheme", "bearer")
                .put("authentication.bearer-token", System.getenv("GITHUB_TOKEN"))
                .put("openApi.http-client.log.enabled", "true")
                .put("openApi.http-client.log.path", requestLogs.toString())
                .put("openApi.http-client.log.flush-interval", "10ms")
                .put("openApi.http-client.log.compression.enabled", "false")
                .put("http-cache.directory", httpCache.toString())
                .buildOrThrow())) {
            SyncResult firstSync = sync(queryRunner);
            assertThat(firstSync.failedSteps()).isEmpty();
            List<String> firstRequests = readRequests(requestLogs);
            Map<String, Long> rowsAfterFirstSync = countRows(queryRunner);
            assertThat(rowsAfterFirstSync).allSatisfy((_, rows) -> assertThat(rows).isPositive());

            SyncResult secondSync = sync(queryRunner);
            assertThat(secondSync.failedSteps()).isEmpty();
            List<String> allRequests = readRequests(requestLogs);
            List<String> secondRequests = allRequests.subList(firstRequests.size(), allRequests.size());

            // the repository might change between the syncs, but only by a few rows
            assertThat(secondSync.changedRows().values().stream().mapToLong(Long::longValue).sum()).isLessThan(5);
            assertThat(secondRequests.size()).isLessThan(firstRequests.size());
            // unchanged pages are revalidated, and GitHub responds without a body
            assertThat(secondRequests).anyMatch(request -> request.contains("\t304\t"));
            assertThat(secondRequests)
                    .filteredOn(request -> request.contains(REPO_PATH + "issues?"))
                    .singleElement()
                    .satisfies(request -> assertThat(request).contains("since="));
            assertThat(secondRequests)
                    .filteredOn(request -> request.contains(REPO_PATH + "pulls?"))
                    .hasSizeLessThanOrEqualTo(2);
            assertThat(secondRequests)
                    .filteredOn(request -> request.contains(REPO_PATH + "actions/runs?"))
                    .hasSizeLessThanOrEqualTo(3);
        }
    }

    private static SyncResult sync(QueryRunner queryRunner)
            throws Exception
    {
        SyncResult result = new Sync(SyncConfig.of(ImmutableMap.<String, String>builder()
                .put(SyncConfig.TRINO_URL, "jdbc:trino://%s:%s".formatted(queryRunner.getCoordinator().getBaseUrl().getHost(), queryRunner.getCoordinator().getBaseUrl().getPort()))
                .put(SyncConfig.TRINO_USER, "test")
                // revalidate cached responses, so the sync never reads stale data, but GitHub doesn't count unchanged responses against the rate limit
                .put(SyncConfig.SESSION_PROPERTIES, "github.http_cache_enabled:false")
                .put(SyncConfig.STEPS, "issues,pulls,workflows,runs")
                .put("source", "github.default")
                .put("target", TARGET)
                .put("owner", "nineinchnick")
                .put("repo", "trino-openapi")
                .buildOrThrow())).run();
        // the request log is written asynchronously
        Thread.sleep(1000);
        return result;
    }

    private static Map<String, Long> countRows(QueryRunner queryRunner)
    {
        ImmutableMap.Builder<String, Long> counts = ImmutableMap.builder();
        for (String table : List.of("issues", "pulls", "workflows", "runs")) {
            counts.put(table, (Long) queryRunner.execute("SELECT count(*) FROM %s.%s".formatted(TARGET, table)).getOnlyValue());
        }
        return counts.buildOrThrow();
    }

    private static List<String> readRequests(Path requestLogs)
            throws IOException
    {
        try (Stream<Path> files = Files.list(requestLogs)) {
            return files.sorted()
                    .flatMap(file -> {
                        try {
                            return Files.readAllLines(file).stream();
                        }
                        catch (IOException e) {
                            throw new UncheckedIOException(e);
                        }
                    })
                    .filter(line -> line.contains("https://api.github.com/"))
                    .toList();
        }
    }
}
