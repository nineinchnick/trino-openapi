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

import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestStep
{
    @Test
    public void testSplit()
    {
        assertThat(SqlSplitter.split(
                """
                -- a comment; with a semicolon
                SELECT 'a;b', "c;d" FROM t;
                /* block; comment */ SELECT 1
                ;
                -- only a comment
                """))
                .containsExactly(
                        "-- a comment; with a semicolon\nSELECT 'a;b', \"c;d\" FROM t",
                        "/* block; comment */ SELECT 1");
        assertThat(SqlSplitter.split("SELECT 'it''s; fine'")).containsExactly("SELECT 'it''s; fine'");
        assertThatThrownBy(() -> SqlSplitter.split("SELECT 'a"))
                .hasMessage("Unterminated string literal at position 7");
    }

    @Test
    public void testParseWithoutLoop()
    {
        assertThat(Step.parse("once", "CREATE TABLE t (x int);\nINSERT INTO t VALUES (1);"))
                .isEqualTo(new Step("once", List.of("CREATE TABLE t (x int)", "INSERT INTO t VALUES (1)"), Optional.empty()));
    }

    @Test
    public void testParseLoop()
    {
        Step step = Step.parse("children",
                """
                CREATE TABLE t (x int);

                -- loop: batches
                -- stop-after: 2
                MERGE INTO t USING s ON t.x = s.x WHEN NOT MATCHED THEN INSERT VALUES (s.x);
                INSERT INTO state SELECT 1;
                """);

        assertThat(step.setup()).containsExactly("CREATE TABLE t (x int)");
        assertThat(step.loop()).hasValueSatisfying(loop -> {
            assertThat(loop.mode()).isEqualTo(Step.LoopMode.BATCHES);
            assertThat(loop.stopAfter()).isEqualTo(2);
            assertThat(loop.statements()).hasSize(2);
        });
    }

    @Test
    public void testParseInvalid()
    {
        assertThatThrownBy(() -> Step.parse("x", "-- loop: sometimes\nSELECT 1"))
                .hasMessageContaining("SOMETIMES");
        assertThatThrownBy(() -> Step.parse("x", "-- loop: pages\nSELECT 1;\n-- loop: batches\nSELECT 2"))
                .hasMessage("Step x has more than one loop directive");
        assertThatThrownBy(() -> Step.parse("x", "-- stop-after: 2\nSELECT 1"))
                .hasMessage("Step x: stop-after requires a loop directive");
    }

    @Test
    public void testRender()
    {
        assertThat(Template.render("SELECT * FROM ${target}.t WHERE page = ${page} LIMIT ${batch_size:30}", Map.of("target", "iceberg.github", "page", "2")))
                .isEqualTo("SELECT * FROM iceberg.github.t WHERE page = 2 LIMIT 30");
        assertThat(Template.render("LIMIT ${batch_size:30}", Map.of("batch_size", "5")))
                .isEqualTo("LIMIT 5");
        assertThatThrownBy(() -> Template.render("SELECT '${owner}'", Map.of()))
                .hasMessage("Missing value of the parameter owner");
    }

    @Test
    public void testBundledSteps()
    {
        StepCatalog catalog = StepCatalog.fromClasspath(StepCatalog.DEFAULT_CLASSPATH_DIR);
        assertThat(catalog.getDefaultSteps()).startsWith("repos", "commits", "issues");
        for (String name : List.of("org_repos", "pull_stats", "artifacts", "teams", "members", "collaborators")) {
            assertThat(catalog.getDefaultSteps()).doesNotContain(name);
            assertThat(catalog.getStep(name).name()).isEqualTo(name);
        }
        catalog.getDefaultSteps().forEach(catalog::getStep);
        assertThatThrownBy(() -> catalog.getStep("job_logs"))
                .hasMessage("Unknown step job_logs, missing classpath:github/job_logs.sql");
    }

    @Test
    public void testConfig()
            throws Exception
    {
        SyncConfig config = SyncConfig.load(Optional.empty(), Map.of(
                "SYNC_TRINO_URL", "jdbc:trino://trino:8080",
                "SYNC_TRINO_PASSWORD", "secret",
                "SYNC_OWNER", "trinodb",
                "SYNC_STEPS", "issues, pulls",
                "HOME", "/root"));

        assertThat(config.getTrinoUrl()).isEqualTo("jdbc:trino://trino:8080");
        assertThat(config.getSteps()).hasValue(List.of("issues", "pulls"));
        assertThat(config.getParameters()).isEqualTo(Map.of("owner", "trinodb"));
        assertThat(config.describe()).containsEntry(SyncConfig.TRINO_PASSWORD, "***");
    }
}
