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
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.QueryRunner;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;

import java.util.Map;

import static java.util.Objects.requireNonNullElse;

@EnabledIfEnvironmentVariable(named = "JIRA_SITE", matches = ".*")
public class TestJira
        extends AbstractTestQueryFramework
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        ImmutableMap.Builder<String, String> properties = ImmutableMap.builder();
        properties.putAll(Map.of(
                "spec-location", "https://developer.atlassian.com/cloud/jira/platform/swagger-v3.v3.json",
                "base-uri", requireNonNullElse(System.getenv("JIRA_SITE"), ""),
                "authentication.type", "http",
                "authentication.scheme", "basic",
                "authentication.username", requireNonNullElse(System.getenv("JIRA_USER"), ""),
                "authentication.password", requireNonNullElse(System.getenv("JIRA_TOKEN"), "")));
        return OpenApiQueryRunner.createQueryRunner(Map.of("jira", properties.buildOrThrow()));
    }

    @Test
    public void searchIssues()
    {
        assertQuery("SELECT i.key, i.fields['summary'] FROM jira.default.rest_api_3_search_jql j CROSS JOIN unnest(issues) i WHERE jql = 'text ~ \"first*\"' AND j.fields = ARRAY['*all']",
                "VALUES ('KAN-1', 'First issue')");
    }
}
