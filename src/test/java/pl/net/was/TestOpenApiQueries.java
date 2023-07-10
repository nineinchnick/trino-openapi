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

import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.QueryRunner;
import org.testng.annotations.Test;

public class TestOpenApiQueries
        extends AbstractTestQueryFramework
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return OpenApiQueryRunner.createQueryRunner();
    }

    @Test
    public void showTables()
    {
        assertQuery("SHOW SCHEMAS FROM openapi", "VALUES 'default', 'information_schema'");
        assertQuery("SHOW TABLES FROM openapi.default", "VALUES 'get_cluster', 'get_schema_discovery', 'list_catalog', 'list_cluster', 'list_schema_discovery_of_catalog'");
    }

    @Test
    public void selectFromTable()
    {
        assertQuery("SELECT cloud_region_id FROM list_cluster WHERE cluster_id='w-8135698509'",
                "VALUES ('aws-us-east1')");
        assertQuery("SELECT catalog_name FROM list_catalog where catalog_id='c-4450430933'",
                "VALUES ('otel')");
    }
}
