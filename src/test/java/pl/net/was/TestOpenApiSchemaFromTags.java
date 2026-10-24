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
import org.junit.jupiter.api.Test;

import java.util.Map;

public class TestOpenApiSchemaFromTags
        extends AbstractTestQueryFramework
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        // only metadata is read, so the API doesn't have to be running
        return OpenApiQueryRunner.createQueryRunner(Map.of(
                "petstore", Map.of(
                        "spec-location", "src/test/resources/petstore.yaml",
                        "base-uri", "http://localhost:8080/api/v3",
                        "schema-from-tags", "true")));
    }

    @Test
    public void showSchemas()
    {
        assertQuery("SHOW SCHEMAS FROM petstore", "VALUES 'default', 'information_schema', 'pet', 'store', 'user'");
        assertQuery(
                "SELECT schema_name FROM petstore.information_schema.schemata",
                "VALUES 'default', 'information_schema', 'pet', 'store', 'user'");
    }

    @Test
    public void showTables()
    {
        assertQuery("SHOW TABLES FROM petstore.pet", "VALUES 'pet', 'pet_find_by_status', 'pet_find_by_tags', 'pet_upload_image'");
        assertQuery("SHOW TABLES FROM petstore.store", "VALUES 'store_inventory', 'store_order'");
        assertQuery("SHOW TABLES FROM petstore.user", "VALUES 'user', 'user_create_with_list', 'user_login'");
        assertQueryReturnsEmptyResult("SHOW TABLES FROM petstore.default");
        assertQuery(
                "SELECT table_schema, table_name FROM petstore.information_schema.tables WHERE table_schema <> 'information_schema'",
                "VALUES ('pet', 'pet'), ('pet', 'pet_find_by_status'), ('pet', 'pet_find_by_tags'), ('pet', 'pet_upload_image'), " +
                        "('store', 'store_inventory'), ('store', 'store_order'), ('user', 'user'), ('user', 'user_create_with_list'), ('user', 'user_login')");
        assertQuery(
                "SELECT DISTINCT table_schema, table_name FROM petstore.information_schema.columns WHERE table_schema = 'store'",
                "VALUES ('store', 'store_inventory'), ('store', 'store_order')");
    }

    @Test
    public void describeTable()
    {
        assertQuery(
                "SELECT column_name FROM petstore.information_schema.columns WHERE table_schema = 'store' AND table_name = 'store_order'",
                "VALUES 'id', 'pet_id', 'quantity', 'ship_date', 'status', 'complete', 'order_id'");
        assertQueryFails("DESCRIBE petstore.default.store_order", ".*Table 'petstore.default.store_order' does not exist");
        assertQueryFails("DESCRIBE petstore.pet.store_order", ".*Table 'petstore.pet.store_order' does not exist");
    }
}
