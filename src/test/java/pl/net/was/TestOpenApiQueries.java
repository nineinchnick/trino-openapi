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
        TestingOpenApiServer server = new TestingOpenApiServer();
        return OpenApiQueryRunner.createQueryRunner(server);
    }

    @Test
    public void showTables()
    {
        assertQuery("SHOW SCHEMAS FROM openapi",
                "VALUES 'default', 'information_schema'");
        assertQuery("SHOW TABLES FROM openapi.default",
                "VALUES 'pet_find_by_status', 'store_inventory', 'store_order', 'pet', 'user', 'user_login', 'pet_upload_image'");
    }

    @Test
    public void selectFromTable()
    {
        assertQuery("SELECT name FROM pet_find_by_status WHERE status='available' AND id != 100",
                "VALUES ('Cat 1'), ('Cat 2'), ('Dog 1'), ('Lion 1'), ('Lion 2'), ('Lion 3'), ('Rabbit 1')");
        assertQuery("SELECT name FROM pet WHERE pet_id = 1",
                "VALUES ('Cat 1')");
    }

    @Test
    public void insertPet()
    {
        assertQueryReturnsEmptyResult("SELECT name FROM pet WHERE pet_id = 100");
        assertQuerySucceeds("INSERT INTO pet (id, name, photo_urls, status) VALUES (100, 'Cat X', ARRAY[], 'available')");
        assertQuery("SELECT name FROM pet WHERE pet_id = 100",
                "VALUES ('Cat X')");
        assertUpdate("UPDATE pet SET name = 'Cat Y' WHERE pet_id = 100", 1);
        assertQuery("SELECT name FROM pet WHERE pet_id = 100",
                "VALUES ('Cat Y')");
        assertUpdate("DELETE FROM pet WHERE pet_id = 100", 1);
        assertQueryReturnsEmptyResult("SELECT name FROM pet WHERE pet_id = 100");
    }
}
