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
import io.trino.testing.MaterializedRow;
import io.trino.testing.QueryRunner;
import io.trino.testing.sql.TestTable;
import io.trino.testing.sql.TrinoSqlExecutor;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

public class TestOpenApiQueries
        extends AbstractTestQueryFramework
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        PetStoreServer petStoreServer = new PetStoreServer();
        ImmutableMap.Builder<String, String> petStoreProperties = ImmutableMap.builder();
        petStoreProperties.putAll(Map.of(
                "spec-location", petStoreServer.getSpecUrl(),
                "base-uri", petStoreServer.getApiUrl(),
                "authentication.type", "oauth",
                "authentication.scheme", "basic",
                "authentication.username", "user",
                "authentication.password", "user",
                "authentication.api-key-name", "api_key",
                "authentication.api-key-value", "special-key"));
        petStoreProperties.putAll(Map.of(
                "authentication.token-endpoint", "/oauth/token",
                "authentication.client-id", "sample-client-id",
                "authentication.client-secret", "secret",
                "authentication.grant-type", "password"));

        FastApiServer fastApiServer = new FastApiServer();
        ImmutableMap.Builder<String, String> fastApiProperties = ImmutableMap.builder();
        fastApiProperties.putAll(Map.of(
                "spec-location", fastApiServer.getSpecUrl(),
                "base-uri", fastApiServer.getApiUrl()));

        return OpenApiQueryRunner.createQueryRunner(Map.of(
                "openmeteo", Map.of("spec-location", "https://raw.githubusercontent.com/open-meteo/open-meteo/main/openapi.yml",
                        "base-uri", "https://api.open-meteo.com"),
                "petstore", petStoreProperties.buildOrThrow(),
                "fastapi", fastApiProperties.buildOrThrow()));
    }

    @Test
    public void showPetStoreTables()
    {
        assertQuery("SHOW SCHEMAS FROM petstore",
                "VALUES 'default', 'information_schema'");
        assertQuery("SHOW TABLES FROM petstore.default",
                "VALUES 'pet_find_by_status', 'store_inventory', 'store_order', 'pet', 'user', 'user_login', 'pet_upload_image'");
    }

    @Test
    public void selectFromPetTable()
    {
        assertQuery("SELECT name FROM petstore.default.pet_find_by_status WHERE status_req = array['available'] AND id != 100",
                "VALUES ('Cat 1'), ('Cat 2'), ('Dog 1'), ('Lion 1'), ('Lion 2'), ('Lion 3'), ('Rabbit 1')");
        assertQuery("SELECT name FROM petstore.default.pet WHERE pet_id = 1",
                "VALUES ('Cat 1')");
    }

    @Test
    public void insertPet()
    {
        assertQueryReturnsEmptyResult("SELECT name FROM petstore.default.pet WHERE pet_id = 100");
        assertQuerySucceeds("INSERT INTO petstore.default.pet (id, name, photo_urls, status) VALUES (100, 'Cat X', ARRAY[], 'available')");
        assertQuery("SELECT name FROM petstore.default.pet WHERE pet_id = 100",
                "VALUES ('Cat X')");
        assertUpdate("UPDATE petstore.default.pet SET name = 'Cat Y' WHERE pet_id = 100", 1);
        assertQuery("SELECT name FROM petstore.default.pet WHERE pet_id = 100",
                "VALUES ('Cat Y')");
        assertUpdate("DELETE FROM petstore.default.pet WHERE pet_id = 100", 1);
        assertQueryReturnsEmptyResult("SELECT name FROM petstore.default.pet WHERE pet_id = 100");
    }

    @Test
    public void selectFromForecastTable()
    {
        assertQuery("SELECT elevation, timezone, current_weather.temperature BETWEEN -50 AND 100 AS is_livable " +
                        "FROM openmeteo.default.v1_forecast WHERE latitude_req = 53.1325 AND longitude_req = 23.1688",
                "VALUES (135.0, 'GMT', null)");
        assertQuery("SELECT elevation, timezone, current_weather.temperature BETWEEN -50 AND 100 AS is_livable " +
                        "FROM openmeteo.default.v1_forecast WHERE latitude_req = 53.1325 AND longitude_req = 23.1688 AND current_weather_req = true",
                "VALUES (135.0, 'GMT', true)");
    }

    @Test
    public void selectItems()
    {
        List<MaterializedRow> rows = getQueryRunner().execute("SELECT name, description, price, tax, tags, map_entries(properties), created_at, valid_until, revised_at FROM fastapi.default.items WHERE item_id = 1").getMaterializedRows();
        // can't use assertQuery, because array of dates read from H2 as not using LocalDate
        assertThat(rows).size().isEqualTo(1);
        assertThat(rows.getFirst().getFields()).containsExactly(
                "Portal Gun",
                null,
                BigDecimal.valueOf(4200000000L, 8),
                null,
                List.of("sci-fi"),
                List.of(),
                null,
                null,
                List.of(LocalDate.of(2007, 10, 10), LocalDate.of(2022, 12, 8)));
    }

    @Test
    public void searchItemsWithInPhrase()
    {
        List<MaterializedRow> rows = getQueryRunner().execute("SELECT name FROM fastapi.default.search WHERE item_ids IN (ARRAY['2'])").getMaterializedRows();
        assertThat(rows).size().isEqualTo(1);
        assertThat(rows.getFirst().getFields()).first().isEqualTo("Plumbus");

        rows = getQueryRunner().execute("SELECT name FROM fastapi.default.search WHERE item_ids IN (ARRAY['1', '2'])").getMaterializedRows();
        assertThat(rows).size().isEqualTo(2);
    }

    @Test
    public void searchItemsWithSubQuery10k()
    {
        try (TestTable table = generateDataset("memory.default.test_items_10k", 10000)) {
            List<MaterializedRow> rows = getQueryRunner().execute("SELECT name FROM fastapi.default.search WHERE item_ids IN (select array_agg(item_id) from %s)".formatted(table.getName())).getMaterializedRows();
            assertThat(rows)
                    .extracting(row -> row.getFields().getFirst())
                    .containsExactly("Portal Gun", "Plumbus");
        }
    }

    @Test
    public void searchItemsWithSubQuery100k()
    {
        try (TestTable table = generateDataset("memory.default.test_items_100k", 100000)) {
            List<MaterializedRow> rows = getQueryRunner().execute("SELECT name FROM fastapi.default.search WHERE item_ids IN (select array_agg(item_id) from %s)".formatted(table.getName())).getMaterializedRows();
            assertThat(rows)
                    .extracting(row -> row.getFields().getFirst())
                    .containsExactly("Portal Gun", "Plumbus");
        }
    }

    private TestTable generateDataset(String namePrefix, int elements)
    {
        return new TestTable(new TrinoSqlExecutor(getQueryRunner()), namePrefix, "AS SELECT CAST(sequential_number AS VARCHAR) AS item_id FROM TABLE(sequence(start=>0, stop=>%d))".formatted(elements - 1));
    }
}
