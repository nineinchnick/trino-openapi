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
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

import static java.util.concurrent.Executors.newVirtualThreadPerTaskExecutor;
import static org.assertj.core.api.Assertions.assertThat;

public class TestOpenApiQueries
        extends AbstractTestQueryFramework
{
    KeycloakServer keycloakServer;
    PetStoreServer petStoreServer;
    FastApiServer fastApiServer;

    @AfterAll
    void tearDown()
    {
        fastApiServer.close();
        petStoreServer.close();
        keycloakServer.close();
    }

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        // this method is called from a @BeforeAll method in the super class
        try (ExecutorService executor = newVirtualThreadPerTaskExecutor()) {
            // building the FastAPI image takes about as long as starting Keycloak, so do both at the same time
            Future<FastApiServer> fastApiServerFuture = executor.submit(FastApiServer::new);
            keycloakServer = new KeycloakServer();
            petStoreServer = new PetStoreServer(keycloakServer);
            fastApiServer = fastApiServerFuture.get();
        }

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
                "authentication.client-id", "sample-client-id",
                "authentication.client-secret", "secret"));

        ImmutableMap.Builder<String, String> fastApiProperties = ImmutableMap.builder();
        fastApiProperties.putAll(Map.of(
                "spec-location", fastApiServer.getSpecUrl(),
                "base-uri", fastApiServer.getApiUrl()));

        return OpenApiQueryRunner.createQueryRunner(Map.of(
                "openmeteo", Map.of(
                        "spec-location", "src/test/resources/openmeteo.yml",
                        "base-uri", "https://api.open-meteo.com"),
                "petstore", petStoreProperties.buildOrThrow(),
                "fastapi", fastApiProperties.buildOrThrow(),
                "echo", Map.of(
                        "spec-location", "src/test/resources/echo.yml",
                        "base-uri", fastApiServer.getApiUrl())));
    }

    @Test
    public void showPetStoreTables()
    {
        assertQuery(
                "SHOW SCHEMAS FROM petstore",
                "VALUES 'default', 'information_schema'");
        assertQuery(
                "SHOW TABLES FROM petstore.default",
                "VALUES 'pet_find_by_status', 'pet_find_by_tags', 'store_inventory', 'store_order', 'pet', 'user', 'user_create_with_list', 'user_login', 'pet_upload_image'");
    }

    @Test
    public void selectFromPetTable()
    {
        assertQuery(
                "SELECT name FROM petstore.default.pet_find_by_status WHERE status = 'available' AND id != 100",
                "VALUES ('Cat 1'), ('Cat 2'), ('Dog 1'), ('Lion 1'), ('Lion 2'), ('Lion 3'), ('Rabbit 1')");
        assertQuery(
                "SELECT name FROM petstore.default.pet WHERE pet_id = 1",
                "VALUES ('Cat 1')");
    }

    @Test
    public void insertPet()
    {
        assertQueryReturnsEmptyResult("SELECT name FROM petstore.default.pet WHERE pet_id = 100");
        assertQuerySucceeds("INSERT INTO petstore.default.pet (id, name, photo_urls, status) VALUES (100, 'Cat X', ARRAY[], 'available')");
        assertQuery(
                "SELECT name FROM petstore.default.pet WHERE pet_id = 100",
                "VALUES ('Cat X')");
        assertUpdate("UPDATE petstore.default.pet SET name = 'Cat Y' WHERE pet_id = 100", 1);
        assertQuery(
                "SELECT name FROM petstore.default.pet WHERE pet_id = 100",
                "VALUES ('Cat Y')");
        assertUpdate("DELETE FROM petstore.default.pet WHERE pet_id = 100", 1);
        assertQueryReturnsEmptyResult("SELECT name FROM petstore.default.pet WHERE pet_id = 100");
    }

    @Test
    public void insertUpdateDeleteNote()
    {
        assertQueryReturnsEmptyResult("SELECT text FROM fastapi.default.notes WHERE note_id = 1");
        assertUpdate("INSERT INTO fastapi.default.notes (note_id, text) VALUES (1, 'first')", 1);
        assertQuery(
                "SELECT id, text FROM fastapi.default.notes WHERE note_id = 1",
                "VALUES (1, 'first')");
        assertUpdate("UPDATE fastapi.default.notes SET text = 'second' WHERE note_id = 1", 1);
        assertQuery(
                "SELECT id, text FROM fastapi.default.notes WHERE note_id = 1",
                "VALUES (1, 'second')");
        assertUpdate("DELETE FROM fastapi.default.notes WHERE note_id = 1", 1);
        assertQueryReturnsEmptyResult("SELECT text FROM fastapi.default.notes WHERE note_id = 1");
    }

    @Test
    public void selectFromForecastTable()
    {
        assertQuery(
                "SELECT elevation, timezone, current_weather.temperature BETWEEN -50 AND 100 AS is_livable " +
                        "FROM openmeteo.default.v1_forecast WHERE latitude_req = 53.1325 AND longitude_req = 23.1688",
                "VALUES (135.0, 'GMT', null)");
        assertQuery(
                "SELECT elevation, timezone, current_weather.temperature BETWEEN -50 AND 100 AS is_livable " +
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
    public void selectRealValues()
    {
        assertThat(selectNumbers(1, "real_value, real_values")).containsExactly(
                -1.5f,
                List.of(1.5f, -2.25f));
    }

    @Test
    public void insertRealAndDecimalValues()
    {
        assertUpdate("INSERT INTO fastapi.default.numbers (id, real_value, decimal_value, real_values, decimal_values, decimal_map) " +
                "VALUES (2, REAL '-0.5', 12.5, ARRAY[REAL '2.5', NULL], ARRAY[0.25, NULL], MAP(ARRAY['a', 'b'], ARRAY[7.75, NULL]))", 1);
        Map<String, BigDecimal> expectedMap = new HashMap<>();
        expectedMap.put("a", new BigDecimal("7.75000000"));
        expectedMap.put("b", null);
        assertThat(selectNumbers(2, "real_value, decimal_value, real_values, decimal_values, decimal_map")).containsExactly(
                -0.5f,
                new BigDecimal("12.50000000"),
                Arrays.asList(2.5f, null),
                Arrays.asList(new BigDecimal("0.25000000"), null),
                expectedMap);
    }

    @Test
    public void selectWithRealAndDecimalsInRequestBody()
    {
        List<MaterializedRow> rows = getQueryRunner().execute("SELECT real_value, real_values, decimal_values FROM fastapi.default.echo_numbers " +
                "WHERE real_value = REAL '-0.5' AND real_values = ARRAY[REAL '2.5', REAL '-1.25'] AND decimal_values = CAST(ARRAY[0.25, -12.5] AS ARRAY(DECIMAL(18, 8)))").getMaterializedRows();
        assertThat(rows).size().isEqualTo(1);
        assertThat(rows.getFirst().getFields()).containsExactly(
                -0.5f,
                List.of(2.5f, -1.25f),
                List.of(new BigDecimal("0.25000000"), new BigDecimal("-12.50000000")));
    }

    @Test
    public void selectDecimalsWithMaximumPrecision()
    {
        assertThat(selectNumbers(1, "decimal_value, decimal_values, decimal_map")).containsExactly(
                new BigDecimal("1234567890.12345678"),
                List.of(new BigDecimal("0.00000001"), new BigDecimal("-9999999999.99999999")),
                Map.of("a", new BigDecimal("1234567890.12345678")));
    }

    @Test
    public void selectWithMaximumPrecisionDecimalsInRequestBody()
    {
        List<MaterializedRow> rows = getQueryRunner().execute("SELECT decimal_value, decimal_values FROM fastapi.default.echo_numbers " +
                "WHERE decimal_value = DECIMAL '-9999999999.99999999' AND decimal_values = CAST(ARRAY[1234567890.12345678, 0.00000001] AS ARRAY(DECIMAL(18, 8)))").getMaterializedRows();
        assertThat(rows).hasSize(1);
        assertThat(rows.getFirst().getFields()).containsExactly(
                new BigDecimal("-9999999999.99999999"),
                List.of(new BigDecimal("1234567890.12345678"), new BigDecimal("0.00000001")));
    }

    @Test
    public void selectWithDecimalParametersInNonUsLocale()
    {
        Locale defaultLocale = Locale.getDefault();
        try {
            Locale.setDefault(Locale.GERMANY);
            assertQuery(
                    "SELECT decimal_value, decimal_text FROM fastapi.default.decimal_parameters WHERE decimal_value = DECIMAL '1234567890.12345678'",
                    "VALUES (1234567890.12345678, '1234567890.12345678')");
            assertQuery(
                    "SELECT decimal_value, decimal_text FROM fastapi.default.decimal_parameters WHERE decimal_value = DECIMAL '0.00000001'",
                    "VALUES (0.00000001, '0.00000001')");
        }
        finally {
            Locale.setDefault(defaultLocale);
        }
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

    @Test
    public void itemCategories()
    {
        List<MaterializedRow> rows = getQueryRunner().execute("SELECT name FROM fastapi.default.item_categories").getMaterializedRows();
        assertThat(rows).size().isEqualTo(1);
        assertThat(rows.getFirst().getFields()).first().isEqualTo("main");
    }

    @Test
    public void items()
    {
        List<MaterializedRow> rows = getQueryRunner().execute("SELECT name FROM fastapi.default.items").getMaterializedRows();
        assertThat(rows)
                .extracting(row -> row.getFields().getFirst())
                .containsExactly("Portal Gun", "Plumbus");
    }

    @Test
    public void itemsWithPageNumbers()
    {
        assertQuery(
                "SELECT name, page FROM fastapi.default.items",
                "VALUES ('Portal Gun', 1), ('Plumbus', 2)");
    }

    @Test
    public void itemsByIntegerArrayQueryParameter()
    {
        assertQuery(
                "SELECT name, item_ids FROM fastapi.default.items_by_ids WHERE item_ids = ARRAY[1, 2]",
                "VALUES ('Portal Gun', ARRAY[1, 2]), ('Plumbus', ARRAY[1, 2])");
    }

    @Test
    public void itemsByDelimitedArrayQueryParameter()
    {
        assertQuery(
                "SELECT name FROM fastapi.default.items_by_delimited_ids WHERE item_ids = ARRAY[1, 2]",
                "VALUES ('Portal Gun'), ('Plumbus')");
    }

    @Test
    public void itemsByArrayHeader()
    {
        assertQuery(
                "SELECT name FROM fastapi.default.items_by_header WHERE x_item_ids = ARRAY[1, 2]",
                "VALUES ('Portal Gun'), ('Plumbus')");
    }

    @Test
    public void itemsByCookie()
    {
        assertQuery(
                "SELECT name FROM fastapi.default.items_by_cookie WHERE item_id = 2",
                "VALUES ('Plumbus')");
    }

    @Test
    public void pathParameterWithReservedCharacters()
    {
        assertQuery(
                "SELECT path FROM echo.default.echo_path WHERE value = 'a/b?c&d#e f'",
                "VALUES '/echo/path/a%2Fb%3Fc%26d%23e%20f'");
    }

    @Test
    public void pathParameterStyles()
    {
        assertQuery(
                "SELECT path FROM echo.default.echo_path_array WHERE \"values\" = ARRAY['a/b', 'c,d']",
                "VALUES '/echo/path_array/a%2Fb,c%2Cd'");
        assertQuery(
                "SELECT path FROM echo.default.echo_label WHERE \"values\" = ARRAY['a.b', 'c d']",
                "VALUES '/echo/label/.a.b,c%20d'");
        assertQuery(
                "SELECT path FROM echo.default.echo_matrix WHERE \"values\" = ARRAY['a;b', 'c=d']",
                "VALUES '/echo/matrix/;values=a%3Bb;values=c%3Dd'");
        assertQuery(
                "SELECT path FROM echo.default.echo_path_object WHERE color = CAST(ROW(100, 'a/b') AS ROW(r bigint, name varchar))",
                "VALUES '/echo/path_object/r=100,name=a%2Fb'");
    }

    @Test
    public void decimalParametersInPlainNotation()
    {
        assertQuery(
                "SELECT path, query FROM echo.default.echo_decimals " +
                        "WHERE \"values\" = CAST(ARRAY[0.00000001, -1000] AS ARRAY(DECIMAL(18, 8))) AND value = CAST(0.00000001 AS DECIMAL(18, 8))",
                "VALUES ('/echo/decimals/0.00000001,-1000.00000000', 'value=0.00000001')");
    }

    @Test
    public void queryParameterWithReservedCharacters()
    {
        assertQuery(
                "SELECT query FROM echo.default.echo_query WHERE value = 'a/b?c&d#e f+g='",
                "VALUES 'value=a%2Fb%3Fc%26d%23e%20f%2Bg%3D'");
    }

    @Test
    public void objectParameterStyles()
    {
        String color = "CAST(ROW(100, 'a/b c') AS ROW(r bigint, name varchar))";
        assertQuery(
                "SELECT query FROM echo.default.echo_query_object WHERE color = " + color,
                "VALUES 'r=100&name=a%2Fb%20c'");
        assertQuery(
                "SELECT query FROM echo.default.echo_deep_object WHERE color = " + color,
                "VALUES 'color[r]=100&color[name]=a%2Fb%20c'");
        assertQuery(
                "SELECT header FROM echo.default.echo_header_object WHERE x_color = " + color,
                "VALUES 'r=100,name=a/b c'");
        assertQuery(
                "SELECT cookie FROM echo.default.echo_cookie_object WHERE color = " + color,
                "VALUES 'color=r%2C100%2Cname%2Ca%2Fb%20c'");
    }

    @Test
    public void itemsWithClampedPages()
    {
        List<MaterializedRow> rows = getQueryRunner().execute("SELECT name FROM fastapi.default.clamped_items").getMaterializedRows();
        assertThat(rows)
                .extracting(row -> row.getFields().getFirst())
                .containsExactly("Portal Gun", "Plumbus");
    }

    @Test
    public void itemsWithPagesInRequestBody()
    {
        List<MaterializedRow> rows = getQueryRunner().execute("SELECT name FROM fastapi.default.paged_search").getMaterializedRows();
        assertThat(rows)
                .extracting(row -> row.getFields().getFirst())
                .containsExactly("Portal Gun", "Plumbus");
    }

    @Test
    public void errors()
    {
        assertQueryFails("SELECT * FROM fastapi.default.error", "Server responded with error 418: \"Oops! Inevitable error happened. There goes a rainbow...\"");
    }

    private List<Object> selectNumbers(long id, String columns)
    {
        // all request body fields of the numbers table are predicates, so the connector doesn't apply a filter on id to responses
        List<MaterializedRow> rows = getQueryRunner().execute("SELECT id, %s FROM fastapi.default.numbers".formatted(columns)).getMaterializedRows().stream()
                .filter(row -> ((Number) row.getField(0)).longValue() == id)
                .toList();
        assertThat(rows).hasSize(1);
        List<Object> fields = rows.getFirst().getFields();
        return fields.subList(1, fields.size());
    }

    private TestTable generateDataset(String namePrefix, int elements)
    {
        return new TestTable(new TrinoSqlExecutor(getQueryRunner()), namePrefix, "AS SELECT CAST(sequential_number AS VARCHAR) AS item_id FROM TABLE(sequence(start=>0, stop=>%d))".formatted(elements - 1));
    }
}
