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
import io.trino.Session;
import io.trino.operator.OperatorStats;
import io.trino.sql.planner.plan.FilterNode;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.MaterializedResult;
import io.trino.testing.MaterializedRow;
import io.trino.testing.QueryRunner;
import io.trino.testing.QueryRunner.MaterializedResultWithPlan;
import io.trino.testing.sql.TestTable;
import io.trino.testing.sql.TrinoSqlExecutor;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.nio.file.Files;
import java.time.LocalDate;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

import static com.google.common.collect.Iterables.getOnlyElement;
import static com.google.common.collect.MoreCollectors.onlyElement;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static java.util.concurrent.Executors.newVirtualThreadPerTaskExecutor;
import static org.assertj.core.api.Assertions.assertThat;

public class TestOpenApiQueries
        extends AbstractTestQueryFramework
{
    KeycloakServer keycloakServer;
    PetStoreServer petStoreServer;
    FastApiServer fastApiServer;
    QueryRunner cachedQueryRunner;

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

        // every node has its own HTTP cache, so a query reuses a cached response only if its split runs on the same node, see https://github.com/nineinchnick/trino-openapi/issues/287
        cachedQueryRunner = closeAfterClass(OpenApiQueryRunner.createQueryRunner(
                Map.of("fastapi_cached", ImmutableMap.<String, String>builder()
                        .putAll(fastApiProperties.buildOrThrow())
                        .put("http-cache.directory", Files.createTempDirectory("trino-openapi-http-cache").toString())
                        .buildOrThrow()),
                0));

        return OpenApiQueryRunner.createQueryRunner(Map.of(
                "petstore", petStoreProperties.buildOrThrow(),
                "fastapi", fastApiProperties.buildOrThrow(),
                "fastapi_probe", ImmutableMap.<String, String>builder()
                        .putAll(fastApiProperties.buildOrThrow())
                        .put("split-strategy", "probe")
                        .buildOrThrow(),
                "fastapi_stride", ImmutableMap.<String, String>builder()
                        .putAll(fastApiProperties.buildOrThrow())
                        .put("split-strategy", "stride")
                        .buildOrThrow(),
                "echo", Map.of(
                        "spec-location", "src/test/resources/echo.yml",
                        "base-uri", fastApiServer.getApiUrl())));
    }

    @Test
    public void compressedResponses()
    {
        assertQuery("SELECT accept_encoding FROM fastapi.default.request_encoding", "VALUES 'gzip'");
        // the FastAPI server compresses responses bigger than 100 bytes
        assertQuery("SELECT count(*) FROM fastapi.default.items_by_ids WHERE item_ids = ARRAY[1, 2]", "VALUES 2");
    }

    @Test
    public void cachedResponses()
    {
        String query = "SELECT name FROM fastapi_cached.default.cached_items";
        assertCachedItems(cachedQueryRunner.getDefaultSession(), query);
        List<Object> requestsBefore = cachedItemRequests();
        // the response is fresh for 5 minutes, so it's reused without a request
        assertCachedItems(cachedQueryRunner.getDefaultSession(), query);
        assertThat(cachedItemRequests()).isEqualTo(requestsBefore);

        // without the cache, the stored response is revalidated, and the server confirms it hasn't changed
        Session withoutCache = Session.builder(cachedQueryRunner.getDefaultSession())
                .setCatalogSessionProperty("fastapi_cached", "http_cache_enabled", "false")
                .build();
        assertCachedItems(withoutCache, query);
        assertThat(cachedItemRequests()).containsExactly((long) requestsBefore.get(0) + 1, (long) requestsBefore.get(1) + 1);
    }

    private void assertCachedItems(Session session, String query)
    {
        assertThat(cachedQueryRunner.execute(session, query).getOnlyColumnAsSet())
                .containsExactlyInAnyOrder("Portal Gun", "Plumbus");
    }

    private List<Object> cachedItemRequests()
    {
        return computeActual("SELECT total, revalidated FROM fastapi.default.cached_item_requests").getMaterializedRows().getFirst().getFields();
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
    public void listColumns()
    {
        // listing columns without a schema must return the same columns as listing them in the only schema
        assertThat(computeActual("SELECT table_schema, column_name FROM petstore.information_schema.columns WHERE table_name = 'store_order'"))
                .isEqualTo(computeActual("SELECT table_schema, column_name FROM petstore.information_schema.columns WHERE table_schema = 'default' AND table_name = 'store_order'"));
        assertThat(computeActual("SELECT DISTINCT table_schema FROM petstore.information_schema.columns WHERE table_name = 'store_order'").getOnlyColumnAsSet())
                .containsExactly("default");
        // all petstore tables except store_inventory, which has no columns
        assertQuery(
                "SELECT count(DISTINCT table_name) FROM petstore.information_schema.columns WHERE table_schema = 'default'",
                "VALUES 8");
        assertQueryReturnsEmptyResult("SELECT column_name FROM petstore.information_schema.columns WHERE table_schema = 'other'");
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
        // notes can be updated using PATCH and PUT, and PATCH requests only include the updated columns
        assertUpdate("UPDATE fastapi.default.notes SET text = 'second' WHERE note_id = 1", 1);
        assertQuery(
                "SELECT id, text, priority, array_join(patched_fields, ',') FROM fastapi.default.notes WHERE note_id = 1",
                "VALUES (1, 'second', NULL, 'text')");
        assertUpdate("UPDATE fastapi.default.notes SET priority = 2 WHERE note_id = 1", 1);
        assertQuery(
                "SELECT id, text, priority, array_join(patched_fields, ',') FROM fastapi.default.notes WHERE note_id = 1",
                "VALUES (1, 'second', 2, 'priority')");
        assertUpdate("UPDATE fastapi.default.notes SET priority = NULL WHERE note_id = 1", 1);
        assertQuery(
                "SELECT id, text, priority, array_join(patched_fields, ',') FROM fastapi.default.notes WHERE note_id = 1",
                "VALUES (1, 'second', NULL, 'priority')");
        assertUpdate("DELETE FROM fastapi.default.notes WHERE note_id = 1", 1);
        assertQueryReturnsEmptyResult("SELECT text FROM fastapi.default.notes WHERE note_id = 1");
    }

    @Test
    public void deleteUsingPost()
    {
        // the POST operation marked for DELETE requires the URL in the request body
        assertQuery("SELECT url FROM fastapi.default.bookmarks", "VALUES 'https://trino.io', 'https://example.com'");
        assertUpdate("DELETE FROM fastapi.default.bookmarks WHERE url = 'https://example.com'", 1);
        assertQuery("SELECT url FROM fastapi.default.bookmarks", "VALUES 'https://trino.io'");
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
    public void filterOnColumnNotSentBySelectedOperation()
    {
        // id is a request body field of the POST operation, but the GET operation used to read rows doesn't send it
        assertUpdate("INSERT INTO fastapi.default.numbers (id) VALUES (3)", 1);
        assertQuery("SELECT id FROM fastapi.default.numbers WHERE id = 1", "VALUES 1");
        assertQuery("SELECT id FROM fastapi.default.numbers WHERE id IN (1, 3)", "VALUES 1, 3");
    }

    @Test
    public void filterOnNullsInPredicateColumn()
    {
        assertUpdate("INSERT INTO fastapi.default.numbers (id) VALUES (4)", 1);
        assertQuery("SELECT id FROM fastapi.default.numbers WHERE real_value IS NULL AND id IN (1, 4)", "VALUES 4");
        assertQuery("SELECT id FROM fastapi.default.numbers WHERE real_value IS NOT NULL AND id IN (1, 4)", "VALUES 1");
        assertQuery("SELECT id FROM fastapi.default.numbers WHERE id = 4 OR id IS NULL", "VALUES 4");
        // a null can't be sent as a required parameter
        assertQueryFails("SELECT decimal_value FROM fastapi.default.decimal_parameters WHERE decimal_value IS NULL", "Missing required constraint for decimal_value");
    }

    @Test
    public void selectPathByAvailablePredicates()
    {
        assertQuery(
                "SELECT name, temperature, units FROM fastapi.default.stations WHERE station_id = 2 AND units = 'imperial'",
                "VALUES ('Bravo', 23.0, 'imperial')");
        // the path with the station ID also requires units, so without them, all stations are read and filtered
        assertQuery(
                "SELECT name, temperature, units FROM fastapi.default.stations WHERE station_id = 2",
                "VALUES ('Bravo', -5.0, NULL)");
        assertQuery(
                "SELECT name FROM fastapi.default.stations",
                "VALUES 'Alpha', 'Bravo'");
        assertQuery(
                "SELECT comment_id, text FROM fastapi.default.comments WHERE author = 'alice'",
                "VALUES (1, 'First'), (3, 'Third')");
        assertQuery(
                "SELECT author, text FROM fastapi.default.comments WHERE comment_id = 2",
                "VALUES ('bob', 'Second')");
        // both paths use two predicates, but only the longer one returns values of the review ID
        assertQuery(
                "SELECT id, text FROM fastapi.default.products_reviews WHERE product_id = 1 AND rating = 5 AND review_id = 2",
                "VALUES (2, 'Good')");
        assertQuery(
                "SELECT id, text FROM fastapi.default.products_reviews WHERE product_id = 1 AND rating = 5",
                "VALUES (1, 'Great'), (2, 'Good')");
        // every value is read in a separate split, using the path with the comment ID
        assertQuery(
                "SELECT author, text FROM fastapi.default.comments WHERE comment_id IN (1, 2)",
                "VALUES ('alice', 'First'), ('bob', 'Second')");
        assertQueryFails(
                "SELECT text FROM fastapi.default.comments",
                "Missing required constraints for every path: GET /comments requires author; GET /comments/\\{comment_id} requires comment_id");
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
    public void selectWithUnionFieldInRowInRequestBody()
    {
        List<MaterializedRow> rows = getQueryRunner().execute("SELECT wrapper.name, wrapper.value FROM fastapi.default.echo_union " +
                "WHERE wrapper = CAST(ROW('answer', '{\"number\":42}') AS ROW(name VARCHAR, value VARCHAR))").getMaterializedRows();
        assertThat(rows).hasSize(1);
        assertThat(rows.getFirst().getFields()).containsExactly("answer", "{\"number\":42}");
    }

    @Test
    public void selectWithMapInRequestBody()
    {
        assertQuery(
                "SELECT labels['a'], labels['b'] FROM echo.default.echo_map WHERE labels = MAP(ARRAY['a', 'b'], ARRAY[1, 2])",
                "VALUES (1, 2)");
    }

    @Test
    public void selectWithUnionFieldInRequestBody()
    {
        assertQuery(
                "SELECT value FROM echo.default.echo_union_value WHERE value = '{\"number\":42}'",
                "VALUES '{\"number\":42}'");
    }

    @Test
    public void selectOpenApi31TypesInRequestBody()
    {
        // the serial number doesn't fit in INTEGER, nor in DOUBLE without losing precision
        MaterializedResult result = getQueryRunner().execute("SELECT in_stock, serial_number, weight, density, dimensions.width, dimensions.height, dimensions.unit " +
                "FROM fastapi.default.echo_gadget " +
                "WHERE in_stock = true AND serial_number = 9007199254740993 AND weight = REAL '1.5' AND density = DOUBLE '0.1' " +
                "AND dimensions = CAST(ROW(2, 3, 'cm') AS ROW(width BIGINT, height BIGINT, unit VARCHAR))");
        assertThat(result.getTypes()).containsExactly(BOOLEAN, BIGINT, REAL, DOUBLE, BIGINT, BIGINT, VARCHAR);
        assertThat(result.getMaterializedRows()).hasSize(1);
        assertThat(result.getMaterializedRows().getFirst().getFields()).containsExactly(
                true,
                9007199254740993L,
                1.5f,
                0.1,
                2L,
                3L,
                "cm");
    }

    @Test
    public void selectFromUnionOfObjectAndArray()
    {
        for (String table : List.of("contents", "contents_results")) {
            assertQuery(
                    "SELECT name, content, size FROM fastapi.default.%s WHERE path = 'readme'".formatted(table),
                    "VALUES ('readme', 'Hello', NULL)");
            assertQuery(
                    "SELECT name, content, size FROM fastapi.default.%s WHERE path = 'docs'".formatted(table),
                    "VALUES ('a', NULL, 1), ('b', NULL, 2)");
        }
    }

    @Test
    public void selectNestedObjectUnion()
    {
        // the id is defined as a string and an integer, so it's raw JSON
        List<MaterializedRow> rows = getQueryRunner().execute("SELECT name, actor.id, actor.bot_name, actor.email FROM fastapi.default.events").getMaterializedRows();
        assertThat(rows).extracting(MaterializedRow::getFields).containsExactly(
                Arrays.asList("build", "ci", "CI", null),
                Arrays.asList("review", "7", null, "jane@example.com"));

        assertQuery(
                "SELECT actor.id, actor.email FROM fastapi.default.echo_event " +
                        "WHERE name = 'review' AND actor = CAST(ROW('7', NULL, 'jane@example.com') AS ROW(id VARCHAR, bot_name VARCHAR, email VARCHAR))",
                "VALUES ('7', 'jane@example.com')");
    }

    @Test
    public void selectFromUnionResponse()
    {
        // FastAPI describes union responses using anyOf, and echo.yml using oneOf
        for (String catalog : List.of("fastapi", "echo")) {
            List<MaterializedRow> rows = getQueryRunner().execute("SELECT views, code, message FROM %s.default.user_views WHERE username = 'jane'".formatted(catalog)).getMaterializedRows();
            assertThat(rows).hasSize(1);
            assertThat(rows.getFirst().getFields()).as(catalog).containsExactly(List.of("home", "settings"), null, null);

            rows = getQueryRunner().execute("SELECT views, code, message FROM %s.default.user_views WHERE username = 'unknown'".formatted(catalog)).getMaterializedRows();
            assertThat(rows).hasSize(1);
            assertThat(rows.getFirst().getFields()).as(catalog).containsExactly(null, 404L, "Unknown user");
        }

        List<MaterializedRow> rows = getQueryRunner().execute("SELECT views, code, message FROM fastapi.default.user_views_list").getMaterializedRows();
        assertThat(rows).extracting(MaterializedRow::getFields).containsExactly(
                Arrays.asList(List.of("home"), null, null),
                Arrays.asList(null, 404L, "Unknown user"));
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
    public void selectWithDateTimeParameters()
    {
        // the server returns the parsed value both in UTC and with a -05:30 offset
        assertQuery(
                "SELECT at, at_text, received_at, shifted_at FROM fastapi.default.datetime_parameters WHERE at = TIMESTAMP '2025-04-18 12:30:45.123'",
                "VALUES (TIMESTAMP '2025-04-18 12:30:45.123', '2025-04-18T12:30:45.123Z', TIMESTAMP '2025-04-18 12:30:45.123', TIMESTAMP '2025-04-18 12:30:45.123')");
        assertQuery(
                "SELECT at_text, received_at, shifted_at FROM fastapi.default.datetime_parameters WHERE at = TIMESTAMP '1969-12-31 23:59:59.999'",
                "VALUES ('1969-12-31T23:59:59.999Z', TIMESTAMP '1969-12-31 23:59:59.999', TIMESTAMP '1969-12-31 23:59:59.999')");
    }

    @Test
    public void selectWithDateParameters()
    {
        assertQuery(
                "SELECT day, day_text, next_day FROM fastapi.default.date_parameters WHERE day = DATE '2025-04-18'",
                "VALUES (DATE '2025-04-18', '2025-04-18', DATE '2025-04-19')");
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
    public void joinProvidingRequiredPredicates()
    {
        try (TestTable table = createIdsTable("memory.default.test_comment_ids", "1, 3")) {
            // comments can only be listed by author, so without one, their IDs can only come from a dynamic filter
            assertQuery(
                    "SELECT author, text FROM fastapi.default.comments WHERE comment_id IN (SELECT id FROM %s)".formatted(table.getName()),
                    "VALUES ('alice', 'First'), ('alice', 'Third')");
            assertQuery(
                    "SELECT c.author, c.text FROM fastapi.default.comments c JOIN %s t ON c.comment_id = t.id".formatted(table.getName()),
                    "VALUES ('alice', 'First'), ('alice', 'Third')");
        }
        try (TestTable table = createIdsTable("memory.default.test_item_ids", "1, 2")) {
            // the path listing items doesn't return item IDs, so the join wouldn't match any of its rows
            assertQuery(
                    "SELECT name FROM fastapi.default.items WHERE item_id IN (SELECT id FROM %s)".formatted(table.getName()),
                    "VALUES 'Portal Gun', 'Plumbus'");
        }
    }

    @Test
    public void joinWithPushedDownPredicates()
    {
        try (TestTable table = createIdsTable("memory.default.test_comment_ids", "1, 3")) {
            assertQuery(
                    "SELECT c.text FROM fastapi.default.comments c JOIN %s t ON c.comment_id = t.id WHERE c.comment_id IN (1, 2)".formatted(table.getName()),
                    "VALUES 'First'");
            assertQueryReturnsEmptyResult("SELECT text FROM fastapi.default.comments WHERE comment_id IN (SELECT id FROM %s WHERE id > 5)".formatted(table.getName()));
        }
    }

    @Test
    public void joinProvidingRangesOfRequiredPredicates()
    {
        // comparisons in join conditions produce dynamic filters with ranges, which are expanded to single values
        String query = "SELECT c.text FROM fastapi.default.comments c JOIN %s t ON c.comment_id >= t.id AND c.comment_id <= t.id";
        try (TestTable table = createIdsTable("memory.default.test_comment_ids", "1, 3")) {
            assertQuery(query.formatted(table.getName()), "VALUES 'First', 'Third'");
        }
        try (TestTable table = createIdsTable("memory.default.test_comment_ids", "1, 1000")) {
            assertQueryFails(
                    query.formatted(table.getName()),
                    "Missing required constraints for every path: GET /comments requires author; GET /comments/\\{comment_id} requires comment_id. " +
                            "Dynamic filters from joins for comment_id have values that can't be sent, like ranges with more than 256 values \\(domain-expansion-limit\\)");
        }
    }

    @Test
    public void filterOnRangeMatchingBounds()
    {
        // the bounds match the comparisons, and the API applies them exactly, so Trino doesn't filter the rows again
        assertThat(query("SELECT id FROM fastapi.default.exact_changes WHERE updated_at >= TIMESTAMP '2024-01-04 00:00:00'"))
                .isFullyPushedDown();
        assertThat(query("SELECT id FROM fastapi.default.exact_changes WHERE updated_at >= TIMESTAMP '2024-01-02 00:00:00' AND updated_at < TIMESTAMP '2024-01-04 00:00:00'"))
                .isFullyPushedDown();
        assertQuery(
                "SELECT id, array_sort(split(query, '&')) FROM fastapi.default.exact_changes WHERE updated_at >= TIMESTAMP '2024-01-02 00:00:00' AND updated_at < TIMESTAMP '2024-01-04 00:00:00'",
                "VALUES " +
                        "(2, ARRAY['since=2024-01-02T00%3A00%3A00.000Z', 'until=2024-01-04T00%3A00%3A00.000Z']), " +
                        "(3, ARRAY['since=2024-01-02T00%3A00%3A00.000Z', 'until=2024-01-04T00%3A00%3A00.000Z'])");
    }

    @Test
    public void filterOnRangeNotMatchingBounds()
    {
        // an inclusive bound is sent for an exclusive comparison, but Trino still filters out rows equal to it
        assertThat(query("SELECT id FROM fastapi.default.exact_changes WHERE updated_at > TIMESTAMP '2024-01-04 00:00:00'"))
                .isNotFullyPushedDown(FilterNode.class);
        assertQuery(
                "SELECT id, query FROM fastapi.default.exact_changes WHERE updated_at > TIMESTAMP '2024-01-04 00:00:00'",
                "VALUES (5, 'since=2024-01-04T00%3A00%3A00.000Z')");
        // an exclusive bound isn't sent for an inclusive comparison, because the API would skip rows equal to it
        assertThat(query("SELECT id FROM fastapi.default.exact_changes WHERE updated_at <= TIMESTAMP '2024-01-02 00:00:00'"))
                .isNotFullyPushedDown(FilterNode.class);
        assertQuery(
                "SELECT id, query FROM fastapi.default.exact_changes WHERE updated_at <= TIMESTAMP '2024-01-02 00:00:00'",
                "VALUES (1, ''), (2, '')");
        // disjoint ranges are sent as their span
        assertThat(query("SELECT id FROM fastapi.default.exact_changes WHERE updated_at IN (TIMESTAMP '2024-01-02 00:00:00', TIMESTAMP '2024-01-04 00:00:00')"))
                .isNotFullyPushedDown(FilterNode.class);
        assertQuery(
                "SELECT id, query FROM fastapi.default.exact_changes WHERE updated_at IN (TIMESTAMP '2024-01-02 00:00:00', TIMESTAMP '2024-01-04 00:00:00')",
                "VALUES (2, 'since=2024-01-02T00%3A00%3A00.000Z'), (4, 'since=2024-01-02T00%3A00%3A00.000Z')");
        assertQuery(
                "SELECT id, query FROM fastapi.default.exact_changes WHERE updated_at < TIMESTAMP '2024-01-02 00:00:00' OR updated_at >= TIMESTAMP '2024-01-05 00:00:00'",
                "VALUES (1, ''), (5, '')");
        // the API can't return rows without a value
        assertQuery(
                "SELECT id, query FROM fastapi.default.exact_changes WHERE updated_at >= TIMESTAMP '2024-01-05 00:00:00' OR updated_at IS NULL",
                "VALUES (5, '')");
        // a filter on the parameter is sent instead of the bound, and the parameter's value is only known when it's sent
        assertThat(query("SELECT id FROM fastapi.default.exact_changes WHERE since = TIMESTAMP '2024-01-04 00:00:00' AND updated_at >= TIMESTAMP '2024-01-02 00:00:00'"))
                .skipResultsCorrectnessCheckForPushdown()
                .isNotFullyPushedDown(FilterNode.class);
        assertQuery(
                "SELECT id, query FROM fastapi.default.exact_changes WHERE since = TIMESTAMP '2024-01-04 00:00:00' AND updated_at >= TIMESTAMP '2024-01-02 00:00:00'",
                "VALUES (4, 'since=2024-01-04T00%3A00%3A00.000Z'), (5, 'since=2024-01-04T00%3A00%3A00.000Z')");
    }

    @Test
    public void filterOnRangeWithInexactBounds()
    {
        // the API ignores the time of the bound and returns more rows, which Trino filters out
        assertThat(query("SELECT id FROM fastapi.default.approximate_changes WHERE updated_at >= TIMESTAMP '2024-01-03 12:00:00'"))
                .isNotFullyPushedDown(FilterNode.class);
        assertQuery(
                "SELECT id, query FROM fastapi.default.approximate_changes WHERE updated_at >= TIMESTAMP '2024-01-03 12:00:00'",
                "VALUES (4, 'since=2024-01-03T12%3A00%3A00.000Z'), (5, 'since=2024-01-03T12%3A00%3A00.000Z')");
    }

    @Test
    public void filterOnRangeNotSentBySelectedPath()
    {
        // the path with the change ID can't send the bounds, so its rows are always filtered by Trino
        assertThat(query("SELECT id FROM fastapi.default.changes WHERE updated_at >= TIMESTAMP '2024-01-04 00:00:00'"))
                .isNotFullyPushedDown(FilterNode.class);
        assertQuery(
                "SELECT id, query FROM fastapi.default.changes WHERE updated_at >= TIMESTAMP '2024-01-04 00:00:00'",
                "VALUES (4, 'since=2024-01-04T00%3A00%3A00.000Z'), (5, 'since=2024-01-04T00%3A00%3A00.000Z')");
        assertQuery(
                "SELECT id, query FROM fastapi.default.changes WHERE change_id IN (1, 4) AND updated_at >= TIMESTAMP '2024-01-04 00:00:00'",
                "VALUES (4, '')");
    }

    @Test
    public void limitWithExactRangeStopsPagination()
    {
        // the API returns one change per page, and Trino doesn't filter the rows of an exact range, so the limit stops pagination
        assertPagedChanges("SELECT id FROM fastapi.default.paged_changes WHERE updated_at >= TIMESTAMP '2024-01-02 00:00:00' LIMIT 1", 1, 2L);
        // Trino filters out the change equal to the bound, so all four pages since the bound are requested
        assertPagedChanges("SELECT id FROM fastapi.default.paged_changes WHERE updated_at > TIMESTAMP '2024-01-02 00:00:00' LIMIT 1", 4, 3L);
    }

    @Test
    public void joinProvidingRangeBounds()
    {
        try (TestTable table = new TestTable(new TrinoSqlExecutor(getQueryRunner()), "memory.default.test_change_times", "AS SELECT TIMESTAMP '2024-01-04 00:00:00.000' AS updated_at")) {
            assertQuery(
                    "SELECT c.id, c.query FROM fastapi.default.exact_changes c JOIN %s t ON c.updated_at = t.updated_at".formatted(table.getName()),
                    "VALUES (4, 'since=2024-01-04T00%3A00%3A00.000Z')");
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
                .containsExactlyInAnyOrder("Portal Gun", "Plumbus");
    }

    @Test
    public void writableItems()
    {
        // tables supporting DELETE have a hidden row ID column, which has no source name
        assertQuery(
                "SELECT name, page FROM fastapi.default.writable_items",
                "VALUES ('Portal Gun', 1), ('Plumbus', 2)");
        assertQuery(
                "SELECT name FROM fastapi.default.writable_items ORDER BY price LIMIT 1",
                "VALUES 'Plumbus'");
    }

    @Test
    public void paginatedResultsNullability()
    {
        assertQuery(
                "SELECT column_name, is_nullable FROM fastapi.information_schema.columns " +
                        "WHERE table_schema = 'default' AND table_name = 'has_more_items' " +
                        "AND column_name IN ('has_more', 'id', 'name', 'price', 'description', 'tax')",
                "VALUES ('has_more', 'NO'), ('id', 'NO'), ('name', 'NO'), ('price', 'NO'), ('description', 'YES'), ('tax', 'YES')");
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
                "VALUES ('Portal Gun', ARRAY[CAST(1 AS BIGINT), CAST(2 AS BIGINT)]), ('Plumbus', ARRAY[CAST(1 AS BIGINT), CAST(2 AS BIGINT)])");
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
    public void unionParameters()
    {
        // union types are mapped to VARCHAR holding JSON, and plain text is sent as-is
        assertQuery(
                "SELECT query FROM echo.default.echo_union_query WHERE \"values\" = ARRAY['\"a b\"', '5', 'c']",
                "VALUES 'values=a%20b&values=5&values=c'");
        assertThat(insertAndGetLastPost("INSERT INTO echo.default.echo_union_write (name, value) VALUES ('x', '\"a/b\"')"))
                .containsExactly("/echo/union_write", "value=a%2Fb");
    }

    @Test
    public void parameterDefaults()
    {
        // rows show the values that were sent, like they show the values of filters
        MaterializedRow row = getOnlyElement(computeActual("SELECT query, header, cookie, \"limit\", archived, x_color, flavor FROM echo.default.echo_defaults").getMaterializedRows());
        assertThat(((String) row.getField(0)).split("&")).containsExactlyInAnyOrder("limit=100", "archived=false");
        assertThat(row.getFields().subList(1, row.getFieldCount())).containsExactly("green", "flavor=mint", 100L, false, "green", "mint");

        row = getOnlyElement(computeActual("SELECT query, header, x_color FROM echo.default.echo_defaults WHERE \"limit\" = 5 AND x_color = 'red'").getMaterializedRows());
        assertThat(((String) row.getField(0)).split("&")).containsExactlyInAnyOrder("limit=5", "archived=false");
        assertThat(row.getFields().subList(1, row.getFieldCount())).containsExactly("red", "red");

        assertThat(insertAndGetLastPost("INSERT INTO echo.default.echo_defaults (name) VALUES ('x')"))
                .containsExactly("/echo/defaults", "limit=10");
        assertThat(insertAndGetLastPost("INSERT INTO echo.default.echo_defaults (name, \"limit\") VALUES ('x', 5)"))
                .containsExactly("/echo/defaults", "limit=5");
    }

    @Test
    public void parameterDefaultsInPaginatedRequests()
    {
        // the page size is the default value, not the one declared in the parameter's schema, so a shorter page ends pagination
        assertThat(assertDefaultedItems("SELECT name, state, per_page FROM fastapi.default.defaulted_items", 1, row("Portal Gun", "all", 3L), row("Plumbus", "all", 3L)))
                .contains("state=all", "per_page=3");
        // filters are sent instead of the defaults
        assertThat(assertDefaultedItems("SELECT name, state FROM fastapi.default.defaulted_items WHERE state = 'closed'", 1, row("Plumbus", "closed")))
                .contains("state=closed", "per_page=3");
        assertThat(assertDefaultedItems("SELECT name, per_page FROM fastapi.default.defaulted_items WHERE per_page = 1", 3, row("Portal Gun", 1L), row("Plumbus", 1L)))
                .contains("state=all", "per_page=1");
        assertDefaultedItems("SELECT name, state FROM fastapi.default.defaulted_items WHERE state IN ('open', 'closed')", 2, row("Portal Gun", "open"), row("Plumbus", "closed"));
        // a filter on the default value returns the same rows
        assertDefaultedItems("SELECT name, state FROM fastapi.default.defaulted_items WHERE state = 'all'", 1, row("Portal Gun", "all"), row("Plumbus", "all"));
        // getting a single item doesn't have the parameters with defaults
        assertThat(assertDefaultedItems("SELECT name, state FROM fastapi.default.defaulted_items WHERE item_id = 2", 1, row("Plumbus", null)))
                .isEmpty();
        // sort parameters are sent with the defaults
        assertThat(assertDefaultedItems("SELECT name FROM fastapi.default.defaulted_items ORDER BY price DESC LIMIT 1", 1, row("Portal Gun")))
                .contains("sort=price", "direction=desc", "state=all", "per_page=3");
    }

    @Test
    public void selectFromTableWithoutSelectOperation()
    {
        // POST operations are used for SELECT only when marked with the sqlOperation extension
        assertQueryFails(
                "SELECT * FROM echo.default.echo_union_write",
                "Table echo_union_write has no operation for SELECT. Set sqlOperation in the x-trino extension of an operation to use it for SELECT");
    }

    @Test
    public void updateTableWithoutUpdateOperation()
    {
        assertQueryFails(
                "UPDATE fastapi.default.search SET name = 'x' WHERE item_ids = ARRAY['1']",
                "Table search has no operation for UPDATE or DELETE. Set sqlOperation in the x-trino extension of an operation to use it for UPDATE or DELETE");
        assertQueryFails(
                "DELETE FROM fastapi.default.search WHERE item_ids = ARRAY['1']",
                "Table search has no operation for UPDATE or DELETE. Set sqlOperation in the x-trino extension of an operation to use it for UPDATE or DELETE");
    }

    @Test
    public void queryParameterWithReservedCharacters()
    {
        assertQuery(
                "SELECT query FROM echo.default.echo_query WHERE value = 'a/b?c&d#e f+g='",
                "VALUES 'value=a%2Fb%3Fc%26d%23e%20f%2Bg%3D'");
    }

    @Test
    public void queryParameterAllowingReservedCharacters()
    {
        assertQuery(
                "SELECT query FROM echo.default.echo_query_reserved WHERE value = 'a/b?c&d#e f+%2F[x]'",
                "VALUES 'value=a/b?c&d%23e%20f+%2F%5Bx%5D'");
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
    public void itemsEndingWithShortPage()
    {
        assertThat(getQueryRunner().execute(withMaxRunTime(), "SELECT name FROM fastapi.default.clamped_sized_items").getMaterializedRows())
                .extracting(row -> row.getFields().getFirst())
                .containsExactly("Portal Gun", "Plumbus");
    }

    @Test
    public void itemsWithPageSizeAboveMaximum()
    {
        assertThat(getQueryRunner().execute(withMaxRunTime(), "SELECT name FROM fastapi.default.capped_items WHERE size = 5").getMaterializedRows())
                .extracting(row -> row.getFields().getFirst())
                .containsExactly("Portal Gun", "Plumbus");
    }

    @Test
    public void itemsEndingWithHasMore()
    {
        assertThat(getQueryRunner().execute(withMaxRunTime(), "SELECT name FROM fastapi.default.has_more_items").getMaterializedRows())
                .extracting(row -> row.getFields().getFirst())
                .containsExactly("Portal Gun", "Plumbus");
    }

    @Test
    public void itemsEndingWithTotalPages()
    {
        assertThat(getQueryRunner().execute(withMaxRunTime(), "SELECT name FROM fastapi.default.total_pages_items").getMaterializedRows())
                .extracting(row -> row.getFields().getFirst())
                .containsExactlyInAnyOrder("Portal Gun", "Plumbus");
    }

    @Test
    public void itemsEndingWithMaxPages()
    {
        assertThat(getQueryRunner().execute(withMaxRunTime(), "SELECT name FROM fastapi.default.max_pages_items").getMaterializedRows())
                .extracting(row -> row.getFields().getFirst())
                .containsExactly("Portal Gun", "Plumbus");
    }

    @Test
    public void itemsWithClampedPages()
    {
        List<MaterializedRow> rows = getQueryRunner().execute("SELECT name FROM fastapi.default.clamped_items").getMaterializedRows();
        assertThat(rows)
                .extracting(row -> row.getFields().getFirst())
                .containsExactlyInAnyOrder("Portal Gun", "Plumbus");
    }

    @Test
    public void limitStopsPagination()
    {
        assertCountedItems("SELECT name FROM fastapi.default.counted_items", 2, "Portal Gun", "Plumbus");
        assertCountedItems("SELECT name FROM fastapi.default.counted_items LIMIT 1", 1, "Portal Gun");
        assertCountedItems("SELECT name FROM fastapi.default.counted_items WHERE page = 2 LIMIT 1", 1, "Plumbus");
    }

    @Test
    public void limitWithRemainingFilterRequestsAllPages()
    {
        // the matching row is on the last page, so stopping after reading enough rows would return no rows
        assertCountedItems("SELECT name FROM fastapi.default.counted_items WHERE price < 40 LIMIT 1", 2, "Plumbus");
    }

    @Test
    public void filterSentByOnlySomePathsAllowsPushdown()
    {
        // getting a single item can't filter by name, but the connector applies the filter if that path is used
        assertThat(assertCountedItems("SELECT name FROM fastapi.default.counted_items WHERE name = 'Plumbus' LIMIT 1", 1, "Plumbus"))
                .contains("name=Plumbus");
        assertThat(assertCountedItems("SELECT name FROM fastapi.default.counted_items WHERE name = 'Plumbus' ORDER BY price DESC LIMIT 1", 1, "Plumbus"))
                .contains("name=Plumbus", "sort=price", "direction=desc");
        // the item IDs select the path getting a single item, which doesn't send the name, so the connector filters the rows
        assertCountedItems("SELECT name FROM fastapi.default.counted_items WHERE item_id IN (1, 2) AND name = 'Plumbus'", 2, "Plumbus");
        assertCountedItems("SELECT name FROM fastapi.default.counted_items WHERE item_id IN (1, 2) AND name = 'Squanchy'", 2);
        // dynamic filters select the same path
        assertCountedItems("SELECT c.name FROM fastapi.default.counted_items c JOIN (VALUES 1, 2) t(id) ON c.item_id = t.id WHERE c.name = 'Plumbus'", 2, "Plumbus");
    }

    @Test
    public void maxResultsStopsPagination()
    {
        assertThat(assertCountedItems("SELECT name FROM fastapi.default.max_results_items", 1, "Portal Gun"))
                .isEqualTo("page=1");
        assertCountedItems("SELECT name FROM fastapi.default.max_results_items LIMIT 5", 1, "Portal Gun");
    }

    @Test
    public void topNSendsSortParameters()
    {
        // the table has an INSERT operation, so the column allows nulls, but every response includes it
        assertQuery(
                "SELECT is_nullable FROM fastapi.information_schema.columns WHERE table_schema = 'default' AND table_name = 'counted_items' AND column_name = 'price'",
                "VALUES 'YES'");
        assertThat(assertCountedItems("SELECT name FROM fastapi.default.counted_items ORDER BY price LIMIT 1", 1, "Plumbus"))
                .contains("sort=price", "direction=asc");
        assertThat(assertCountedItems("SELECT name FROM fastapi.default.counted_items ORDER BY price DESC LIMIT 1", 1, "Portal Gun"))
                .contains("sort=price", "direction=desc");
        assertThat(assertCountedItems("SELECT name FROM fastapi.default.counted_items WHERE sort = 'price' AND direction = 'asc' ORDER BY price LIMIT 1", 1, "Plumbus"))
                .contains("sort=price", "direction=asc");
    }

    @Test
    public void topNWithoutSortParametersRequestsAllPages()
    {
        // not declared in sortValues
        assertThat(assertCountedItems("SELECT name FROM fastapi.default.counted_items ORDER BY name LIMIT 1", 2, "Plumbus"))
                .doesNotContain("sort=");
        // the server might place nulls differently than the query
        assertThat(assertCountedItems("SELECT tax FROM fastapi.default.counted_items ORDER BY tax LIMIT 1", 2, (String) null))
                .doesNotContain("sort=");
        // the query filters on the direction parameter with a different value
        assertThat(assertCountedItems("SELECT name FROM fastapi.default.counted_items WHERE direction = 'desc' ORDER BY price LIMIT 1", 2, "Plumbus"))
                .doesNotContain("sort=");
        // rows would contain the values of the sort parameters
        assertThat(assertCountedItems("SELECT name, sort FROM fastapi.default.counted_items ORDER BY price LIMIT 1", 2, "Plumbus"))
                .doesNotContain("sort=");
        // Trino filters the returned rows
        assertThat(assertCountedItems("SELECT name FROM fastapi.default.counted_items WHERE name <> 'Plumbus' ORDER BY price LIMIT 1", 2, "Portal Gun"))
                .doesNotContain("sort=");
    }

    @Test
    public void itemsWithPagesInRequestBody()
    {
        List<MaterializedRow> rows = getQueryRunner().execute("SELECT name FROM fastapi.default.paged_search").getMaterializedRows();
        assertThat(rows)
                .extracting(row -> row.getFields().getFirst())
                .containsExactlyInAnyOrder("Portal Gun", "Plumbus");
    }

    @Test
    public void itemsWithSplitPerPage()
    {
        // the number of pages is known from the total number of results or pages in the first page
        assertSplits("SELECT name FROM fastapi_probe.default.items", 2);
        assertSplits("SELECT name FROM fastapi_probe.default.items WHERE per_page = 1", 2);
        assertSplits("SELECT name FROM fastapi_probe.default.items WHERE per_page = 2", 1);
        assertSplits("SELECT name FROM fastapi_probe.default.clamped_items", 2);
        assertSplits("SELECT name FROM fastapi_probe.default.total_pages_items", 2);
        assertSplits("SELECT name FROM fastapi_probe.default.paged_search", 2);
        // without a total, pages are read in a single split
        assertSplits("SELECT name FROM fastapi_probe.default.has_more_items", 1);
        // every value is read in a separate split, without probing for the number of pages
        assertSplits("SELECT DISTINCT name FROM fastapi_probe.default.items WHERE per_page IN (1, 2)", 2);
        // pagination stops once the limit is reached
        assertSplits("SELECT name FROM fastapi_probe.default.items LIMIT 5", 1);
        // pages are not split by default
        assertSplits("SELECT name FROM fastapi.default.items", 1);
    }

    @Test
    public void itemsWithStrideSplits()
    {
        assertSplits("SELECT name FROM fastapi_stride.default.items", 3);
        // the server returns the last page for every page number past it, which must be skipped
        assertSplits("SELECT name FROM fastapi_stride.default.clamped_items", 3);
        // without a signal of the last page, pages are read in a single split
        assertSplits("SELECT name FROM fastapi_stride.default.max_pages_items", 1);
        // pagination stops once the limit is reached
        assertSplits("SELECT name FROM fastapi_stride.default.items LIMIT 5", 1);
    }

    @Test
    @Disabled("Stride splits past the last page return its rows again, see https://github.com/nineinchnick/trino-openapi/issues/283")
    public void itemsWithStrideSplitsPastLastPage()
    {
        // the server returns the last page for every page number past it, which must be skipped
        assertSplits("SELECT name FROM fastapi_stride.default.has_more_items", getQueryRunner().getNodeCount());
        assertSplits("SELECT name FROM fastapi_stride.default.clamped_sized_items", getQueryRunner().getNodeCount());
    }

    @Test
    public void itemsWithSplitStrategySessionProperties()
    {
        // the session property overrides the catalog property
        assertSplits(withSession("split_strategy", "off"), "SELECT name FROM fastapi_probe.default.items", 1);
        assertSplits(withSession("split_strategy", "probe"), "SELECT name FROM fastapi.default.items", 2);
        // and the spec, which sets strideSplits only for some operations
        assertSplits(withSession("split_strategy", "stride"), "SELECT name FROM fastapi.default.items", 3);
        assertSplits(withSession("probe_limit", "0"), "SELECT name FROM fastapi_probe.default.items WHERE per_page = 1", 2);
        assertQueryFails(withSession("probe_limit", "few"), "SELECT name FROM fastapi_probe.default.items", ".*Invalid probe limit: 'few', must be a number or none.*");
    }

    @Test
    public void errors()
    {
        assertQueryFails(
                "SELECT * FROM fastapi.default.error",
                "Request GET http://[^ ?]+/error for operation error_error_get failed with HTTP 418: \"Oops! Inevitable error happened\\. There goes a rainbow\\.\\.\\.\"");
    }

    private List<Object> selectNumbers(long id, String columns)
    {
        List<MaterializedRow> rows = getQueryRunner().execute("SELECT %s FROM fastapi.default.numbers WHERE id = %d".formatted(columns, id)).getMaterializedRows();
        assertThat(rows).hasSize(1);
        return rows.getFirst().getFields();
    }

    // tests run concurrently, so requests made by other tests must not be counted, and the query string of the last request is returned
    private synchronized String assertCountedItems(String query, long expectedRequests, String... expectedNames)
    {
        long requestsBefore = (long) getCountedItemsRequests().getField(0);
        assertThat(getQueryRunner().execute(withMaxRunTime(), query).getMaterializedRows())
                .extracting(row -> row.getFields().getFirst())
                .containsExactly((Object[]) expectedNames);
        MaterializedRow requests = getCountedItemsRequests();
        assertThat((long) requests.getField(0) - requestsBefore).isEqualTo(expectedRequests);
        return (String) requests.getField(1);
    }

    private synchronized String assertDefaultedItems(String query, long expectedRequests, List<?>... expectedRows)
    {
        long requestsBefore = (long) getDefaultedItemsRequests().getField(0);
        assertThat(getQueryRunner().execute(withMaxRunTime(), query).getMaterializedRows())
                .extracting(row -> (Object) row.getFields())
                .containsExactlyInAnyOrder((Object[]) expectedRows);
        MaterializedRow requests = getDefaultedItemsRequests();
        assertThat((long) requests.getField(0) - requestsBefore).isEqualTo(expectedRequests);
        return (String) requests.getField(1);
    }

    private MaterializedRow getDefaultedItemsRequests()
    {
        return getOnlyElement(computeActual("SELECT requests, last_query FROM fastapi.default.defaulted_items_requests").getMaterializedRows());
    }

    private static List<Object> row(Object... fields)
    {
        return Arrays.asList(fields);
    }

    // tests run concurrently, and the server saves only the last POST request
    private synchronized List<Object> insertAndGetLastPost(String insert)
    {
        assertUpdate(insert, 1);
        return getOnlyElement(computeActual("SELECT path, query FROM echo.default.echo_last_post").getMaterializedRows()).getFields();
    }

    private synchronized void assertPagedChanges(String query, long expectedRequests, Long... expectedIds)
    {
        long requestsBefore = getPagedChangesRequests();
        assertThat(getQueryRunner().execute(withMaxRunTime(), query).getMaterializedRows())
                .extracting(row -> row.getFields().getFirst())
                .containsExactly((Object[]) expectedIds);
        assertThat(getPagedChangesRequests() - requestsBefore).isEqualTo(expectedRequests);
    }

    private long getPagedChangesRequests()
    {
        return (long) computeScalar("SELECT requests FROM fastapi.default.paged_changes_requests");
    }

    private MaterializedRow getCountedItemsRequests()
    {
        return getOnlyElement(computeActual("SELECT requests, last_query FROM fastapi.default.counted_items_requests").getMaterializedRows());
    }

    private void assertSplits(String sql, long expectedSplits)
    {
        assertSplits(getSession(), sql, expectedSplits);
    }

    private void assertSplits(Session session, String sql, long expectedSplits)
    {
        MaterializedResultWithPlan result = getDistributedQueryRunner().executeWithPlan(withMaxRunTime(session), sql);
        assertThat(result.result().getMaterializedRows())
                .extracting(row -> row.getFields().getFirst())
                .containsExactlyInAnyOrder("Portal Gun", "Plumbus");
        OperatorStats scanStats = getDistributedQueryRunner().getCoordinator()
                .getQueryManager()
                .getFullQueryInfo(result.queryId())
                .getQueryStats()
                .getOperatorSummaries()
                .stream()
                .filter(summary -> summary.getOperatorType().contains("Scan"))
                .collect(onlyElement());
        assertThat(scanStats.getTotalDrivers()).isEqualTo(expectedSplits);
    }

    // fail instead of hanging when pagination never ends
    private Session withMaxRunTime()
    {
        return withMaxRunTime(getSession());
    }

    private static Session withMaxRunTime(Session session)
    {
        return Session.builder(session)
                .setSystemProperty("query_max_run_time", "1m")
                .build();
    }

    private Session withSession(String property, String value)
    {
        return Session.builder(getSession())
                .setCatalogSessionProperty("fastapi", property, value)
                .setCatalogSessionProperty("fastapi_probe", property, value)
                .build();
    }

    private TestTable generateDataset(String namePrefix, int elements)
    {
        return new TestTable(new TrinoSqlExecutor(getQueryRunner()), namePrefix, "AS SELECT CAST(sequential_number AS VARCHAR) AS item_id FROM TABLE(sequence(start=>0, stop=>%d))".formatted(elements - 1));
    }

    private TestTable createIdsTable(String namePrefix, String ids)
    {
        return new TestTable(new TrinoSqlExecutor(getQueryRunner()), namePrefix, "AS SELECT CAST(id AS BIGINT) AS id FROM (VALUES %s) t(id)".formatted(ids));
    }
}
