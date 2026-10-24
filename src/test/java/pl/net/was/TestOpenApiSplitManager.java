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

import io.airlift.http.client.testing.TestingHttpClient;
import io.airlift.units.Duration;
import io.trino.spi.TrinoException;
import io.trino.spi.catalog.CatalogName;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplitSource;
import io.trino.spi.connector.Constraint;
import io.trino.spi.connector.DynamicFilterSnapshot;
import io.trino.spi.predicate.TupleDomain;
import io.trino.testing.TestingConnectorSession;
import io.trino.testing.TestingNodeManager;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import pl.net.was.authentication.Authentication;
import pl.net.was.authentication.CatalogCredentials;
import pl.net.was.authentication.ExtraCredentialProvider;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.Set;

import static io.trino.spi.StandardErrorCode.INVALID_ROW_FILTER;
import static io.trino.spi.connector.SchemaTableName.schemaTableName;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static pl.net.was.OpenApiSpec.SCHEMA_NAME;
import static pl.net.was.TestingOpenApiSpec.createSpec;

class TestOpenApiSplitManager
{
    private static final String SPEC =
            """
            openapi: 3.0.0
            info:
              title: test
              version: '1'
            servers:
              - url: https://api.example.com
            paths:
              /comments/{comment_id}:
                get:
                  operationId: getComment
                  parameters:
                    - name: comment_id
                      in: path
                      required: true
                      schema:
                        type: integer
                  responses:
                    '200':
                      description: OK
                      content:
                        application/json:
                          schema:
                            type: object
                            properties:
                              comment_id:
                                type: integer
                              text:
                                type: string
              /items:
                get:
                  operationId: listItems
                  parameters:
                    - name: page
                      in: query
                      schema:
                        type: integer
                  responses:
                    '200':
                      description: OK
                      content:
                        application/json:
                          schema:
                            type: object
                            properties:
                              results:
                                type: array
                                items:
                                  type: object
                                  properties:
                                    name:
                                      type: string
                              total_pages:
                                type: integer
                  x-trino:
                    pageParam: page
                    resultsPath: $response.body#/results
                    totalPagesPath: $response.body#/total_pages
            """;

    @TempDir
    Path tempDir;

    @Test
    void testStrideSplits()
            throws IOException
    {
        OpenApiConfig config = newConfig().setSplitStrategy(SplitStrategy.STRIDE);
        assertThat(getSplitCount(config, Map.of())).isEqualTo(1);
        assertThat(getSplitCount(config.setStrideSplits(4), Map.of())).isEqualTo(4);
        assertThat(getSplitCount(config, Map.of("stride_splits", 2))).isEqualTo(2);
    }

    @Test
    void testWaitsForDynamicFiltersOnSentColumns()
            throws IOException
    {
        OpenApiConfig config = newConfig().setDynamicFilteringWaitTimeout(new Duration(5, SECONDS));
        assertThat(getDynamicFilterWaitMillis(config, Map.of(), "comment_id")).isEqualTo(5000);
        assertThat(getDynamicFilterWaitMillis(config, Map.of("dynamic_filtering_wait_timeout", "1s"), "comment_id")).isEqualTo(1000);
    }

    @Test
    void testDoesNotWaitForDynamicFiltersOnColumnsNotSent()
            throws IOException
    {
        assertThat(getDynamicFilterWaitMillis(newConfig(), Map.of(), "text")).isZero();
    }

    @Test
    void testDynamicFiltersNotReadyFail()
            throws IOException
    {
        OpenApiConfig config = newConfig().setDynamicFilteringWaitTimeout(new Duration(5, SECONDS));
        OpenApiSpec spec = createSpec(config);
        OpenApiSplitManager splitManager = createSplitManager(config, spec);
        ColumnHandle commentId = getColumnHandle(spec, "comment_id");
        // the wait for dynamic filters timed out before the join had all values, so the required parameter has no value
        DynamicFilterSnapshot incomplete = new DynamicFilterSnapshot(TupleDomain.all(), false);
        try (ConnectorSplitSource splitSource = splitManager.getSplits(null, createSession(config, Map.of()), spec.getTableHandle(schemaTableName(SCHEMA_NAME, "comments")), Set.of(commentId), Constraint.alwaysTrue())) {
            assertThatThrownBy(() -> splitSource.getNextBatch(100, incomplete))
                    .isInstanceOfSatisfying(TrinoException.class, e -> assertThat(e.getErrorCode()).isEqualTo(INVALID_ROW_FILTER.toErrorCode()))
                    .hasMessage("Missing required constraint for comment_id. Dynamic filters from joins for comment_id were not ready after 5.00s (dynamic_filtering_wait_timeout)");
        }
        finally {
            splitManager.shutdown();
        }
    }

    private int getSplitCount(OpenApiConfig config, Map<String, Object> sessionProperties)
    {
        OpenApiSpec spec = createSpec(config);
        OpenApiSplitManager splitManager = createSplitManager(config, spec);
        try (ConnectorSplitSource splitSource = splitManager.getSplits(null, createSession(config, sessionProperties), spec.getTableHandle(schemaTableName(SCHEMA_NAME, "items")), Set.of(), Constraint.alwaysTrue())) {
            int splits = 0;
            while (!splitSource.isFinished()) {
                splits += splitSource.getNextBatch(100, DynamicFilterSnapshot.EMPTY).join().size();
            }
            return splits;
        }
        finally {
            splitManager.shutdown();
        }
    }

    private long getDynamicFilterWaitMillis(OpenApiConfig config, Map<String, Object> sessionProperties, String dynamicFilterColumn)
    {
        OpenApiSpec spec = createSpec(config);
        OpenApiSplitManager splitManager = createSplitManager(config, spec);
        OpenApiTableHandle table = spec.getTableHandle(schemaTableName(SCHEMA_NAME, "comments"));
        ColumnHandle column = getColumnHandle(spec, dynamicFilterColumn);
        try (ConnectorSplitSource splitSource = splitManager.getSplits(null, createSession(config, sessionProperties), table, Set.of(column), Constraint.alwaysTrue())) {
            return splitSource.getRequestedDynamicFilterWaitTimeoutMillis();
        }
        finally {
            splitManager.shutdown();
        }
    }

    private static ColumnHandle getColumnHandle(OpenApiSpec spec, String name)
    {
        return spec.getTables().get("comments").stream()
                .filter(openApiColumn -> openApiColumn.getName().equals(name))
                .map(OpenApiColumn::getHandle)
                .findFirst()
                .orElseThrow();
    }

    // the split manager doesn't send requests to create splits in these tests
    private static OpenApiSplitManager createSplitManager(OpenApiConfig config, OpenApiSpec spec)
    {
        TestingHttpClient httpClient = new TestingHttpClient(request -> {
            throw new AssertionError("Unexpected request to " + request.getUri());
        });
        OpenApiServers servers = new OpenApiServers(config, spec);
        OpenApiClient client = new OpenApiClient(config, new CatalogName("test"), httpClient, new Authentication(new CatalogCredentials(config), spec, servers, httpClient), new CatalogCredentials(config), new ExtraCredentialProvider(config), spec, servers);
        return new OpenApiSplitManager(spec, client, TestingNodeManager.create(), config);
    }

    private static ConnectorSession createSession(OpenApiConfig config, Map<String, Object> sessionProperties)
    {
        return TestingConnectorSession.builder()
                .setPropertyMetadata(new OpenApiSessionProperties(config).getSessionProperties())
                .setPropertyValues(sessionProperties)
                .build();
    }

    private OpenApiConfig newConfig()
            throws IOException
    {
        return new OpenApiConfig().setSpecLocation(Files.writeString(tempDir.resolve("spec.yaml"), SPEC).toString());
    }
}
