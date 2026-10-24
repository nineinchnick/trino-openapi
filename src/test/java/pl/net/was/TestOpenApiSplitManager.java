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
import io.trino.spi.catalog.CatalogName;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplitSource;
import io.trino.spi.connector.Constraint;
import io.trino.testing.TestingConnectorSession;
import io.trino.testing.TestingNodeManager;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import pl.net.was.authentication.Authentication;
import pl.net.was.authentication.ExtraCredentialProvider;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.Set;

import static io.trino.spi.connector.SchemaTableName.schemaTableName;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static pl.net.was.OpenApiSpec.SCHEMA_NAME;

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
            """;

    @TempDir
    Path tempDir;

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

    private long getDynamicFilterWaitMillis(OpenApiConfig config, Map<String, Object> sessionProperties, String dynamicFilterColumn)
    {
        OpenApiSpec spec = new OpenApiSpec(config, new TestingHttpClient(request -> {
            throw new AssertionError("Unexpected request to " + request.getUri());
        }));
        OpenApiServers servers = new OpenApiServers(config, spec);
        TestingHttpClient httpClient = new TestingHttpClient(request -> {
            throw new AssertionError("Unexpected request to " + request.getUri());
        });
        OpenApiClient client = new OpenApiClient(config, new CatalogName("test"), httpClient, new Authentication(config, spec, servers, httpClient), new ExtraCredentialProvider(config), spec, servers);
        OpenApiSplitManager splitManager = new OpenApiSplitManager(spec, client, TestingNodeManager.create(), config);
        ConnectorSession session = TestingConnectorSession.builder()
                .setPropertyMetadata(new OpenApiSessionProperties(config).getSessionProperties())
                .setPropertyValues(sessionProperties)
                .build();
        OpenApiTableHandle table = spec.getTableHandle(schemaTableName(SCHEMA_NAME, "comments"));
        ColumnHandle column = spec.getTables().get("comments").stream()
                .filter(openApiColumn -> openApiColumn.getName().equals(dynamicFilterColumn))
                .map(OpenApiColumn::getHandle)
                .findFirst()
                .orElseThrow();
        try (ConnectorSplitSource splitSource = splitManager.getSplits(null, session, table, Set.of(column), Constraint.alwaysTrue())) {
            return splitSource.getRequestedDynamicFilterWaitTimeoutMillis();
        }
        finally {
            splitManager.shutdown();
        }
    }

    private OpenApiConfig newConfig()
            throws IOException
    {
        return new OpenApiConfig().setSpecLocation(Files.writeString(tempDir.resolve("spec.yaml"), SPEC).toString());
    }
}
