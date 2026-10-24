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

import io.trino.spi.TrinoException;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTableVersion;
import io.trino.spi.connector.PointerType;
import io.trino.spi.connector.SchemaTableName;
import org.junit.jupiter.api.Test;

import java.net.URL;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.trino.spi.connector.RetryMode.RETRIES_ENABLED;
import static io.trino.spi.connector.SchemaTableName.schemaTableName;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.testing.TestingConnectorSession.SESSION;
import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static pl.net.was.OpenApiSpec.SCHEMA_NAME;
import static pl.net.was.TestingOpenApiSpec.createSpec;

class TestOpenApiMetadata
{
    private static final SchemaTableName ITEMS = schemaTableName(SCHEMA_NAME, "items");

    private final OpenApiMetadata metadata = createMetadata();

    @Test
    void rejectVersionedTables()
    {
        ConnectorTableVersion version = new ConnectorTableVersion(PointerType.TARGET_ID, BIGINT, 1L);

        assertThatThrownBy(() -> metadata.getTableHandle(SESSION, ITEMS, Optional.of(version), Optional.empty()))
                .isInstanceOfSatisfying(TrinoException.class, e -> assertThat(e.getErrorCode()).isEqualTo(NOT_SUPPORTED.toErrorCode()))
                .hasMessage("This connector does not support versioned tables");
        assertThatThrownBy(() -> metadata.getTableHandle(SESSION, ITEMS, Optional.empty(), Optional.of(version)))
                .isInstanceOfSatisfying(TrinoException.class, e -> assertThat(e.getErrorCode()).isEqualTo(NOT_SUPPORTED.toErrorCode()))
                .hasMessage("This connector does not support versioned tables");
    }

    @Test
    void rejectWritesWithQueryRetries()
    {
        // requests sent by a failed attempt can't be undone, so a retried write would send them again
        ConnectorTableHandle table = metadata.getTableHandle(SESSION, ITEMS, Optional.empty(), Optional.empty());

        assertThatThrownBy(() -> metadata.beginInsert(SESSION, table, List.of(), RETRIES_ENABLED))
                .isInstanceOfSatisfying(TrinoException.class, e -> assertThat(e.getErrorCode()).isEqualTo(NOT_SUPPORTED.toErrorCode()))
                .hasMessage("This connector does not support query retries");
        assertThatThrownBy(() -> metadata.beginMerge(SESSION, table, Map.of(), RETRIES_ENABLED))
                .isInstanceOfSatisfying(TrinoException.class, e -> assertThat(e.getErrorCode()).isEqualTo(NOT_SUPPORTED.toErrorCode()))
                .hasMessage("This connector does not support query retries");
    }

    private static OpenApiMetadata createMetadata()
    {
        URL specResource = requireNonNull(TestOpenApiMetadata.class.getClassLoader().getResource("paged.yml"));
        OpenApiConfig config = new OpenApiConfig().setSpecLocation(specResource.getFile());
        return new OpenApiMetadata(createSpec(config), config);
    }
}
