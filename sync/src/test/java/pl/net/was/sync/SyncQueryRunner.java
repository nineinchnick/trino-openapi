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

package pl.net.was.sync;

import com.google.common.collect.ImmutableMap;
import io.airlift.log.Level;
import io.airlift.log.Logging;
import io.trino.Session;
import io.trino.plugin.iceberg.IcebergPlugin;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.QueryRunner;
import pl.net.was.OpenApiPlugin;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;

import static io.trino.testing.TestingSession.testSessionBuilder;

public final class SyncQueryRunner
{
    public static final String SOURCE_CATALOG = "github";
    public static final String TARGET_CATALOG = "iceberg";

    private SyncQueryRunner() {}

    public static QueryRunner createQueryRunner(Map<String, String> sourceProperties)
            throws Exception
    {
        Logging logging = Logging.initialize();
        logging.setLevel("org.apache.iceberg", Level.OFF);
        logging.setLevel("io.trino", Level.WARN);
        logging.setLevel("io.airlift", Level.WARN);
        logging.setLevel("pl.net.was.OpenApiSpec", Level.WARN);

        Session session = testSessionBuilder()
                .setCatalog(SOURCE_CATALOG)
                .setSchema("default")
                .build();
        QueryRunner queryRunner = DistributedQueryRunner.builder(session)
                .setWorkerCount(0)
                .build();
        queryRunner.installPlugin(new OpenApiPlugin());
        queryRunner.createCatalog(SOURCE_CATALOG, "openapi", sourceProperties);

        Path warehouse = Files.createTempDirectory("sync-iceberg");
        warehouse.toFile().deleteOnExit();
        queryRunner.installPlugin(new IcebergPlugin());
        queryRunner.createCatalog(TARGET_CATALOG, "iceberg", ImmutableMap.of(
                "iceberg.catalog.type", "TESTING_FILE_METASTORE",
                "hive.metastore.catalog.dir", "local:///",
                "fs.native-local.enabled", "true",
                "local.location", warehouse.toString()));
        return queryRunner;
    }
}
