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

package io.starburst;

import com.google.common.collect.ImmutableMap;
import io.airlift.log.Level;
import io.airlift.log.Logger;
import io.airlift.log.Logging;
import io.trino.Session;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.QueryRunner;

import java.util.Map;

import static io.trino.testing.TestingSession.testSessionBuilder;
import static java.util.Objects.requireNonNull;
import static java.util.Objects.requireNonNullElse;

public class OpenApiQueryRunner
{
    private OpenApiQueryRunner() {}

    public static QueryRunner createQueryRunner()
            throws Exception
    {
        Session defaultSession = testSessionBuilder()
                .setCatalog("openapi")
                .setSchema("default")
                .build();

        Map<String, String> extraProperties = ImmutableMap.<String, String>builder()
                .put("http-server.http.port", requireNonNullElse(System.getenv("TRINO_PORT"), "8080"))
                .build();
        QueryRunner queryRunner = DistributedQueryRunner.builder(defaultSession)
                .setExtraProperties(extraProperties)
                .setNodeCount(1)
                .build();
        queryRunner.installPlugin(new OpenApiPlugin());

        queryRunner.createCatalog(
                "openapi",
                "openapi",
                Map.of(
                        "spec-location", requireNonNullElse(System.getenv("OPENAPI_SPEC_LOCATION"), "galaxy.spec.json"),
                        "base-uri", requireNonNullElse(System.getenv("OPENAPI_BASE_URI"), "https://ping.galaxy-dev.io"),
                        "authentication.type", "client-credentials",
                        "authentication.client-id", requireNonNull(System.getenv("OPENAPI_CLIENT_ID"), "\"OPENAPI_CLIENT_ID\" environment variable must be set"),
                        "authentication.client-secret", requireNonNull(System.getenv("OPENAPI_CLIENT_SECRET"), "\"OPENAPI_CLIENT_SECRET\" environment variable must be set"),
                        "openApi.http-client.log.enabled", "true",
                        "openApi.http-client.log.path", "logs"));

        return queryRunner;
    }

    public static void main(String[] args)
            throws Exception
    {
        Logging logger = Logging.initialize();
        logger.setLevel("io.starburst", Level.DEBUG);
        logger.setLevel("io.trino", Level.INFO);
        logger.setLevel("io.airlift", Level.DEBUG);

        QueryRunner queryRunner = createQueryRunner();

        Logger log = Logger.get(OpenApiQueryRunner.class);
        log.info("======== SERVER STARTED ========");
        log.info("\n====\n%s\n====", ((DistributedQueryRunner) queryRunner).getCoordinator().getBaseUrl());
    }
}
