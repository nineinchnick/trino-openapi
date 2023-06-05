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

        ImmutableMap.Builder<String, String> catalogProperties = ImmutableMap.builder();
        catalogProperties.putAll(Map.of(
                "spec-location", requireNonNullElse(System.getenv("OPENAPI_SPEC_LOCATION"), "galaxy.spec.json"),
                "base-uri", requireNonNullElse(System.getenv("OPENAPI_BASE_URI"), "https://ping.galaxy-dev.io"),
                "openApi.http-client.log.enabled", "true",
                "openApi.http-client.log.path", "logs"));
        String authType = requireNonNullElse(System.getenv("OPENAPI_AUTH_TYPE"), "client-credentials");
        if (authType.equals("basic")) {
            if (System.getenv("OPENAPI_USERNAME") == null || System.getenv("OPENAPI_PASSWORD") == null) {
                throw new IllegalArgumentException("OPENAPI_USERNAME and OPENAPI_PASSWORD must be set when OPENAPI_AUTH_TYPE is basic");
            }
            catalogProperties.putAll(Map.of(
                    "authentication.type", authType,
                    "authentication.username", System.getenv("OPENAPI_USERNAME"),
                    "authentication.password", System.getenv("OPENAPI_PASSWORD")));
        }
        if (authType.equals("client-credentials")) {
            if (System.getenv("OPENAPI_CLIENT_ID") == null || System.getenv("OPENAPI_CLIENT_SECRET") == null) {
                throw new IllegalArgumentException("OPENAPI_CLIENT_ID and OPENAPI_CLIENT_SECRET must be set when OPENAPI_AUTH_TYPE is client-credentials");
            }
            catalogProperties.putAll(Map.of(
                    "authentication.type", authType,
                    "authentication.client-id", System.getenv("OPENAPI_CLIENT_ID"),
                    "authentication.client-secret", System.getenv("OPENAPI_CLIENT_SECRET")));
        }
        queryRunner.createCatalog("openapi", "openapi", catalogProperties.build());

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
