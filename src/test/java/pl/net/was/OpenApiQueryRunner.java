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
import io.airlift.log.Level;
import io.airlift.log.Logger;
import io.airlift.log.Logging;
import io.trino.Session;
import io.trino.plugin.memory.MemoryPlugin;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.QueryRunner;

import java.util.Map;

import static com.google.common.base.Verify.verify;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static java.util.Objects.requireNonNullElse;

public class OpenApiQueryRunner
{
    private OpenApiQueryRunner() {}

    public static QueryRunner createQueryRunner(Map<String, Map<String, String>> catalogProperties)
            throws Exception
    {
        Logging logger = Logging.initialize();
        logger.setLevel("pl.net.was", Level.DEBUG);
        logger.setLevel("io.trino", Level.INFO);
        logger.setLevel("io.airlift", Level.INFO);

        catalogProperties.values().forEach(properties -> verify(
                properties.containsKey("spec-location") && properties.containsKey("base-uri"),
                "catalogProperties must include spec-location and base-uri"));
        Session defaultSession = testSessionBuilder()
                .setCatalog(catalogProperties.keySet().stream().findFirst().orElseThrow())
                .setSchema("default")
                .build();

        ImmutableMap.Builder<String, String> extraProperties = ImmutableMap.<String, String>builder();
        if (System.getenv("TRINO_PORT") != null) {
            extraProperties.put("http-server.http.port", System.getenv("TRINO_PORT"));
        }

        QueryRunner queryRunner = DistributedQueryRunner.builder(defaultSession)
                .setExtraProperties(extraProperties.buildOrThrow())
                .setWorkerCount(0)
                .build();
        queryRunner.installPlugin(new OpenApiPlugin());
        queryRunner.installPlugin(new MemoryPlugin());

        queryRunner.createCatalog("memory", "memory");
        catalogProperties.forEach((name, properties) -> queryRunner.createCatalog(name, "openapi", properties));

        return queryRunner;
    }

    public static void main(String[] args)
            throws Exception
    {
        ImmutableMap.Builder<String, String> properties = ImmutableMap.builder();
        if (System.getenv("OPENAPI_SPEC_LOCATION") == null || System.getenv("OPENAPI_BASE_URI") == null) {
            PetStoreServer server = new PetStoreServer();
            properties.put("spec-location", server.getSpecUrl());
            properties.put("base-uri", server.getApiUrl());
        }
        else {
            properties.put("spec-location", System.getenv("OPENAPI_SPEC_LOCATION"));
            properties.put("base-uri", System.getenv("OPENAPI_BASE_URI"));
        }
        properties.putAll(Map.of(
                "openApi.http-client.log.enabled", "true",
                "openApi.http-client.log.path", "logs",
                "authentication.type", requireNonNullElse(System.getenv("OPENAPI_AUTH_TYPE"), "oauth"),
                "authentication.scheme", requireNonNullElse(System.getenv("OPENAPI_AUTH_SCHEME"), "basic"),
                "authentication.username", requireNonNullElse(System.getenv("OPENAPI_USERNAME"), "user"),
                "authentication.password", requireNonNullElse(System.getenv("OPENAPI_PASSWORD"), "user"),
                "authentication.bearer-token", requireNonNullElse(System.getenv("OPENAPI_BEARER_TOKEN"), "")));
        if (System.getenv("OPENAPI_GRANT_TYPE") != null) {
            properties.putAll(Map.of(
                    "authentication.token-endpoint", requireNonNullElse(System.getenv("OPENAPI_TOKEN_ENDPOINT"), "/oauth/token"),
                    "authentication.client-id", requireNonNullElse(System.getenv("OPENAPI_CLIENT_ID"), "sample-client-id"),
                    "authentication.client-secret", requireNonNullElse(System.getenv("OPENAPI_CLIENT_SECRET"), "secret"),
                    "authentication.grant-type", System.getenv("OPENAPI_GRANT_TYPE")));
        }
        if (System.getenv("OPENAPI_API_KEYS") != null) {
            properties.put("authentication.api-keys", System.getenv("OPENAPI_API_KEYS"));
        }
        else {
            properties.putAll(Map.of(
                    "authentication.api-key-name", requireNonNullElse(System.getenv("OPENAPI_API_KEY_NAME"), "api_key"),
                    "authentication.api-key-value", requireNonNullElse(System.getenv("OPENAPI_API_KEY_VALUE"), "special-key")));
        }
        QueryRunner queryRunner = createQueryRunner(Map.of("openapi", properties.buildOrThrow()));

        Logger log = Logger.get(OpenApiQueryRunner.class);
        log.info("======== SERVER STARTED ========");
        log.info("\n====\n%s\n====", queryRunner.getCoordinator().getBaseUrl());
    }
}
