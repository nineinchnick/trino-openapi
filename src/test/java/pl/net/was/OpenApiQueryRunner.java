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
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.QueryRunner;

import java.util.Map;

import static io.trino.testing.TestingSession.testSessionBuilder;
import static java.util.Objects.requireNonNullElse;

public class OpenApiQueryRunner
{
    private OpenApiQueryRunner() {}

    public static QueryRunner createQueryRunner(TestingOpenApiServer server)
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
                "spec-location", requireNonNullElse(System.getenv("OPENAPI_SPEC_LOCATION"), server.getSpecUrl()),
                "base-uri", requireNonNullElse(System.getenv("OPENAPI_BASE_URI"), server.getApiUrl()),
                "openApi.http-client.log.enabled", "true",
                "openApi.http-client.log.path", "logs"));
        catalogProperties.putAll(Map.of(
                "authentication.type", requireNonNullElse(System.getenv("OPENAPI_AUTH_TYPE"), "oauth"),
                "authentication.scheme", requireNonNullElse(System.getenv("OPENAPI_AUTH_SCHEME"), "basic")));
        catalogProperties.putAll(Map.of(
                "authentication.token-endpoint", requireNonNullElse(System.getenv("OPENAPI_TOKEN_ENDPOINT"), "/oauth/token"),
                "authentication.client-id", requireNonNullElse(System.getenv("OPENAPI_CLIENT_ID"), "sample-client-id"),
                "authentication.client-secret", requireNonNullElse(System.getenv("OPENAPI_CLIENT_SECRET"), "secret"),
                "authentication.grant-type", requireNonNullElse(System.getenv("OPENAPI_GRANT_TYPE"), "password"),
                "authentication.username", requireNonNullElse(System.getenv("OPENAPI_USERNAME"), "user"),
                "authentication.password", requireNonNullElse(System.getenv("OPENAPI_PASSWORD"), "user"),
                "authentication.bearer-token", requireNonNullElse(System.getenv("OPENAPI_BEARER_TOKEN"), ""),
                "authentication.api-key-name", requireNonNullElse(System.getenv("OPENAPI_API_KEY_NAME"), "api_key"),
                "authentication.api-key-value", requireNonNullElse(System.getenv("OPENAPI_API_KEY_VALUE"), "special-key")));
        queryRunner.createCatalog("openapi", "openapi", catalogProperties.build());

        return queryRunner;
    }

    public static void main(String[] args)
            throws Exception
    {
        Logging logger = Logging.initialize();
        logger.setLevel("pl.net.was", Level.DEBUG);
        logger.setLevel("io.trino", Level.INFO);
        logger.setLevel("io.airlift", Level.DEBUG);

        TestingOpenApiServer server = new TestingOpenApiServer();
        QueryRunner queryRunner = createQueryRunner(server);

        Logger log = Logger.get(OpenApiQueryRunner.class);
        log.info("======== SERVER STARTED ========");
        log.info("\n====\n%s\n====", ((DistributedQueryRunner) queryRunner).getCoordinator().getBaseUrl());
    }
}
