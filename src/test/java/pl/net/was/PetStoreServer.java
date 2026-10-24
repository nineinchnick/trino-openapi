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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.trino.testing.containers.junit.ReportLeakedContainers;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.HttpWaitStrategy;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.nio.file.Files;

import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static java.util.Objects.requireNonNullElse;

public class PetStoreServer
        implements Closeable
{
    private static final boolean REUSE = requireNonNullElse(System.getenv("TESTCONTAINERS_REUSE_ENABLE"), "false").equals("true");
    private static final int API_PORT = 8080;
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private static final String BASE_PATH = "/api/v3";
    private static final String SPEC_PATH = "/api/v3/openapi.json";
    private final GenericContainer<?> dockerContainer;
    private final File specFile;

    public PetStoreServer(KeycloakServer keycloakServer)
    {
        // Use the oldest supported OpenAPI version
        dockerContainer = new GenericContainer<>("swaggerapi/petstore3:unstable")
                .withReuse(REUSE)
                .withExposedPorts(8080)
                .withStartupAttempts(3)
                .withEnv("OPENAPI_BASE_PATH", BASE_PATH)
                .waitingFor(new HttpWaitStrategy().forPort(8080).forPath("/api/v3/openapi.json").forStatusCode(200));
        dockerContainer.withCreateContainerCmdModifier(cmd -> cmd
                .withHostConfig(requireNonNull(cmd.getHostConfig(), "hostConfig is null")
                        .withPublishAllPorts(true)));
        dockerContainer.start();
        ReportLeakedContainers.ignoreContainerId(dockerContainer.getContainerId());

        try {
            ObjectNode spec;
            try (InputStream input = URI.create(getContainerSpecUrl()).toURL().openStream()) {
                spec = (ObjectNode) OBJECT_MAPPER.readTree(input);
            }
            ObjectNode flows = spec.withObject("/components/securitySchemes/petstore_auth/flows");
            // the implicit flow requires a browser to log in, so replace it with a flow the connector can run on its own
            JsonNode implicitFlow = requireNonNull(flows.remove("implicit"), "implicit flow is missing in the petstore spec");
            ObjectNode clientCredentialsFlow = flows.putObject("clientCredentials");
            clientCredentialsFlow.put("tokenUrl", keycloakServer.getTokenUrl());
            clientCredentialsFlow.set("scopes", implicitFlow.get("scopes"));

            specFile = Files.createTempFile("spec-", ".json").toFile();
            specFile.deleteOnExit();
            OBJECT_MAPPER.writeValue(specFile, spec);
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public String getSpecUrl()
    {
        return specFile.getAbsolutePath();
    }

    private String getContainerSpecUrl()
    {
        return format("http://%s:%s%s", dockerContainer.getHost(), dockerContainer.getMappedPort(API_PORT), SPEC_PATH);
    }

    public String getApiUrl()
    {
        return format("http://%s:%s%s", dockerContainer.getHost(), dockerContainer.getMappedPort(API_PORT), BASE_PATH);
    }

    @Override
    public void close()
    {
        if (!REUSE) {
            dockerContainer.close();
        }
    }
}
