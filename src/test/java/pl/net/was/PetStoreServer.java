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

import io.trino.testing.containers.junit.ReportLeakedContainers;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.HttpWaitStrategy;

import java.io.Closeable;

import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class PetStoreServer
        implements Closeable
{
    private static final int API_PORT = 8080;
    private static final String BASE_PATH = "/api/v3";
    private static final String SPEC_PATH = "/api/v3/openapi.json";
    private final GenericContainer<?> dockerContainer;

    public PetStoreServer()
    {
        // Use the oldest supported OpenAPI version
        dockerContainer = new GenericContainer<>("swaggerapi/petstore3:unstable")
                .withExposedPorts(8080)
                .withStartupAttempts(3)
                .withEnv("OPENAPI_BASE_PATH", BASE_PATH)
                .waitingFor(new HttpWaitStrategy().forPort(8080).forPath("/api/v3/openapi.json").forStatusCode(200));
        dockerContainer.withCreateContainerCmdModifier(cmd -> cmd
                .withHostConfig(requireNonNull(cmd.getHostConfig(), "hostConfig is null")
                        .withPublishAllPorts(true)));
        dockerContainer.start();
        ReportLeakedContainers.ignoreContainerId(dockerContainer.getContainerId());
    }

    public String getSpecUrl()
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
        dockerContainer.close();
    }
}
