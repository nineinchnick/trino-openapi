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
import org.testcontainers.utility.MountableFile;

import java.io.Closeable;

import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class KeycloakServer
        implements Closeable
{
    private static final int API_PORT = 8080;
    private static final String TOKEN_PATH = "/realms/trino-realm/protocol/openid-connect/token";
    private final GenericContainer<?> dockerContainer;

    public KeycloakServer()
    {
        dockerContainer = new GenericContainer<>("quay.io/keycloak/keycloak:26.1.4")
                .withExposedPorts(8080)
                .withStartupAttempts(3)
                .withEnv("KC_BOOTSTRAP_ADMIN_USERNAME", "admin")
                .withEnv("KC_BOOTSTRAP_ADMIN_PASSWORD", "admin")
                .withCopyFileToContainer(MountableFile.forClasspathResource("test-keycloak-realm.json", 0644), "/opt/keycloak/data/import/realm.json")
                .waitingFor(new HttpWaitStrategy().forPort(8080).forPath("/realms/master").forStatusCode(200));
        dockerContainer.withCreateContainerCmdModifier(cmd -> cmd
                .withCmd("start-dev", "--import-realm")
                .withHostConfig(requireNonNull(cmd.getHostConfig(), "hostConfig is null")
                        .withPublishAllPorts(true)));
        dockerContainer.start();
        ReportLeakedContainers.ignoreContainerId(dockerContainer.getContainerId());
    }

    public String getTokenUrl()
    {
        return format("http://%s:%s%s", dockerContainer.getHost(), dockerContainer.getMappedPort(API_PORT), TOKEN_PATH);
    }

    @Override
    public void close()
    {
        dockerContainer.close();
    }
}
