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

import io.trino.spi.connector.Connector;
import io.trino.testing.TestingConnectorContext;
import org.junit.jupiter.api.Test;

import java.net.URL;
import java.time.Duration;
import java.util.Map;
import java.util.Set;

import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static com.google.common.collect.Sets.difference;
import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;

class TestOpenApiConnectorFactory
{
    @Test
    void testShutdownStopsHttpClients()
            throws InterruptedException
    {
        URL spec = requireNonNull(getClass().getClassLoader().getResource("echo.yml"));
        Set<Thread> threadsBefore = httpClientThreads();

        Connector connector = new OpenApiConnectorFactory().create(
                "test",
                Map.of(
                        "spec-location", spec.getFile(),
                        "base-uri", "http://localhost:1"),
                new TestingConnectorContext());
        Set<Thread> connectorThreads = difference(httpClientThreads(), threadsBefore).immutableCopy();
        assertThat(connectorThreads)
                .extracting(Thread::getName)
                .anyMatch(name -> name.startsWith("http-client-openApi-"))
                .anyMatch(name -> name.startsWith("http-client-openApiAuthentication-"));

        connector.shutdown();

        // the clients stop their thread pools asynchronously
        for (Thread thread : connectorThreads) {
            thread.join(Duration.ofSeconds(10));
        }
        assertThat(connectorThreads).noneMatch(Thread::isAlive);
    }

    private static Set<Thread> httpClientThreads()
    {
        return Thread.getAllStackTraces().keySet().stream()
                .filter(thread -> thread.getName().startsWith("http-client-openApi"))
                .collect(toImmutableSet());
    }
}
