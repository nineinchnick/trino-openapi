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

import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.airlift.units.Duration;

import java.io.IOException;
import java.io.Reader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableMap.toImmutableMap;

/**
 * Settings read from a properties file and environment variables, where {@code SYNC_TRINO_URL} sets {@code trino_url}.
 * Keys other than the settings of the driver are template parameters.
 */
public final class SyncConfig
{
    public static final String ENV_PREFIX = "SYNC_";
    public static final String TRINO_URL = "trino_url";
    public static final String TRINO_USER = "trino_user";
    public static final String TRINO_PASSWORD = "trino_password";
    public static final String SESSION_PROPERTIES = "session_properties";
    public static final String STEPS_DIR = "steps_dir";
    public static final String STEPS = "steps";
    public static final String MAX_RETRIES = "max_retries";
    public static final String RETRY_DELAY = "retry_delay";
    public static final String MAX_ITERATIONS = "max_iterations";

    private static final Set<String> SETTINGS = ImmutableSet.of(TRINO_URL, TRINO_USER, TRINO_PASSWORD, SESSION_PROPERTIES, STEPS_DIR, STEPS, MAX_RETRIES, RETRY_DELAY, MAX_ITERATIONS);

    private final Map<String, String> values;

    private SyncConfig(Map<String, String> values)
    {
        this.values = ImmutableMap.copyOf(values);
        checkArgument(getMaxRetries() >= 0, "%s must not be negative", MAX_RETRIES);
        checkArgument(getMaxIterations() > 0, "%s must be positive", MAX_ITERATIONS);
    }

    public static SyncConfig of(Map<String, String> values)
    {
        return new SyncConfig(values);
    }

    public static SyncConfig load(Optional<Path> propertiesFile, Map<String, String> environment)
            throws IOException
    {
        Map<String, String> values = new HashMap<>();
        if (propertiesFile.isPresent()) {
            Properties properties = new Properties();
            try (Reader reader = Files.newBufferedReader(propertiesFile.get())) {
                properties.load(reader);
            }
            properties.stringPropertyNames().forEach(name -> values.put(name, properties.getProperty(name)));
        }
        environment.forEach((name, value) -> {
            if (name.startsWith(ENV_PREFIX)) {
                values.put(name.substring(ENV_PREFIX.length()).toLowerCase(Locale.ENGLISH), value);
            }
        });
        return new SyncConfig(values);
    }

    public String getTrinoUrl()
    {
        return values.getOrDefault(TRINO_URL, "jdbc:trino://localhost:8080");
    }

    public String getTrinoUser()
    {
        return values.getOrDefault(TRINO_USER, "sync");
    }

    public Optional<String> getTrinoPassword()
    {
        return Optional.ofNullable(values.get(TRINO_PASSWORD));
    }

    /**
     * Session properties in the format of the {@code sessionProperties} parameter of the JDBC driver, like {@code github.http_cache_enabled:false}.
     */
    public Optional<String> getSessionProperties()
    {
        return Optional.ofNullable(values.get(SESSION_PROPERTIES));
    }

    public Optional<Path> getStepsDir()
    {
        return Optional.ofNullable(values.get(STEPS_DIR)).map(Path::of);
    }

    public Optional<List<String>> getSteps()
    {
        return Optional.ofNullable(values.get(STEPS))
                .map(steps -> Splitter.on(',').trimResults().omitEmptyStrings().splitToList(steps));
    }

    public int getMaxRetries()
    {
        return Integer.parseInt(values.getOrDefault(MAX_RETRIES, "3"));
    }

    public Duration getRetryDelay()
    {
        return Duration.valueOf(values.getOrDefault(RETRY_DELAY, "10s"));
    }

    public int getMaxIterations()
    {
        return Integer.parseInt(values.getOrDefault(MAX_ITERATIONS, "1000"));
    }

    public Map<String, String> getParameters()
    {
        return values.entrySet().stream()
                .filter(entry -> !SETTINGS.contains(entry.getKey()))
                .collect(toImmutableMap(Map.Entry::getKey, Map.Entry::getValue));
    }

    /**
     * All values, with the password masked, for logging.
     */
    public Map<String, String> describe()
    {
        return values.entrySet().stream()
                .collect(toImmutableMap(Map.Entry::getKey, entry -> entry.getKey().equals(TRINO_PASSWORD) ? "***" : entry.getValue()));
    }
}
