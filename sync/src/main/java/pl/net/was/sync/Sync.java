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

import com.google.common.base.Stopwatch;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.log.Logger;

import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;

import static java.util.Objects.requireNonNull;

/**
 * Runs SQL steps that copy data from one Trino catalog to another, like from a catalog using the OpenAPI connector
 * to an Iceberg catalog. A failed step is reported, and the next steps still run.
 */
public final class Sync
{
    private static final Logger log;

    static {
        // one line per message, so the output of scheduled runs is easy to search
        System.setProperty("java.util.logging.SimpleFormatter.format", "%1$tF %1$tT %4$s %3$s %5$s%6$s%n");
        log = Logger.get(Sync.class);
    }

    private final SyncConfig config;
    private final StepCatalog catalog;

    public Sync(SyncConfig config)
    {
        this.config = requireNonNull(config, "config is null");
        this.catalog = config.getStepsDir()
                .map(StepCatalog::fromDirectory)
                .orElseGet(() -> StepCatalog.fromClasspath(StepCatalog.DEFAULT_CLASSPATH_DIR));
    }

    public static void main(String[] args)
            throws Exception
    {
        Optional<Path> propertiesFile = args.length > 0 ? Optional.of(Path.of(args[0])) : Optional.empty();
        SyncConfig config = SyncConfig.load(propertiesFile, System.getenv());
        config.describe().forEach((name, value) -> log.info("%s=%s", name, value));
        SyncResult result = new Sync(config).run();
        if (!result.failedSteps().isEmpty()) {
            log.error("Failed steps: %s", String.join(", ", result.failedSteps()));
            System.exit(1);
        }
    }

    public SyncResult run()
            throws SQLException, InterruptedException
    {
        List<String> stepNames = config.getSteps().orElseGet(catalog::getDefaultSteps);
        // load all steps first, so a typo in a step name or a script fails before changing any data
        List<Step> steps = stepNames.stream()
                .map(catalog::getStep)
                .toList();
        Map<String, String> parameters = config.getParameters();
        ImmutableMap.Builder<String, Long> changedRows = ImmutableMap.builder();
        ImmutableList.Builder<String> failedSteps = ImmutableList.builder();
        try (Connection connection = connect()) {
            StepRunner runner = new StepRunner(connection, config);
            for (Step step : steps) {
                Stopwatch stopwatch = Stopwatch.createStarted();
                try {
                    long stepChangedRows = runner.run(step, parameters);
                    changedRows.put(step.name(), stepChangedRows);
                    log.info("%s: finished, changed %d rows in %s", step.name(), stepChangedRows, stopwatch);
                }
                catch (SQLException | RuntimeException e) {
                    log.error(e, "%s: failed after %s", step.name(), stopwatch);
                    failedSteps.add(step.name());
                }
            }
        }
        return new SyncResult(changedRows.buildKeepingLast(), failedSteps.build());
    }

    private Connection connect()
            throws SQLException
    {
        Properties properties = new Properties();
        properties.setProperty("user", config.getTrinoUser());
        // steps compare timestamps from APIs, which are in UTC, with the current time
        properties.setProperty("timezone", "UTC");
        config.getTrinoPassword().ifPresent(password -> properties.setProperty("password", password));
        config.getSessionProperties().ifPresent(sessionProperties -> properties.setProperty("sessionProperties", sessionProperties));
        return DriverManager.getConnection(config.getTrinoUrl(), properties);
    }
}
