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
import com.google.common.collect.ImmutableMap;
import io.airlift.log.Logger;
import io.airlift.units.Duration;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.Map;

import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

/**
 * Runs the statements of a step, retrying ones that failed with errors that might be transient.
 * Statements must be safe to repeat, like a {@code MERGE} keyed by an identifier.
 */
public class StepRunner
{
    private static final Logger log = Logger.get(StepRunner.class);

    // Trino reports errors caused by the query, like a syntax error or a missing table, with codes below this value
    private static final int MIN_NON_USER_ERROR_CODE = 0x0001_0000;

    private final Connection connection;
    private final int maxRetries;
    private final Duration retryDelay;
    private final int maxIterations;

    public StepRunner(Connection connection, SyncConfig config)
    {
        this.connection = requireNonNull(connection, "connection is null");
        this.maxRetries = config.getMaxRetries();
        this.retryDelay = config.getRetryDelay();
        this.maxIterations = config.getMaxIterations();
    }

    /**
     * Returns the number of rows changed by all statements.
     */
    public long run(Step step, Map<String, String> parameters)
            throws SQLException, InterruptedException
    {
        long changedRows = 0;
        for (String statement : step.setup()) {
            changedRows += execute(step.name(), Template.render(statement, parameters));
        }
        if (step.loop().isEmpty()) {
            return changedRows;
        }
        Step.Loop loop = step.loop().get();
        int unchangedIterations = 0;
        for (int iteration = 1; unchangedIterations < loop.stopAfter(); iteration++) {
            checkState(iteration <= maxIterations, "Step %s didn't finish after %s iterations", step.name(), maxIterations);
            Map<String, String> iterationParameters = parameters;
            if (loop.mode() == Step.LoopMode.PAGES) {
                iterationParameters = ImmutableMap.<String, String>builder()
                        .putAll(parameters)
                        .put("page", String.valueOf(iteration))
                        .buildKeepingLast();
            }
            long lastChangedRows = 0;
            for (String statement : loop.statements()) {
                lastChangedRows = execute(step.name(), Template.render(statement, iterationParameters));
                changedRows += lastChangedRows;
            }
            unchangedIterations = lastChangedRows == 0 ? unchangedIterations + 1 : 0;
        }
        return changedRows;
    }

    private long execute(String stepName, String sql)
            throws SQLException, InterruptedException
    {
        for (int attempt = 0; ; attempt++) {
            Stopwatch stopwatch = Stopwatch.createStarted();
            try (Statement statement = connection.createStatement()) {
                long changedRows = Math.max(statement.executeLargeUpdate(sql), 0);
                log.info("%s: changed %d rows in %s: %s", stepName, changedRows, stopwatch, summarize(sql));
                return changedRows;
            }
            catch (SQLException e) {
                if (attempt >= maxRetries || !isTransient(e)) {
                    throw e;
                }
                long delayMillis = retryDelay.toMillis() << attempt;
                log.warn("%s: retrying in %d ms, attempt %d of %d, after failure: %s", stepName, delayMillis, attempt + 1, maxRetries, e.getMessage());
                Thread.sleep(delayMillis);
            }
        }
    }

    private static boolean isTransient(SQLException e)
    {
        // a code of 0 means that the query didn't run, for example because the server is restarting
        return e.getErrorCode() == 0 || e.getErrorCode() >= MIN_NON_USER_ERROR_CODE;
    }

    private static String summarize(String sql)
    {
        List<String> lines = sql.lines()
                .map(String::strip)
                .filter(line -> !line.isEmpty() && !line.startsWith("--"))
                .toList();
        return lines.isEmpty() ? "" : lines.getFirst();
    }
}
