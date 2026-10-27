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

import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Locale;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

/**
 * Statements read from a SQL script. Statements before a {@code -- loop: <mode>} directive run once,
 * and the ones after it run repeatedly, until the last one changes no rows.
 */
public record Step(String name, List<String> setup, Optional<Loop> loop)
{
    private static final Pattern DIRECTIVE = Pattern.compile("^--\\s*(loop|stop-after)\\s*:\\s*(\\S+)\\s*$", Pattern.MULTILINE);

    public Step
    {
        requireNonNull(name, "name is null");
        setup = ImmutableList.copyOf(setup);
        requireNonNull(loop, "loop is null");
    }

    public static Step parse(String name, String script)
    {
        ImmutableList.Builder<String> setup = ImmutableList.builder();
        ImmutableList.Builder<String> body = ImmutableList.builder();
        Optional<LoopMode> mode = Optional.empty();
        int stopAfter = 1;
        for (String statement : SqlSplitter.split(script)) {
            Matcher matcher = DIRECTIVE.matcher(leadingComments(statement));
            while (matcher.find()) {
                String value = matcher.group(2);
                if (matcher.group(1).equals("stop-after")) {
                    stopAfter = Integer.parseInt(value);
                    checkArgument(stopAfter > 0, "Step %s: stop-after must be positive, got %s", name, stopAfter);
                    continue;
                }
                checkArgument(mode.isEmpty(), "Step %s has more than one loop directive", name);
                mode = Optional.of(LoopMode.valueOf(value.toUpperCase(Locale.ENGLISH)));
            }
            if (mode.isEmpty()) {
                setup.add(statement);
                continue;
            }
            body.add(statement);
        }
        if (mode.isEmpty()) {
            checkArgument(stopAfter == 1, "Step %s: stop-after requires a loop directive", name);
            return new Step(name, setup.build(), Optional.empty());
        }
        List<String> statements = body.build();
        checkArgument(!statements.isEmpty(), "Step %s has no statements after the loop directive", name);
        return new Step(name, setup.build(), Optional.of(new Loop(mode.get(), stopAfter, statements)));
    }

    private static String leadingComments(String statement)
    {
        StringBuilder comments = new StringBuilder();
        for (String line : statement.lines().toList()) {
            String trimmed = line.strip();
            if (!trimmed.isEmpty() && !trimmed.startsWith("--")) {
                break;
            }
            comments.append(trimmed).append('\n');
        }
        return comments.toString();
    }

    public enum LoopMode
    {
        /**
         * Repeats the statements, for example to process parents missing children in batches.
         */
        BATCHES,
        /**
         * Repeats the statements with the {@code page} parameter set to 1, 2, and so on.
         */
        PAGES,
    }

    /**
     * @param stopAfter the number of consecutive iterations in which the last statement changed no rows, that stops the loop
     */
    public record Loop(LoopMode mode, int stopAfter, List<String> statements)
    {
        public Loop
        {
            requireNonNull(mode, "mode is null");
            checkArgument(stopAfter > 0, "stopAfter must be positive");
            statements = ImmutableList.copyOf(statements);
            checkArgument(!statements.isEmpty(), "statements is empty");
        }
    }
}
