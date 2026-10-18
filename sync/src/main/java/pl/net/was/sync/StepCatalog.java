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

import com.google.common.io.Resources;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.util.List;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;

/**
 * Steps stored as {@code <name>.sql} scripts in a directory, or in the classpath,
 * with a {@code steps.txt} file listing the names of the steps to run by default, in order.
 */
public final class StepCatalog
{
    public static final String DEFAULT_CLASSPATH_DIR = "github";
    private static final String MANIFEST = "steps.txt";

    private final Optional<Path> directory;
    private final String classpathDirectory;

    private StepCatalog(Optional<Path> directory, String classpathDirectory)
    {
        this.directory = requireNonNull(directory, "directory is null");
        this.classpathDirectory = requireNonNull(classpathDirectory, "classpathDirectory is null");
    }

    public static StepCatalog fromDirectory(Path directory)
    {
        return new StepCatalog(Optional.of(directory), "");
    }

    public static StepCatalog fromClasspath(String directory)
    {
        return new StepCatalog(Optional.empty(), directory);
    }

    public List<String> getDefaultSteps()
    {
        return read(MANIFEST).orElseThrow(() -> new IllegalArgumentException("Missing " + describe(MANIFEST)))
                .lines()
                .map(String::strip)
                .filter(line -> !line.isEmpty() && !line.startsWith("#"))
                .toList();
    }

    public Step getStep(String name)
    {
        checkArgument(name.matches("[A-Za-z0-9_-]+"), "Invalid step name: %s", name);
        String fileName = name + ".sql";
        String script = read(fileName).orElseThrow(() -> new IllegalArgumentException("Unknown step %s, missing %s".formatted(name, describe(fileName))));
        return Step.parse(name, script);
    }

    private Optional<String> read(String fileName)
    {
        try {
            if (directory.isPresent()) {
                return Optional.of(Files.readString(directory.get().resolve(fileName)));
            }
            URL resource = getClass().getClassLoader().getResource(classpathDirectory + "/" + fileName);
            if (resource == null) {
                return Optional.empty();
            }
            return Optional.of(Resources.toString(resource, UTF_8));
        }
        catch (NoSuchFileException _) {
            return Optional.empty();
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private String describe(String fileName)
    {
        return directory.map(path -> path.resolve(fileName).toString())
                .orElse("classpath:" + classpathDirectory + "/" + fileName);
    }
}
