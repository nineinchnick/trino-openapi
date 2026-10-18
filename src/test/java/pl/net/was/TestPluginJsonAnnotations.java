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

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.lang.annotation.Annotation;
import java.lang.reflect.AnnotatedElement;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

class TestPluginJsonAnnotations
{
    // Trino only shares jackson-annotations with plugins, so annotations from jackson-databind,
    // like JsonDeserialize, are ignored when Trino deserializes handles sent between nodes,
    // even though they work in tests, where the plugin and Trino use the same classes
    @Test
    void testNoJacksonDatabindAnnotations()
            throws IOException, URISyntaxException
    {
        Path classesRoot = Path.of(OpenApiPlugin.class.getProtectionDomain().getCodeSource().getLocation().toURI());
        List<String> databindAnnotations;
        try (Stream<Path> classFiles = Files.walk(classesRoot)) {
            databindAnnotations = classFiles
                    .filter(path -> path.toString().endsWith(".class"))
                    .map(path -> loadClass(classesRoot, path))
                    .flatMap(TestPluginJsonAnnotations::annotatedElements)
                    .flatMap(element -> Arrays.stream(element.getDeclaredAnnotations())
                            .map(Annotation::annotationType)
                            .filter(type -> type.getPackageName().startsWith("com.fasterxml.jackson.databind"))
                            .map(type -> element + ": " + type.getName()))
                    .toList();
        }
        assertThat(databindAnnotations).isEmpty();
    }

    private static Class<?> loadClass(Path classesRoot, Path classFile)
    {
        String relativePath = classesRoot.relativize(classFile).toString();
        String className = relativePath.substring(0, relativePath.length() - ".class".length()).replace('/', '.');
        try {
            return Class.forName(className, false, TestPluginJsonAnnotations.class.getClassLoader());
        }
        catch (ClassNotFoundException e) {
            throw new IllegalStateException(e);
        }
    }

    private static Stream<AnnotatedElement> annotatedElements(Class<?> type)
    {
        return Stream.of(
                        Stream.<AnnotatedElement>of(type),
                        Arrays.stream(type.getDeclaredConstructors()),
                        Arrays.stream(type.getDeclaredMethods()),
                        Arrays.stream(type.getDeclaredFields()))
                .flatMap(elements -> elements);
    }
}
