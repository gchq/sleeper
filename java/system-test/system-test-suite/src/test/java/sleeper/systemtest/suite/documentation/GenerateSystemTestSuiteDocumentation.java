/*
 * Copyright 2026 Crown Copyright
 *
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
package sleeper.systemtest.suite.documentation;

import org.junit.platform.commons.support.ReflectionSupport;

import sleeper.systemtest.suite.testutil.parallel.Expensive1;
import sleeper.systemtest.suite.testutil.parallel.Expensive2;
import sleeper.systemtest.suite.testutil.parallel.Expensive3;
import sleeper.systemtest.suite.testutil.parallel.Slow1;
import sleeper.systemtest.suite.testutil.parallel.Slow2;
import sleeper.systemtest.suite.testutil.parallel.Slow3;

import java.io.IOException;
import java.io.InputStream;
import java.lang.annotation.Annotation;
import java.nio.charset.StandardCharsets;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.Comparator;
import java.util.List;

/**
 * Generates documentation listing the system tests assigned to parallel suites.
 */
public class GenerateSystemTestSuiteDocumentation {

    private static final String SYSTEM_TEST_PACKAGE = "sleeper.systemtest.suite";
    private static final List<Class<? extends Annotation>> SLOW_SUITES = List.of(Slow1.class, Slow2.class, Slow3.class);
    private static final List<Class<? extends Annotation>> EXPENSIVE_SUITES = List.of(
            Expensive1.class, Expensive2.class, Expensive3.class);

    private GenerateSystemTestSuiteDocumentation() {
    }

    /**
     * Generates system test suite documentation from a template, and writes it to
     * docs/development/system-test-suites.md.
     *
     * @param  args                     exactly one command line argument containing the project root
     * @throws IllegalArgumentException if exactly one argument is not supplied, or the project root is not a directory
     * @throws IOException              if the documentation cannot be read from or written to
     */
    public static void main(String[] args) throws IOException {
        if (args.length != 1) {
            throw new IllegalArgumentException(
                    "Usage: GenerateSystemTestSuiteDocumentation <project-root>");
        }
        Path projectRoot = Path.of(args[0]).toAbsolutePath().normalize();
        if (!Files.isDirectory(projectRoot)) {
            throw new IllegalArgumentException(
                    "Project root is not a directory: " + projectRoot);
        }
        generateDocumentation(projectRoot);
    }

    private static void generateDocumentation(Path projectRoot) throws IOException {
        generateDocumentation(projectRoot, ReflectionSupport.findAllClassesInPackage(
                SYSTEM_TEST_PACKAGE,
                GenerateSystemTestSuiteDocumentation::isParallelSystemTest,
                name -> true));
    }

    private static boolean isParallelSystemTest(Class<?> clazz) {
        return isInSystemTestPackage(clazz.getPackageName())
                && !clazz.isMemberClass()
                && clazz.getSimpleName().endsWith("ST")
                && (SLOW_SUITES.stream().anyMatch(clazz::isAnnotationPresent)
                        || EXPENSIVE_SUITES.stream().anyMatch(clazz::isAnnotationPresent));
    }

    private static boolean isInSystemTestPackage(String packageName) {
        return packageName.equals(SYSTEM_TEST_PACKAGE)
                || packageName.startsWith(SYSTEM_TEST_PACKAGE + ".");
    }

    private static void generateDocumentation(Path projectRoot, List<Class<?>> systemTests) throws IOException {
        List<Class<?>> sortedTests = systemTests.stream()
                .sorted(Comparator.comparing(Class::getName))
                .toList();
        String output = loadTemplate()
                .replace("%SLOW_SYSTEM_TEST_SUITES%", SystemTestSuiteTable.create(SLOW_SUITES, sortedTests))
                .replace("%EXPENSIVE_SYSTEM_TEST_SUITES%", SystemTestSuiteTable.create(EXPENSIVE_SUITES, sortedTests));
        writeOutput(projectRoot.resolve("docs/development/system-test-suites.md"), output);
    }

    /**
     * Writes generated output to a temporary file. Then, when supported by the filesystem,
     * requests an atomic move to overwrite the current documentation file.
     * Has a fallback for filesystems that do not support atomic moves.
     * Removes temporary file after moving.
     *
     * @param  destination the file to replace with the output
     * @param  output      the output to write
     * @throws IOException if the destination directory cannot be created, or if the output cannot be written or moved to
     */
    private static void writeOutput(Path destination, String output) throws IOException {
        Files.createDirectories(destination.getParent());
        Path temporary = Files.createTempFile(
                destination.getParent(),
                destination.getFileName().toString(),
                ".tmp");
        try {
            Files.writeString(temporary, output, StandardCharsets.UTF_8);
            try {
                Files.move(
                        temporary,
                        destination,
                        StandardCopyOption.REPLACE_EXISTING,
                        StandardCopyOption.ATOMIC_MOVE);
            } catch (AtomicMoveNotSupportedException e) {
                Files.move(
                        temporary,
                        destination,
                        StandardCopyOption.REPLACE_EXISTING);
            }
        } finally {
            Files.deleteIfExists(temporary);
        }
    }

    private static String loadTemplate() throws IOException {
        String resource = "system-test-suites.template.md";
        InputStream stream = GenerateSystemTestSuiteDocumentation.class
                .getClassLoader()
                .getResourceAsStream(resource);
        if (stream == null) {
            throw new IOException("Classpath resource not found: " + resource);
        }
        try (stream) {
            return new String(stream.readAllBytes(), StandardCharsets.UTF_8);
        }
    }

}
