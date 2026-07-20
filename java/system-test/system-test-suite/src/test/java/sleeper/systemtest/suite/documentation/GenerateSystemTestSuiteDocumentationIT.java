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

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class GenerateSystemTestSuiteDocumentationIT {

    @TempDir
    private Path tempDir;

    @ParameterizedTest(name = "{0} contains {1}")
    @MethodSource("parallelSystemTests")
    void shouldGenerateDocumentationForParallelSystemTests(String suiteName, String systemTestName) throws Exception {
        // Given
        Files.createDirectories(tempDir.resolve("docs/development"));

        // When
        GenerateSystemTestSuiteDocumentation.main(new String[]{tempDir.toString()});

        // Then
        assertThat(columnContaining(readDocumentation(), systemTestName)).isEqualTo(suiteName);
    }

    @Test
    void shouldNotIncludeSystemTestsThatAreNotInParallelSuites() throws Exception {
        // Given
        Files.createDirectories(tempDir.resolve("docs/development"));

        // When
        GenerateSystemTestSuiteDocumentation.main(new String[]{tempDir.toString()});

        // Then
        assertThat(readDocumentation()).doesNotContain("QueryST");
    }

    @Test
    void shouldOverwriteExistingDocumentation() throws Exception {
        // Given
        Path documentation = tempDir.resolve("docs/development/system-test-suites.md");
        Files.createDirectories(documentation.getParent());
        Files.writeString(documentation, "out of date");

        // When
        GenerateSystemTestSuiteDocumentation.main(new String[]{tempDir.toString()});

        // Then
        assertThat(readDocumentation())
                .contains("# Current Slow and Expensive test suites")
                .doesNotContain("out of date");
    }

    @Test
    void shouldCreateDocumentationDirectory() throws Exception {
        // When
        GenerateSystemTestSuiteDocumentation.main(new String[]{tempDir.toString()});

        // Then
        assertThat(tempDir.resolve("docs/development/system-test-suites.md"))
                .isRegularFile();
    }

    @ParameterizedTest
    @ValueSource(ints = {0, 2})
    void shouldRejectWrongNumberOfArguments(int argumentCount) {
        // Given
        String[] args = IntStream.range(0, argumentCount)
                .mapToObj(index -> tempDir.toString())
                .toArray(String[]::new);

        // When/Then
        assertThatThrownBy(() -> GenerateSystemTestSuiteDocumentation.main(args))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Usage: GenerateSystemTestSuiteDocumentation <project-root>");
    }

    @Test
    void shouldRejectProjectRootThatIsNotADirectory() throws Exception {
        // Given
        Path file = Files.createFile(tempDir.resolve("not-a-directory"));

        // When/Then
        assertThatThrownBy(() -> GenerateSystemTestSuiteDocumentation.main(new String[]{file.toString()}))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Project root is not a directory: " + file.toAbsolutePath().normalize());
    }

    private String readDocumentation() throws Exception {
        return Files.readString(tempDir.resolve("docs/development/system-test-suites.md"));
    }

    private static Stream<Arguments> parallelSystemTests() {
        return Stream.of(
                Arguments.of("Slow1", "AutoStopEcsTaskST"),
                Arguments.of("Slow2", "EksFargateBulkImportST"),
                Arguments.of("Slow3", "MultipleTablesST"),
                Arguments.of("Expensive1", "CompactionDataFusionPerformanceST"),
                Arguments.of("Expensive2", "IngestPerformanceST"),
                Arguments.of("Expensive3", "CompactionPerformanceST"));
    }

    private static String columnContaining(String documentation, String systemTestName) {
        List<String[]> rows = documentation.lines()
                .map(GenerateSystemTestSuiteDocumentationIT::splitRow)
                .toList();
        int rowIndex = IntStream.range(0, rows.size())
                .filter(index -> Arrays.asList(rows.get(index)).contains(systemTestName))
                .findFirst()
                .orElseThrow();
        String[] row = rows.get(rowIndex);
        int columnIndex = IntStream.range(0, row.length)
                .filter(index -> systemTestName.equals(row[index]))
                .findFirst()
                .orElseThrow();
        for (int index = rowIndex - 1; index >= 0; index--) {
            String[] cells = rows.get(index);
            if (cells.length > columnIndex
                    && (cells[columnIndex].startsWith("Slow") || cells[columnIndex].startsWith("Expensive"))) {
                return cells[columnIndex];
            }
        }
        throw new IllegalStateException("No suite header found for " + systemTestName);
    }

    private static String[] splitRow(String line) {
        return Arrays.stream(line.split("\\|", -1))
                .map(String::trim)
                .toArray(String[]::new);
    }
}
