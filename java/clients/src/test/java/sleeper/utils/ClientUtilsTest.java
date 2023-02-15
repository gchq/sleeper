/*
 * Copyright 2022-2023 Crown Copyright
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

package sleeper.utils;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import sleeper.util.ClientUtils;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.util.ClientUtils.formatPropertyDescription;

class ClientUtilsTest {

    @DisplayName("Format property descriptions")
    @Nested
    class FormatPropertyDescriptions {
        @Test
        void shouldFormatSingleLineString() {
            // Given
            String singleLineString = "Test string that can fit on one line";

            // When
            String formattedString = formatPropertyDescription(singleLineString);

            // Then
            assertThat(formattedString)
                    .isEqualTo("# Test string that can fit on one line");
        }

        @Test
        void shouldFormatAndLineWrapDescription() {
            // Given
            String multiLineString = "Test string that cannot fit on one line, so needs one or more than one lines to fit it all on the screen";

            // When
            String formattedDescription = formatPropertyDescription(multiLineString);

            // Then
            assertThat(formattedDescription)
                    .isEqualTo("" +
                            "# Test string that cannot fit on one line, so needs one or more than one lines to fit it all on the\n" +
                            "# screen");
        }

        @Test
        void shouldFormatAndLineWrapDescriptionWithCustomLineBreaks() {
            // Given
            String multiLineString = "Test string that cannot fit on one line\nbut with a custom line break. " +
                    "This is to verify if the line still wraps even after after a custom line break";

            // When
            String formattedDescription = formatPropertyDescription(multiLineString);

            // Then
            assertThat(formattedDescription)
                    .isEqualTo("" +
                            "# Test string that cannot fit on one line\n" +
                            "# but with a custom line break. This is to verify if the line still wraps even after after a custom\n" +
                            "# line break");
        }
    }

    @DisplayName("Clear directory")
    @Nested
    class ClearDirectory {
        @TempDir
        private Path tempDir;

        @Test
        void shouldRemoveFileIfExists() throws IOException {
            // Given
            Path newFile = tempDir.resolve("newFile");
            Files.createFile(newFile);

            // When
            ClientUtils.clearDirectory(tempDir);

            // Then
            assertThat(newFile).doesNotExist();
        }

        @Test
        void shouldNotRemoveRootDirectory() {
            // Given/When
            ClientUtils.clearDirectory(tempDir);

            // Then
            assertThat(tempDir).exists();
        }

        @Test
        void shouldRemoveMultipleDirectoriesAndFiles() throws IOException {
            // Given
            Files.createDirectory(tempDir.resolve("dir1"));
            Files.createDirectory(tempDir.resolve("dir1/nested1"));
            Files.createFile(tempDir.resolve("dir1/nested1/file1"));
            Files.createDirectory(tempDir.resolve("dir2"));
            Files.createDirectory(tempDir.resolve("dir2/nested2"));
            Files.createFile(tempDir.resolve("dir2/nested2/file2"));

            // When
            ClientUtils.clearDirectory(tempDir);

            // Then
            assertThat(tempDir).isEmptyDirectory();
        }
    }

    @DisplayName("Run a shell command")
    @Nested
    class RunCommand {
        @TempDir
        private Path tempDir;

        @Test
        void shouldPassAFileToACommandWithoutQuotes() throws IOException, InterruptedException {
            // Given
            Path path = Files.createFile(tempDir.resolve("test1.jar"));

            // When/Then
            assertThat(ClientUtils.runCommand("cat", String.format("%s", path)))
                    .isZero();
        }

        @Test
        void shouldFailToFindFileWhenCommandHasExtraQuotes() throws IOException, InterruptedException {
            // Given
            Path path = Files.createFile(tempDir.resolve("test1.jar"));

            // When/Then
            assertThat(ClientUtils.runCommand("cat", String.format("\"%s\"", path)))
                    .isNotZero();
        }
    }
}
