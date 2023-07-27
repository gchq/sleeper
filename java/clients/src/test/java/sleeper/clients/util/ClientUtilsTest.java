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

package sleeper.clients.util;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;

class ClientUtilsTest {

    @DisplayName("Clear directories")
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
        void shouldNotRemoveRootDirectory() throws IOException {
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

    @DisplayName("Run shell commands")
    @Nested
    class RunCommand {
        @TempDir
        private Path tempDir;

        @Test
        void shouldPassAFileToACommandWithoutQuotes() throws IOException, InterruptedException {
            // Given
            Path path = Files.createFile(tempDir.resolve("test1.jar"));

            // When/Then
            assertThat(ClientUtils.runCommandLogOutput("cat", String.format("%s", path)))
                    .isZero();
        }

        @Test
        void shouldFailToFindFileWhenCommandHasExtraQuotes() throws IOException, InterruptedException {
            // Given
            Path path = Files.createFile(tempDir.resolve("test1.jar"));

            // When/Then
            assertThat(ClientUtils.runCommandLogOutput("cat", String.format("\"%s\"", path)))
                    .isNotZero();
        }

        @Test
        void shouldOutputFileAndFailToFindFile() throws IOException, InterruptedException {
            // Given
            Path path1 = Files.writeString(tempDir.resolve("test1.txt"), "Line 1\nLine 2\nLine 3\n");
            Path path2 = tempDir.resolve("test2.txt");
            Path path3 = Files.writeString(tempDir.resolve("test3.txt"), "Some content");

            // When/Then
            assertThat(ClientUtils.runCommandLogOutput(
                    "cat", path1.toString(), path2.toString(), path3.toString()))
                    .isNotZero();
        }
    }
}
