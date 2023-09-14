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
import static sleeper.clients.util.ClientUtils.formatBytes;
import static sleeper.clients.util.Command.command;
import static sleeper.clients.util.CommandPipeline.pipeline;

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

        @Test
        void shouldPipeOutputOfOneCommandIntoNext() throws Exception {
            // Given
            Path path = Files.writeString(tempDir.resolve("test.txt"), "ab\ncd");
            CommandPipeline command = pipeline(
                    command("cat", path.toString()),
                    command("grep", "ab"));

            // When/Then
            assertThat(ClientUtils.runCommandLogOutput(command))
                    .isEqualTo(new CommandPipelineResult(0, 0));
        }

        @Test
        void shouldFailWhenLastCommandFails() throws Exception {
            // Given
            Path path = Files.writeString(tempDir.resolve("test.txt"), "ab\ncd");
            CommandPipeline command = pipeline(
                    command("cat", path.toString()),
                    command("grep", "ef"));

            // When/Then
            assertThat(ClientUtils.runCommandLogOutput(command))
                    .isEqualTo(new CommandPipelineResult(0, 1));
        }

        @Test
        void shouldIgnoreFailureInEarlierCommandToMatchBashPipelineBehaviour() throws Exception {
            // Given
            Path path = tempDir.resolve("doesNotExist.txt");
            CommandPipeline command = pipeline(
                    command("cat", path.toString()),
                    command("echo", "ab"));

            // When/Then
            assertThat(ClientUtils.runCommandLogOutput(command))
                    .isEqualTo(new CommandPipelineResult(1, 0));
        }

        @Test
        void shouldRunPipelineInheritingIO() throws Exception {
            // Given
            Path path = Files.writeString(tempDir.resolve("test.txt"), "ab\ncd");
            CommandPipeline command = pipeline(
                    command("cat", path.toString()),
                    command("grep", "ab"));

            // When/Then
            assertThat(ClientUtils.runCommandInheritIO(command))
                    .isEqualTo(new CommandPipelineResult(0, 0));
        }

        @Test
        void shouldIgnoreFailureInEarlierCommandToMatchBashPipelineBehaviourWhenInheritingIO() throws Exception {
            // Given
            Path path = tempDir.resolve("doesNotExist.txt");
            CommandPipeline command = pipeline(
                    command("cat", path.toString()),
                    command("echo", "ab"));

            // When/Then
            assertThat(ClientUtils.runCommandInheritIO(command))
                    .isEqualTo(new CommandPipelineResult(1, 0));
        }
    }

    @Nested
    @DisplayName("Format bytes as string")
    class FormatBytesAsString {
        @Test
        void shouldFormatNumberOfBytesBelow1KB() {
            assertThat(formatBytes(123L))
                    .isEqualTo("123B");
        }

        @Test
        void shouldFormatNumberOfBytesAsKB() {
            assertThat(formatBytes(1_234L))
                    .isEqualTo("1.2KB");
        }

        @Test
        void shouldFormatNumberOfBytesAsKBWithRounding() {
            assertThat(formatBytes(5_678L))
                    .isEqualTo("5.7KB");
        }

        @Test
        void shouldFormatNumberOfBytesEqualTo1KB() {
            assertThat(formatBytes(1_000L))
                    .isEqualTo("1.0KB");
        }

        @Test
        void shouldFormatNumberOfBytesAs10KB() {
            assertThat(formatBytes(10_000L))
                    .isEqualTo("10.0KB");
        }

        @Test
        void shouldFormatNumberOfBytesAsMB() {
            assertThat(formatBytes(1_234_000L))
                    .isEqualTo("1.2MB");
        }

        @Test
        void shouldFormatNumberOfBytesAsGB() {
            assertThat(formatBytes(1_234_000_000L))
                    .isEqualTo("1.2GB");
        }

        @Test
        void shouldFormatNumberOfBytesAbove1TB() {
            assertThat(formatBytes(1_234_000_000_000L))
                    .isEqualTo("1TB");
        }

        @Test
        void shouldFormatNumberOfBytesAbove1000TB() {
            assertThat(formatBytes(1_234_000_000_000_000L))
                    .isEqualTo("1,234TB");
        }
    }
}
