/*
 * Copyright 2022-2025 Crown Copyright
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
package sleeper.clients.util.command;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import sleeper.clients.util.ClientUtils;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.clients.util.command.Command.command;
import static sleeper.clients.util.command.CommandPipeline.pipeline;

public class CommandUtilsTest {
    @TempDir
    private Path tempDir;

    @Test
    void shouldPassAFileToACommandWithoutQuotes() throws IOException, InterruptedException {
        // Given
        Path path = Files.createFile(tempDir.resolve("test1.jar"));

        // When/Then
        assertThat(CommandUtils.runCommandLogOutput("cat", String.format("%s", path)))
                .isZero();
    }

    @Test
    void shouldFailToFindFileWhenCommandHasExtraQuotes() throws IOException, InterruptedException {
        // Given
        Path path = Files.createFile(tempDir.resolve("test1.jar"));

        // When/Then
        assertThat(CommandUtils.runCommandLogOutput("cat", String.format("\"%s\"", path)))
                .isNotZero();
    }

    @Test
    void shouldOutputFileAndFailToFindFile() throws IOException, InterruptedException {
        // Given
        Path path1 = Files.writeString(tempDir.resolve("test1.txt"), "Line 1\nLine 2\nLine 3\n");
        Path path2 = tempDir.resolve("test2.txt");
        Path path3 = Files.writeString(tempDir.resolve("test3.txt"), "Some content");

        // When/Then
        assertThat(CommandUtils.runCommandLogOutput(
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
