/*
 * Copyright 2022-2026 Crown Copyright
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

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.clients.util.command.Command.command;
import static sleeper.clients.util.command.Command.envAndCommand;
import static sleeper.clients.util.command.CommandPipeline.pipeline;

public class CommandTest {

    @Test
    void shouldConvertCommandToString() {
        // Given
        CommandPipeline command = pipeline(command("cat", "abc.txt"));

        // When / Then
        assertThat(command).hasToString("cat abc.txt");
    }

    @Test
    void shouldConvertCommandPipelineToString() {
        // Given
        CommandPipeline command = pipeline(
                command("cat", "abc.txt"),
                command("grep", "ef"));

        // When / Then
        assertThat(command).hasToString("cat abc.txt | grep ef");
    }

    @Test
    void shouldConvertCommandWithEnvironmentVariablesToString() {
        // Given
        CommandPipeline command = pipeline(envAndCommand(
                Map.of("VAR_A", "value A",
                        "OTHER_VAR", "other value"),
                "cat", "abc.txt"));

        // When / Then
        assertThat(command).hasToString("OTHER_VAR=? VAR_A=? cat abc.txt");
    }

    @Test
    void shouldConvertCommandPipelineWithEnvironmentVariablesToString() {
        // Given
        CommandPipeline command = pipeline(
                envAndCommand(Map.of("VAR_A", "value A"), "cat", "abc.txt"),
                envAndCommand(Map.of("OTHER_VAR", "other value"), "grep", "ef"));

        // When / Then
        assertThat(command).hasToString("VAR_A=? cat abc.txt | OTHER_VAR=? grep ef");
    }

    @Test
    void shouldConvertCommandArgumentWithQuotesToString() {
        // Given
        CommandPipeline command = pipeline(command("grep", "\"ready\"", "abc.txt"));

        // When / Then
        assertThat(command).hasToString("grep \"\\\"ready\\\"\" abc.txt");
    }

    @Test
    void shouldConvertCommandArgumentWithSpaceToString() {
        // Given
        CommandPipeline command = pipeline(command("grep", " ", "abc.txt"));

        // When / Then
        assertThat(command).hasToString("grep \" \" abc.txt");
    }

}
