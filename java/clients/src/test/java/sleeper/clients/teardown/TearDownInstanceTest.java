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
package sleeper.clients.teardown;

import org.junit.jupiter.api.Test;

import sleeper.clients.util.console.ConsoleInput;
import sleeper.core.util.PollWithRetries;
import sleeper.core.util.cli.CommandArgumentReader;
import sleeper.core.util.cli.CommandArgumentsException;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Scanner;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class TearDownInstanceTest {

    @Test
    void shouldRequireInstanceId() {
        assertThatThrownBy(() -> readArguments("/scripts"))
                .isInstanceOf(CommandArgumentsException.class)
                .hasMessage("Expected 1 positional argument, found 0: []");
    }

    @Test
    void shouldReadInstanceIdAndForceFlag() {
        assertThat(readArguments("/scripts", "test-instance", "--force"))
                .isEqualTo(new TearDownInstance.Arguments(Path.of("/scripts"), "test-instance", true));
    }

    @Test
    void shouldConfirmTearDown() {
        ByteArrayOutputStream output = new ByteArrayOutputStream();

        assertThat(TearDownInstance.confirmTearDown(
                consoleInput("y\n", output), readArguments("/scripts", "test-instance")))
                .isTrue();
        assertThat(output.toString(StandardCharsets.UTF_8))
                .contains("Are you sure you want to tear down Sleeper instance test-instance? [y/N]");
    }

    @Test
    void shouldCancelTearDownUnlessConfirmed() {
        assertThat(TearDownInstance.confirmTearDown(
                consoleInput("\n", new ByteArrayOutputStream()), readArguments("/scripts", "test-instance")))
                .isFalse();
    }

    @Test
    void shouldWaitUpToOneHourForStackDeletion() {
        assertThat(WaitForStackToDelete.defaultPoll())
                .isEqualTo(PollWithRetries.intervalAndPollingTimeout(
                        Duration.ofSeconds(30), Duration.ofHours(1)));
    }

    @Test
    void shouldSkipConfirmationWhenForced() {
        ByteArrayOutputStream output = new ByteArrayOutputStream();

        assertThat(TearDownInstance.confirmTearDown(
                consoleInput("", output), readArguments("/scripts", "test-instance", "--force")))
                .isTrue();
        assertThat(output.size()).isZero();
    }

    private static TearDownInstance.Arguments readArguments(String... args) {
        return TearDownInstance.readArguments(CommandArgumentReader.parse(TearDownInstance.USAGE, args));
    }

    private static ConsoleInput consoleInput(String input, ByteArrayOutputStream output) {
        return new ConsoleInput(
                null, new PrintStream(output),
                new Scanner(new ByteArrayInputStream(input.getBytes(StandardCharsets.UTF_8)), StandardCharsets.UTF_8));
    }
}
