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
package sleeper.clients.deploy.container;

import org.junit.jupiter.api.Test;

import sleeper.clients.util.command.CommandFailedException;
import sleeper.clients.util.command.CommandPipeline;
import sleeper.clients.util.command.CommandPipelineResult;
import sleeper.clients.util.command.CommandPipelineRunner;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static sleeper.clients.deploy.container.DockerImageCommandTestData.createBaseImageRegistryCommand;
import static sleeper.clients.deploy.container.DockerImageCommandTestData.startBaseImageRegistryCommand;
import static sleeper.clients.testutil.RunCommandTestHelper.recordCommandsRun;
import static sleeper.clients.testutil.RunCommandTestHelper.returnExitCode;
import static sleeper.clients.testutil.RunCommandTestHelper.returnExitCodeForCommand;

public class ManagedBaseImageRegistryTest {

    private final List<CommandPipeline> commandsThatRan = new ArrayList<>();

    @Test
    void shouldStartTheContainerWhenItExists() throws Exception {
        // Given a container that is stopped or already running, so that starting it succeeds

        // When
        createIfMissingWithPort(5000, returnExitCode(0));

        // Then
        assertThat(commandsThatRan).containsExactly(
                startBaseImageRegistryCommand());
    }

    @Test
    void shouldCreateTheContainerWhenItDoesNotExist() throws Exception {
        // Given
        CommandPipelineRunner runner = returnExitCodeForCommand(1, startBaseImageRegistryCommand());

        // When
        createIfMissingWithPort(5000, runner);

        // Then
        assertThat(commandsThatRan).containsExactly(
                startBaseImageRegistryCommand(),
                createBaseImageRegistryCommand(5000));
    }

    @Test
    void shouldCreateTheContainerOnAConfiguredPort() throws Exception {
        // Given
        CommandPipelineRunner runner = returnExitCodeForCommand(1, startBaseImageRegistryCommand());

        // When
        createIfMissingWithPort(5001, runner);

        // Then
        assertThat(commandsThatRan).containsExactly(
                startBaseImageRegistryCommand(),
                createBaseImageRegistryCommand(5001));
    }

    @Test
    void shouldStartTheContainerWhenAConcurrentDeployCreatedItFirst() throws Exception {
        // Given the container was absent when we looked, so creating it clashes with the deploy that got there first
        CommandPipelineRunner runner = failFirstAttemptToStartAndEveryAttemptToCreate();

        // When
        createIfMissingWithPort(5000, runner);

        // Then
        assertThat(commandsThatRan).containsExactly(
                startBaseImageRegistryCommand(),
                createBaseImageRegistryCommand(5000),
                startBaseImageRegistryCommand());
    }

    @Test
    void shouldFailNamingTheContainerWhenItCanNeitherBeStartedNorCreated() {
        // Given
        CommandPipelineRunner runner = returnExitCode(125);

        // When / Then
        assertThatThrownBy(() -> createIfMissingWithPort(5000, runner))
                .isInstanceOf(CommandFailedException.class)
                .hasMessageContaining("sleeper-base-image-registry");
        assertThat(commandsThatRan).containsExactly(
                startBaseImageRegistryCommand(),
                createBaseImageRegistryCommand(5000),
                startBaseImageRegistryCommand());
    }

    private CommandPipelineRunner failFirstAttemptToStartAndEveryAttemptToCreate() {
        AtomicBoolean startedOnce = new AtomicBoolean();
        return command -> {
            if (command.equals(createBaseImageRegistryCommand(5000))) {
                return new CommandPipelineResult(125);
            }
            return new CommandPipelineResult(startedOnce.getAndSet(true) ? 0 : 1);
        };
    }

    private void createIfMissingWithPort(int port, CommandPipelineRunner runner) throws Exception {
        BaseImageDestination.managedRegistry(port)
                .createIfMissing(recordCommandsRun(commandsThatRan, runner));
    }
}
