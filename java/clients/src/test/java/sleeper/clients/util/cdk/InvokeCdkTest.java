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

package sleeper.clients.util.cdk;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import sleeper.clients.util.command.CommandPipeline;
import sleeper.clients.util.command.CommandRunner;
import sleeper.core.properties.model.SleeperInternalCdkApp;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static sleeper.clients.testutil.RunCommandTestHelper.recordCommandsRun;
import static sleeper.clients.testutil.RunCommandTestHelper.returnExitCode;
import static sleeper.clients.util.command.Command.command;
import static sleeper.clients.util.command.CommandPipeline.pipeline;

class InvokeCdkTest {

    private final List<CommandPipeline> commandsThatRan = new ArrayList<>();
    private final Path propertiesFile = Path.of("instance.properties");

    private InvokeCdk cdk() {
        return cdk(recordCommandsRun(commandsThatRan));
    }

    private InvokeCdk cdk(CommandRunner commandRunner) {
        return InvokeCdk.builder()
                .jarsDirectory(Path.of("."))
                .runCommand(commandRunner)
                .version("1.0").build();
    }

    @Nested
    @DisplayName("Run deploy command")
    class RunDeploy {

        @Test
        void shouldDeployStandardCdkApp() throws IOException, InterruptedException {
            // When
            cdk().invoke(SleeperInternalCdkApp.STANDARD, CdkCommand.deployPropertiesChange(propertiesFile));

            // Then
            assertThat(commandsThatRan).containsExactly(pipeline(command(
                    "cdk",
                    "-a", "java -cp \"./cdk-1.0.jar\" sleeper.cdk.SleeperCdkApp",
                    "deploy",
                    "--require-approval", "never",
                    "-c", "propertiesfile=instance.properties",
                    "*")));
        }

        @Test
        void shouldDeployDemonstrationCdkApp() throws IOException, InterruptedException {
            // When
            cdk().invoke(SleeperInternalCdkApp.DEMONSTRATION, CdkCommand.deployPropertiesChange(propertiesFile));

            // Then
            assertThat(commandsThatRan).containsExactly(pipeline(command("cdk",
                    "-a", "java -cp \"./system-test-cdk-1.0.jar\" sleeper.systemtest.cdk.SleeperDemonstrationCdkApp",
                    "deploy",
                    "--require-approval", "never",
                    "-c", "propertiesfile=instance.properties",
                    "*")));
        }

        @Test
        void shouldSetEnsureNewInstanceFlagWhenDeployingNewInstance() throws IOException, InterruptedException {
            // When
            cdk().invoke(SleeperInternalCdkApp.STANDARD, CdkCommand.deployNew().withPropertiesFile(propertiesFile));

            // Then
            assertThat(commandsThatRan).containsExactly(pipeline(command("cdk",
                    "-a", "java -cp \"./cdk-1.0.jar\" sleeper.cdk.SleeperCdkApp",
                    "deploy",
                    "--require-approval", "never",
                    "-c", "propertiesfile=instance.properties",
                    "-c", "newinstance=true",
                    "*")));
        }

        @Test
        void shouldSetSkipVersionCheckFlagWhenDeployingExistingInstance() throws IOException, InterruptedException {
            // When
            cdk().invoke(SleeperInternalCdkApp.STANDARD, CdkCommand.deployExisting().withPropertiesFile(propertiesFile));

            // Then
            assertThat(commandsThatRan).containsExactly(pipeline(command("cdk",
                    "-a", "java -cp \"./cdk-1.0.jar\" sleeper.cdk.SleeperCdkApp",
                    "deploy",
                    "--require-approval", "never",
                    "-c", "propertiesfile=instance.properties",
                    "-c", "skipVersionCheck=true",
                    "*")));
        }

        @Test
        void shouldSetDeployPausedFlagWhenDeployingNewInstance() throws IOException, InterruptedException {
            // When
            cdk().invoke(SleeperInternalCdkApp.DEMONSTRATION, CdkCommand.deployNewPaused().withPropertiesFile(propertiesFile));

            // Then
            assertThat(commandsThatRan).containsExactly(pipeline(command("cdk",
                    "-a", "java -cp \"./system-test-cdk-1.0.jar\" sleeper.systemtest.cdk.SleeperDemonstrationCdkApp",
                    "deploy",
                    "--require-approval", "never",
                    "-c", "propertiesfile=instance.properties",
                    "-c", "newinstance=true",
                    "-c", "deployPaused=true",
                    "*")));
        }

        @Test
        void shouldThrowIOExceptionWhenCommandFails() {
            // Given
            InvokeCdk cdk = cdk(returnExitCode(1)); // Anything but 0 is a failed exit code
            CdkCommand cdkCommand = CdkCommand.deployExisting().withPropertiesFile(propertiesFile);

            // When / Then
            assertThatThrownBy(() -> cdk.invoke(SleeperInternalCdkApp.STANDARD, cdkCommand))
                    .isInstanceOf(IOException.class);
        }
    }

    @Nested
    @DisplayName("Run destroy command")
    class RunDestroy {

        @Test
        void shouldDestroyStandardCdkApp() throws IOException, InterruptedException {
            // When
            cdk().invoke(SleeperInternalCdkApp.STANDARD, CdkCommand.destroy().withPropertiesFile(propertiesFile));

            // Then
            assertThat(commandsThatRan).containsExactly(pipeline(command("cdk",
                    "-a", "java -cp \"./cdk-1.0.jar\" sleeper.cdk.SleeperCdkApp",
                    "destroy", "--force",
                    "-c", "propertiesfile=instance.properties",
                    "-c", "validate=false",
                    "*")));
        }

        @Test
        void shouldDestroyDemonstrationCdkApp() throws IOException, InterruptedException {
            // When
            cdk().invoke(SleeperInternalCdkApp.DEMONSTRATION, CdkCommand.destroy().withPropertiesFile(propertiesFile));

            // Then
            assertThat(commandsThatRan).containsExactly(pipeline(command("cdk",
                    "-a", "java -cp \"./system-test-cdk-1.0.jar\" sleeper.systemtest.cdk.SleeperDemonstrationCdkApp",
                    "destroy", "--force",
                    "-c", "propertiesfile=instance.properties",
                    "-c", "validate=false",
                    "*")));
        }

        @Test
        void shouldThrowIOExceptionWhenCommandFails() {
            // Given
            InvokeCdk cdk = cdk(returnExitCode(1));
            CdkCommand cdkCommand = CdkCommand.destroy().withPropertiesFile(propertiesFile);

            // When / Then
            assertThatThrownBy(() -> cdk.invoke(SleeperInternalCdkApp.STANDARD, cdkCommand))
                    .isInstanceOf(IOException.class);
        }
    }
}
