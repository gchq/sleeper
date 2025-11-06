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

package sleeper.clients.util.cdk;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import sleeper.clients.util.command.CommandPipeline;
import sleeper.clients.util.command.CommandRunner;
import sleeper.core.properties.instance.InstanceProperties;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static sleeper.clients.testutil.RunCommandTestHelper.recordCommandsRun;
import static sleeper.clients.testutil.RunCommandTestHelper.returnExitCode;
import static sleeper.clients.testutil.RunCommandTestHelper.singleCommand;
import static sleeper.clients.util.command.Command.command;
import static sleeper.clients.util.command.CommandPipeline.pipeline;
import static sleeper.core.properties.PropertiesUtils.loadProperties;
import static sleeper.core.properties.testutils.InstancePropertiesTestHelper.createTestInstanceProperties;

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
        void shouldRunStandardCdkDeploySuccessfully() throws IOException, InterruptedException {
            // When
            cdk().invoke(InvokeCdk.Type.STANDARD, CdkCommand.deployPropertiesChange(propertiesFile));

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
        void shouldRunSystemTestCdkDeploySuccessfully() throws IOException, InterruptedException {
            // When
            cdk().invoke(InvokeCdk.Type.SYSTEM_TEST, CdkCommand.deployPropertiesChange(propertiesFile));

            // Then
            assertThat(commandsThatRan).containsExactly(pipeline(command("cdk",
                    "-a", "java -cp \"./system-test-cdk-1.0.jar\" sleeper.systemtest.cdk.SystemTestApp",
                    "deploy",
                    "--require-approval", "never",
                    "-c", "propertiesfile=instance.properties",
                    "*")));
        }

        @Test
        void shouldSetEnsureNewInstanceFlagWhenDeployingNewInstance() throws IOException, InterruptedException {
            // When
            cdk().invoke(InvokeCdk.Type.STANDARD, CdkCommand.deployNew().withPropertiesFile(propertiesFile));

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
            cdk().invoke(InvokeCdk.Type.STANDARD, CdkCommand.deployExisting().withPropertiesFile(propertiesFile));

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
            cdk().invoke(InvokeCdk.Type.SYSTEM_TEST, CdkCommand.deployNewPaused().withPropertiesFile(propertiesFile));

            // Then
            assertThat(commandsThatRan).containsExactly(pipeline(command("cdk",
                    "-a", "java -cp \"./system-test-cdk-1.0.jar\" sleeper.systemtest.cdk.SystemTestApp",
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
            assertThatThrownBy(() -> cdk.invoke(InvokeCdk.Type.STANDARD, cdkCommand))
                    .isInstanceOf(IOException.class);
        }
    }

    @Nested
    @DisplayName("Run destroy command")
    class RunDestroy {

        @Test
        void shouldRunStandardCdkDestroySuccessfully() throws IOException, InterruptedException {
            // When
            cdk().invoke(InvokeCdk.Type.STANDARD, CdkCommand.destroy().withPropertiesFile(propertiesFile));

            // Then
            assertThat(commandsThatRan).containsExactly(pipeline(command("cdk",
                    "-a", "java -cp \"./cdk-1.0.jar\" sleeper.cdk.SleeperCdkApp",
                    "destroy", "--force",
                    "-c", "propertiesfile=instance.properties",
                    "-c", "validate=false",
                    "*")));
        }

        @Test
        void shouldRunSystemTestCdkDeploySuccessfully() throws IOException, InterruptedException {
            // When
            cdk().invoke(InvokeCdk.Type.SYSTEM_TEST, CdkCommand.destroy().withPropertiesFile(propertiesFile));

            // Then
            assertThat(commandsThatRan).containsExactly(pipeline(command("cdk",
                    "-a", "java -cp \"./system-test-cdk-1.0.jar\" sleeper.systemtest.cdk.SystemTestApp",
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
            assertThatThrownBy(() -> cdk.invoke(InvokeCdk.Type.STANDARD, cdkCommand))
                    .isInstanceOf(IOException.class);
        }
    }

    @Nested
    @DisplayName("Infer whether to invoke as a standard or system test instance")
    class InferStandardOrSystemTest {

        @Test
        void shouldInferStandardDeploymentWhenNoSystemTestPropertiesAreSet() throws IOException, InterruptedException {
            // Given
            InstanceProperties instanceProperties = createTestInstanceProperties();

            // When
            cdk().invokeInferringType(instanceProperties, CdkCommand.deployExisting().withPropertiesFile(propertiesFile));

            // Then
            assertThat(singleCommand(commandsThatRan))
                    .startsWith("cdk",
                            "-a", "java -cp \"./cdk-1.0.jar\" sleeper.cdk.SleeperCdkApp");
        }

        @Test
        void shouldInferSystemTestDeploymentWhenSystemTestPropertyIsSet() throws IOException, InterruptedException {
            // Given
            InstanceProperties instanceProperties = InstanceProperties.createWithoutValidation(loadProperties(
                    createTestInstanceProperties().saveAsString() + "\n" +
                            "sleeper.systemtest.writers=123"));

            // When
            cdk().invokeInferringType(instanceProperties, CdkCommand.deployExisting().withPropertiesFile(propertiesFile));

            // Then
            assertThat(singleCommand(commandsThatRan))
                    .startsWith("cdk",
                            "-a", "java -cp \"./system-test-cdk-1.0.jar\" sleeper.systemtest.cdk.SystemTestApp");
        }
    }
}
