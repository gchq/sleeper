/*
 * Copyright 2022-2024 Crown Copyright
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

import sleeper.clients.util.CommandRunner;
import sleeper.configuration.properties.instance.InstanceProperties;

import java.io.IOException;
import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static sleeper.clients.testutil.RunCommandTestHelper.commandRunOn;
import static sleeper.configuration.properties.InstancePropertiesTestHelper.createTestInstanceProperties;
import static sleeper.configuration.properties.PropertiesUtils.loadProperties;

class InvokeCdkForInstanceTest {

    private final InvokeCdkForInstance cdk = InvokeCdkForInstance.builder()
            .propertiesFile(Path.of("instance.properties"))
            .jarsDirectory(Path.of("."))
            .version("1.0").build();

    @Nested
    @DisplayName("Run deploy command")
    class RunDeploy {

        @Test
        void shouldRunStandardCdkDeploySuccessfully() throws IOException, InterruptedException {
            // When / Then
            assertThat(commandRunOn(runner -> cdk.invoke(
                    InvokeCdkForInstance.Type.STANDARD, CdkCommand.deployPropertiesChange(), runner)))
                    .containsExactly("cdk",
                            "-a", "java -cp \"./cdk-1.0.jar\" sleeper.cdk.SleeperCdkApp",
                            "deploy",
                            "--require-approval", "never",
                            "-c", "propertiesfile=instance.properties",
                            "*");
        }

        @Test
        void shouldRunSystemTestCdkDeploySuccessfully() throws IOException, InterruptedException {
            // When / Then
            assertThat(commandRunOn(runner -> cdk.invoke(
                    InvokeCdkForInstance.Type.SYSTEM_TEST, CdkCommand.deployPropertiesChange(), runner)))
                    .containsExactly("cdk",
                            "-a", "java -cp \"./system-test-cdk-1.0.jar\" sleeper.systemtest.cdk.SystemTestApp",
                            "deploy",
                            "--require-approval", "never",
                            "-c", "propertiesfile=instance.properties",
                            "*");
        }

        @Test
        void shouldSetEnsureNewInstanceFlagWhenDeployingNewInstance() throws IOException, InterruptedException {
            // When / Then
            assertThat(commandRunOn(runner -> cdk.invoke(
                    InvokeCdkForInstance.Type.STANDARD, CdkCommand.deployNew(), runner)))
                    .containsExactly("cdk",
                            "-a", "java -cp \"./cdk-1.0.jar\" sleeper.cdk.SleeperCdkApp",
                            "deploy",
                            "--require-approval", "never",
                            "-c", "propertiesfile=instance.properties",
                            "-c", "newinstance=true",
                            "*");
        }

        @Test
        void shouldSetSkipVersionCheckFlagWhenDeployingExistingInstance() throws IOException, InterruptedException {
            // When / Then
            assertThat(commandRunOn(runner -> cdk.invoke(
                    InvokeCdkForInstance.Type.STANDARD, CdkCommand.deployExisting(), runner)))
                    .containsExactly("cdk",
                            "-a", "java -cp \"./cdk-1.0.jar\" sleeper.cdk.SleeperCdkApp",
                            "deploy",
                            "--require-approval", "never",
                            "-c", "propertiesfile=instance.properties",
                            "-c", "skipVersionCheck=true",
                            "*");
        }

        @Test
        void shouldSetDeployPausedFlagWhenDeployingNewInstance() throws IOException, InterruptedException {
            // When / Then
            assertThat(commandRunOn(runner -> cdk.invoke(
                    InvokeCdkForInstance.Type.SYSTEM_TEST, CdkCommand.deployNewPaused(), runner)))
                    .containsExactly("cdk",
                            "-a", "java -cp \"./system-test-cdk-1.0.jar\" sleeper.systemtest.cdk.SystemTestApp",
                            "deploy",
                            "--require-approval", "never",
                            "-c", "propertiesfile=instance.properties",
                            "-c", "newinstance=true",
                            "-c", "deployPaused=true",
                            "*");
        }

        @Test
        void shouldThrowIOExceptionWhenCommandFails() {
            // Given
            CdkCommand cdkCommand = CdkCommand.deployExisting();
            CommandRunner runner = command -> 1; // Anything but 0 is a failed exit code

            // When / Then
            assertThatThrownBy(() -> cdk.invoke(InvokeCdkForInstance.Type.STANDARD, cdkCommand, runner))
                    .isInstanceOf(IOException.class);
        }
    }

    @Nested
    @DisplayName("Run destroy command")
    class RunDestroy {

        @Test
        void shouldRunStandardCdkDestroySuccessfully() throws IOException, InterruptedException {
            // When / Then
            assertThat(commandRunOn(runner -> cdk.invoke(
                    InvokeCdkForInstance.Type.STANDARD, CdkCommand.destroy(), runner)))
                    .containsExactly("cdk",
                            "-a", "java -cp \"./cdk-1.0.jar\" sleeper.cdk.SleeperCdkApp",
                            "destroy", "--force",
                            "-c", "propertiesfile=instance.properties",
                            "-c", "validate=false",
                            "*");
        }

        @Test
        void shouldRunSystemTestCdkDeploySuccessfully() throws IOException, InterruptedException {
            // When / Then
            assertThat(commandRunOn(runner -> cdk.invoke(
                    InvokeCdkForInstance.Type.SYSTEM_TEST, CdkCommand.destroy(), runner)))
                    .containsExactly("cdk",
                            "-a", "java -cp \"./system-test-cdk-1.0.jar\" sleeper.systemtest.cdk.SystemTestApp",
                            "destroy", "--force",
                            "-c", "propertiesfile=instance.properties",
                            "-c", "validate=false",
                            "*");
        }

        @Test
        void shouldThrowIOExceptionWhenCommandFails() {
            // Given
            CdkCommand cdkCommand = CdkCommand.destroy();
            CommandRunner runner = command -> 1; // Anything but 0 is a failed exit code

            // When / Then
            assertThatThrownBy(() -> cdk.invoke(InvokeCdkForInstance.Type.STANDARD, cdkCommand, runner))
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

            // When / Then
            assertThat(commandRunOn(runner -> cdk.invokeInferringType(instanceProperties, CdkCommand.deployExisting(), runner)))
                    .startsWith("cdk",
                            "-a", "java -cp \"./cdk-1.0.jar\" sleeper.cdk.SleeperCdkApp");
        }

        @Test
        void shouldInferSystemTestDeploymentWhenSystemTestPropertyIsSet() throws IOException, InterruptedException {
            // Given
            InstanceProperties instanceProperties = new InstanceProperties(loadProperties(
                    createTestInstanceProperties().saveAsString() + "\n" +
                            "sleeper.systemtest.writers=123"));

            // When / Then
            assertThat(commandRunOn(runner -> cdk.invokeInferringType(instanceProperties, CdkCommand.deployExisting(), runner)))
                    .startsWith("cdk",
                            "-a", "java -cp \"./system-test-cdk-1.0.jar\" sleeper.systemtest.cdk.SystemTestApp");
        }
    }
}
