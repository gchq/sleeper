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

package sleeper.clients.cdk;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import sleeper.configuration.properties.InstanceProperties;
import sleeper.util.RunCommand;

import java.io.IOException;
import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static sleeper.configuration.properties.InstancePropertiesTestHelper.createTestInstanceProperties;
import static sleeper.configuration.properties.PropertiesUtils.loadProperties;
import static sleeper.utils.RunCommandTestHelper.commandRunOn;

class InvokeCdkForInstanceTest {

    @Nested
    @DisplayName("Run deploy command")
    class RunDeploy {

        @Test
        void shouldRunStandardCdkDeploySuccessfully() throws IOException, InterruptedException {
            // Given
            InvokeCdkForInstance cdk = InvokeCdkForInstance.builder()
                    .instancePropertiesFile(Path.of("instance.properties"))
                    .jarsDirectory(Path.of("."))
                    .version("1.0").build();

            // Then
            assertThat(commandRunOn(runner -> cdk.invoke(
                    InvokeCdkForInstance.Type.STANDARD, CdkDeploy.updateProperties(), runner)))
                    .containsExactly("cdk",
                            "-a", "java -cp \"./cdk-1.0.jar\" sleeper.cdk.SleeperCdkApp",
                            "deploy",
                            "--require-approval", "never",
                            "-c", "propertiesfile=instance.properties",
                            "*");
        }

        @Test
        void shouldRunSystemTestCdkDeploySuccessfully() throws IOException, InterruptedException {
            // Given
            InvokeCdkForInstance cdk = InvokeCdkForInstance.builder()
                    .instancePropertiesFile(Path.of("instance.properties"))
                    .jarsDirectory(Path.of("."))
                    .version("1.0").build();

            // Then
            assertThat(commandRunOn(runner -> cdk.invoke(
                    InvokeCdkForInstance.Type.SYSTEM_TEST, CdkDeploy.updateProperties(), runner)))
                    .containsExactly("cdk",
                            "-a", "java -cp \"./system-test-1.0-utility.jar\" sleeper.systemtest.cdk.SystemTestApp",
                            "deploy",
                            "--require-approval", "never",
                            "-c", "propertiesfile=instance.properties",
                            "*");
        }

        @Test
        void shouldSetEnsureNewInstanceFlagWhenDeployingNewInstance() throws IOException, InterruptedException {
            // Given
            InvokeCdkForInstance cdk = InvokeCdkForInstance.builder()
                    .instancePropertiesFile(Path.of("instance.properties"))
                    .jarsDirectory(Path.of("."))
                    .version("1.0").build();

            // Then
            assertThat(commandRunOn(runner -> cdk.invoke(
                    InvokeCdkForInstance.Type.STANDARD, CdkDeploy.deployNew(), runner)))
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
            // Given
            InvokeCdkForInstance cdk = InvokeCdkForInstance.builder()
                    .instancePropertiesFile(Path.of("instance.properties"))
                    .jarsDirectory(Path.of("."))
                    .version("1.0").build();

            // When / Then
            assertThat(commandRunOn(runner -> cdk.invoke(
                    InvokeCdkForInstance.Type.STANDARD, CdkDeploy.deployExisting(), runner)))
                    .containsExactly("cdk",
                            "-a", "java -cp \"./cdk-1.0.jar\" sleeper.cdk.SleeperCdkApp",
                            "deploy",
                            "--require-approval", "never",
                            "-c", "propertiesfile=instance.properties",
                            "-c", "skipVersionCheck=true",
                            "*");
        }

        @Test
        void shouldThrowIOExceptionWhenCommandFails() {
            // Given
            InvokeCdkForInstance cdk = InvokeCdkForInstance.builder()
                    .instancePropertiesFile(Path.of("instance.properties"))
                    .jarsDirectory(Path.of("."))
                    .version("1.0").build();
            CdkCommand cdkCommand = CdkDeploy.deployExisting();
            RunCommand runner = command -> 1; // Anything but 0 is a failed exit code

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
            // Given
            InvokeCdkForInstance cdk = InvokeCdkForInstance.builder()
                    .instancePropertiesFile(Path.of("instance.properties"))
                    .jarsDirectory(Path.of("."))
                    .version("1.0").build();

            // Then
            assertThat(commandRunOn(runner -> cdk.invoke(
                    InvokeCdkForInstance.Type.STANDARD, CdkDestroy.destroy(), runner)))
                    .containsExactly("cdk",
                            "-a", "java -cp \"./cdk-1.0.jar\" sleeper.cdk.SleeperCdkApp",
                            "destroy", "--force",
                            "-c", "propertiesfile=instance.properties",
                            "-c", "validate=false",
                            "*");
        }

        @Test
        void shouldRunSystemTestCdkDeploySuccessfully() throws IOException, InterruptedException {
            // Given
            InvokeCdkForInstance cdk = InvokeCdkForInstance.builder()
                    .instancePropertiesFile(Path.of("instance.properties"))
                    .jarsDirectory(Path.of("."))
                    .version("1.0").build();

            // Then
            assertThat(commandRunOn(runner -> cdk.invoke(
                    InvokeCdkForInstance.Type.SYSTEM_TEST, CdkDestroy.destroy(), runner)))
                    .containsExactly("cdk",
                            "-a", "java -cp \"./system-test-1.0-utility.jar\" sleeper.systemtest.cdk.SystemTestApp",
                            "destroy", "--force",
                            "-c", "propertiesfile=instance.properties",
                            "-c", "validate=false",
                            "*");
        }

        @Test
        void shouldThrowIOExceptionWhenCommandFails() {
            // Given
            InvokeCdkForInstance cdk = InvokeCdkForInstance.builder()
                    .instancePropertiesFile(Path.of("instance.properties"))
                    .jarsDirectory(Path.of("."))
                    .version("1.0").build();
            CdkCommand cdkCommand = CdkDestroy.destroy();
            RunCommand runner = command -> 1; // Anything but 0 is a failed exit code

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
            InvokeCdkForInstance cdk = InvokeCdkForInstance.builder()
                    .instancePropertiesFile(Path.of("instance.properties"))
                    .jarsDirectory(Path.of("."))
                    .version("1.0").build();

            InstanceProperties instanceProperties = createTestInstanceProperties();

            // Then
            assertThat(commandRunOn(runner -> cdk.invokeInferringType(instanceProperties, CdkDeploy.deployExisting(), runner)))
                    .startsWith("cdk",
                            "-a", "java -cp \"./cdk-1.0.jar\" sleeper.cdk.SleeperCdkApp");
        }

        @Test
        void shouldInferSystemTestDeploymentWhenSystemTestPropertyIsSet() throws IOException, InterruptedException {
            // Given
            InvokeCdkForInstance cdk = InvokeCdkForInstance.builder()
                    .instancePropertiesFile(Path.of("instance.properties"))
                    .jarsDirectory(Path.of("."))
                    .version("1.0").build();

            InstanceProperties instanceProperties = new InstanceProperties(loadProperties(
                    createTestInstanceProperties().saveAsString() + "\n" +
                            "sleeper.systemtest.writers=123"));

            // Then
            assertThat(commandRunOn(runner -> cdk.invokeInferringType(instanceProperties, CdkDeploy.deployExisting(), runner)))
                    .startsWith("cdk",
                            "-a", "java -cp \"./system-test-1.0-utility.jar\" sleeper.systemtest.cdk.SystemTestApp");
        }
    }
}
