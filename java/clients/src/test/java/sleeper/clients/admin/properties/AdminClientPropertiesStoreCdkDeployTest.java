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
package sleeper.clients.admin.properties;

import org.junit.jupiter.api.Test;

import sleeper.clients.admin.testutils.InMemoryAdminClientProperties;
import sleeper.clients.deploy.DeployConfiguration;
import sleeper.clients.deploy.container.DockerImageConfiguration;
import sleeper.clients.deploy.container.UploadDockerImages;
import sleeper.clients.deploy.container.UploadDockerImagesToEcr;
import sleeper.clients.util.cdk.InvokeCdk;
import sleeper.clients.util.command.CommandPipeline;
import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.model.OptionalStack;
import sleeper.core.properties.model.SleeperInternalCdkApp;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.clients.testutil.RunCommandTestHelper.recordCommandsRun;
import static sleeper.clients.util.command.Command.command;
import static sleeper.clients.util.command.CommandPipeline.pipeline;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.ACCOUNT;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.CDK_APP;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.REGION;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.VERSION;
import static sleeper.core.properties.instance.CommonProperty.ID;
import static sleeper.core.properties.instance.CommonProperty.OPTIONAL_STACKS;
import static sleeper.core.properties.testutils.InstancePropertiesTestHelper.createTestInstanceProperties;

public class AdminClientPropertiesStoreCdkDeployTest {

    private final InstanceProperties instanceProperties = createTestInstanceProperties();
    private final InMemoryAdminClientProperties clientProperties = InMemoryAdminClientProperties.create();
    private final DockerImageConfiguration dockerImageConfiguration = new DockerImageConfiguration(List.of(), List.of());
    private final Path scriptsDirectory = Path.of("./scripts");
    private final List<CommandPipeline> commandsThatRan = new ArrayList<>();
    private final List<CommandPipeline> cdkCommandsThatRan = new ArrayList<>();
    private final List<CommandPipeline> dockerCommandsThatRan = new ArrayList<>();
    private final Map<Path, String> files = new HashMap<>();

    @Test
    void shouldRedeployStandardCdkAppWhenCdkDeployedPropertyIsUpdated() {
        // Given an instance that was deployed with the standard CDK app
        instanceProperties.setEnum(CDK_APP, SleeperInternalCdkApp.STANDARD);
        instanceProperties.setList(OPTIONAL_STACKS, List.of());
        clientProperties.setInstanceProperties(instanceProperties);
        // And a property change to force a redeploy
        InstanceProperties propertiesBefore = InstanceProperties.copyOf(instanceProperties);
        instanceProperties.setEnumList(OPTIONAL_STACKS, List.of(OptionalStack.CompactionStack));
        PropertiesDiff diff = new PropertiesDiff(propertiesBefore, instanceProperties);

        // When we apply the change
        store().saveInstanceProperties(instanceProperties, diff);

        // Then the properties are not saved to the instance, as the CDK will apply the change
        assertThat(clientProperties.loadInstancePropertiesNoValidation(instanceProperties.get(ID)))
                .isEqualTo(propertiesBefore);
        // And the properties are saved to the local directory, to be read by the CDK
        assertThat(clientProperties.getLocalInstanceProperties()).isEqualTo(instanceProperties);
        // And the CDK is invoked
        assertThat(cdkCommandsThatRan).containsExactly(pipeline(command(
                "cdk",
                "-a", "java -cp \"./scripts/jars/cdk-1.2.3.jar\" sleeper.cdk.SleeperCdkApp",
                "deploy",
                "--require-approval", "never",
                "-c", "propertiesfile=./scripts/generated/instance.properties",
                "*")));
    }

    private AdminClientPropertiesStore store() {
        return new AdminClientPropertiesStore(
                clientProperties, invokeCdk(), scriptsDirectory.resolve("generated"), uploadDockerImages(),
                dockerImageConfiguration);
    }

    private InvokeCdk invokeCdk() {
        return InvokeCdk.builder()
                .version(instanceProperties.get(VERSION))
                .scriptsDirectory(scriptsDirectory)
                .runCommand(recordCommandsRun(commandsThatRan, recordCommandsRun(cdkCommandsThatRan)))
                .build();
    }

    private UploadDockerImagesToEcr uploadDockerImages() {
        return new UploadDockerImagesToEcr(
                UploadDockerImages.builder()
                        .deployConfig(DeployConfiguration.fromLocalBuild())
                        .commandRunner(recordCommandsRun(commandsThatRan, recordCommandsRun(dockerCommandsThatRan)))
                        .copyFile((source, target) -> files.put(target, files.get(source)))
                        .scriptsDirectory(scriptsDirectory)
                        .version(instanceProperties.get(VERSION))
                        .build(),
                instanceProperties.get(ACCOUNT), instanceProperties.get(REGION));
    }

}
