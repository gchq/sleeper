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
package sleeper.cdk.stack.compaction;

import software.amazon.awscdk.Stack;
import software.amazon.awscdk.services.ecs.ContainerDefinitionOptions;
import software.amazon.awscdk.services.ecs.ContainerImage;
import software.amazon.awscdk.services.ecs.CpuArchitecture;
import software.amazon.awscdk.services.ecs.FargateTaskDefinition;
import software.amazon.awscdk.services.ecs.ITaskDefinition;
import software.amazon.awscdk.services.ecs.OperatingSystemFamily;
import software.amazon.awscdk.services.ecs.RuntimePlatform;

import sleeper.cdk.stack.SleeperCoreStacks;
import sleeper.cdk.stack.core.LoggingStack.LogGroupRef;
import sleeper.cdk.util.Utils;
import sleeper.core.ContainerConstants;
import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.model.CompactionTaskRequirements;

import java.util.Locale;
import java.util.Map;

import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.COMPACTION_TASK_FARGATE_DEFINITION_FAMILY;
import static sleeper.core.properties.instance.CompactionProperty.COMPACTION_TASK_CPU_ARCHITECTURE;

public class CompactionOnFargateResources {
    private final InstanceProperties instanceProperties;
    private final Stack stack;
    private final SleeperCoreStacks coreStacks;

    public CompactionOnFargateResources(
            InstanceProperties instanceProperties, Stack stack, SleeperCoreStacks coreStacks) {
        this.instanceProperties = instanceProperties;
        this.stack = stack;
        this.coreStacks = coreStacks;
    }

    public ITaskDefinition createTaskDefinition(
            ContainerImage containerImage, Map<String, String> environmentVariables) {

        FargateTaskDefinition taskDefinition = createTaskDefinition();
        instanceProperties.set(COMPACTION_TASK_FARGATE_DEFINITION_FAMILY, taskDefinition.getFamily());
        taskDefinition.addContainer(ContainerConstants.COMPACTION_CONTAINER_NAME,
                createContainerDefinition(containerImage, environmentVariables));
        return taskDefinition;
    }

    private FargateTaskDefinition createTaskDefinition() {
        String architecture = instanceProperties.get(COMPACTION_TASK_CPU_ARCHITECTURE).toUpperCase(Locale.ROOT);
        CompactionTaskRequirements requirements = CompactionTaskRequirements.getArchRequirements(architecture, instanceProperties);
        return FargateTaskDefinition.Builder
                .create(stack, "CompactionFargateTaskDefinition")
                .family(String.join("-", "sleeper", Utils.cleanInstanceId(instanceProperties), "CompactionTaskOnFargate"))
                .cpu(requirements.getCpu())
                .memoryLimitMiB(requirements.getMemoryLimitMiB())
                .runtimePlatform(RuntimePlatform.builder()
                        .cpuArchitecture(CpuArchitecture.of(architecture))
                        .operatingSystemFamily(OperatingSystemFamily.LINUX)
                        .build())
                .build();
    }

    private ContainerDefinitionOptions createContainerDefinition(ContainerImage image, Map<String, String> environment) {
        String architecture = instanceProperties.get(COMPACTION_TASK_CPU_ARCHITECTURE).toUpperCase(Locale.ROOT);
        CompactionTaskRequirements requirements = CompactionTaskRequirements.getArchRequirements(architecture, instanceProperties);
        return ContainerDefinitionOptions.builder()
                .image(image)
                .environment(environment)
                .cpu(requirements.getCpu())
                .memoryLimitMiB(requirements.getMemoryLimitMiB())
                .logging(Utils.createECSContainerLogDriver(coreStacks.getLogGroup(LogGroupRef.COMPACTION_TASKS_FARGATE)))
                .build();
    }
}
