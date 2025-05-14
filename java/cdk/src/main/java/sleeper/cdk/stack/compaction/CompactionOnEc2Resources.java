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

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import software.amazon.awscdk.Duration;
import software.amazon.awscdk.Stack;
import software.amazon.awscdk.services.autoscaling.AutoScalingGroup;
import software.amazon.awscdk.services.autoscaling.TerminationPolicy;
import software.amazon.awscdk.services.ec2.BlockDevice;
import software.amazon.awscdk.services.ec2.BlockDeviceVolume;
import software.amazon.awscdk.services.ec2.EbsDeviceOptions;
import software.amazon.awscdk.services.ec2.EbsDeviceVolumeType;
import software.amazon.awscdk.services.ec2.ISecurityGroup;
import software.amazon.awscdk.services.ec2.IVpc;
import software.amazon.awscdk.services.ec2.InstanceClass;
import software.amazon.awscdk.services.ec2.InstanceSize;
import software.amazon.awscdk.services.ec2.InstanceType;
import software.amazon.awscdk.services.ec2.LaunchTemplate;
import software.amazon.awscdk.services.ec2.SecurityGroup;
import software.amazon.awscdk.services.ec2.UserData;
import software.amazon.awscdk.services.ecs.AddAutoScalingGroupCapacityOptions;
import software.amazon.awscdk.services.ecs.AmiHardwareType;
import software.amazon.awscdk.services.ecs.AsgCapacityProvider;
import software.amazon.awscdk.services.ecs.Cluster;
import software.amazon.awscdk.services.ecs.ContainerDefinitionOptions;
import software.amazon.awscdk.services.ecs.ContainerImage;
import software.amazon.awscdk.services.ecs.Ec2TaskDefinition;
import software.amazon.awscdk.services.ecs.EcsOptimizedImage;
import software.amazon.awscdk.services.ecs.EcsOptimizedImageOptions;
import software.amazon.awscdk.services.ecs.ITaskDefinition;
import software.amazon.awscdk.services.ecs.MachineImageType;
import software.amazon.awscdk.services.ecs.NetworkMode;
import software.amazon.awscdk.services.iam.IRole;
import software.amazon.awscdk.services.iam.InstanceProfile;
import software.amazon.awscdk.services.iam.PolicyStatement;
import software.amazon.awscdk.services.lambda.CfnPermission;
import software.amazon.awscdk.services.lambda.IFunction;
import software.constructs.Construct;
import software.constructs.IDependable;

import sleeper.cdk.jars.LambdaCode;
import sleeper.cdk.stack.core.CoreStacks;
import sleeper.cdk.stack.core.LoggingStack.LogGroupRef;
import sleeper.cdk.util.Utils;
import sleeper.core.ContainerConstants;
import sleeper.core.deploy.LambdaHandler;
import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.model.CompactionTaskRequirements;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.COMPACTION_AUTO_SCALING_GROUP;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.COMPACTION_TASK_EC2_DEFINITION_FAMILY;
import static sleeper.core.properties.instance.CommonProperty.ACCOUNT;
import static sleeper.core.properties.instance.CommonProperty.ECS_SECURITY_GROUPS;
import static sleeper.core.properties.instance.CompactionProperty.COMPACTION_EC2_POOL_DESIRED;
import static sleeper.core.properties.instance.CompactionProperty.COMPACTION_EC2_POOL_MAXIMUM;
import static sleeper.core.properties.instance.CompactionProperty.COMPACTION_EC2_POOL_MINIMUM;
import static sleeper.core.properties.instance.CompactionProperty.COMPACTION_EC2_ROOT_SIZE;
import static sleeper.core.properties.instance.CompactionProperty.COMPACTION_EC2_TYPE;
import static sleeper.core.properties.instance.CompactionProperty.COMPACTION_TASK_CPU_ARCHITECTURE;

public class CompactionOnEc2Resources {
    private final InstanceProperties instanceProperties;
    private final Stack stack;
    private final CoreStacks coreStacks;

    public CompactionOnEc2Resources(
            InstanceProperties instanceProperties, Stack stack, CoreStacks coreStacks) {
        this.instanceProperties = instanceProperties;
        this.stack = stack;
        this.coreStacks = coreStacks;
    }

    public ITaskDefinition createTaskDefinition(
            Cluster cluster, IVpc vpc, LambdaCode taskCreatorJar,
            ContainerImage containerImage, Map<String, String> environmentVariables) {

        Ec2TaskDefinition taskDefinition = Ec2TaskDefinition.Builder
                .create(stack, "CompactionEC2TaskDefinition")
                .family(String.join("-", Utils.cleanInstanceId(instanceProperties), "CompactionTaskOnEC2"))
                .networkMode(NetworkMode.BRIDGE)
                .build();
        instanceProperties.set(COMPACTION_TASK_EC2_DEFINITION_FAMILY, taskDefinition.getFamily());
        ContainerDefinitionOptions ec2ContainerDefinitionOptions = createEC2ContainerDefinition(
                coreStacks, containerImage, environmentVariables, instanceProperties);
        taskDefinition.addContainer(ContainerConstants.COMPACTION_CONTAINER_NAME, ec2ContainerDefinitionOptions);
        addEC2CapacityProvider(cluster, vpc, taskCreatorJar);
        return taskDefinition;
    }

    private ContainerDefinitionOptions createEC2ContainerDefinition(
            CoreStacks coreStacks, ContainerImage image, Map<String, String> environment, InstanceProperties instanceProperties) {
        String architecture = instanceProperties.get(COMPACTION_TASK_CPU_ARCHITECTURE).toUpperCase(Locale.ROOT);
        CompactionTaskRequirements requirements = CompactionTaskRequirements.getArchRequirements(architecture, instanceProperties);
        return ContainerDefinitionOptions.builder()
                .image(image)
                .environment(environment)
                .cpu(requirements.getCpu())
                .memoryLimitMiB(requirements.getMemoryLimitMiB())
                .logging(Utils.createECSContainerLogDriver(coreStacks.getLogGroup(LogGroupRef.COMPACTION_TASKS_EC2)))
                .build();
    }

    @SuppressFBWarnings("NP_NULL_ON_SOME_PATH_FROM_RETURN_VALUE")
    private void addEC2CapacityProvider(Cluster cluster, IVpc vpc, LambdaCode taskCreatorJar) {

        // Create some extra user data to enable ECS container metadata file
        UserData customUserData = UserData.forLinux();
        customUserData.addCommands("echo ECS_ENABLE_CONTAINER_METADATA=true >> /etc/ecs/ecs.config");

        IFunction customTermination = lambdaForCustomTerminationPolicy(coreStacks, taskCreatorJar);

        IDependable autoScalingPermission = CfnPermission.Builder.create(stack, "AutoscalingCall")
                .action("lambda:InvokeFunction")
                .principal("arn:aws:iam::" + instanceProperties.get(ACCOUNT)
                        + ":role/aws-service-role/autoscaling.amazonaws.com/AWSServiceRoleForAutoScaling")
                .functionName(customTermination.getFunctionArn())
                .build();

        SecurityGroup scalingSecurityGroup = SecurityGroup.Builder.create(stack, "CompactionScalingDefaultSG")
                .vpc(vpc)
                .allowAllOutbound(true)
                .build();

        InstanceProfile roleProfile = InstanceProfile.Builder.create(stack, "CompactionScalingInstanceProfile")
                .build();

        LaunchTemplate scalingLaunchTemplate = LaunchTemplate.Builder.create(stack, "CompactionScalingTemplate")
                .associatePublicIpAddress(false)
                .requireImdsv2(true)
                .blockDevices(List.of(BlockDevice.builder()
                        .deviceName("/dev/xvda") // root volume
                        .volume(BlockDeviceVolume.ebs(instanceProperties.getInt(COMPACTION_EC2_ROOT_SIZE),
                                EbsDeviceOptions.builder()
                                        .deleteOnTermination(true)
                                        .encrypted(true)
                                        .volumeType(EbsDeviceVolumeType.GP3)
                                        .build()))
                        .build()))
                .userData(customUserData)
                .instanceType(lookupEC2InstanceType(instanceProperties.get(COMPACTION_EC2_TYPE)))
                .machineImage(EcsOptimizedImage.amazonLinux2(AmiHardwareType.STANDARD,
                        EcsOptimizedImageOptions.builder()
                                .cachedInContext(false)
                                .build()))
                .securityGroup(scalingSecurityGroup)
                .instanceProfile(roleProfile)
                .build();
        addSecurityGroupReferences(stack, instanceProperties)
                .forEach(scalingLaunchTemplate::addSecurityGroup);
        AutoScalingGroup ec2scalingGroup = AutoScalingGroup.Builder.create(stack, "CompactionScalingGroup")
                .vpc(vpc)
                .launchTemplate(scalingLaunchTemplate)
                .minCapacity(instanceProperties.getInt(COMPACTION_EC2_POOL_MINIMUM))
                .desiredCapacity(instanceProperties.getInt(COMPACTION_EC2_POOL_DESIRED))
                .maxCapacity(instanceProperties.getInt(COMPACTION_EC2_POOL_MAXIMUM))
                .terminationPolicies(List.of(TerminationPolicy.CUSTOM_LAMBDA_FUNCTION))
                .terminationPolicyCustomLambdaFunctionArn(customTermination.getFunctionArn())
                .build();
        ec2scalingGroup.getNode().addDependency(autoScalingPermission);

        AsgCapacityProvider ec2Provider = AsgCapacityProvider.Builder
                .create(stack, "CompactionCapacityProvider")
                .enableManagedScaling(false)
                .enableManagedTerminationProtection(false)
                .autoScalingGroup(ec2scalingGroup)
                .spotInstanceDraining(true)
                .canContainersAccessInstanceRole(false)
                .machineImageType(MachineImageType.AMAZON_LINUX_2)
                .build();

        cluster.addAsgCapacityProvider(ec2Provider,
                AddAutoScalingGroupCapacityOptions.builder()
                        .canContainersAccessInstanceRole(false)
                        .machineImageType(MachineImageType.AMAZON_LINUX_2)
                        .spotInstanceDraining(true)
                        .build());

        instanceProperties.set(COMPACTION_AUTO_SCALING_GROUP, ec2scalingGroup.getAutoScalingGroupName());
    }

    @SuppressFBWarnings("NP_NULL_ON_SOME_PATH_FROM_RETURN_VALUE")
    private IFunction lambdaForCustomTerminationPolicy(CoreStacks coreStacks, LambdaCode lambdaCode) {

        // Run tasks function
        Map<String, String> environmentVariables = Utils.createDefaultEnvironment(instanceProperties);

        String functionName = String.join("-", "sleeper",
                Utils.cleanInstanceId(instanceProperties), "compaction-custom-termination");

        IFunction handler = lambdaCode.buildFunction(stack, LambdaHandler.COMPACTION_TASK_TERMINATOR, "CompactionTerminator", builder -> builder
                .functionName(functionName)
                .description("Custom termination policy for ECS auto scaling group. Only terminate empty instances.")
                .environment(environmentVariables)
                .logGroup(coreStacks.getLogGroup(LogGroupRef.COMPACTION_CUSTOM_TERMINATION))
                .memorySize(512)
                .timeout(Duration.seconds(10)));

        coreStacks.grantReadInstanceConfig(handler);
        // Grant this function permission to query ECS for the number of tasks.
        PolicyStatement policyStatement = PolicyStatement.Builder
                .create()
                .resources(Collections.singletonList("*"))
                .actions(Arrays.asList("ecs:DescribeContainerInstances", "ecs:ListContainerInstances"))
                .build();
        IRole role = Objects.requireNonNull(handler.getRole());
        role.addToPrincipalPolicy(policyStatement);

        return handler;
    }

    private static InstanceType lookupEC2InstanceType(String ec2InstanceType) {
        Objects.requireNonNull(ec2InstanceType, "instance type cannot be null");
        int pos = ec2InstanceType.indexOf('.');

        if (ec2InstanceType.trim().isEmpty() || pos < 0 || (pos + 1) >= ec2InstanceType.length()) {
            throw new IllegalArgumentException("instance type is empty or invalid");
        }

        String family = ec2InstanceType.substring(0, pos).toUpperCase(Locale.getDefault());
        String size = ec2InstanceType.substring(pos + 1).toUpperCase(Locale.getDefault());

        // Since Java identifiers can't start with a number, sizes like "2xlarge"
        // become "xlarge2" in the enum namespace.
        String normalisedSize = Utils.normaliseSize(size);

        // Now perform lookup of these against known types
        InstanceClass instanceClass = InstanceClass.valueOf(family);
        InstanceSize instanceSize = InstanceSize.valueOf(normalisedSize);

        return InstanceType.of(instanceClass, instanceSize);
    }

    private static List<ISecurityGroup> addSecurityGroupReferences(Construct scope, InstanceProperties instanceProperties) {
        AtomicInteger index = new AtomicInteger(1);
        return instanceProperties.getList(ECS_SECURITY_GROUPS).stream()
                .filter(Predicate.not(String::isBlank))
                .map(groupId -> SecurityGroup.fromLookupById(scope, "CompactionScalingSG" + index.getAndIncrement(), groupId))
                .collect(Collectors.toList());
    }

}
