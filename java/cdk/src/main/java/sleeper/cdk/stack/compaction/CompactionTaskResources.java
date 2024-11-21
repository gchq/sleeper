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
package sleeper.cdk.stack.compaction;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import software.amazon.awscdk.CfnOutput;
import software.amazon.awscdk.CfnOutputProps;
import software.amazon.awscdk.Duration;
import software.amazon.awscdk.Stack;
import software.amazon.awscdk.services.autoscaling.AutoScalingGroup;
import software.amazon.awscdk.services.autoscaling.TerminationPolicy;
import software.amazon.awscdk.services.cloudwatch.IMetric;
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
import software.amazon.awscdk.services.ec2.Vpc;
import software.amazon.awscdk.services.ec2.VpcLookupOptions;
import software.amazon.awscdk.services.ecr.IRepository;
import software.amazon.awscdk.services.ecr.Repository;
import software.amazon.awscdk.services.ecs.AddAutoScalingGroupCapacityOptions;
import software.amazon.awscdk.services.ecs.AmiHardwareType;
import software.amazon.awscdk.services.ecs.AsgCapacityProvider;
import software.amazon.awscdk.services.ecs.Cluster;
import software.amazon.awscdk.services.ecs.ContainerDefinitionOptions;
import software.amazon.awscdk.services.ecs.ContainerImage;
import software.amazon.awscdk.services.ecs.CpuArchitecture;
import software.amazon.awscdk.services.ecs.Ec2TaskDefinition;
import software.amazon.awscdk.services.ecs.EcsOptimizedImage;
import software.amazon.awscdk.services.ecs.EcsOptimizedImageOptions;
import software.amazon.awscdk.services.ecs.FargateTaskDefinition;
import software.amazon.awscdk.services.ecs.ITaskDefinition;
import software.amazon.awscdk.services.ecs.MachineImageType;
import software.amazon.awscdk.services.ecs.NetworkMode;
import software.amazon.awscdk.services.ecs.OperatingSystemFamily;
import software.amazon.awscdk.services.ecs.RuntimePlatform;
import software.amazon.awscdk.services.events.Rule;
import software.amazon.awscdk.services.events.Schedule;
import software.amazon.awscdk.services.events.targets.LambdaFunction;
import software.amazon.awscdk.services.iam.Effect;
import software.amazon.awscdk.services.iam.IRole;
import software.amazon.awscdk.services.iam.InstanceProfile;
import software.amazon.awscdk.services.iam.ManagedPolicy;
import software.amazon.awscdk.services.iam.PolicyStatement;
import software.amazon.awscdk.services.lambda.CfnPermission;
import software.amazon.awscdk.services.lambda.IFunction;
import software.amazon.awscdk.services.s3.IBucket;
import software.amazon.awscdk.services.sns.Topic;
import software.amazon.awscdk.services.sqs.Queue;
import software.constructs.Construct;
import software.constructs.IDependable;

import sleeper.cdk.jars.LambdaCode;
import sleeper.cdk.stack.core.CoreStacks;
import sleeper.cdk.util.Utils;
import sleeper.configuration.CompactionTaskRequirements;
import sleeper.core.ContainerConstants;
import sleeper.core.deploy.LambdaHandler;
import sleeper.core.deploy.SleeperScheduleRule;
import sleeper.core.properties.instance.InstanceProperties;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static sleeper.cdk.util.Utils.shouldDeployPaused;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.COMPACTION_AUTO_SCALING_GROUP;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.COMPACTION_CLUSTER;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.COMPACTION_TASK_CREATION_CLOUDWATCH_RULE;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.COMPACTION_TASK_CREATION_LAMBDA_FUNCTION;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.COMPACTION_TASK_EC2_DEFINITION_FAMILY;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.COMPACTION_TASK_FARGATE_DEFINITION_FAMILY;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.VERSION;
import static sleeper.core.properties.instance.CommonProperty.ACCOUNT;
import static sleeper.core.properties.instance.CommonProperty.ECS_SECURITY_GROUPS;
import static sleeper.core.properties.instance.CommonProperty.REGION;
import static sleeper.core.properties.instance.CommonProperty.TASK_RUNNER_LAMBDA_MEMORY_IN_MB;
import static sleeper.core.properties.instance.CommonProperty.TASK_RUNNER_LAMBDA_TIMEOUT_IN_SECONDS;
import static sleeper.core.properties.instance.CommonProperty.VPC_ID;
import static sleeper.core.properties.instance.CompactionProperty.COMPACTION_EC2_POOL_DESIRED;
import static sleeper.core.properties.instance.CompactionProperty.COMPACTION_EC2_POOL_MAXIMUM;
import static sleeper.core.properties.instance.CompactionProperty.COMPACTION_EC2_POOL_MINIMUM;
import static sleeper.core.properties.instance.CompactionProperty.COMPACTION_EC2_ROOT_SIZE;
import static sleeper.core.properties.instance.CompactionProperty.COMPACTION_EC2_TYPE;
import static sleeper.core.properties.instance.CompactionProperty.COMPACTION_ECS_LAUNCHTYPE;
import static sleeper.core.properties.instance.CompactionProperty.COMPACTION_TASK_CPU_ARCHITECTURE;
import static sleeper.core.properties.instance.CompactionProperty.COMPACTION_TASK_CREATION_PERIOD_IN_MINUTES;
import static sleeper.core.properties.instance.CompactionProperty.ECR_COMPACTION_REPO;

public class CompactionTaskResources {

    private static final String COMPACTION_CLUSTER_NAME = "CompactionClusterName";
    private final InstanceProperties instanceProperties;
    private final Stack stack;

    public CompactionTaskResources(Stack stack,
            InstanceProperties instanceProperties,
            LambdaCode lambdaCode,
            IBucket jarsBucket,
            Queue compactionJobsQueue,
            Topic topic,
            CoreStacks coreStacks,
            List<IMetric> errorMetrics) {

        this.instanceProperties = instanceProperties;
        this.stack = stack;

        ecsClusterForCompactionTasks(coreStacks, jarsBucket, lambdaCode, compactionJobsQueue);
        lambdaToCreateCompactionTasks(coreStacks, lambdaCode, compactionJobsQueue);

        // Allow running compaction tasks
        coreStacks.getInvokeCompactionPolicyForGrants().addStatements(PolicyStatement.Builder.create()
                .effect(Effect.ALLOW)
                .actions(List.of("ecs:DescribeClusters", "ecs:RunTask", "iam:PassRole",
                        "ecs:DescribeContainerInstances", "ecs:DescribeTasks", "ecs:ListContainerInstances",
                        "autoscaling:SetDesiredCapacity", "autoscaling:DescribeAutoScalingGroups"))
                .resources(List.of("*"))
                .build());

    }

    private void ecsClusterForCompactionTasks(CoreStacks coreStacks, IBucket jarsBucket, LambdaCode taskCreatorJar, Queue compactionJobsQueue) {
        VpcLookupOptions vpcLookupOptions = VpcLookupOptions.builder()
                .vpcId(instanceProperties.get(VPC_ID))
                .build();
        IVpc vpc = Vpc.fromLookup(stack, "VPC1", vpcLookupOptions);
        String clusterName = String.join("-", "sleeper",
                Utils.cleanInstanceId(instanceProperties), "compaction-cluster");
        Cluster cluster = Cluster.Builder
                .create(stack, "CompactionCluster")
                .clusterName(clusterName)
                .containerInsights(Boolean.TRUE)
                .vpc(vpc)
                .build();
        instanceProperties.set(COMPACTION_CLUSTER, cluster.getClusterName());

        IRepository repository = Repository.fromRepositoryName(stack, "ECR1",
                instanceProperties.get(ECR_COMPACTION_REPO));
        ContainerImage containerImage = ContainerImage.fromEcrRepository(repository, instanceProperties.get(VERSION));

        Map<String, String> environmentVariables = Utils.createDefaultEnvironment(instanceProperties);
        environmentVariables.put(Utils.AWS_REGION, instanceProperties.get(REGION));

        String launchType = instanceProperties.get(COMPACTION_ECS_LAUNCHTYPE);
        ITaskDefinition taskDefinition;
        if (launchType.equalsIgnoreCase("FARGATE")) {
            FargateTaskDefinition fargateTaskDefinition = compactionFargateTaskDefinition();
            String fargateTaskDefinitionFamily = fargateTaskDefinition.getFamily();
            instanceProperties.set(COMPACTION_TASK_FARGATE_DEFINITION_FAMILY, fargateTaskDefinitionFamily);
            ContainerDefinitionOptions fargateContainerDefinitionOptions = createFargateContainerDefinition(
                    coreStacks, containerImage, environmentVariables, instanceProperties);
            fargateTaskDefinition.addContainer(ContainerConstants.COMPACTION_CONTAINER_NAME,
                    fargateContainerDefinitionOptions);
            taskDefinition = fargateTaskDefinition;
        } else {
            Ec2TaskDefinition ec2TaskDefinition = compactionEC2TaskDefinition();
            String ec2TaskDefinitionFamily = ec2TaskDefinition.getFamily();
            instanceProperties.set(COMPACTION_TASK_EC2_DEFINITION_FAMILY, ec2TaskDefinitionFamily);
            ContainerDefinitionOptions ec2ContainerDefinitionOptions = createEC2ContainerDefinition(
                    coreStacks, containerImage, environmentVariables, instanceProperties);
            ec2TaskDefinition.addContainer(ContainerConstants.COMPACTION_CONTAINER_NAME, ec2ContainerDefinitionOptions);
            addEC2CapacityProvider(cluster, vpc, coreStacks, taskCreatorJar);
            taskDefinition = ec2TaskDefinition;
        }
        coreStacks.grantRunCompactionJobs(taskDefinition.getTaskRole());
        jarsBucket.grantRead(taskDefinition.getTaskRole());

        taskDefinition.getTaskRole().addToPrincipalPolicy(PolicyStatement.Builder
                .create()
                .resources(Collections.singletonList("*"))
                .actions(List.of("ecs:DescribeContainerInstances"))
                .build());

        compactionJobsQueue.grantConsumeMessages(taskDefinition.getTaskRole());

        CfnOutputProps compactionClusterProps = new CfnOutputProps.Builder()
                .value(cluster.getClusterName())
                .build();
        new CfnOutput(stack, COMPACTION_CLUSTER_NAME, compactionClusterProps);
    }

    private FargateTaskDefinition compactionFargateTaskDefinition() {
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

    private Ec2TaskDefinition compactionEC2TaskDefinition() {
        return Ec2TaskDefinition.Builder
                .create(stack, "CompactionEC2TaskDefinition")
                .family(String.join("-", Utils.cleanInstanceId(instanceProperties), "CompactionTaskOnEC2"))
                .networkMode(NetworkMode.BRIDGE)
                .build();
    }

    private ContainerDefinitionOptions createFargateContainerDefinition(
            CoreStacks coreStacks, ContainerImage image, Map<String, String> environment, InstanceProperties instanceProperties) {
        String architecture = instanceProperties.get(COMPACTION_TASK_CPU_ARCHITECTURE).toUpperCase(Locale.ROOT);
        CompactionTaskRequirements requirements = CompactionTaskRequirements.getArchRequirements(architecture, instanceProperties);
        return ContainerDefinitionOptions.builder()
                .image(image)
                .environment(environment)
                .cpu(requirements.getCpu())
                .memoryLimitMiB(requirements.getMemoryLimitMiB())
                .logging(Utils.createECSContainerLogDriver(coreStacks, "FargateCompactionTasks"))
                .build();
    }

    private ContainerDefinitionOptions createEC2ContainerDefinition(
            CoreStacks coreStacks, ContainerImage image, Map<String, String> environment, InstanceProperties instanceProperties) {
        String architecture = instanceProperties.get(COMPACTION_TASK_CPU_ARCHITECTURE).toUpperCase(Locale.ROOT);
        CompactionTaskRequirements requirements = CompactionTaskRequirements.getArchRequirements(architecture, instanceProperties);
        return ContainerDefinitionOptions.builder()
                .image(image)
                .environment(environment)
                .cpu(requirements.getCpu())
                // bit hacky: Reduce memory requirement for EC2 to prevent
                // container allocation failing when we need almost entire resources
                // of machine
                .memoryLimitMiB((int) (requirements.getMemoryLimitMiB() * 0.95))
                .logging(Utils.createECSContainerLogDriver(coreStacks, "EC2CompactionTasks"))
                .build();
    }

    @SuppressFBWarnings("NP_NULL_ON_SOME_PATH_FROM_RETURN_VALUE")
    private void addEC2CapacityProvider(
            Cluster cluster, IVpc vpc, CoreStacks coreStacks, LambdaCode taskCreatorJar) {

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
                .logGroup(coreStacks.getLogGroupByFunctionName(functionName))
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

    @SuppressFBWarnings("NP_NULL_ON_SOME_PATH_FROM_RETURN_VALUE")
    private void lambdaToCreateCompactionTasks(
            CoreStacks coreStacks, LambdaCode lambdaCode, Queue compactionJobsQueue) {
        String functionName = String.join("-", "sleeper",
                Utils.cleanInstanceId(instanceProperties), "compaction-tasks-creator");

        IFunction handler = lambdaCode.buildFunction(stack, LambdaHandler.COMPACTION_TASK_CREATOR, "CompactionTasksCreator", builder -> builder
                .functionName(functionName)
                .description("If there are compaction jobs on queue create tasks to run them")
                .memorySize(instanceProperties.getInt(TASK_RUNNER_LAMBDA_MEMORY_IN_MB))
                .timeout(Duration.seconds(instanceProperties.getInt(TASK_RUNNER_LAMBDA_TIMEOUT_IN_SECONDS)))
                .environment(Utils.createDefaultEnvironment(instanceProperties))
                .reservedConcurrentExecutions(1)
                .logGroup(coreStacks.getLogGroupByFunctionName(functionName)));

        // Grant this function permission to read from the S3 bucket
        coreStacks.grantReadInstanceConfig(handler);

        // Grant this function permission to query the queue for number of messages
        compactionJobsQueue.grant(handler, "sqs:GetQueueAttributes");
        compactionJobsQueue.grantSendMessages(handler);
        compactionJobsQueue.grantSendMessages(coreStacks.getInvokeCompactionPolicyForGrants());
        Utils.grantInvokeOnPolicy(handler, coreStacks.getInvokeCompactionPolicyForGrants());
        coreStacks.grantInvokeScheduled(handler);

        // Grant this function permission to query ECS for the number of tasks, etc
        PolicyStatement policyStatement = PolicyStatement.Builder
                .create()
                .resources(Collections.singletonList("*"))
                .actions(Arrays.asList("ecs:DescribeClusters", "ecs:RunTask", "iam:PassRole",
                        "ecs:DescribeContainerInstances", "ecs:DescribeTasks", "ecs:ListContainerInstances",
                        "autoscaling:SetDesiredCapacity", "autoscaling:DescribeAutoScalingGroups"))
                .build();
        IRole role = Objects.requireNonNull(handler.getRole());
        role.addToPrincipalPolicy(policyStatement);
        role.addManagedPolicy(ManagedPolicy.fromAwsManagedPolicyName("service-role/AmazonECSTaskExecutionRolePolicy"));

        // Cloudwatch rule to trigger this lambda
        Rule rule = Rule.Builder
                .create(stack, "CompactionTasksCreationPeriodicTrigger")
                .ruleName(SleeperScheduleRule.COMPACTION_TASK_CREATION.buildRuleName(instanceProperties))
                .description("A rule to periodically trigger the compaction task creation lambda")
                .enabled(!shouldDeployPaused(stack))
                .schedule(Schedule.rate(Duration.minutes(instanceProperties.getInt(COMPACTION_TASK_CREATION_PERIOD_IN_MINUTES))))
                .targets(Collections.singletonList(new LambdaFunction(handler)))
                .build();
        instanceProperties.set(COMPACTION_TASK_CREATION_LAMBDA_FUNCTION, handler.getFunctionName());
        instanceProperties.set(COMPACTION_TASK_CREATION_CLOUDWATCH_RULE, rule.getRuleName());
    }
}
