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
package sleeper.cdk.stack;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import software.amazon.awscdk.CfnOutput;
import software.amazon.awscdk.CfnOutputProps;
import software.amazon.awscdk.Duration;
import software.amazon.awscdk.NestedStack;
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
import software.amazon.awscdk.services.lambda.eventsources.SqsEventSource;
import software.amazon.awscdk.services.s3.Bucket;
import software.amazon.awscdk.services.s3.IBucket;
import software.amazon.awscdk.services.sns.Topic;
import software.amazon.awscdk.services.sqs.DeadLetterQueue;
import software.amazon.awscdk.services.sqs.Queue;
import software.constructs.Construct;
import software.constructs.IDependable;

import sleeper.cdk.jars.BuiltJars;
import sleeper.cdk.jars.LambdaCode;
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
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static sleeper.cdk.util.Utils.createAlarmForDlq;
import static sleeper.cdk.util.Utils.shouldDeployPaused;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.COMPACTION_AUTO_SCALING_GROUP;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.COMPACTION_CLUSTER;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.COMPACTION_JOB_CREATION_CLOUDWATCH_RULE;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.COMPACTION_JOB_CREATION_DLQ_ARN;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.COMPACTION_JOB_CREATION_DLQ_URL;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.COMPACTION_JOB_CREATION_QUEUE_ARN;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.COMPACTION_JOB_CREATION_QUEUE_URL;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.COMPACTION_JOB_CREATION_TRIGGER_LAMBDA_FUNCTION;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.COMPACTION_JOB_DLQ_ARN;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.COMPACTION_JOB_DLQ_URL;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.COMPACTION_JOB_QUEUE_ARN;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.COMPACTION_JOB_QUEUE_URL;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.COMPACTION_TASK_CREATION_CLOUDWATCH_RULE;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.COMPACTION_TASK_CREATION_LAMBDA_FUNCTION;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.COMPACTION_TASK_EC2_DEFINITION_FAMILY;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.COMPACTION_TASK_FARGATE_DEFINITION_FAMILY;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.VERSION;
import static sleeper.core.properties.instance.CommonProperty.ACCOUNT;
import static sleeper.core.properties.instance.CommonProperty.ECS_SECURITY_GROUPS;
import static sleeper.core.properties.instance.CommonProperty.REGION;
import static sleeper.core.properties.instance.CommonProperty.TABLE_BATCHING_LAMBDAS_MEMORY_IN_MB;
import static sleeper.core.properties.instance.CommonProperty.TABLE_BATCHING_LAMBDAS_TIMEOUT_IN_SECONDS;
import static sleeper.core.properties.instance.CommonProperty.TASK_RUNNER_LAMBDA_MEMORY_IN_MB;
import static sleeper.core.properties.instance.CommonProperty.TASK_RUNNER_LAMBDA_TIMEOUT_IN_SECONDS;
import static sleeper.core.properties.instance.CommonProperty.VPC_ID;
import static sleeper.core.properties.instance.CompactionProperty.COMPACTION_EC2_POOL_DESIRED;
import static sleeper.core.properties.instance.CompactionProperty.COMPACTION_EC2_POOL_MAXIMUM;
import static sleeper.core.properties.instance.CompactionProperty.COMPACTION_EC2_POOL_MINIMUM;
import static sleeper.core.properties.instance.CompactionProperty.COMPACTION_EC2_ROOT_SIZE;
import static sleeper.core.properties.instance.CompactionProperty.COMPACTION_EC2_TYPE;
import static sleeper.core.properties.instance.CompactionProperty.COMPACTION_ECS_LAUNCHTYPE;
import static sleeper.core.properties.instance.CompactionProperty.COMPACTION_JOB_CREATION_BATCH_SIZE;
import static sleeper.core.properties.instance.CompactionProperty.COMPACTION_JOB_CREATION_LAMBDA_CONCURRENCY_MAXIMUM;
import static sleeper.core.properties.instance.CompactionProperty.COMPACTION_JOB_CREATION_LAMBDA_CONCURRENCY_RESERVED;
import static sleeper.core.properties.instance.CompactionProperty.COMPACTION_JOB_CREATION_LAMBDA_MEMORY_IN_MB;
import static sleeper.core.properties.instance.CompactionProperty.COMPACTION_JOB_CREATION_LAMBDA_PERIOD_IN_MINUTES;
import static sleeper.core.properties.instance.CompactionProperty.COMPACTION_JOB_CREATION_LAMBDA_TIMEOUT_IN_SECONDS;
import static sleeper.core.properties.instance.CompactionProperty.COMPACTION_JOB_MAX_RETRIES;
import static sleeper.core.properties.instance.CompactionProperty.COMPACTION_QUEUE_VISIBILITY_TIMEOUT_IN_SECONDS;
import static sleeper.core.properties.instance.CompactionProperty.COMPACTION_TASK_CPU_ARCHITECTURE;
import static sleeper.core.properties.instance.CompactionProperty.COMPACTION_TASK_CREATION_PERIOD_IN_MINUTES;
import static sleeper.core.properties.instance.CompactionProperty.ECR_COMPACTION_REPO;

/**
 * Deploys the resources needed to perform compaction jobs. Specifically, there is:
 * <p>
 * - A lambda, that is periodically triggered by a CloudWatch rule, to query the state store for
 * information about active files with no job id, to create compaction job definitions as
 * appropriate and post them to a queue.
 * - An ECS {@link Cluster} and either a {@link FargateTaskDefinition} or a {@link Ec2TaskDefinition}
 * for tasks that will perform compaction jobs.
 * - A lambda, that is periodically triggered by a CloudWatch rule, to look at the
 * size of the queue and the number of running tasks and create more tasks if necessary.
 */
public class CompactionStack extends NestedStack {
    public static final String COMPACTION_STACK_QUEUE_URL = "CompactionStackQueueUrlKey";
    public static final String COMPACTION_STACK_DLQ_URL = "CompactionStackDLQUrlKey";
    public static final String COMPACTION_CLUSTER_NAME = "CompactionClusterName";

    private Queue compactionJobQ;
    private Queue compactionDLQ;
    private final InstanceProperties instanceProperties;

    public CompactionStack(
            Construct scope,
            String id,
            InstanceProperties instanceProperties,
            BuiltJars jars,
            Topic topic,
            CoreStacks coreStacks,
            List<IMetric> errorMetrics) {
        super(scope, id);
        this.instanceProperties = instanceProperties;
        // The compaction stack consists of the following components:
        // - An SQS queue for the compaction jobs.
        // - A lambda to periodically check for compaction jobs that should be created.
        //   This lambda is fired periodically by a CloudWatch rule. It queries the
        //   StateStore for information about the current partitions and files,
        //   identifies files that should be compacted, creates a job definition
        //   and sends it to an SQS queue.
        // - An ECS cluster, task definition, etc., for compaction jobs.
        // - A lambda that periodically checks the number of running compaction tasks
        //   and if there are not enough (i.e. there is a backlog on the queue
        //   then it creates more tasks).

        // Jars bucket
        IBucket jarsBucket = Bucket.fromBucketName(this, "JarsBucket", jars.bucketName());
        LambdaCode lambdaCode = jars.lambdaCode(jarsBucket);

        // SQS queue for the compaction jobs
        Queue compactionJobsQueue = sqsQueueForCompactionJobs(coreStacks, topic, errorMetrics);

        // Lambda to periodically check for compaction jobs that should be created
        lambdaToCreateCompactionJobsBatchedViaSQS(coreStacks, topic, errorMetrics, jarsBucket, lambdaCode, compactionJobsQueue);

        // ECS cluster for compaction tasks
        ecsClusterForCompactionTasks(coreStacks, jarsBucket, lambdaCode, compactionJobsQueue);

        // Lambda to create compaction tasks
        lambdaToCreateCompactionTasks(coreStacks, lambdaCode, compactionJobsQueue);

        // Allow running compaction tasks
        coreStacks.getInvokeCompactionPolicyForGrants().addStatements(PolicyStatement.Builder.create()
                .effect(Effect.ALLOW)
                .actions(List.of("ecs:DescribeClusters", "ecs:RunTask", "iam:PassRole",
                        "ecs:DescribeContainerInstances", "ecs:DescribeTasks", "ecs:ListContainerInstances",
                        "autoscaling:SetDesiredCapacity", "autoscaling:DescribeAutoScalingGroups", "ec2:DescribeInstanceTypes"))
                .resources(List.of("*"))
                .build());

        Utils.addStackTagIfSet(this, instanceProperties);
    }

    private Queue sqsQueueForCompactionJobs(CoreStacks coreStacks, Topic topic, List<IMetric> errorMetrics) {
        // Create queue for compaction job definitions
        String instanceId = Utils.cleanInstanceId(instanceProperties);
        String dlQueueName = String.join("-", "sleeper", instanceId, "CompactionJobDLQ");
        compactionDLQ = Queue.Builder
                .create(this, "CompactionJobDefinitionsDeadLetterQueue")
                .queueName(dlQueueName)
                .build();
        DeadLetterQueue compactionJobDefinitionsDeadLetterQueue = DeadLetterQueue.builder()
                .maxReceiveCount(instanceProperties.getInt(COMPACTION_JOB_MAX_RETRIES))
                .queue(compactionDLQ)
                .build();
        String queueName = String.join("-", "sleeper", instanceId, "CompactionJobQ");
        compactionJobQ = Queue.Builder
                .create(this, "CompactionJobDefinitionsQueue")
                .queueName(queueName)
                .deadLetterQueue(compactionJobDefinitionsDeadLetterQueue)
                .visibilityTimeout(
                        Duration.seconds(instanceProperties.getInt(COMPACTION_QUEUE_VISIBILITY_TIMEOUT_IN_SECONDS)))
                .build();
        instanceProperties.set(COMPACTION_JOB_QUEUE_URL, compactionJobQ.getQueueUrl());
        instanceProperties.set(COMPACTION_JOB_QUEUE_ARN, compactionJobQ.getQueueArn());
        instanceProperties.set(COMPACTION_JOB_DLQ_URL,
                compactionJobDefinitionsDeadLetterQueue.getQueue().getQueueUrl());
        instanceProperties.set(COMPACTION_JOB_DLQ_ARN,
                compactionJobDefinitionsDeadLetterQueue.getQueue().getQueueArn());

        // Add alarm to send message to SNS if there are any messages on the dead letter queue
        createAlarmForDlq(this, "CompactionAlarm",
                "Alarms if there are any messages on the dead letter queue for the compactions queue",
                compactionDLQ, topic);
        errorMetrics.add(Utils.createErrorMetric("Compaction Errors", compactionDLQ, instanceProperties));
        compactionJobQ.grantPurge(coreStacks.getPurgeQueuesPolicyForGrants());

        CfnOutputProps compactionJobDefinitionsQueueProps = new CfnOutputProps.Builder()
                .value(compactionJobQ.getQueueUrl())
                .build();
        new CfnOutput(this, COMPACTION_STACK_QUEUE_URL, compactionJobDefinitionsQueueProps);
        CfnOutputProps compactionJobDefinitionsDLQueueProps = new CfnOutputProps.Builder()
                .value(compactionJobDefinitionsDeadLetterQueue.getQueue().getQueueUrl())
                .build();
        new CfnOutput(this, COMPACTION_STACK_DLQ_URL, compactionJobDefinitionsDLQueueProps);

        return compactionJobQ;
    }

    private void lambdaToCreateCompactionJobsBatchedViaSQS(
            CoreStacks coreStacks, Topic topic, List<IMetric> errorMetrics,
            IBucket jarsBucket, LambdaCode lambdaCode, Queue compactionJobsQueue) {

        // Function to create compaction jobs
        Map<String, String> environmentVariables = Utils.createDefaultEnvironment(instanceProperties);

        String instanceId = Utils.cleanInstanceId(instanceProperties);
        String triggerFunctionName = String.join("-", "sleeper", instanceId, "compaction-job-creation-trigger");
        String functionName = String.join("-", "sleeper", instanceId, "compaction-job-creation-handler");

        IFunction triggerFunction = lambdaCode.buildFunction(this, LambdaHandler.COMPACTION_JOB_CREATOR_TRIGGER, "CompactionJobsCreationTrigger", builder -> builder
                .functionName(triggerFunctionName)
                .description("Create batches of tables and send requests to create compaction jobs for those batches")
                .memorySize(instanceProperties.getInt(TABLE_BATCHING_LAMBDAS_MEMORY_IN_MB))
                .timeout(Duration.seconds(instanceProperties.getInt(TABLE_BATCHING_LAMBDAS_TIMEOUT_IN_SECONDS)))
                .environment(environmentVariables)
                .reservedConcurrentExecutions(1)
                .logGroup(coreStacks.getLogGroupByFunctionName(triggerFunctionName)));

        IFunction handlerFunction = lambdaCode.buildFunction(this, LambdaHandler.COMPACTION_JOB_CREATOR, "CompactionJobsCreationHandler", builder -> builder
                .functionName(functionName)
                .description("Scan the state stores of the provided tables looking for compaction jobs to create")
                .memorySize(instanceProperties.getInt(COMPACTION_JOB_CREATION_LAMBDA_MEMORY_IN_MB))
                .timeout(Duration.seconds(instanceProperties.getInt(COMPACTION_JOB_CREATION_LAMBDA_TIMEOUT_IN_SECONDS)))
                .environment(environmentVariables)
                .reservedConcurrentExecutions(instanceProperties.getInt(COMPACTION_JOB_CREATION_LAMBDA_CONCURRENCY_RESERVED))
                .logGroup(coreStacks.getLogGroupByFunctionName(functionName)));

        // Send messages from the trigger function to the handler function
        Queue jobCreationQueue = sqsQueueForCompactionJobCreation(coreStacks, topic, errorMetrics);
        handlerFunction.addEventSource(SqsEventSource.Builder.create(jobCreationQueue)
                .batchSize(instanceProperties.getInt(COMPACTION_JOB_CREATION_BATCH_SIZE))
                .maxConcurrency(instanceProperties.getInt(COMPACTION_JOB_CREATION_LAMBDA_CONCURRENCY_MAXIMUM))
                .build());

        // Grant permissions
        // - Read through tables in trigger, send batches
        // - Read/write for creating compaction jobs, access to jars bucket for compaction strategies
        jobCreationQueue.grantSendMessages(triggerFunction);
        coreStacks.grantReadTablesStatus(triggerFunction);
        coreStacks.grantCreateCompactionJobs(handlerFunction);
        jarsBucket.grantRead(handlerFunction);
        compactionJobsQueue.grantSendMessages(handlerFunction);
        coreStacks.grantInvokeScheduled(triggerFunction, jobCreationQueue);
        coreStacks.grantCreateCompactionJobs(coreStacks.getInvokeCompactionPolicyForGrants());
        compactionJobsQueue.grantSendMessages(coreStacks.getInvokeCompactionPolicyForGrants());

        // Cloudwatch rule to trigger this lambda
        Rule rule = Rule.Builder
                .create(this, "CompactionJobCreationPeriodicTrigger")
                .ruleName(SleeperScheduleRule.COMPACTION_JOB_CREATION.buildRuleName(instanceProperties))
                .description("A rule to periodically trigger the compaction job creation lambda")
                .enabled(!shouldDeployPaused(this))
                .schedule(Schedule.rate(Duration.minutes(instanceProperties.getInt(COMPACTION_JOB_CREATION_LAMBDA_PERIOD_IN_MINUTES))))
                .targets(List.of(new LambdaFunction(triggerFunction)))
                .build();
        instanceProperties.set(COMPACTION_JOB_CREATION_TRIGGER_LAMBDA_FUNCTION, triggerFunction.getFunctionName());
        instanceProperties.set(COMPACTION_JOB_CREATION_CLOUDWATCH_RULE, rule.getRuleName());
    }

    private Queue sqsQueueForCompactionJobCreation(CoreStacks coreStacks, Topic topic, List<IMetric> errorMetrics) {
        // Create queue for compaction job creation invocation
        String instanceId = Utils.cleanInstanceId(instanceProperties);
        Queue deadLetterQueue = Queue.Builder
                .create(this, "CompactionJobCreationDLQ")
                .queueName(String.join("-", "sleeper", instanceId, "CompactionJobCreationDLQ.fifo"))
                .fifo(true)
                .build();
        Queue queue = Queue.Builder
                .create(this, "CompactionJobCreationQueue")
                .queueName(String.join("-", "sleeper", instanceId, "CompactionJobCreationQ.fifo"))
                .deadLetterQueue(DeadLetterQueue.builder()
                        .maxReceiveCount(1)
                        .queue(deadLetterQueue)
                        .build())
                .fifo(true)
                .visibilityTimeout(
                        Duration.seconds(instanceProperties.getInt(COMPACTION_JOB_CREATION_LAMBDA_TIMEOUT_IN_SECONDS)))
                .build();
        instanceProperties.set(COMPACTION_JOB_CREATION_QUEUE_URL, queue.getQueueUrl());
        instanceProperties.set(COMPACTION_JOB_CREATION_QUEUE_ARN, queue.getQueueArn());
        instanceProperties.set(COMPACTION_JOB_CREATION_DLQ_URL, deadLetterQueue.getQueueUrl());
        instanceProperties.set(COMPACTION_JOB_CREATION_DLQ_ARN, deadLetterQueue.getQueueArn());

        createAlarmForDlq(this, "CompactionJobCreationBatchAlarm",
                "Alarms if there are any messages on the dead letter queue for compaction job creation",
                deadLetterQueue, topic);
        errorMetrics.add(Utils.createErrorMetric("Compaction Batching Errors", deadLetterQueue, instanceProperties));
        queue.grantPurge(coreStacks.getPurgeQueuesPolicyForGrants());
        return queue;
    }

    private void ecsClusterForCompactionTasks(
            CoreStacks coreStacks, IBucket jarsBucket, LambdaCode taskCreatorJar, Queue compactionJobsQueue) {
        VpcLookupOptions vpcLookupOptions = VpcLookupOptions.builder()
                .vpcId(instanceProperties.get(VPC_ID))
                .build();
        IVpc vpc = Vpc.fromLookup(this, "VPC1", vpcLookupOptions);
        String clusterName = String.join("-", "sleeper",
                Utils.cleanInstanceId(instanceProperties), "compaction-cluster");
        Cluster cluster = Cluster.Builder
                .create(this, "CompactionCluster")
                .clusterName(clusterName)
                .containerInsights(Boolean.TRUE)
                .vpc(vpc)
                .build();
        instanceProperties.set(COMPACTION_CLUSTER, cluster.getClusterName());

        IRepository repository = Repository.fromRepositoryName(this, "ECR1",
                instanceProperties.get(ECR_COMPACTION_REPO));
        ContainerImage containerImage = ContainerImage.fromEcrRepository(repository, instanceProperties.get(VERSION));

        Map<String, String> environmentVariables = Utils.createDefaultEnvironment(instanceProperties);
        environmentVariables.put(Utils.AWS_REGION, instanceProperties.get(REGION));

        Consumer<ITaskDefinition> grantPermissions = taskDef -> {
            coreStacks.grantRunCompactionJobs(taskDef.getTaskRole());
            jarsBucket.grantRead(taskDef.getTaskRole());

            taskDef.getTaskRole().addToPrincipalPolicy(PolicyStatement.Builder
                    .create()
                    .resources(Collections.singletonList("*"))
                    .actions(List.of("ecs:DescribeContainerInstances"))
                    .build());

            compactionJobsQueue.grantConsumeMessages(taskDef.getTaskRole());
        };

        String launchType = instanceProperties.get(COMPACTION_ECS_LAUNCHTYPE);
        if (launchType.equalsIgnoreCase("FARGATE")) {
            FargateTaskDefinition fargateTaskDefinition = compactionFargateTaskDefinition();
            String fargateTaskDefinitionFamily = fargateTaskDefinition.getFamily();
            instanceProperties.set(COMPACTION_TASK_FARGATE_DEFINITION_FAMILY, fargateTaskDefinitionFamily);
            ContainerDefinitionOptions fargateContainerDefinitionOptions = createFargateContainerDefinition(
                    coreStacks, containerImage, environmentVariables, instanceProperties);
            fargateTaskDefinition.addContainer(ContainerConstants.COMPACTION_CONTAINER_NAME,
                    fargateContainerDefinitionOptions);
            grantPermissions.accept(fargateTaskDefinition);
        } else {
            Ec2TaskDefinition ec2TaskDefinition = compactionEC2TaskDefinition();
            String ec2TaskDefinitionFamily = ec2TaskDefinition.getFamily();
            instanceProperties.set(COMPACTION_TASK_EC2_DEFINITION_FAMILY, ec2TaskDefinitionFamily);
            ContainerDefinitionOptions ec2ContainerDefinitionOptions = createEC2ContainerDefinition(
                    coreStacks, containerImage, environmentVariables, instanceProperties);
            ec2TaskDefinition.addContainer(ContainerConstants.COMPACTION_CONTAINER_NAME, ec2ContainerDefinitionOptions);
            grantPermissions.accept(ec2TaskDefinition);
            addEC2CapacityProvider(cluster, vpc, coreStacks, taskCreatorJar);
        }

        CfnOutputProps compactionClusterProps = new CfnOutputProps.Builder()
                .value(cluster.getClusterName())
                .build();
        new CfnOutput(this, COMPACTION_CLUSTER_NAME, compactionClusterProps);
    }

    @SuppressFBWarnings("NP_NULL_ON_SOME_PATH_FROM_RETURN_VALUE")
    private void addEC2CapacityProvider(
            Cluster cluster, IVpc vpc, CoreStacks coreStacks, LambdaCode taskCreatorJar) {

        // Create some extra user data to enable ECS container metadata file
        UserData customUserData = UserData.forLinux();
        customUserData.addCommands("echo ECS_ENABLE_CONTAINER_METADATA=true >> /etc/ecs/ecs.config");

        IFunction customTermination = lambdaForCustomTerminationPolicy(coreStacks, taskCreatorJar);

        IDependable autoScalingPermission = CfnPermission.Builder.create(this, "AutoscalingCall")
                .action("lambda:InvokeFunction")
                .principal("arn:aws:iam::" + instanceProperties.get(ACCOUNT)
                        + ":role/aws-service-role/autoscaling.amazonaws.com/AWSServiceRoleForAutoScaling")
                .functionName(customTermination.getFunctionArn())
                .build();

        SecurityGroup scalingSecurityGroup = SecurityGroup.Builder.create(this, "CompactionScalingDefaultSG")
                .vpc(vpc)
                .allowAllOutbound(true)
                .build();

        InstanceProfile roleProfile = InstanceProfile.Builder.create(this, "CompactionScalingInstanceProfile")
                .build();

        LaunchTemplate scalingLaunchTemplate = LaunchTemplate.Builder.create(this, "CompactionScalingTemplate")
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
        addSecurityGroupReferences(this, instanceProperties)
                .forEach(scalingLaunchTemplate::addSecurityGroup);
        AutoScalingGroup ec2scalingGroup = AutoScalingGroup.Builder.create(this, "CompactionScalingGroup")
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
                .create(this, "CompactionCapacityProvider")
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

    private static List<ISecurityGroup> addSecurityGroupReferences(Construct scope, InstanceProperties instanceProperties) {
        AtomicInteger index = new AtomicInteger(1);
        return instanceProperties.getList(ECS_SECURITY_GROUPS).stream()
                .filter(Predicate.not(String::isBlank))
                .map(groupId -> SecurityGroup.fromLookupById(scope, "CompactionScalingSG" + index.getAndIncrement(), groupId))
                .collect(Collectors.toList());
    }

    public static InstanceType lookupEC2InstanceType(String ec2InstanceType) {
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

    private FargateTaskDefinition compactionFargateTaskDefinition() {
        String architecture = instanceProperties.get(COMPACTION_TASK_CPU_ARCHITECTURE).toUpperCase(Locale.ROOT);
        CompactionTaskRequirements requirements = CompactionTaskRequirements.getArchRequirements(architecture, instanceProperties);
        return FargateTaskDefinition.Builder
                .create(this, "CompactionFargateTaskDefinition")
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
                .create(this, "CompactionEC2TaskDefinition")
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
                .memoryLimitMiB(requirements.getMemoryLimitMiB())
                .logging(Utils.createECSContainerLogDriver(coreStacks, "EC2CompactionTasks"))
                .build();
    }

    @SuppressFBWarnings("NP_NULL_ON_SOME_PATH_FROM_RETURN_VALUE")
    private IFunction lambdaForCustomTerminationPolicy(CoreStacks coreStacks, LambdaCode lambdaCode) {

        // Run tasks function
        Map<String, String> environmentVariables = Utils.createDefaultEnvironment(instanceProperties);

        String functionName = String.join("-", "sleeper",
                Utils.cleanInstanceId(instanceProperties), "compaction-custom-termination");

        IFunction handler = lambdaCode.buildFunction(this, LambdaHandler.COMPACTION_TASK_TERMINATOR, "CompactionTerminator", builder -> builder
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

    @SuppressFBWarnings("NP_NULL_ON_SOME_PATH_FROM_RETURN_VALUE")
    private void lambdaToCreateCompactionTasks(
            CoreStacks coreStacks, LambdaCode lambdaCode, Queue compactionJobsQueue) {
        String functionName = String.join("-", "sleeper",
                Utils.cleanInstanceId(instanceProperties), "compaction-tasks-creator");

        IFunction handler = lambdaCode.buildFunction(this, LambdaHandler.COMPACTION_TASK_CREATOR, "CompactionTasksCreator", builder -> builder
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
                .create(this, "CompactionTasksCreationPeriodicTrigger")
                .ruleName(SleeperScheduleRule.COMPACTION_TASK_CREATION.buildRuleName(instanceProperties))
                .description("A rule to periodically trigger the compaction task creation lambda")
                .enabled(!shouldDeployPaused(this))
                .schedule(Schedule.rate(Duration.minutes(instanceProperties.getInt(COMPACTION_TASK_CREATION_PERIOD_IN_MINUTES))))
                .targets(Collections.singletonList(new LambdaFunction(handler)))
                .build();
        instanceProperties.set(COMPACTION_TASK_CREATION_LAMBDA_FUNCTION, handler.getFunctionName());
        instanceProperties.set(COMPACTION_TASK_CREATION_CLOUDWATCH_RULE, rule.getRuleName());
    }

    public Queue getCompactionJobsQueue() {
        return compactionJobQ;
    }

    public Queue getCompactionDeadLetterQueue() {
        return compactionDLQ;
    }
}
