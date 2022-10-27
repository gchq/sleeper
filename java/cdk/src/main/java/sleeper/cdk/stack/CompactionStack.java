/*
 * Copyright 2022 Crown Copyright
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
import sleeper.cdk.Utils;
import sleeper.configuration.properties.InstanceProperties;
import sleeper.core.ContainerConstants;
import software.amazon.awscdk.CfnOutput;
import software.amazon.awscdk.CfnOutputProps;
import software.amazon.awscdk.Duration;
import software.amazon.awscdk.NestedStack;
import software.amazon.awscdk.services.autoscaling.AutoScalingGroup;
import software.amazon.awscdk.services.autoscaling.BlockDevice;
import software.amazon.awscdk.services.autoscaling.BlockDeviceVolume;
import software.amazon.awscdk.services.autoscaling.EbsDeviceOptions;
import software.amazon.awscdk.services.autoscaling.EbsDeviceVolumeType;
import software.amazon.awscdk.services.cloudwatch.Alarm;
import software.amazon.awscdk.services.cloudwatch.ComparisonOperator;
import software.amazon.awscdk.services.cloudwatch.MetricOptions;
import software.amazon.awscdk.services.cloudwatch.TreatMissingData;
import software.amazon.awscdk.services.cloudwatch.actions.SnsAction;
import software.amazon.awscdk.services.ec2.IVpc;
import software.amazon.awscdk.services.ec2.InstanceClass;
import software.amazon.awscdk.services.ec2.InstanceSize;
import software.amazon.awscdk.services.ec2.InstanceType;
import software.amazon.awscdk.services.ec2.Vpc;
import software.amazon.awscdk.services.ec2.VpcLookupOptions;
import software.amazon.awscdk.services.ecr.IRepository;
import software.amazon.awscdk.services.ecr.Repository;
import software.amazon.awscdk.services.ecs.AddAutoScalingGroupCapacityOptions;
import software.amazon.awscdk.services.ecs.AmiHardwareType;
import software.amazon.awscdk.services.ecs.AsgCapacityProvider;
import software.amazon.awscdk.services.ecs.AwsLogDriver;
import software.amazon.awscdk.services.ecs.AwsLogDriverProps;
import software.amazon.awscdk.services.ecs.Cluster;
import software.amazon.awscdk.services.ecs.ContainerDefinitionOptions;
import software.amazon.awscdk.services.ecs.ContainerImage;
import software.amazon.awscdk.services.ecs.Ec2TaskDefinition;
import software.amazon.awscdk.services.ecs.EcsOptimizedImage;
import software.amazon.awscdk.services.ecs.EcsOptimizedImageOptions;
import software.amazon.awscdk.services.ecs.FargateTaskDefinition;
import software.amazon.awscdk.services.ecs.ITaskDefinition;
import software.amazon.awscdk.services.ecs.LogDriver;
import software.amazon.awscdk.services.ecs.MachineImageType;
import software.amazon.awscdk.services.ecs.NetworkMode;
import software.amazon.awscdk.services.events.Rule;
import software.amazon.awscdk.services.events.Schedule;
import software.amazon.awscdk.services.events.targets.LambdaFunction;
import software.amazon.awscdk.services.iam.IRole;
import software.amazon.awscdk.services.iam.ManagedPolicy;
import software.amazon.awscdk.services.iam.PolicyStatement;
import software.amazon.awscdk.services.lambda.Code;
import software.amazon.awscdk.services.lambda.Function;
import software.amazon.awscdk.services.s3.Bucket;
import software.amazon.awscdk.services.s3.IBucket;
import software.amazon.awscdk.services.sns.Topic;
import software.amazon.awscdk.services.sqs.DeadLetterQueue;
import software.amazon.awscdk.services.sqs.Queue;
import software.constructs.Construct;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.function.Consumer;

import static sleeper.configuration.properties.SystemDefinedInstanceProperty.COMPACTION_CLUSTER;
import static sleeper.configuration.properties.SystemDefinedInstanceProperty.COMPACTION_JOB_CREATION_CLOUDWATCH_RULE;
import static sleeper.configuration.properties.SystemDefinedInstanceProperty.COMPACTION_JOB_DLQ_URL;
import static sleeper.configuration.properties.SystemDefinedInstanceProperty.COMPACTION_JOB_QUEUE_URL;
import static sleeper.configuration.properties.SystemDefinedInstanceProperty.COMPACTION_TASK_CREATION_CLOUDWATCH_RULE;
import static sleeper.configuration.properties.SystemDefinedInstanceProperty.COMPACTION_TASK_EC2_DEFINITION_FAMILY;
import static sleeper.configuration.properties.SystemDefinedInstanceProperty.COMPACTION_TASK_FARGATE_DEFINITION_FAMILY;
import static sleeper.configuration.properties.SystemDefinedInstanceProperty.CONFIG_BUCKET;
import static sleeper.configuration.properties.SystemDefinedInstanceProperty.SPLITTING_COMPACTION_CLUSTER;
import static sleeper.configuration.properties.SystemDefinedInstanceProperty.SPLITTING_COMPACTION_JOB_DLQ_URL;
import static sleeper.configuration.properties.SystemDefinedInstanceProperty.SPLITTING_COMPACTION_JOB_QUEUE_URL;
import static sleeper.configuration.properties.SystemDefinedInstanceProperty.SPLITTING_COMPACTION_TASK_CREATION_CLOUDWATCH_RULE;
import static sleeper.configuration.properties.SystemDefinedInstanceProperty.SPLITTING_COMPACTION_TASK_EC2_DEFINITION_FAMILY;
import static sleeper.configuration.properties.SystemDefinedInstanceProperty.SPLITTING_COMPACTION_TASK_FARGATE_DEFINITION_FAMILY;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.COMPACTION_EC2_POOL_DESIRED;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.COMPACTION_EC2_POOL_MAXIMUM;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.COMPACTION_EC2_POOL_MINIMUM;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.COMPACTION_EC2_ROOT_SIZE;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.COMPACTION_EC2_TYPE;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.COMPACTION_JOB_CREATION_LAMBDA_MEMORY_IN_MB;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.COMPACTION_JOB_CREATION_LAMBDA_PERIOD_IN_MINUTES;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.COMPACTION_JOB_CREATION_LAMBDA_TIMEOUT_IN_SECONDS;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.COMPACTION_QUEUE_VISIBILITY_TIMEOUT_IN_SECONDS;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.COMPACTION_TASK_CPU;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.COMPACTION_TASK_CREATION_PERIOD_IN_MINUTES;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.COMPACTION_TASK_MEMORY;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.ECR_COMPACTION_REPO;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.ID;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.JARS_BUCKET;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.LOG_RETENTION_IN_DAYS;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.REGION;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.TASK_RUNNER_LAMBDA_MEMORY_IN_MB;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.TASK_RUNNER_LAMBDA_TIMEOUT_IN_SECONDS;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.VERSION;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.VPC_ID;


/**
 * A {@link Stack} to deploy the {@link Queue}s, ECS {@link Cluster}s,
 * {@link FargateTaskDefinition}s, {@link Function}s, CloudWatch {@link Rule}s
 * needed to perform compaction jobs. Specifically, there is:
 * <p>
 * - a lambda, that is periodically triggered by a CloudWatch rule, to query the
 * state store for information about active files with no job id, to create
 * compaction job definitions as appropriate and post them to a queue;
 * - an ECS {@link Cluster} and a {@link FargateTaskDefinition} for tasks that
 * will perform compaction jobs;
 * - a lambda, that is periodically triggered by a CloudWatch rule, to look at
 * the size of the queue and the number of running tasks and create more tasks
 * if necessary.
 * <p>
 * Note that there are two of each of the above: one for non-splitting compaction
 * jobs and one for splitting compaction jobs.
 */
public class CompactionStack extends NestedStack {
    public static final String COMPACTION_STACK_QUEUE_URL = "CompactionStackQueueUrlKey";
    public static final String COMPACTION_STACK_DL_QUEUE_URL = "CompactionStackDLQueueUrlKey";
    public static final String SPLITTING_COMPACTION_STACK_QUEUE_URL = "SplittingCompactionStackQueueUrlKey";
    public static final String SPLITTING_COMPACTION_STACK_DL_QUEUE_URL = "SplittingCompactionStackDLQueueUrlKey";
    public static final String COMPACTION_CLUSTER_NAME = "CompactionClusterName";
    public static final String SPLITTING_COMPACTION_CLUSTER_NAME = "SplittingCompactionClusterName";

    private Queue compactionJobQ;
    private Queue compactionDLQ;
    private Queue splittingJobQ;
    private Queue splittingDLQ;
    private final InstanceProperties instanceProperties;
    private final CompactionStatusStoreStack eventStore;

    public CompactionStack(
            Construct scope,
            String id,
            Topic topic,
            List<StateStoreStack> stateStoreStacks,
            List<IBucket> dataBuckets,
            InstanceProperties instanceProperties) {
        super(scope, id);
        this.instanceProperties = instanceProperties;
        eventStore = CompactionStatusStoreStack.from(this, instanceProperties);

        // The compaction stack consists of the following components:
        //  - An SQS queue for the compaction jobs.
        //  - An SQS queue for the splitting compaction jobs.
        //  - A lambda to periodically check for compaction jobs that should be created.
        //      This lambda is fired periodically by a CloudWatch rule. It queries the
        //      StateStore for information about the current partitions and files,
        //      identifies files that should be compacted, creates a job definition
        //      and sends it to an SQS queue.
        //  - An ECS cluster, task definition, etc., for compaction jobs.
        //  - An ECS cluster, task definition, etc., for splitting compaction jobs.
        //  - A lambda that periodically checks the number of running compaction tasks
        //      and if there are not enough (i.e. there is a backlog on the queue
        //      then it creates more tasks).
        //  - A lambda that periodically checks the number of running splitting compaction tasks
        //      and if there are not enough (i.e. there is a backlog on the queue
        //      then it creates more tasks).

        // Config bucket
        IBucket configBucket = Bucket.fromBucketName(this, "ConfigBucket", instanceProperties.get(CONFIG_BUCKET));

        // Jars bucket
        IBucket jarsBucket = Bucket.fromBucketName(this, "JarsBucket", instanceProperties.get(JARS_BUCKET));

        // SQS queue for the compaction jobs
        Queue compactionJobsQueue = sqsQueueForCompactionJobs(topic);

        // SQS queue for the splitting compaction jobs
        Queue splittingCompactionJobsQueue = sqsQueueForSplittingCompactionJobs(topic);

        // Lambda to periodically check for compaction jobs that should be created
        lambdaToFindCompactionJobsThatShouldBeCreated(configBucket, jarsBucket, stateStoreStacks, compactionJobsQueue, splittingCompactionJobsQueue);

        // ECS cluster for compaction tasks
        ecsClusterForCompactionTasks(configBucket, jarsBucket, stateStoreStacks, dataBuckets, compactionJobsQueue);

        // ECS cluster for splitting compaction tasks
        ecsClusterForSplittingCompactionTasks(configBucket, jarsBucket, stateStoreStacks, dataBuckets, splittingCompactionJobsQueue);

        // Lambda to create compaction tasks
        lambdaToCreateCompactionTasks(configBucket, jarsBucket, compactionJobsQueue);

        // Lambda to create splitting compaction tasks
        lambdaToCreateSplittingCompactionTasks(configBucket, jarsBucket, splittingCompactionJobsQueue);

        Utils.addStackTagIfSet(this, instanceProperties);
    }

    // TODO Code duplication because we have separate queues for splitting compaction
    // jobs and non-splitting compaction jobs. Either merge them both into one
    // queue, ECS cluster, etc., or reduce code duplication.
    private Queue sqsQueueForCompactionJobs(Topic topic) {
        // Create queue for compaction job definitions
        String dlQueueName = Utils.truncateTo64Characters(instanceProperties.get(ID) + "-CompactionJobDLQ");
        compactionDLQ = Queue.Builder
                .create(this, "CompactionMergeJobDefinitionsDeadLetterQueue")
                .queueName(dlQueueName)
                .build();
        DeadLetterQueue compactionMergeJobDefinitionsDeadLetterQueue = DeadLetterQueue.builder()
                .maxReceiveCount(1)
                .queue(compactionDLQ)
                .build();
        String queueName = Utils.truncateTo64Characters(instanceProperties.get(ID) + "-CompactionJobQ");
        compactionJobQ = Queue.Builder
                .create(this, "CompactionJobDefinitionsQueue")
                .queueName(queueName)
                .deadLetterQueue(compactionMergeJobDefinitionsDeadLetterQueue)
                .visibilityTimeout(Duration.seconds(instanceProperties.getInt(COMPACTION_QUEUE_VISIBILITY_TIMEOUT_IN_SECONDS)))
                .build();
        instanceProperties.set(COMPACTION_JOB_QUEUE_URL, compactionJobQ.getQueueUrl());
        instanceProperties.set(COMPACTION_JOB_DLQ_URL, compactionMergeJobDefinitionsDeadLetterQueue.getQueue().getQueueUrl());

        // Add alarm to send message to SNS if there are any messages on the dead letter queue
        Alarm compactionMergeAlarm = Alarm.Builder
                .create(this, "CompactionMergeAlarm")
                .alarmDescription("Alarms if there are any messages on the dead letter queue for the merging compactions queue")
                .metric(compactionDLQ.metricApproximateNumberOfMessagesVisible()
                        .with(MetricOptions.builder().statistic("Sum").period(Duration.seconds(60)).build())
                )
                .comparisonOperator(ComparisonOperator.GREATER_THAN_THRESHOLD)
                .threshold(0)
                .evaluationPeriods(1)
                .datapointsToAlarm(1)
                .treatMissingData(TreatMissingData.IGNORE)
                .build();
        compactionMergeAlarm.addAlarmAction(new SnsAction(topic));

        CfnOutputProps compactionJobDefinitionsQueueProps = new CfnOutputProps.Builder()
                .value(compactionJobQ.getQueueUrl())
                .build();
        new CfnOutput(this, COMPACTION_STACK_QUEUE_URL, compactionJobDefinitionsQueueProps);
        CfnOutputProps compactionJobDefinitionsDLQueueProps = new CfnOutputProps.Builder()
                .value(compactionMergeJobDefinitionsDeadLetterQueue.getQueue().getQueueUrl())
                .build();
        new CfnOutput(this, COMPACTION_STACK_DL_QUEUE_URL, compactionJobDefinitionsDLQueueProps);

        return compactionJobQ;
    }

    private Queue sqsQueueForSplittingCompactionJobs(Topic topic) {
        // Create queue for compaction job definitions
        String dlQueueName = Utils.truncateTo64Characters(instanceProperties.get(ID) + "-SplittingCompactionJobDLQ");
        splittingDLQ = Queue.Builder
                .create(this, "CompactionSplittingMergeJobDefinitionsDeadLetterQueue")
                .queueName(dlQueueName)
                .build();
        DeadLetterQueue compactionJobDefinitionsDeadLetterQueue = DeadLetterQueue.builder()
                .maxReceiveCount(1)
                .queue(splittingDLQ)
                .build();
        String queueName = Utils.truncateTo64Characters(instanceProperties.get(ID) + "-SplittingCompactionJobQ");
        splittingJobQ = Queue.Builder
                .create(this, "CompactionSplittingMergeJobDefinitionsQueue")
                .queueName(queueName)
                .deadLetterQueue(compactionJobDefinitionsDeadLetterQueue)
                .visibilityTimeout(Duration.seconds(instanceProperties.getInt(COMPACTION_QUEUE_VISIBILITY_TIMEOUT_IN_SECONDS)))
                .build();
        instanceProperties.set(SPLITTING_COMPACTION_JOB_QUEUE_URL, splittingJobQ.getQueueUrl());
        instanceProperties.set(SPLITTING_COMPACTION_JOB_DLQ_URL, compactionJobDefinitionsDeadLetterQueue.getQueue().getQueueUrl());

        // Add alarm to send message to SNS if there are any messages on the dead letter queue
        Alarm compactionMergeAlarm = Alarm.Builder
                .create(this, "CompactionSplittingMergeAlarm")
                .alarmDescription("Alarms if there are any messages on the dead letter queue for the splitting merging compactions queue")
                .metric(splittingDLQ.metricApproximateNumberOfMessagesVisible()
                        .with(MetricOptions.builder().statistic("Sum").period(Duration.seconds(60)).build())
                )
                .comparisonOperator(ComparisonOperator.GREATER_THAN_THRESHOLD)
                .threshold(0)
                .evaluationPeriods(1)
                .datapointsToAlarm(1)
                .treatMissingData(TreatMissingData.IGNORE)
                .build();
        compactionMergeAlarm.addAlarmAction(new SnsAction(topic));

        CfnOutputProps compactionJobDefinitionsQueueProps = new CfnOutputProps.Builder()
                .value(splittingJobQ.getQueueUrl())
                .build();
        new CfnOutput(this, SPLITTING_COMPACTION_STACK_QUEUE_URL, compactionJobDefinitionsQueueProps);
        CfnOutputProps compactionJobDefinitionsDLQueueProps = new CfnOutputProps.Builder()
                .value(compactionJobDefinitionsDeadLetterQueue.getQueue().getQueueUrl())
                .build();
        new CfnOutput(this, SPLITTING_COMPACTION_STACK_DL_QUEUE_URL, compactionJobDefinitionsDLQueueProps);

        return splittingJobQ;
    }

    private void lambdaToFindCompactionJobsThatShouldBeCreated(IBucket configBucket,
                                                               IBucket jarsBucket,
                                                               List<StateStoreStack> stateStoreStacks,
                                                               Queue compactionMergeJobsQueue,
                                                               Queue compactionSplittingMergeJobsQueue) {
        // CompactionJob creation code
        Code code = Code.fromBucket(jarsBucket, "lambda-jobSpecCreationLambda-" + instanceProperties.get(VERSION) + ".jar");

        // Function to create compaction jobs
        Map<String, String> environmentVariables = Utils.createDefaultEnvironment(instanceProperties);

        String functionName = Utils.truncateTo64Characters(String.join("-", "sleeper",
                instanceProperties.get(ID).toLowerCase(Locale.ROOT), "job-creator"));

        Function handler = Function.Builder
                .create(this, "JobCreationLambda")
                .functionName(functionName)
                .description("Scan DynamoDB looking for files that need merging and create appropriate job specs in DynamoDB")
                .runtime(software.amazon.awscdk.services.lambda.Runtime.JAVA_8)
                .memorySize(instanceProperties.getInt(COMPACTION_JOB_CREATION_LAMBDA_MEMORY_IN_MB))
                .timeout(Duration.seconds(instanceProperties.getInt(COMPACTION_JOB_CREATION_LAMBDA_TIMEOUT_IN_SECONDS)))
                .code(code)
                .handler("sleeper.compaction.job.creation.CreateJobsLambda::eventHandler")
                .environment(environmentVariables)
                .reservedConcurrentExecutions(1)
                .logRetention(Utils.getRetentionDays(instanceProperties.getInt(LOG_RETENTION_IN_DAYS)))
                .build();

        // Grant this function permission to read from / write to the DynamoDB table
        configBucket.grantRead(handler);
        jarsBucket.grantRead(handler);
        stateStoreStacks.forEach(stateStoreStack -> stateStoreStack.grantReadWriteActiveFileMetadata(handler));
        stateStoreStacks.forEach(stateStoreStack -> stateStoreStack.grantReadPartitionMetadata(handler));
        eventStore.grantWriteJobEvent(handler);

        // Grant this function permission to put messages on the compaction
        // queue and the compaction splitting queue
        compactionMergeJobsQueue.grantSendMessages(handler);
        compactionSplittingMergeJobsQueue.grantSendMessages(handler);

        // Cloudwatch rule to trigger this lambda
        String ruleName = Utils.truncateTo64Characters(instanceProperties.get(ID) + "-CompactionJobCreationRule");
        Rule rule = Rule.Builder
                .create(this, "CompactionJobCreationPeriodicTrigger")
                .ruleName(ruleName)
                .description("A rule to periodically trigger the job creation lambda")
                .enabled(Boolean.TRUE)
                .schedule(Schedule.rate(Duration.minutes(instanceProperties.getInt(COMPACTION_JOB_CREATION_LAMBDA_PERIOD_IN_MINUTES))))
                .targets(Collections.singletonList(new LambdaFunction(handler)))
                .build();
        instanceProperties.set(COMPACTION_JOB_CREATION_CLOUDWATCH_RULE, rule.getRuleName());
    }

    private Cluster ecsClusterForCompactionTasks(IBucket configBucket,
                                                 IBucket jarsBucket,
                                                 List<StateStoreStack> stateStoreStacks,
                                                 List<IBucket> dataBuckets,
                                                 Queue compactionMergeJobsQueue) {
        VpcLookupOptions vpcLookupOptions = VpcLookupOptions.builder()
                .vpcId(instanceProperties.get(VPC_ID))
                .build();
        IVpc vpc = Vpc.fromLookup(this, "VPC1", vpcLookupOptions);
        String clusterName = Utils.truncateTo64Characters(String.join("-", "sleeper",
                instanceProperties.get(ID).toLowerCase(Locale.ROOT), "merge-compaction-cluster"));
        Cluster cluster = Cluster.Builder
                .create(this, "MergeCompactionCluster")
                .clusterName(clusterName)
                .containerInsights(Boolean.TRUE)
                .vpc(vpc)
                .build();
        instanceProperties.set(COMPACTION_CLUSTER, cluster.getClusterName());

        FargateTaskDefinition fargateTaskDefinition = FargateTaskDefinition.Builder
                .create(this, "MergeCompactionFGTaskDefinition")
                .family(instanceProperties.get(ID) + "MergeCompactionFGTaskFamily")
                .cpu(instanceProperties.getInt(COMPACTION_TASK_CPU))
                .memoryLimitMiB(instanceProperties.getInt(COMPACTION_TASK_MEMORY))
                .build();

        String fargateTaskDefinitionFamily = fargateTaskDefinition.getFamily();
        instanceProperties.set(COMPACTION_TASK_FARGATE_DEFINITION_FAMILY, fargateTaskDefinitionFamily);

        Ec2TaskDefinition ec2TaskDefinition = Ec2TaskDefinition.Builder
               .create(this, "MergeCompactionEC2TaskDefinition")
               .family(instanceProperties.get(ID) + "MergeCompactionEC2TaskFamily")
               .networkMode(NetworkMode.BRIDGE)
               .build();

        String ec2TaskDefinitionFamily = ec2TaskDefinition.getFamily();
        instanceProperties.set(COMPACTION_TASK_EC2_DEFINITION_FAMILY, ec2TaskDefinitionFamily);

        IRepository repository = Repository.fromRepositoryName(this, "ECR1", instanceProperties.get(ECR_COMPACTION_REPO));
        ContainerImage containerImage = ContainerImage.fromEcrRepository(repository, instanceProperties.get(VERSION));

        AwsLogDriverProps logDriverProps = AwsLogDriverProps.builder()
                .streamPrefix(instanceProperties.get(ID) + "-MergeTasks")
                .logRetention(Utils.getRetentionDays(instanceProperties.getInt(LOG_RETENTION_IN_DAYS)))
                .build();
        LogDriver logDriver = AwsLogDriver.awsLogs(logDriverProps);

        Map<String, String> environmentVariables = Utils.createDefaultEnvironment(instanceProperties);
        environmentVariables.put(Utils.AWS_REGION, instanceProperties.get(REGION));

        ContainerDefinitionOptions containerDefinitionOptions = ContainerDefinitionOptions.builder()
                .image(containerImage)
                .logging(logDriver)
                .environment(environmentVariables)
                .cpu(instanceProperties.getInt(COMPACTION_TASK_CPU))
                .memoryLimitMiB(instanceProperties.getInt(COMPACTION_TASK_MEMORY))
                .build();
        fargateTaskDefinition.addContainer(ContainerConstants.COMPACTION_CONTAINER_NAME, containerDefinitionOptions);
        ec2TaskDefinition.addContainer(ContainerConstants.COMPACTION_CONTAINER_NAME, containerDefinitionOptions);

        Consumer<ITaskDefinition> grantPermissions = (taskDef) -> {
            configBucket.grantRead(taskDef.getTaskRole());
            jarsBucket.grantRead(taskDef.getTaskRole());
            dataBuckets.forEach(bucket -> bucket.grantReadWrite(taskDef.getTaskRole()));
            stateStoreStacks.forEach(stateStoreStack -> stateStoreStack.grantReadWriteActiveFileMetadata(taskDef.getTaskRole()));
            stateStoreStacks.forEach(stateStoreStack -> stateStoreStack.grantReadWriteReadyForGCFileMetadata(taskDef.getTaskRole()));
            eventStore.grantWriteJobEvent(taskDef.getTaskRole());
            eventStore.grantWriteTaskEvent(taskDef.getTaskRole());

            compactionMergeJobsQueue.grantConsumeMessages(taskDef.getTaskRole());
        };

        grantPermissions.accept(fargateTaskDefinition);
        grantPermissions.accept(ec2TaskDefinition);

        addEC2CapacityProvider(cluster, "MergeCompaction", vpc);

        CfnOutputProps compactionClusterProps = new CfnOutputProps.Builder()
                .value(cluster.getClusterName())
                .build();
        new CfnOutput(this, COMPACTION_CLUSTER_NAME, compactionClusterProps);

        return cluster;
    }

    private Cluster ecsClusterForSplittingCompactionTasks(IBucket configBucket,
                                                          IBucket jarsBucket,
                                                          List<StateStoreStack> stateStoreStacks,
                                                          List<IBucket> dataBuckets,
                                                          Queue compactionSplittingMergeJobsQueue) {
        VpcLookupOptions vpcLookupOptions = VpcLookupOptions.builder()
                .vpcId(instanceProperties.get(VPC_ID))
                .build();
        IVpc vpc = Vpc.fromLookup(this, "VPC2", vpcLookupOptions);
        String clusterName = Utils.truncateTo64Characters(String.join("-", "sleeper",
                instanceProperties.get(ID).toLowerCase(Locale.ROOT), "splitting-merge-compaction-cluster"));
        Cluster cluster = Cluster.Builder
                .create(this, "SplittingMergeCompactionCluster")
                .clusterName(clusterName)
                .containerInsights(Boolean.TRUE)
                .vpc(vpc)
                .build();
        instanceProperties.set(SPLITTING_COMPACTION_CLUSTER, cluster.getClusterName());

        FargateTaskDefinition fargateTaskDefinition = FargateTaskDefinition.Builder
                .create(this, "SplittingMergeCompactionFGTaskDefinition")
                .family(instanceProperties.get(ID) + "SplittingMergeCompactionFGTaskFamily")
                .cpu(instanceProperties.getInt(COMPACTION_TASK_CPU))
                .memoryLimitMiB(instanceProperties.getInt(COMPACTION_TASK_MEMORY))
                .build();

        String fargateTaskDefinitionFamily = fargateTaskDefinition.getFamily();
        instanceProperties.set(SPLITTING_COMPACTION_TASK_FARGATE_DEFINITION_FAMILY, fargateTaskDefinitionFamily);

        Ec2TaskDefinition ec2TaskDefinition = Ec2TaskDefinition.Builder
               .create(this, "SplittingMergeCompactionEC2TaskDefinition")
               .family(instanceProperties.get(ID) + "SplittingMergeCompactionEC2TaskFamily")
               .networkMode(NetworkMode.BRIDGE)
               .build();

        String ec2TaskDefinitionFamily = ec2TaskDefinition.getFamily();
        instanceProperties.set(SPLITTING_COMPACTION_TASK_EC2_DEFINITION_FAMILY, ec2TaskDefinitionFamily);

        IRepository repository = Repository.fromRepositoryName(this, "ECR2", instanceProperties.get(ECR_COMPACTION_REPO));
        ContainerImage containerImage = ContainerImage.fromEcrRepository(repository, instanceProperties.get(VERSION));

        AwsLogDriverProps logDriverProps = AwsLogDriverProps.builder()
                .streamPrefix(instanceProperties.get(ID) + "-SplittingMergeTasks")
                .logRetention(Utils.getRetentionDays(instanceProperties.getInt(LOG_RETENTION_IN_DAYS)))
                .build();
        LogDriver logDriver = AwsLogDriver.awsLogs(logDriverProps);

        Map<String, String> environmentVariables = Utils.createDefaultEnvironment(instanceProperties);
        environmentVariables.put(Utils.AWS_REGION, instanceProperties.get(REGION));

        ContainerDefinitionOptions containerDefinitionOptions = ContainerDefinitionOptions.builder()
                .image(containerImage)
                .logging(logDriver)
                .environment(environmentVariables)
                .cpu(instanceProperties.getInt(COMPACTION_TASK_CPU))
                .memoryLimitMiB(instanceProperties.getInt(COMPACTION_TASK_MEMORY))
                .build();
        fargateTaskDefinition.addContainer(ContainerConstants.SPLITTING_COMPACTION_CONTAINER_NAME, containerDefinitionOptions);
        ec2TaskDefinition.addContainer(ContainerConstants.SPLITTING_COMPACTION_CONTAINER_NAME, containerDefinitionOptions);

        Consumer<ITaskDefinition> grantPermissions = (taskDef) -> {
            configBucket.grantRead(taskDef.getTaskRole());
            jarsBucket.grantRead(taskDef.getTaskRole());
            dataBuckets.forEach(bucket -> bucket.grantReadWrite(taskDef.getTaskRole()));
            stateStoreStacks.forEach(stateStoreStack -> stateStoreStack.grantReadWriteActiveFileMetadata(taskDef.getTaskRole()));
            stateStoreStacks.forEach(stateStoreStack -> stateStoreStack.grantReadWriteReadyForGCFileMetadata(taskDef.getTaskRole()));
            eventStore.grantWriteJobEvent(taskDef.getTaskRole());
            eventStore.grantWriteTaskEvent(taskDef.getTaskRole());

            compactionSplittingMergeJobsQueue.grantConsumeMessages(taskDef.getTaskRole());
        };

        grantPermissions.accept(fargateTaskDefinition);
        grantPermissions.accept(ec2TaskDefinition);

        addEC2CapacityProvider(cluster, "SplittingMergeCompaction", vpc);

        CfnOutputProps splittingCompactionClusterProps = new CfnOutputProps.Builder()
                .value(cluster.getClusterName())
                .build();
        new CfnOutput(this, SPLITTING_COMPACTION_CLUSTER_NAME, splittingCompactionClusterProps);

        return cluster;
    }

    private void addEC2CapacityProvider(Cluster cluster, String clusterName, IVpc vpc) {
        AutoScalingGroup ec2scalingGroup = AutoScalingGroup.Builder.create(this, clusterName + "ScalingGroup").vpc(vpc)
                .allowAllOutbound(true).associatePublicIpAddress(false)
                .blockDevices(Arrays.asList(BlockDevice.builder().deviceName("/dev/xvda") // root volume
                        .volume(BlockDeviceVolume.ebs(instanceProperties.getInt(COMPACTION_EC2_ROOT_SIZE),
                                EbsDeviceOptions.builder().deleteOnTermination(true).encrypted(true)
                                        .volumeType(EbsDeviceVolumeType.GP2).build()))
                        .build()))
                .minCapacity(instanceProperties.getInt(COMPACTION_EC2_POOL_MINIMUM))
                .desiredCapacity(instanceProperties.getInt(COMPACTION_EC2_POOL_DESIRED))
                .maxCapacity(instanceProperties.getInt(COMPACTION_EC2_POOL_MAXIMUM)).requireImdsv2(true)
                .instanceType(lookupEC2InstanceType(instanceProperties.get(COMPACTION_EC2_TYPE)))
                .machineImage(EcsOptimizedImage.amazonLinux2(AmiHardwareType.STANDARD, EcsOptimizedImageOptions.builder()
                        .cachedInContext(false)
                        .build()
                        )
                )
                .build();

        AsgCapacityProvider ec2Provider = AsgCapacityProvider.Builder
                .create(this, clusterName + "CapacityProvider").enableManagedScaling(false)
                .enableManagedTerminationProtection(true)
                .autoScalingGroup(ec2scalingGroup).spotInstanceDraining(true)
                .canContainersAccessInstanceRole(false).machineImageType(MachineImageType.AMAZON_LINUX_2).build();

        cluster.addAsgCapacityProvider(ec2Provider,
                AddAutoScalingGroupCapacityOptions.builder().canContainersAccessInstanceRole(false)
                        .machineImageType(MachineImageType.AMAZON_LINUX_2).spotInstanceDraining(true).build());
    }

    public static InstanceType lookupEC2InstanceType(String ec2InstanceType) {
        Objects.requireNonNull(ec2InstanceType, "instance type cannot be null");
        int pos = ec2InstanceType.indexOf('.');

        if (pos < 0 || ec2InstanceType.trim().isEmpty()) {
            throw new IllegalArgumentException("instance type is empty or invalid");
        }

        String family = ec2InstanceType.substring(0, pos).toUpperCase(Locale.getDefault());
        String size = ec2InstanceType.substring(pos + 1).toUpperCase(Locale.getDefault());

        //now perform lookup of these against known types
        InstanceClass instanceClass = InstanceClass.valueOf(family);
        InstanceSize instanceSize = InstanceSize.valueOf(size);

        return InstanceType.of(instanceClass, instanceSize);
    }

    @SuppressFBWarnings("NP_NULL_ON_SOME_PATH_FROM_RETURN_VALUE")
    private void lambdaToCreateCompactionTasks(IBucket configBucket,
                                               IBucket jarsBucket,
                                               Queue compactionMergeJobsQueue) {
        // CompactionJob creation code
        Code code = Code.fromBucket(jarsBucket, "runningjobs-" + instanceProperties.get(VERSION) + ".jar");

        // Run tasks function
        Map<String, String> environmentVariables = Utils.createDefaultEnvironment(instanceProperties);
        environmentVariables.put("type", "compaction");

        String functionName = Utils.truncateTo64Characters(String.join("-", "sleeper",
                instanceProperties.get(ID).toLowerCase(Locale.ROOT), "compaction-tasks-creator"));

        Function handler = Function.Builder
                .create(this, "CompactionTasksCreator")
                .functionName(functionName)
                .description("If there are compaction jobs on queue create tasks to run them")
                .runtime(software.amazon.awscdk.services.lambda.Runtime.JAVA_8)
                .memorySize(instanceProperties.getInt(TASK_RUNNER_LAMBDA_MEMORY_IN_MB))
                .timeout(Duration.seconds(instanceProperties.getInt(TASK_RUNNER_LAMBDA_TIMEOUT_IN_SECONDS)))
                .code(code)
                .handler("sleeper.compaction.jobexecution.RunTasksLambda::eventHandler")
                .environment(environmentVariables)
                .reservedConcurrentExecutions(1)
                .logRetention(Utils.getRetentionDays(instanceProperties.getInt(LOG_RETENTION_IN_DAYS)))
                .build();

        // Grant this function permission to read from the S3 bucket
        configBucket.grantRead(handler);

        // Grant this function permission to query the queue for number of messages
        compactionMergeJobsQueue.grantSendMessages(handler);
        compactionMergeJobsQueue.grant(handler, "sqs:GetQueueAttributes");

        // Grant this function permission to query ECS for the number of tasks, etc
        PolicyStatement policyStatement = PolicyStatement.Builder
                .create()
                .resources(Collections.singletonList("*"))
                .actions(Arrays.asList("ecs:ListTasks", "ecs:RunTask", "iam:PassRole"))
                .build();
        IRole role = Objects.requireNonNull(handler.getRole());
        role.addToPrincipalPolicy(policyStatement);
        role.addManagedPolicy(ManagedPolicy.fromAwsManagedPolicyName("service-role/AmazonECSTaskExecutionRolePolicy"));

        // Cloudwatch rule to trigger this lambda
        String ruleName = Utils.truncateTo64Characters(instanceProperties.get(ID) + "-CompactionTasksCreationRule");
        Rule rule = Rule.Builder
                .create(this, "CompactionMergeTasksCreationPeriodicTrigger")
                .ruleName(ruleName)
                .description("A rule to periodically trigger the compaction tasks lambda")
                .enabled(Boolean.TRUE)
                .schedule(Schedule.rate(Duration.minutes(instanceProperties.getInt(COMPACTION_TASK_CREATION_PERIOD_IN_MINUTES))))
                .targets(Collections.singletonList(new LambdaFunction(handler)))
                .build();
        instanceProperties.set(COMPACTION_TASK_CREATION_CLOUDWATCH_RULE, rule.getRuleName());
    }

    @SuppressFBWarnings("NP_NULL_ON_SOME_PATH_FROM_RETURN_VALUE")
    private void lambdaToCreateSplittingCompactionTasks(IBucket configBucket,
                                                        IBucket jarsBucket,
                                                        Queue compactionSplittingMergeJobsQueue) {
        // CompactionJob creation code
        Code code = Code.fromBucket(jarsBucket, "runningjobs-" + instanceProperties.get(VERSION) + ".jar");

        // Run tasks function
        Map<String, String> environmentVariables = Utils.createDefaultEnvironment(instanceProperties);
        environmentVariables.put("type", "splittingcompaction");

        String functionName = Utils.truncateTo64Characters(String.join("-", "sleeper",
                instanceProperties.get(ID).toLowerCase(Locale.ROOT), "splitting-compaction-tasks-creator"));

        Function handler = Function.Builder
                .create(this, "SplittingCompactionTasksCreator")
                .functionName(functionName)
                .description("If there are splitting compaction jobs on queue create tasks to run them")
                .runtime(software.amazon.awscdk.services.lambda.Runtime.JAVA_8)
                .memorySize(instanceProperties.getInt(TASK_RUNNER_LAMBDA_MEMORY_IN_MB))
                .timeout(Duration.seconds(instanceProperties.getInt(TASK_RUNNER_LAMBDA_TIMEOUT_IN_SECONDS)))
                .code(code)
                .handler("sleeper.compaction.jobexecution.RunTasksLambda::eventHandler")
                .environment(environmentVariables)
                .reservedConcurrentExecutions(1)
                .logRetention(Utils.getRetentionDays(instanceProperties.getInt(LOG_RETENTION_IN_DAYS)))
                .build();

        // Grant this function permission to read from the config S3 bucket
        configBucket.grantRead(handler);

        // Grant this function permission to query the queue for number of messages
        compactionSplittingMergeJobsQueue.grantSendMessages(handler);
        compactionSplittingMergeJobsQueue.grant(handler, "sqs:GetQueueAttributes");

        // Grant this function permission to query ECS for the number of tasks, etc
        PolicyStatement policyStatement = PolicyStatement.Builder
                .create()
                .resources(Collections.singletonList("*"))
                .actions(Arrays.asList("ecs:ListTasks", "ecs:RunTask", "iam:PassRole"))
                .build();
        IRole role = Objects.requireNonNull(handler.getRole());
        role.addToPrincipalPolicy(policyStatement);
        role.addManagedPolicy(ManagedPolicy.fromAwsManagedPolicyName("service-role/AmazonECSTaskExecutionRolePolicy"));

        // Cloudwatch rule to trigger this lambda
        String ruleName = Utils.truncateTo64Characters(instanceProperties.get(ID) + "-SplittingCompactionTasksCreationRule");
        Rule rule = Rule.Builder
                .create(this, "CompactionSplittingMergeTasksCreationPeriodicTrigger")
                .ruleName(ruleName)
                .description("A rule to periodically trigger the splitting compaction tasks lambda")
                .enabled(Boolean.TRUE)
                .schedule(Schedule.rate(Duration.minutes(instanceProperties.getInt(COMPACTION_TASK_CREATION_PERIOD_IN_MINUTES))))
                .targets(Collections.singletonList(new LambdaFunction(handler)))
                .build();
        instanceProperties.set(SPLITTING_COMPACTION_TASK_CREATION_CLOUDWATCH_RULE, rule.getRuleName());
    }

    public Queue getCompactionJobsQueue() {
        return compactionJobQ;
    }

    public Queue getCompactionDeadLetterQueue() {
        return compactionDLQ;
    }

    public Queue getSplittingJobsQueue() {
        return splittingJobQ;
    }

    public Queue getSplittingDeadLetterQueue() {
        return splittingDLQ;
    }
}
