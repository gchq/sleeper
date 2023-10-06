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
package sleeper.cdk.stack;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.apache.commons.lang3.tuple.Pair;
import software.amazon.awscdk.CfnOutput;
import software.amazon.awscdk.CfnOutputProps;
import software.amazon.awscdk.Duration;
import software.amazon.awscdk.NestedStack;
import software.amazon.awscdk.services.autoscaling.AutoScalingGroup;
import software.amazon.awscdk.services.autoscaling.BlockDevice;
import software.amazon.awscdk.services.autoscaling.BlockDeviceVolume;
import software.amazon.awscdk.services.autoscaling.CfnAutoScalingGroup;
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
import software.amazon.awscdk.services.iam.IRole;
import software.amazon.awscdk.services.iam.ManagedPolicy;
import software.amazon.awscdk.services.iam.PolicyStatement;
import software.amazon.awscdk.services.iam.Role;
import software.amazon.awscdk.services.lambda.Function;
import software.amazon.awscdk.services.lambda.IFunction;
import software.amazon.awscdk.services.lambda.Permission;
import software.amazon.awscdk.services.lambda.eventsources.SqsEventSource;
import software.amazon.awscdk.services.s3.Bucket;
import software.amazon.awscdk.services.s3.IBucket;
import software.amazon.awscdk.services.sns.Topic;
import software.amazon.awscdk.services.sqs.DeadLetterQueue;
import software.amazon.awscdk.services.sqs.Queue;
import software.constructs.Construct;

import sleeper.cdk.Utils;
import sleeper.cdk.jars.BuiltJar;
import sleeper.cdk.jars.BuiltJars;
import sleeper.cdk.jars.LambdaCode;
import sleeper.configuration.Requirements;
import sleeper.configuration.properties.SleeperScheduleRule;
import sleeper.configuration.properties.instance.InstanceProperties;
import sleeper.configuration.properties.instance.SystemDefinedInstanceProperty;
import sleeper.core.ContainerConstants;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.function.Consumer;

import static sleeper.cdk.Utils.shouldDeployPaused;
import static sleeper.configuration.properties.instance.CommonProperty.ACCOUNT;
import static sleeper.configuration.properties.instance.CommonProperty.ID;
import static sleeper.configuration.properties.instance.CommonProperty.LOG_RETENTION_IN_DAYS;
import static sleeper.configuration.properties.instance.CommonProperty.QUEUE_VISIBILITY_TIMEOUT_IN_SECONDS;
import static sleeper.configuration.properties.instance.CommonProperty.REGION;
import static sleeper.configuration.properties.instance.CommonProperty.TASK_RUNNER_LAMBDA_MEMORY_IN_MB;
import static sleeper.configuration.properties.instance.CommonProperty.TASK_RUNNER_LAMBDA_TIMEOUT_IN_SECONDS;
import static sleeper.configuration.properties.instance.CommonProperty.VPC_ID;
import static sleeper.configuration.properties.instance.CompactionProperty.COMPACTION_EC2_POOL_DESIRED;
import static sleeper.configuration.properties.instance.CompactionProperty.COMPACTION_EC2_POOL_MAXIMUM;
import static sleeper.configuration.properties.instance.CompactionProperty.COMPACTION_EC2_POOL_MINIMUM;
import static sleeper.configuration.properties.instance.CompactionProperty.COMPACTION_EC2_ROOT_SIZE;
import static sleeper.configuration.properties.instance.CompactionProperty.COMPACTION_EC2_TYPE;
import static sleeper.configuration.properties.instance.CompactionProperty.COMPACTION_ECS_LAUNCHTYPE;
import static sleeper.configuration.properties.instance.CompactionProperty.COMPACTION_JOB_CREATION_LAMBDA_MEMORY_IN_MB;
import static sleeper.configuration.properties.instance.CompactionProperty.COMPACTION_JOB_CREATION_LAMBDA_TIMEOUT_IN_SECONDS;
import static sleeper.configuration.properties.instance.CompactionProperty.COMPACTION_QUEUE_VISIBILITY_TIMEOUT_IN_SECONDS;
import static sleeper.configuration.properties.instance.CompactionProperty.COMPACTION_TASK_CPU_ARCHITECTURE;
import static sleeper.configuration.properties.instance.CompactionProperty.COMPACTION_TASK_CREATION_PERIOD_IN_MINUTES;
import static sleeper.configuration.properties.instance.CompactionProperty.ECR_COMPACTION_REPO;
import static sleeper.configuration.properties.instance.CompactionProperty.TABLE_BATCHER_LAMBDA_MEMORY_IN_MB;
import static sleeper.configuration.properties.instance.CompactionProperty.TABLE_BATCHER_LAMBDA_PERIOD_IN_MINUTES;
import static sleeper.configuration.properties.instance.CompactionProperty.TABLE_BATCHER_LAMBDA_TIMEOUT_IN_SECONDS;
import static sleeper.configuration.properties.instance.SystemDefinedInstanceProperty.COMPACTION_AUTO_SCALING_GROUP;
import static sleeper.configuration.properties.instance.SystemDefinedInstanceProperty.COMPACTION_CLUSTER;
import static sleeper.configuration.properties.instance.SystemDefinedInstanceProperty.COMPACTION_JOB_CREATION_LAMBDA_FUNCTION;
import static sleeper.configuration.properties.instance.SystemDefinedInstanceProperty.COMPACTION_JOB_DLQ_ARN;
import static sleeper.configuration.properties.instance.SystemDefinedInstanceProperty.COMPACTION_JOB_DLQ_URL;
import static sleeper.configuration.properties.instance.SystemDefinedInstanceProperty.COMPACTION_JOB_QUEUE_ARN;
import static sleeper.configuration.properties.instance.SystemDefinedInstanceProperty.COMPACTION_JOB_QUEUE_URL;
import static sleeper.configuration.properties.instance.SystemDefinedInstanceProperty.COMPACTION_TASK_CREATION_CLOUDWATCH_RULE;
import static sleeper.configuration.properties.instance.SystemDefinedInstanceProperty.COMPACTION_TASK_CREATION_LAMBDA_FUNCTION;
import static sleeper.configuration.properties.instance.SystemDefinedInstanceProperty.COMPACTION_TASK_EC2_DEFINITION_FAMILY;
import static sleeper.configuration.properties.instance.SystemDefinedInstanceProperty.COMPACTION_TASK_FARGATE_DEFINITION_FAMILY;
import static sleeper.configuration.properties.instance.SystemDefinedInstanceProperty.CONFIG_BUCKET;
import static sleeper.configuration.properties.instance.SystemDefinedInstanceProperty.SPLITTING_COMPACTION_AUTO_SCALING_GROUP;
import static sleeper.configuration.properties.instance.SystemDefinedInstanceProperty.SPLITTING_COMPACTION_CLUSTER;
import static sleeper.configuration.properties.instance.SystemDefinedInstanceProperty.SPLITTING_COMPACTION_JOB_DLQ_ARN;
import static sleeper.configuration.properties.instance.SystemDefinedInstanceProperty.SPLITTING_COMPACTION_JOB_DLQ_URL;
import static sleeper.configuration.properties.instance.SystemDefinedInstanceProperty.SPLITTING_COMPACTION_JOB_QUEUE_ARN;
import static sleeper.configuration.properties.instance.SystemDefinedInstanceProperty.SPLITTING_COMPACTION_JOB_QUEUE_URL;
import static sleeper.configuration.properties.instance.SystemDefinedInstanceProperty.SPLITTING_COMPACTION_TASK_CREATION_CLOUDWATCH_RULE;
import static sleeper.configuration.properties.instance.SystemDefinedInstanceProperty.SPLITTING_COMPACTION_TASK_CREATION_LAMBDA_FUNCTION;
import static sleeper.configuration.properties.instance.SystemDefinedInstanceProperty.SPLITTING_COMPACTION_TASK_EC2_DEFINITION_FAMILY;
import static sleeper.configuration.properties.instance.SystemDefinedInstanceProperty.SPLITTING_COMPACTION_TASK_FARGATE_DEFINITION_FAMILY;
import static sleeper.configuration.properties.instance.SystemDefinedInstanceProperty.TABLE_BATCHER_CLOUDWATCH_RULE;
import static sleeper.configuration.properties.instance.SystemDefinedInstanceProperty.TABLE_BATCHER_DLQ_ARN;
import static sleeper.configuration.properties.instance.SystemDefinedInstanceProperty.TABLE_BATCHER_DLQ_URL;
import static sleeper.configuration.properties.instance.SystemDefinedInstanceProperty.TABLE_BATCHER_LAMBDA_FUNCTION;
import static sleeper.configuration.properties.instance.SystemDefinedInstanceProperty.TABLE_BATCHER_QUEUE_ARN;
import static sleeper.configuration.properties.instance.SystemDefinedInstanceProperty.TABLE_BATCHER_QUEUE_URL;
import static sleeper.configuration.properties.instance.SystemDefinedInstanceProperty.VERSION;

/**
 * A {@link NestedStack} to deploy the {@link Queue}s, ECS {@link Cluster}s, a {@link FargateTaskDefinition} or
 * an {@link Ec2TaskDefinition}, {@link Function}s, CloudWatch {@link Rule}s needed to perform compaction
 * jobs. Specifically, there is:
 * <p>
 * - a lambda, that is periodically triggered by a CloudWatch rule, to query the state store for
 * information about active files with no job id, to create compaction job definitions as
 * appropriate and post them to a queue;
 * - an ECS {@link Cluster} and either a {@link FargateTaskDefinition} or a {@link Ec2TaskDefinition}
 * for tasks that will perform compaction jobs;
 * - a lambda, that is periodically triggered by a CloudWatch rule, to look at the
 * size of the queue and the number of running tasks and create more tasks if necessary.
 * <p>
 * Note that there are two of each of the above: one for non-splitting compaction jobs and one for
 * splitting compaction jobs.
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
            InstanceProperties instanceProperties,
            BuiltJars jars,
            Topic topic,
            StateStoreStacks stateStoreStacks, TableDataStack dataStack) {
        super(scope, id);
        this.instanceProperties = instanceProperties;
        eventStore = CompactionStatusStoreStack.from(this, instanceProperties);
        // The compaction stack consists of the following components:
        // - An SQS queue for the compaction jobs.
        // - An SQS queue for the splitting compaction jobs.
        // - A lambda to periodically check for compaction jobs that should be created.
        //   This lambda is fired periodically by a CloudWatch rule. It queries the
        //   StateStore for information about the current partitions and files,
        //   identifies files that should be compacted, creates a job definition
        //   and sends it to an SQS queue.
        // - An ECS cluster, task definition, etc., for compaction jobs.
        // - An ECS cluster, task definition, etc., for splitting compaction jobs.
        // - A lambda that periodically checks the number of running compaction tasks
        //   and if there are not enough (i.e. there is a backlog on the queue
        //   then it creates more tasks).
        // - A lambda that periodically checks the number of running splitting
        //   compaction tasks and if there are not enough (i.e. there is a backlog
        //   on the queue then it creates more tasks).

        // Config bucket
        IBucket configBucket = Bucket.fromBucketName(this, "ConfigBucket", instanceProperties.get(CONFIG_BUCKET));

        // Jars bucket
        IBucket jarsBucket = Bucket.fromBucketName(this, "JarsBucket", jars.bucketName());
        LambdaCode jobCreatorJar = jars.lambdaCode(BuiltJar.COMPACTION_JOB_CREATOR, jarsBucket);
        LambdaCode taskCreatorJar = jars.lambdaCode(BuiltJar.COMPACTION_TASK_CREATOR, jarsBucket);

        // SQS queue for the compaction jobs
        Queue compactionJobsQueue = sqsQueueForCompactionJobs(topic);

        // SQS queue for the splitting compaction jobs
        Queue splittingCompactionJobsQueue = sqsQueueForSplittingCompactionJobs(topic);

        // Lambda to periodically check for compaction jobs that should be created
        lambdaToFindCompactionJobsThatShouldBeCreated(configBucket, jarsBucket, jobCreatorJar,
                stateStoreStacks, compactionJobsQueue, splittingCompactionJobsQueue);

        // ECS cluster for compaction tasks
        ecsClusterForCompactionTasks(configBucket, jarsBucket, taskCreatorJar, stateStoreStacks, dataStack, compactionJobsQueue);

        // ECS cluster for splitting compaction tasks
        ecsClusterForSplittingCompactionTasks(configBucket, jarsBucket, taskCreatorJar, stateStoreStacks, dataStack,
                splittingCompactionJobsQueue);

        // Lambda to create compaction tasks
        lambdaToCreateCompactionTasks(configBucket, taskCreatorJar, compactionJobsQueue);

        // Lambda to create splitting compaction tasks
        lambdaToCreateSplittingCompactionTasks(configBucket, taskCreatorJar, splittingCompactionJobsQueue);

        Utils.addStackTagIfSet(this, instanceProperties);
    }

    // TODO Code duplication because we have separate queues for splitting
    // compaction jobs and non-splitting compaction jobs. Either merge
    // them both into one queue, ECS cluster, etc., or reduce code duplication.
    private Queue sqsQueueForCompactionJobs(Topic topic) {
        // Create queue for compaction job definitions
        String dlQueueName = Utils.truncateTo64Characters(instanceProperties.get(ID) + "-CompactionJobDLQ");
        compactionDLQ = Queue.Builder
                .create(this, "CompactionMergeJobDefinitionsDeadLetterQueue")
                .queueName(dlQueueName)
                .build();
        DeadLetterQueue compactionMergeJobDefinitionsDeadLetterQueue = DeadLetterQueue.builder()
                .maxReceiveCount(3)
                .queue(compactionDLQ)
                .build();
        String queueName = Utils.truncateTo64Characters(instanceProperties.get(ID) + "-CompactionJobQ");
        compactionJobQ = Queue.Builder
                .create(this, "CompactionJobDefinitionsQueue")
                .queueName(queueName)
                .deadLetterQueue(compactionMergeJobDefinitionsDeadLetterQueue)
                .visibilityTimeout(
                        Duration.seconds(instanceProperties.getInt(COMPACTION_QUEUE_VISIBILITY_TIMEOUT_IN_SECONDS)))
                .build();
        instanceProperties.set(COMPACTION_JOB_QUEUE_URL, compactionJobQ.getQueueUrl());
        instanceProperties.set(COMPACTION_JOB_QUEUE_ARN, compactionJobQ.getQueueArn());
        instanceProperties.set(COMPACTION_JOB_DLQ_URL,
                compactionMergeJobDefinitionsDeadLetterQueue.getQueue().getQueueUrl());
        instanceProperties.set(COMPACTION_JOB_DLQ_ARN,
                compactionMergeJobDefinitionsDeadLetterQueue.getQueue().getQueueArn());

        // Add alarm to send message to SNS if there are any messages on the dead letter
        // queue
        Alarm compactionMergeAlarm = Alarm.Builder
                .create(this, "CompactionMergeAlarm")
                .alarmDescription(
                        "Alarms if there are any messages on the dead letter queue for the merging compactions queue")
                .metric(compactionDLQ.metricApproximateNumberOfMessagesVisible()
                        .with(MetricOptions.builder().statistic("Sum").period(Duration.seconds(60)).build()))
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
                .maxReceiveCount(3)
                .queue(splittingDLQ)
                .build();
        String queueName = Utils.truncateTo64Characters(instanceProperties.get(ID) + "-SplittingCompactionJobQ");
        splittingJobQ = Queue.Builder
                .create(this, "CompactionSplittingMergeJobDefinitionsQueue")
                .queueName(queueName)
                .deadLetterQueue(compactionJobDefinitionsDeadLetterQueue)
                .visibilityTimeout(
                        Duration.seconds(instanceProperties.getInt(COMPACTION_QUEUE_VISIBILITY_TIMEOUT_IN_SECONDS)))
                .build();
        instanceProperties.set(SPLITTING_COMPACTION_JOB_QUEUE_URL, splittingJobQ.getQueueUrl());
        instanceProperties.set(SPLITTING_COMPACTION_JOB_QUEUE_ARN, splittingJobQ.getQueueArn());
        instanceProperties.set(SPLITTING_COMPACTION_JOB_DLQ_URL,
                compactionJobDefinitionsDeadLetterQueue.getQueue().getQueueUrl());
        instanceProperties.set(SPLITTING_COMPACTION_JOB_DLQ_ARN,
                compactionJobDefinitionsDeadLetterQueue.getQueue().getQueueArn());

        // Add alarm to send message to SNS if there are any messages on the dead letter
        // queue
        Alarm compactionMergeAlarm = Alarm.Builder
                .create(this, "CompactionSplittingMergeAlarm")
                .alarmDescription(
                        "Alarms if there are any messages on the dead letter queue for the splitting merging compactions queue")
                .metric(splittingDLQ.metricApproximateNumberOfMessagesVisible()
                        .with(MetricOptions.builder().statistic("Sum").period(Duration.seconds(60)).build()))
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
                                                               LambdaCode jobCreatorJar,
                                                               StateStoreStacks stateStoreStacks,
                                                               Queue compactionMergeJobsQueue,
                                                               Queue compactionSplittingMergeJobsQueue) {

        Map<String, String> environmentVariables = Utils.createDefaultEnvironment(instanceProperties);

        String dlqName = Utils.truncateTo64Characters(instanceProperties.get(ID) + "-TableBatcherDLQ");
        Queue tableBatcherDLQ = Queue.Builder
                .create(this, "TableBatcherDLQ")
                .queueName(dlqName)
                .visibilityTimeout(Duration.seconds(instanceProperties.getInt(QUEUE_VISIBILITY_TIMEOUT_IN_SECONDS)))
                .build();
        DeadLetterQueue tableBatcherDeadLetterQueue = DeadLetterQueue.builder()
                .maxReceiveCount(1)
                .queue(tableBatcherDLQ)
                .build();
        String queueName = Utils.truncateTo64Characters(instanceProperties.get(ID) + "-TableBatcherQ");
        Queue tableBatcherQueue = Queue.Builder
                .create(this, "TableBatcherQueue")
                .queueName(queueName)
                .deadLetterQueue(tableBatcherDeadLetterQueue)
                .visibilityTimeout(Duration.seconds(instanceProperties.getInt(QUEUE_VISIBILITY_TIMEOUT_IN_SECONDS)))
                .build();

        instanceProperties.set(TABLE_BATCHER_QUEUE_ARN, tableBatcherQueue.getQueueArn());
        instanceProperties.set(TABLE_BATCHER_QUEUE_URL, tableBatcherQueue.getQueueUrl());
        instanceProperties.set(TABLE_BATCHER_DLQ_ARN, tableBatcherDLQ.getQueueArn());
        instanceProperties.set(TABLE_BATCHER_DLQ_URL, tableBatcherDLQ.getQueueUrl());

        String tableBatcherFunctionName = Utils.truncateTo64Characters(String.join("-", "sleeper",
                instanceProperties.get(ID).toLowerCase(Locale.ROOT), "table-batcher"));
        IFunction tableBatcherHandler = jobCreatorJar.buildFunction(this, "JobCreationLambda", builder -> builder
                .functionName(tableBatcherFunctionName)
                .description("Batch tables to parallelise creation of compaction jobs")
                .runtime(software.amazon.awscdk.services.lambda.Runtime.JAVA_11)
                .memorySize(instanceProperties.getInt(TABLE_BATCHER_LAMBDA_MEMORY_IN_MB))
                .timeout(Duration.minutes(instanceProperties.getInt(TABLE_BATCHER_LAMBDA_TIMEOUT_IN_SECONDS)))
                .handler("sleeper.compaction.job.creation.batcher.TableBatcherLambda::eventHandler")
                .environment(environmentVariables)
                .reservedConcurrentExecutions(1)
                .logRetention(Utils.getRetentionDays(instanceProperties.getInt(LOG_RETENTION_IN_DAYS))));

        tableBatcherQueue.grantSendMessages(tableBatcherHandler);
        configBucket.grantRead(tableBatcherHandler);

        // Cloudwatch rule to trigger the table batcher lambda
        Rule rule = Rule.Builder
                .create(this, "TableBatcherPeriodicTrigger")
                .ruleName(SleeperScheduleRule.COMPACTION_JOB_CREATION.buildRuleName(instanceProperties))
                .description("A rule to periodically trigger the table batcher lambda")
                .enabled(!shouldDeployPaused(this))
                .schedule(Schedule.rate(Duration.minutes(instanceProperties.getInt(TABLE_BATCHER_LAMBDA_PERIOD_IN_MINUTES))))
                .targets(Collections.singletonList(new LambdaFunction(tableBatcherHandler)))
                .build();
        instanceProperties.set(TABLE_BATCHER_LAMBDA_FUNCTION, tableBatcherHandler.getFunctionName());
        instanceProperties.set(TABLE_BATCHER_CLOUDWATCH_RULE, rule.getRuleName());

        // Function to create compaction jobs
        String functionName = Utils.truncateTo64Characters(String.join("-", "sleeper",
                instanceProperties.get(ID).toLowerCase(Locale.ROOT), "job-creator"));

        IFunction createJobsHandler = jobCreatorJar.buildFunction(this, "JobCreationLambda", builder -> builder
                .functionName(functionName)
                .description("Scan DynamoDB looking for files that need merging and create appropriate job specs in DynamoDB")
                .runtime(software.amazon.awscdk.services.lambda.Runtime.JAVA_11)
                .memorySize(instanceProperties.getInt(COMPACTION_JOB_CREATION_LAMBDA_MEMORY_IN_MB))
                .timeout(Duration.seconds(instanceProperties.getInt(COMPACTION_JOB_CREATION_LAMBDA_TIMEOUT_IN_SECONDS)))
                .handler("sleeper.compaction.job.creation.CreateJobsLambda::eventHandler")
                .environment(environmentVariables)
                .reservedConcurrentExecutions(1)
                .logRetention(Utils.getRetentionDays(instanceProperties.getInt(LOG_RETENTION_IN_DAYS)))
                .events(List.of(new SqsEventSource(tableBatcherQueue))));

        // Grant this function permission to read from / write to the DynamoDB table
        configBucket.grantRead(createJobsHandler);
        jarsBucket.grantRead(createJobsHandler);
        stateStoreStacks.grantReadPartitionsReadWriteActiveFiles(createJobsHandler);
        eventStore.grantWriteJobEvent(createJobsHandler);
        tableBatcherQueue.grantConsumeMessages(createJobsHandler);

        // Grant this function permission to put messages on the compaction
        // queue and the compaction splitting queue
        compactionMergeJobsQueue.grantSendMessages(createJobsHandler);
        compactionSplittingMergeJobsQueue.grantSendMessages(createJobsHandler);

        instanceProperties.set(COMPACTION_JOB_CREATION_LAMBDA_FUNCTION, createJobsHandler.getFunctionName());
    }

    private Cluster ecsClusterForCompactionTasks(IBucket configBucket,
                                                 IBucket jarsBucket,
                                                 LambdaCode taskCreatorJar,
                                                 StateStoreStacks stateStoreStacks, TableDataStack dataStack,
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

        IRepository repository = Repository.fromRepositoryName(this, "ECR1",
                instanceProperties.get(ECR_COMPACTION_REPO));
        ContainerImage containerImage = ContainerImage.fromEcrRepository(repository, instanceProperties.get(VERSION));

        Map<String, String> environmentVariables = Utils.createDefaultEnvironment(instanceProperties);
        environmentVariables.put(Utils.AWS_REGION, instanceProperties.get(REGION));

        Consumer<ITaskDefinition> grantPermissions = taskDef -> {
            configBucket.grantRead(taskDef.getTaskRole());
            jarsBucket.grantRead(taskDef.getTaskRole());
            dataStack.getDataBucket().grantReadWrite(taskDef.getTaskRole());
            stateStoreStacks.grantReadWriteActiveAndReadyForGCFiles(taskDef.getTaskRole());
            eventStore.grantWriteJobEvent(taskDef.getTaskRole());
            eventStore.grantWriteTaskEvent(taskDef.getTaskRole());

            taskDef.getTaskRole().addToPrincipalPolicy(PolicyStatement.Builder
                    .create()
                    .resources(Collections.singletonList("*"))
                    .actions(List.of("ecs:DescribeContainerInstances"))
                    .build());

            compactionMergeJobsQueue.grantConsumeMessages(taskDef.getTaskRole());
        };

        String launchType = instanceProperties.get(COMPACTION_ECS_LAUNCHTYPE);
        if (launchType.equalsIgnoreCase("FARGATE")) {
            FargateTaskDefinition fargateTaskDefinition = compactionFargateTaskDefinition("Merge");
            String fargateTaskDefinitionFamily = fargateTaskDefinition.getFamily();
            instanceProperties.set(COMPACTION_TASK_FARGATE_DEFINITION_FAMILY, fargateTaskDefinitionFamily);
            ContainerDefinitionOptions fargateContainerDefinitionOptions = createFargateContainerDefinition(containerImage,
                    environmentVariables, instanceProperties, "Merge");
            fargateTaskDefinition.addContainer(ContainerConstants.COMPACTION_CONTAINER_NAME,
                    fargateContainerDefinitionOptions);
            grantPermissions.accept(fargateTaskDefinition);
        } else {
            Ec2TaskDefinition ec2TaskDefinition = compactionEC2TaskDefinition("Merge");
            String ec2TaskDefinitionFamily = ec2TaskDefinition.getFamily();
            instanceProperties.set(COMPACTION_TASK_EC2_DEFINITION_FAMILY, ec2TaskDefinitionFamily);
            ContainerDefinitionOptions ec2ContainerDefinitionOptions = createEC2ContainerDefinition(containerImage,
                    environmentVariables, instanceProperties, "Merge");
            ec2TaskDefinition.addContainer(ContainerConstants.COMPACTION_CONTAINER_NAME, ec2ContainerDefinitionOptions);
            grantPermissions.accept(ec2TaskDefinition);
            addEC2CapacityProvider(cluster, "MergeCompaction", vpc, COMPACTION_AUTO_SCALING_GROUP, configBucket, taskCreatorJar, "compaction");
        }

        CfnOutputProps compactionClusterProps = new CfnOutputProps.Builder()
                .value(cluster.getClusterName())
                .build();
        new CfnOutput(this, COMPACTION_CLUSTER_NAME, compactionClusterProps);

        return cluster;
    }

    private Cluster ecsClusterForSplittingCompactionTasks(IBucket configBucket,
                                                          IBucket jarsBucket,
                                                          LambdaCode taskCreatorJar,
                                                          StateStoreStacks stateStoreStacks, TableDataStack dataStack,
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

        IRepository repository = Repository.fromRepositoryName(this, "ECR2",
                instanceProperties.get(ECR_COMPACTION_REPO));
        ContainerImage containerImage = ContainerImage.fromEcrRepository(repository, instanceProperties.get(VERSION));

        Map<String, String> environmentVariables = Utils.createDefaultEnvironment(instanceProperties);
        environmentVariables.put(Utils.AWS_REGION, instanceProperties.get(REGION));

        Consumer<ITaskDefinition> grantPermissions = taskDef -> {
            configBucket.grantRead(taskDef.getTaskRole());
            jarsBucket.grantRead(taskDef.getTaskRole());
            dataStack.getDataBucket().grantReadWrite(taskDef.getTaskRole());
            stateStoreStacks.grantReadWriteActiveAndReadyForGCFiles(taskDef.getTaskRole());
            eventStore.grantWriteJobEvent(taskDef.getTaskRole());
            eventStore.grantWriteTaskEvent(taskDef.getTaskRole());

            taskDef.getTaskRole().addToPrincipalPolicy(PolicyStatement.Builder
                    .create()
                    .resources(Collections.singletonList("*"))
                    .actions(List.of("ecs:DescribeContainerInstances"))
                    .build());

            compactionSplittingMergeJobsQueue.grantConsumeMessages(taskDef.getTaskRole());
        };

        String launchType = instanceProperties.get(COMPACTION_ECS_LAUNCHTYPE);

        if (launchType.equalsIgnoreCase("FARGATE")) {
            FargateTaskDefinition fargateTaskDefinition = compactionFargateTaskDefinition("SplittingMerge");
            String fargateTaskDefinitionFamily = fargateTaskDefinition.getFamily();
            instanceProperties.set(SPLITTING_COMPACTION_TASK_FARGATE_DEFINITION_FAMILY, fargateTaskDefinitionFamily);
            ContainerDefinitionOptions fargateContainerDefinitionOptions = createFargateContainerDefinition(containerImage,
                    environmentVariables, instanceProperties, "SplittingMerge");
            fargateTaskDefinition.addContainer(ContainerConstants.SPLITTING_COMPACTION_CONTAINER_NAME,
                    fargateContainerDefinitionOptions);
            grantPermissions.accept(fargateTaskDefinition);
        } else {
            Ec2TaskDefinition ec2TaskDefinition = compactionEC2TaskDefinition("SplittingMerge");
            String ec2TaskDefinitionFamily = ec2TaskDefinition.getFamily();
            instanceProperties.set(SPLITTING_COMPACTION_TASK_EC2_DEFINITION_FAMILY, ec2TaskDefinitionFamily);
            ContainerDefinitionOptions ec2ContainerDefinitionOptions = createEC2ContainerDefinition(containerImage,
                    environmentVariables, instanceProperties, "SplittingMerge");
            ec2TaskDefinition.addContainer(ContainerConstants.SPLITTING_COMPACTION_CONTAINER_NAME,
                    ec2ContainerDefinitionOptions);
            grantPermissions.accept(ec2TaskDefinition);
            addEC2CapacityProvider(cluster, "SplittingMergeCompaction", vpc, SPLITTING_COMPACTION_AUTO_SCALING_GROUP, configBucket, taskCreatorJar, "splittingcompaction");
        }

        CfnOutputProps splittingCompactionClusterProps = new CfnOutputProps.Builder()
                .value(cluster.getClusterName())
                .build();
        new CfnOutput(this, SPLITTING_COMPACTION_CLUSTER_NAME, splittingCompactionClusterProps);

        return cluster;
    }

    @SuppressFBWarnings("NP_NULL_ON_SOME_PATH_FROM_RETURN_VALUE")
    private void addEC2CapacityProvider(Cluster cluster, String clusterName, IVpc vpc,
                                        SystemDefinedInstanceProperty scalingProperty, IBucket configBucket, LambdaCode taskCreatorJar, String type) {

        // Create some extra user data to enable ECS container metadata file
        UserData customUserData = UserData.forLinux();
        customUserData.addCommands("echo ECS_ENABLE_CONTAINER_METADATA=true >> /etc/ecs/ecs.config");

        AutoScalingGroup ec2scalingGroup = AutoScalingGroup.Builder.create(this, clusterName + "ScalingGroup").vpc(vpc)
                .allowAllOutbound(true)
                .associatePublicIpAddress(false)
                .requireImdsv2(true)
                .userData(customUserData)
                .blockDevices(List.of(BlockDevice.builder()
                        .deviceName("/dev/xvda") // root volume
                        .volume(BlockDeviceVolume.ebs(instanceProperties.getInt(COMPACTION_EC2_ROOT_SIZE),
                                EbsDeviceOptions.builder()
                                        .deleteOnTermination(true)
                                        .encrypted(true)
                                        .volumeType(EbsDeviceVolumeType.GP2)
                                        .build()))
                        .build()))
                .minCapacity(instanceProperties.getInt(COMPACTION_EC2_POOL_MINIMUM))
                .desiredCapacity(instanceProperties.getInt(COMPACTION_EC2_POOL_DESIRED))
                .maxCapacity(instanceProperties.getInt(COMPACTION_EC2_POOL_MAXIMUM)).requireImdsv2(true)
                .instanceType(lookupEC2InstanceType(instanceProperties.get(COMPACTION_EC2_TYPE)))
                .machineImage(EcsOptimizedImage.amazonLinux2(AmiHardwareType.STANDARD,
                        EcsOptimizedImageOptions.builder()
                                .cachedInContext(false)
                                .build()))
                .build();

        IFunction customTermination = lambdaForCustomTerminationPolicy(configBucket, taskCreatorJar, type);
        // Set this by accessing underlying CloudFormation as CDK doesn't yet support custom
        // lambda termination policies: https://github.com/aws/aws-cdk/issues/19750
        ((CfnAutoScalingGroup) ec2scalingGroup.getNode().getDefaultChild()).setTerminationPolicies(
                List.of(customTermination.getFunctionArn()));

        customTermination.addPermission("AutoscalingCall", Permission.builder()
                .action("lambda:InvokeFunction")
                .principal(Role.fromRoleArn(this, type + "_role_arn", "arn:aws:iam::" + instanceProperties.get(ACCOUNT)
                        + ":role/aws-service-role/autoscaling.amazonaws.com/AWSServiceRoleForAutoScaling"))
                .build());

        AsgCapacityProvider ec2Provider = AsgCapacityProvider.Builder
                .create(this, clusterName + "CapacityProvider")
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

        instanceProperties.set(scalingProperty, ec2scalingGroup.getAutoScalingGroupName());
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

    private FargateTaskDefinition compactionFargateTaskDefinition(String compactionTypeName) {
        String architecture = instanceProperties.get(COMPACTION_TASK_CPU_ARCHITECTURE).toUpperCase(Locale.ROOT);
        String launchType = instanceProperties.get(COMPACTION_ECS_LAUNCHTYPE);
        Pair<Integer, Integer> requirements = Requirements.getArchRequirements(architecture, launchType,
                instanceProperties);
        return FargateTaskDefinition.Builder
                .create(this, compactionTypeName + "CompactionFargateTaskDefinition")
                .family(instanceProperties.get(ID) + compactionTypeName + "CompactionFargateTaskFamily")
                .cpu(requirements.getLeft())
                .memoryLimitMiB(requirements.getRight())
                .runtimePlatform(RuntimePlatform.builder()
                        .cpuArchitecture(CpuArchitecture.of(architecture))
                        .operatingSystemFamily(OperatingSystemFamily.LINUX)
                        .build())
                .build();
    }

    private Ec2TaskDefinition compactionEC2TaskDefinition(String compactionTypeName) {
        return Ec2TaskDefinition.Builder
                .create(this, compactionTypeName + "CompactionEC2TaskDefinition")
                .family(instanceProperties.get(ID) + compactionTypeName + "CompactionEC2TaskFamily")
                .networkMode(NetworkMode.BRIDGE)
                .build();
    }

    private ContainerDefinitionOptions createFargateContainerDefinition(ContainerImage image,
                                                                        Map<String, String> environment, InstanceProperties instanceProperties, String compactionTypeName) {
        String architecture = instanceProperties.get(COMPACTION_TASK_CPU_ARCHITECTURE).toUpperCase(Locale.ROOT);
        String launchType = instanceProperties.get(COMPACTION_ECS_LAUNCHTYPE);
        Pair<Integer, Integer> requirements = Requirements.getArchRequirements(architecture, launchType,
                instanceProperties);
        return ContainerDefinitionOptions.builder()
                .image(image)
                .environment(environment)
                .cpu(requirements.getLeft())
                .memoryLimitMiB(requirements.getRight())
                .logging(Utils.createECSContainerLogDriver(this, instanceProperties,
                        compactionTypeName + "FargateCompactionTasks"))
                .build();
    }

    private ContainerDefinitionOptions createEC2ContainerDefinition(ContainerImage image,
                                                                    Map<String, String> environment, InstanceProperties instanceProperties, String compactionTypeName) {
        String architecture = instanceProperties.get(COMPACTION_TASK_CPU_ARCHITECTURE).toUpperCase(Locale.ROOT);
        String launchType = instanceProperties.get(COMPACTION_ECS_LAUNCHTYPE);
        Pair<Integer, Integer> requirements = Requirements.getArchRequirements(architecture, launchType,
                instanceProperties);
        return ContainerDefinitionOptions.builder()
                .image(image)
                .environment(environment)
                .cpu(requirements.getLeft())
                // bit hacky: Reduce memory requirement for EC2 to prevent
                // container allocation failing when we need almost entire resources
                // of machine
                .memoryLimitMiB((int) (requirements.getRight() * 0.95))
                .logging(Utils.createECSContainerLogDriver(this, instanceProperties,
                        compactionTypeName + "EC2CompactionTasks"))
                .build();
    }

    @SuppressFBWarnings("NP_NULL_ON_SOME_PATH_FROM_RETURN_VALUE")
    private IFunction lambdaForCustomTerminationPolicy(IBucket configBucket, LambdaCode taskCreatorJar, String type) {
        if (!Arrays.asList("splittingcompaction", "compaction").contains(type)) {
            throw new IllegalArgumentException("type must be splittingcompaction or compaction");
        }

        // Run tasks function
        Map<String, String> environmentVariables = Utils.createDefaultEnvironment(instanceProperties);
        environmentVariables.put("type", type);

        String functionName = Utils.truncateTo64Characters(String.join("-", "sleeper",
                instanceProperties.get(ID).toLowerCase(Locale.ROOT), type, "custom-termination"));

        IFunction handler = taskCreatorJar.buildFunction(this, type + "-custom-termination", builder -> builder
                .functionName(functionName)
                .description("Custom termination policy for ECS auto scaling group. Only terminate empty instances.")
                .environment(environmentVariables)
                .handler("sleeper.compaction.taskcreation.SafeTerminationLambda::handleRequest")
                .logRetention(Utils.getRetentionDays(instanceProperties.getInt(LOG_RETENTION_IN_DAYS)))
                .memorySize(512)
                .runtime(software.amazon.awscdk.services.lambda.Runtime.JAVA_11)
                .timeout(Duration.seconds(10)));

        // Grant read to the config bucket
        configBucket.grantRead(handler);
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
    private void lambdaToCreateCompactionTasks(IBucket configBucket,
                                               LambdaCode taskCreatorJar,
                                               Queue compactionMergeJobsQueue) {
        // Run tasks function
        Map<String, String> environmentVariables = Utils.createDefaultEnvironment(instanceProperties);
        environmentVariables.put("type", "compaction");

        String functionName = Utils.truncateTo64Characters(String.join("-", "sleeper",
                instanceProperties.get(ID).toLowerCase(Locale.ROOT), "compaction-tasks-creator"));

        IFunction handler = taskCreatorJar.buildFunction(this, "CompactionTasksCreator", builder -> builder
                .functionName(functionName)
                .description("If there are compaction jobs on queue create tasks to run them")
                .runtime(software.amazon.awscdk.services.lambda.Runtime.JAVA_11)
                .memorySize(instanceProperties.getInt(TASK_RUNNER_LAMBDA_MEMORY_IN_MB))
                .timeout(Duration.seconds(instanceProperties.getInt(TASK_RUNNER_LAMBDA_TIMEOUT_IN_SECONDS)))
                .handler("sleeper.compaction.taskcreation.RunTasksLambda::eventHandler")
                .environment(environmentVariables)
                .reservedConcurrentExecutions(1)
                .logRetention(Utils.getRetentionDays(instanceProperties.getInt(LOG_RETENTION_IN_DAYS))));

        // Grant this function permission to read from the S3 bucket
        configBucket.grantRead(handler);

        // Grant this function permission to query the queue for number of messages
        compactionMergeJobsQueue.grantSendMessages(handler);
        compactionMergeJobsQueue.grant(handler, "sqs:GetQueueAttributes");

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
                .create(this, "CompactionMergeTasksCreationPeriodicTrigger")
                .ruleName(SleeperScheduleRule.COMPACTION_TASK_CREATION.buildRuleName(instanceProperties))
                .description("A rule to periodically trigger the compaction tasks lambda")
                .enabled(!shouldDeployPaused(this))
                .schedule(Schedule.rate(Duration.minutes(instanceProperties.getInt(COMPACTION_TASK_CREATION_PERIOD_IN_MINUTES))))
                .targets(Collections.singletonList(new LambdaFunction(handler)))
                .build();
        instanceProperties.set(COMPACTION_TASK_CREATION_LAMBDA_FUNCTION, handler.getFunctionName());
        instanceProperties.set(COMPACTION_TASK_CREATION_CLOUDWATCH_RULE, rule.getRuleName());
    }

    @SuppressFBWarnings("NP_NULL_ON_SOME_PATH_FROM_RETURN_VALUE")
    private void lambdaToCreateSplittingCompactionTasks(IBucket configBucket,
                                                        LambdaCode taskCreatorJar,
                                                        Queue compactionSplittingMergeJobsQueue) {
        // Run tasks function
        Map<String, String> environmentVariables = Utils.createDefaultEnvironment(instanceProperties);
        environmentVariables.put("type", "splittingcompaction");

        String functionName = Utils.truncateTo64Characters(String.join("-", "sleeper",
                instanceProperties.get(ID).toLowerCase(Locale.ROOT), "splitting-compaction-tasks-creator"));

        IFunction handler = taskCreatorJar.buildFunction(this, "SplittingCompactionTasksCreator", builder -> builder
                .functionName(functionName)
                .description("If there are splitting compaction jobs on queue create tasks to run them")
                .runtime(software.amazon.awscdk.services.lambda.Runtime.JAVA_11)
                .memorySize(instanceProperties.getInt(TASK_RUNNER_LAMBDA_MEMORY_IN_MB))
                .timeout(Duration.seconds(instanceProperties.getInt(TASK_RUNNER_LAMBDA_TIMEOUT_IN_SECONDS)))
                .handler("sleeper.compaction.taskcreation.RunTasksLambda::eventHandler")
                .environment(environmentVariables)
                .reservedConcurrentExecutions(1)
                .logRetention(Utils.getRetentionDays(instanceProperties.getInt(LOG_RETENTION_IN_DAYS))));

        // Grant this function permission to read from the config S3 bucket
        configBucket.grantRead(handler);

        // Grant this function permission to query the queue for number of messages
        compactionSplittingMergeJobsQueue.grantSendMessages(handler);
        compactionSplittingMergeJobsQueue.grant(handler, "sqs:GetQueueAttributes");

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
                .create(this, "CompactionSplittingMergeTasksCreationPeriodicTrigger")
                .ruleName(SleeperScheduleRule.SPLITTING_COMPACTION_TASK_CREATION.buildRuleName(instanceProperties))
                .description("A rule to periodically trigger the splitting compaction tasks lambda")
                .enabled(!shouldDeployPaused(this))
                .schedule(Schedule.rate(Duration.minutes(instanceProperties.getInt(COMPACTION_TASK_CREATION_PERIOD_IN_MINUTES))))
                .targets(Collections.singletonList(new LambdaFunction(handler)))
                .build();
        instanceProperties.set(SPLITTING_COMPACTION_TASK_CREATION_LAMBDA_FUNCTION, handler.getFunctionName());
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
