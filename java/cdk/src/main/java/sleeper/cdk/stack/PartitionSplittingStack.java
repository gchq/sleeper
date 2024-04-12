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

import software.amazon.awscdk.CfnOutput;
import software.amazon.awscdk.CfnOutputProps;
import software.amazon.awscdk.Duration;
import software.amazon.awscdk.NestedStack;
import software.amazon.awscdk.services.events.Rule;
import software.amazon.awscdk.services.events.Schedule;
import software.amazon.awscdk.services.events.targets.LambdaFunction;
import software.amazon.awscdk.services.lambda.IFunction;
import software.amazon.awscdk.services.lambda.eventsources.SqsEventSource;
import software.amazon.awscdk.services.lambda.eventsources.SqsEventSourceProps;
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
import sleeper.configuration.properties.SleeperScheduleRule;
import sleeper.configuration.properties.instance.InstanceProperties;

import java.util.List;
import java.util.Locale;
import java.util.Map;

import static sleeper.cdk.Utils.createAlarmForDlq;
import static sleeper.cdk.Utils.createLambdaLogGroup;
import static sleeper.cdk.Utils.shouldDeployPaused;
import static sleeper.configuration.properties.instance.CdkDefinedInstanceProperty.PARTITION_SPLITTING_CLOUDWATCH_RULE;
import static sleeper.configuration.properties.instance.CdkDefinedInstanceProperty.PARTITION_SPLITTING_JOB_DLQ_ARN;
import static sleeper.configuration.properties.instance.CdkDefinedInstanceProperty.PARTITION_SPLITTING_JOB_DLQ_URL;
import static sleeper.configuration.properties.instance.CdkDefinedInstanceProperty.PARTITION_SPLITTING_JOB_QUEUE_ARN;
import static sleeper.configuration.properties.instance.CdkDefinedInstanceProperty.PARTITION_SPLITTING_JOB_QUEUE_URL;
import static sleeper.configuration.properties.instance.CdkDefinedInstanceProperty.PARTITION_SPLITTING_TABLE_BATCH_DLQ_ARN;
import static sleeper.configuration.properties.instance.CdkDefinedInstanceProperty.PARTITION_SPLITTING_TABLE_BATCH_DLQ_URL;
import static sleeper.configuration.properties.instance.CdkDefinedInstanceProperty.PARTITION_SPLITTING_TABLE_BATCH_QUEUE_ARN;
import static sleeper.configuration.properties.instance.CdkDefinedInstanceProperty.PARTITION_SPLITTING_TABLE_BATCH_QUEUE_URL;
import static sleeper.configuration.properties.instance.CdkDefinedInstanceProperty.PARTITION_SPLITTING_TRIGGER_LAMBDA_FUNCTION;
import static sleeper.configuration.properties.instance.CommonProperty.ID;
import static sleeper.configuration.properties.instance.CommonProperty.TABLE_BATCHING_LAMBDAS_MEMORY_IN_MB;
import static sleeper.configuration.properties.instance.CommonProperty.TABLE_BATCHING_LAMBDAS_TIMEOUT_IN_SECONDS;
import static sleeper.configuration.properties.instance.PartitionSplittingProperty.FIND_PARTITIONS_TO_SPLIT_LAMBDA_MEMORY_IN_MB;
import static sleeper.configuration.properties.instance.PartitionSplittingProperty.FIND_PARTITIONS_TO_SPLIT_TIMEOUT_IN_SECONDS;
import static sleeper.configuration.properties.instance.PartitionSplittingProperty.PARTITION_SPLITTING_TRIGGER_PERIOD_IN_MINUTES;
import static sleeper.configuration.properties.instance.PartitionSplittingProperty.SPLIT_PARTITIONS_LAMBDA_MEMORY_IN_MB;
import static sleeper.configuration.properties.instance.PartitionSplittingProperty.SPLIT_PARTITIONS_TIMEOUT_IN_SECONDS;

/**
 * Deploys resources to perform partition splitting. A CloudWatch rule will periodically trigger to check every Sleeper
 * table for partitions that need splitting, and split them.
 */
public class PartitionSplittingStack extends NestedStack {
    public static final String PARTITION_SPLITTING_QUEUE_URL = "PartitionSplittingQueueUrl";
    public static final String PARTITION_SPLITTING_DL_QUEUE_URL = "PartitionSplittingDLQueueUrl";
    private final Queue partitionSplittingJobQueue;
    private final Queue partitionSplittingJobDlq;
    private final Queue partitionSplittingBatchQueue;

    public PartitionSplittingStack(Construct scope,
            String id,
            InstanceProperties instanceProperties,
            BuiltJars jars,
            Topic topic,
            CoreStacks coreStacks) {
        super(scope, id);

        // Jars bucket
        IBucket jarsBucket = Bucket.fromBucketName(this, "JarsBucket", jars.bucketName());

        // Create queue for batching tables
        QueueAndDlq batchQueueAndDlq = createBatchQueues(instanceProperties);
        this.partitionSplittingBatchQueue = batchQueueAndDlq.queue;
        // Create queue for partition splitting job definitions
        QueueAndDlq jobQueueAndDlq = createJobQueues(instanceProperties, topic, coreStacks);
        this.partitionSplittingJobQueue = jobQueueAndDlq.queue;
        this.partitionSplittingJobDlq = jobQueueAndDlq.dlq;

        // Partition splitting code
        LambdaCode splitterJar = jars.lambdaCode(BuiltJar.PARTITION_SPLITTER, jarsBucket);
        Map<String, String> environmentVariables = Utils.createDefaultEnvironment(instanceProperties);

        // Lambda to batch tables and put requests on the batch SQS queue, to be consumed by FindPartitionsToSplit
        createTriggerFunction(instanceProperties, splitterJar, coreStacks, environmentVariables);

        // Lambda to look for partitions that need splitting (for each partition that
        // needs splitting it puts a definition of the splitting job onto a queue)
        createFindPartitionsToSplitFunction(instanceProperties, splitterJar, coreStacks, environmentVariables);

        // Lambda to split partitions (triggered by partition splitting job
        // arriving on partitionSplittingQueue)
        createSplitPartitionFunction(instanceProperties, splitterJar, coreStacks, environmentVariables);

        Utils.addStackTagIfSet(this, instanceProperties);
    }

    private QueueAndDlq createBatchQueues(InstanceProperties instanceProperties) {
        // Create queue for batching tables
        Queue partitionSplittingBatchDlq = Queue.Builder
                .create(this, "PartitionSplittingBatchDeadLetterQueue")
                .queueName(instanceProperties.get(ID) + "-PartitionSplittingBatchDLQueue")
                .build();
        Queue partitionSplittingBatchQueue = Queue.Builder
                .create(this, "PartitionSplittingBatchQueue")
                .queueName(instanceProperties.get(ID) + "-PartitionSplittingBatchQueue")
                .deadLetterQueue(
                        DeadLetterQueue.builder()
                                .maxReceiveCount(1)
                                .queue(partitionSplittingBatchDlq)
                                .build())
                .visibilityTimeout(Duration.seconds(instanceProperties.getInt(FIND_PARTITIONS_TO_SPLIT_TIMEOUT_IN_SECONDS)))
                .build();
        instanceProperties.set(PARTITION_SPLITTING_TABLE_BATCH_QUEUE_URL, partitionSplittingBatchQueue.getQueueUrl());
        instanceProperties.set(PARTITION_SPLITTING_TABLE_BATCH_QUEUE_ARN, partitionSplittingBatchQueue.getQueueArn());
        instanceProperties.set(PARTITION_SPLITTING_TABLE_BATCH_DLQ_URL, partitionSplittingBatchDlq.getQueueUrl());
        instanceProperties.set(PARTITION_SPLITTING_TABLE_BATCH_DLQ_ARN, partitionSplittingBatchDlq.getQueueArn());
        return new QueueAndDlq(partitionSplittingBatchQueue, partitionSplittingBatchDlq);
    }

    private QueueAndDlq createJobQueues(InstanceProperties instanceProperties, Topic topic, CoreStacks coreStacks) {
        // Create queue for partition splitting job definitions
        Queue partitionSplittingJobDlq = Queue.Builder
                .create(this, "PartitionSplittingDeadLetterQueue")
                .queueName(instanceProperties.get(ID) + "-PartitionSplittingJobDLQueue")
                .build();
        Queue partitionSplittingJobQueue = Queue.Builder
                .create(this, "PartitionSplittingJobQueue")
                .queueName(instanceProperties.get(ID) + "-PartitionSplittingJobQueue")
                .deadLetterQueue(DeadLetterQueue.builder()
                        .maxReceiveCount(1)
                        .queue(partitionSplittingJobDlq)
                        .build())
                .visibilityTimeout(Duration.seconds(instanceProperties.getInt(SPLIT_PARTITIONS_TIMEOUT_IN_SECONDS))) // TODO Needs to be >= function timeout
                .build();
        partitionSplittingJobQueue.grantPurge(coreStacks.getPurgeQueuesPolicy());
        instanceProperties.set(PARTITION_SPLITTING_JOB_QUEUE_URL, partitionSplittingJobQueue.getQueueUrl());
        instanceProperties.set(PARTITION_SPLITTING_JOB_QUEUE_ARN, partitionSplittingJobQueue.getQueueArn());
        instanceProperties.set(PARTITION_SPLITTING_JOB_DLQ_URL, partitionSplittingJobDlq.getQueueUrl());
        instanceProperties.set(PARTITION_SPLITTING_JOB_DLQ_ARN, partitionSplittingJobDlq.getQueueArn());

        // Add alarm to send message to SNS if there are any messages on the dead letter queue
        createAlarmForDlq(this, "PartitionSplittingAlarm",
                "Alarms if there are any messages on the dead letter queue for the partition splitting queue",
                partitionSplittingJobDlq, topic);

        CfnOutputProps partitionSplittingQueueOutputProps = new CfnOutputProps.Builder()
                .value(partitionSplittingJobQueue.getQueueUrl())
                .build();
        new CfnOutput(this, PARTITION_SPLITTING_QUEUE_URL, partitionSplittingQueueOutputProps);

        CfnOutputProps partitionSplittingDLQueueOutputProps = new CfnOutputProps.Builder()
                .value(partitionSplittingJobDlq.getQueueUrl())
                .build();
        new CfnOutput(this, PARTITION_SPLITTING_DL_QUEUE_URL, partitionSplittingDLQueueOutputProps);
        return new QueueAndDlq(partitionSplittingJobQueue, partitionSplittingJobDlq);
    }

    private void createTriggerFunction(InstanceProperties instanceProperties, LambdaCode splitterJar, CoreStacks coreStacks, Map<String, String> environmentVariables) {
        String triggerFunctionName = Utils.truncateTo64Characters(String.join("-", "sleeper",
                instanceProperties.get(ID).toLowerCase(Locale.ROOT), "split-partition-trigger"));
        IFunction triggerFunction = splitterJar.buildFunction(this, "FindPartitionsToSplitTriggerLambda", builder -> builder
                .functionName(triggerFunctionName)
                .description("Creates batches of Sleeper tables to perform partition splitting for and puts them on a queue to be processed")
                .runtime(software.amazon.awscdk.services.lambda.Runtime.JAVA_11)
                .memorySize(instanceProperties.getInt(TABLE_BATCHING_LAMBDAS_MEMORY_IN_MB))
                .timeout(Duration.seconds(instanceProperties.getInt(TABLE_BATCHING_LAMBDAS_TIMEOUT_IN_SECONDS)))
                .handler("sleeper.splitter.lambda.FindPartitionsToSplitTriggerLambda::handleRequest")
                .environment(environmentVariables)
                .reservedConcurrentExecutions(1)
                .logGroup(createLambdaLogGroup(this, "FindPartitionsToSplitTriggerLogGroup", triggerFunctionName, instanceProperties)));
        // Cloudwatch rule to trigger this lambda
        Rule rule = Rule.Builder
                .create(this, "FindPartitionsToSplitPeriodicTrigger")
                .ruleName(SleeperScheduleRule.PARTITION_SPLITTING.buildRuleName(instanceProperties))
                .description("A rule to periodically trigger the lambda to look for partitions to split")
                .enabled(!shouldDeployPaused(this))
                .schedule(Schedule.rate(Duration.minutes(instanceProperties.getInt(PARTITION_SPLITTING_TRIGGER_PERIOD_IN_MINUTES))))
                .targets(List.of(new LambdaFunction(triggerFunction)))
                .build();
        instanceProperties.set(PARTITION_SPLITTING_TRIGGER_LAMBDA_FUNCTION, triggerFunction.getFunctionName());
        instanceProperties.set(PARTITION_SPLITTING_CLOUDWATCH_RULE, rule.getRuleName());

        coreStacks.grantReadTablesStatus(triggerFunction);
        partitionSplittingBatchQueue.grantSendMessages(triggerFunction);
        triggerFunction.grantInvoke(coreStacks.getInvokeSchedulesPolicy());
        partitionSplittingBatchQueue.grantSendMessages(coreStacks.getInvokeSchedulesPolicy());
    }

    private void createFindPartitionsToSplitFunction(InstanceProperties instanceProperties, LambdaCode splitterJar, CoreStacks coreStacks, Map<String, String> environmentVariables) {
        String functionName = Utils.truncateTo64Characters(String.join("-", "sleeper",
                instanceProperties.get(ID).toLowerCase(Locale.ROOT), "find-partitions-to-split"));
        IFunction findPartitionsToSplitLambda = splitterJar.buildFunction(this, "FindPartitionsToSplitLambda", builder -> builder
                .functionName(functionName)
                .description("Scan DynamoDB looking for partitions that need splitting")
                .runtime(software.amazon.awscdk.services.lambda.Runtime.JAVA_11)
                .memorySize(instanceProperties.getInt(FIND_PARTITIONS_TO_SPLIT_LAMBDA_MEMORY_IN_MB))
                .timeout(Duration.seconds(instanceProperties.getInt(FIND_PARTITIONS_TO_SPLIT_TIMEOUT_IN_SECONDS)))
                .handler("sleeper.splitter.lambda.FindPartitionsToSplitLambda::handleRequest")
                .environment(environmentVariables)
                .logGroup(createLambdaLogGroup(this, "FindPartitionsToSplitLogGroup", functionName, instanceProperties)));

        coreStacks.grantReadTablesMetadata(findPartitionsToSplitLambda);
        partitionSplittingJobQueue.grantSendMessages(findPartitionsToSplitLambda);
        findPartitionsToSplitLambda.addEventSource(new SqsEventSource(partitionSplittingBatchQueue,
                SqsEventSourceProps.builder().batchSize(1).build()));
    }

    private void createSplitPartitionFunction(InstanceProperties instanceProperties, LambdaCode splitterJar, CoreStacks coreStacks, Map<String, String> environmentVariables) {
        String splitFunctionName = Utils.truncateTo64Characters(String.join("-", "sleeper",
                instanceProperties.get(ID).toLowerCase(Locale.ROOT), "split-partition"));

        // Lambda to split partitions (triggered by partition splitting job
        // arriving on partitionSplittingQueue)
        IFunction splitPartitionLambda = splitterJar.buildFunction(this, "SplitPartitionLambda", builder -> builder
                .functionName(splitFunctionName)
                .description("Triggered by an SQS event that contains a partition to split")
                .runtime(software.amazon.awscdk.services.lambda.Runtime.JAVA_11)
                .memorySize(instanceProperties.getInt(SPLIT_PARTITIONS_LAMBDA_MEMORY_IN_MB))
                .timeout(Duration.seconds(instanceProperties.getInt(SPLIT_PARTITIONS_TIMEOUT_IN_SECONDS)))
                .handler("sleeper.splitter.lambda.SplitPartitionLambda::handleRequest")
                .environment(environmentVariables)
                .logGroup(createLambdaLogGroup(this, "SplitPartitionLogGroup", splitFunctionName, instanceProperties)));

        coreStacks.grantSplitPartitions(splitPartitionLambda);
        splitPartitionLambda.addEventSource(new SqsEventSource(partitionSplittingJobQueue,
                SqsEventSourceProps.builder().batchSize(1).build()));
    }

    private static class QueueAndDlq {
        private Queue queue;
        private Queue dlq;

        QueueAndDlq(Queue queue, Queue dlq) {
            this.queue = queue;
            this.dlq = dlq;
        }

    }

    public Queue getJobQueue() {
        return partitionSplittingJobQueue;
    }

    public Queue getDeadLetterQueue() {
        return partitionSplittingJobDlq;
    }
}
