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
import software.amazon.awscdk.services.cloudwatch.IMetric;
import software.amazon.awscdk.services.events.Rule;
import software.amazon.awscdk.services.events.Schedule;
import software.amazon.awscdk.services.events.targets.LambdaFunction;
import software.amazon.awscdk.services.lambda.IFunction;
import software.amazon.awscdk.services.lambda.Runtime;
import software.amazon.awscdk.services.lambda.eventsources.SqsEventSource;
import software.amazon.awscdk.services.s3.Bucket;
import software.amazon.awscdk.services.s3.IBucket;
import software.amazon.awscdk.services.sns.Topic;
import software.amazon.awscdk.services.sqs.DeadLetterQueue;
import software.amazon.awscdk.services.sqs.IQueue;
import software.amazon.awscdk.services.sqs.Queue;
import software.constructs.Construct;

import sleeper.cdk.jars.BuiltJars;
import sleeper.cdk.jars.LambdaCode;
import sleeper.cdk.jars.LambdaJar;
import sleeper.cdk.util.Utils;
import sleeper.core.deploy.SleeperScheduleRule;
import sleeper.core.properties.instance.InstanceProperties;

import java.util.List;
import java.util.Map;

import static sleeper.cdk.util.Utils.createAlarmForDlq;
import static sleeper.cdk.util.Utils.shouldDeployPaused;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.FIND_PARTITIONS_TO_SPLIT_DLQ_ARN;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.FIND_PARTITIONS_TO_SPLIT_DLQ_URL;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.FIND_PARTITIONS_TO_SPLIT_QUEUE_ARN;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.FIND_PARTITIONS_TO_SPLIT_QUEUE_URL;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.PARTITION_SPLITTING_CLOUDWATCH_RULE;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.PARTITION_SPLITTING_JOB_DLQ_ARN;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.PARTITION_SPLITTING_JOB_DLQ_URL;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.PARTITION_SPLITTING_JOB_QUEUE_ARN;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.PARTITION_SPLITTING_JOB_QUEUE_URL;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.PARTITION_SPLITTING_TRIGGER_LAMBDA_FUNCTION;
import static sleeper.core.properties.instance.CommonProperty.TABLE_BATCHING_LAMBDAS_MEMORY_IN_MB;
import static sleeper.core.properties.instance.CommonProperty.TABLE_BATCHING_LAMBDAS_TIMEOUT_IN_SECONDS;
import static sleeper.core.properties.instance.PartitionSplittingProperty.FIND_PARTITIONS_TO_SPLIT_BATCH_SIZE;
import static sleeper.core.properties.instance.PartitionSplittingProperty.FIND_PARTITIONS_TO_SPLIT_LAMBDA_CONCURRENCY_MAXIMUM;
import static sleeper.core.properties.instance.PartitionSplittingProperty.FIND_PARTITIONS_TO_SPLIT_LAMBDA_CONCURRENCY_RESERVED;
import static sleeper.core.properties.instance.PartitionSplittingProperty.FIND_PARTITIONS_TO_SPLIT_LAMBDA_MEMORY_IN_MB;
import static sleeper.core.properties.instance.PartitionSplittingProperty.FIND_PARTITIONS_TO_SPLIT_TIMEOUT_IN_SECONDS;
import static sleeper.core.properties.instance.PartitionSplittingProperty.PARTITION_SPLITTING_TRIGGER_PERIOD_IN_MINUTES;
import static sleeper.core.properties.instance.PartitionSplittingProperty.SPLIT_PARTITIONS_LAMBDA_MEMORY_IN_MB;
import static sleeper.core.properties.instance.PartitionSplittingProperty.SPLIT_PARTITIONS_RESERVED_CONCURRENCY;
import static sleeper.core.properties.instance.PartitionSplittingProperty.SPLIT_PARTITIONS_TIMEOUT_IN_SECONDS;

/**
 * Deploys resources to perform partition splitting. A CloudWatch rule will periodically trigger to check every Sleeper
 * table for partitions that need splitting, and split them.
 */
public class PartitionSplittingStack extends NestedStack {
    public static final String PARTITION_SPLITTING_QUEUE_URL = "PartitionSplittingQueueUrl";
    public static final String PARTITION_SPLITTING_DLQ_URL = "PartitionSplittingDLQUrl";
    private final Queue partitionSplittingJobQueue;
    private final Queue findPartitionsToSplitQueue;

    public PartitionSplittingStack(Construct scope,
            String id,
            InstanceProperties instanceProperties,
            BuiltJars jars,
            Topic topic,
            CoreStacks coreStacks,
            List<IMetric> errorMetrics) {
        super(scope, id);

        // Jars bucket
        IBucket jarsBucket = Bucket.fromBucketName(this, "JarsBucket", jars.bucketName());

        // Create queue for batching tables
        this.findPartitionsToSplitQueue = createBatchQueues(instanceProperties, topic, errorMetrics);
        // Create queue for partition splitting job definitions
        this.partitionSplittingJobQueue = createJobQueues(instanceProperties, topic, coreStacks, errorMetrics);

        // Partition splitting code
        LambdaCode splitterJar = jars.lambdaCode(LambdaJar.PARTITION_SPLITTER, jarsBucket);
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

    private Queue createBatchQueues(InstanceProperties instanceProperties, Topic topic, List<IMetric> errorMetrics) {
        // Create queue for batching tables
        String instanceId = Utils.cleanInstanceId(instanceProperties);
        Queue findPartitionsToSplitDlq = Queue.Builder
                .create(this, "FindPartitionsToSplitDeadLetterQueue")
                .queueName(String.join("-", "sleeper", instanceId, "FindPartitionsToSplitDLQ.fifo"))
                .fifo(true)
                .build();
        Queue findPartitionsToSplitQueue = Queue.Builder
                .create(this, "FindPartitionsToSplitBatchQueue")
                .queueName(String.join("-", "sleeper", instanceId, "FindPartitionsToSplitQ.fifo"))
                .deadLetterQueue(
                        DeadLetterQueue.builder()
                                .maxReceiveCount(1)
                                .queue(findPartitionsToSplitDlq)
                                .build())
                .fifo(true)
                .visibilityTimeout(Duration.seconds(instanceProperties.getInt(FIND_PARTITIONS_TO_SPLIT_TIMEOUT_IN_SECONDS)))
                .build();
        instanceProperties.set(FIND_PARTITIONS_TO_SPLIT_QUEUE_URL, findPartitionsToSplitQueue.getQueueUrl());
        instanceProperties.set(FIND_PARTITIONS_TO_SPLIT_QUEUE_ARN, findPartitionsToSplitQueue.getQueueArn());
        instanceProperties.set(FIND_PARTITIONS_TO_SPLIT_DLQ_URL, findPartitionsToSplitDlq.getQueueUrl());
        instanceProperties.set(FIND_PARTITIONS_TO_SPLIT_DLQ_ARN, findPartitionsToSplitDlq.getQueueArn());
        createAlarmForDlq(this, "FindPartitionsToSplitAlarm",
                "Alarms if there are any messages on the dead letter queue for finding partitions to split",
                findPartitionsToSplitDlq, topic);
        errorMetrics.add(Utils.createErrorMetric("Find Partitions To Split Errors", findPartitionsToSplitDlq, instanceProperties));
        return findPartitionsToSplitQueue;
    }

    private Queue createJobQueues(InstanceProperties instanceProperties, Topic topic, CoreStacks coreStacks, List<IMetric> errorMetrics) {
        // Create queue for partition splitting job definitions
        String instanceId = Utils.cleanInstanceId(instanceProperties);
        Queue partitionSplittingJobDlq = Queue.Builder
                .create(this, "PartitionSplittingDeadLetterQueue")
                .queueName(String.join("-", "sleeper", instanceId, "PartitionSplittingJobDLQ"))
                .build();
        Queue partitionSplittingJobQueue = Queue.Builder
                .create(this, "PartitionSplittingJobQueue")
                .queueName(String.join("-", "sleeper", instanceId, "PartitionSplittingJobQueue"))
                .deadLetterQueue(DeadLetterQueue.builder()
                        .maxReceiveCount(1)
                        .queue(partitionSplittingJobDlq)
                        .build())
                .visibilityTimeout(Duration.seconds(instanceProperties.getInt(SPLIT_PARTITIONS_TIMEOUT_IN_SECONDS))) // TODO Needs to be >= function timeout
                .build();
        partitionSplittingJobQueue.grantPurge(coreStacks.getPurgeQueuesPolicyForGrants());
        instanceProperties.set(PARTITION_SPLITTING_JOB_QUEUE_URL, partitionSplittingJobQueue.getQueueUrl());
        instanceProperties.set(PARTITION_SPLITTING_JOB_QUEUE_ARN, partitionSplittingJobQueue.getQueueArn());
        instanceProperties.set(PARTITION_SPLITTING_JOB_DLQ_URL, partitionSplittingJobDlq.getQueueUrl());
        instanceProperties.set(PARTITION_SPLITTING_JOB_DLQ_ARN, partitionSplittingJobDlq.getQueueArn());

        // Add alarm to send message to SNS if there are any messages on the dead letter queue
        createAlarmForDlq(this, "PartitionSplittingAlarm",
                "Alarms if there are any messages on the dead letter queue for the partition splitting queue",
                partitionSplittingJobDlq, topic);
        errorMetrics.add(Utils.createErrorMetric("Partition Split Errors", partitionSplittingJobDlq, instanceProperties));

        CfnOutputProps partitionSplittingQueueOutputProps = new CfnOutputProps.Builder()
                .value(partitionSplittingJobQueue.getQueueUrl())
                .build();
        new CfnOutput(this, PARTITION_SPLITTING_QUEUE_URL, partitionSplittingQueueOutputProps);

        CfnOutputProps partitionSplittingDLQueueOutputProps = new CfnOutputProps.Builder()
                .value(partitionSplittingJobDlq.getQueueUrl())
                .build();
        new CfnOutput(this, PARTITION_SPLITTING_DLQ_URL, partitionSplittingDLQueueOutputProps);
        return partitionSplittingJobQueue;
    }

    private void createTriggerFunction(InstanceProperties instanceProperties, LambdaCode splitterJar, CoreStacks coreStacks, Map<String, String> environmentVariables) {
        String triggerFunctionName = String.join("-", "sleeper",
                Utils.cleanInstanceId(instanceProperties), "partition-splitting-trigger");
        IFunction triggerFunction = splitterJar.buildFunction(this, "FindPartitionsToSplitTriggerLambda", builder -> builder
                .functionName(triggerFunctionName)
                .description("Creates batches of Sleeper tables to perform partition splitting for and puts them on a queue to be processed")
                .runtime(Runtime.JAVA_17)
                .memorySize(instanceProperties.getInt(TABLE_BATCHING_LAMBDAS_MEMORY_IN_MB))
                .timeout(Duration.seconds(instanceProperties.getInt(TABLE_BATCHING_LAMBDAS_TIMEOUT_IN_SECONDS)))
                .handler("sleeper.splitter.lambda.FindPartitionsToSplitTriggerLambda::handleRequest")
                .environment(environmentVariables)
                .reservedConcurrentExecutions(1)
                .logGroup(coreStacks.getLogGroupByFunctionName(triggerFunctionName)));
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
        findPartitionsToSplitQueue.grantSendMessages(triggerFunction);
        coreStacks.grantInvokeScheduled(triggerFunction, findPartitionsToSplitQueue);
    }

    private void createFindPartitionsToSplitFunction(InstanceProperties instanceProperties, LambdaCode splitterJar, CoreStacks coreStacks, Map<String, String> environmentVariables) {
        String functionName = String.join("-", "sleeper",
                Utils.cleanInstanceId(instanceProperties), "partition-splitting-find-to-split");
        IFunction findPartitionsToSplitLambda = splitterJar.buildFunction(this, "FindPartitionsToSplitLambda", builder -> builder
                .functionName(functionName)
                .description("Scan the state stores of the provided tables looking for partitions that need splitting")
                .runtime(Runtime.JAVA_17)
                .memorySize(instanceProperties.getInt(FIND_PARTITIONS_TO_SPLIT_LAMBDA_MEMORY_IN_MB))
                .timeout(Duration.seconds(instanceProperties.getInt(FIND_PARTITIONS_TO_SPLIT_TIMEOUT_IN_SECONDS)))
                .handler("sleeper.splitter.lambda.FindPartitionsToSplitLambda::handleRequest")
                .environment(environmentVariables)
                .reservedConcurrentExecutions(instanceProperties.getInt(FIND_PARTITIONS_TO_SPLIT_LAMBDA_CONCURRENCY_RESERVED))
                .logGroup(coreStacks.getLogGroupByFunctionName(functionName)));

        coreStacks.grantReadTablesMetadata(findPartitionsToSplitLambda);
        partitionSplittingJobQueue.grantSendMessages(findPartitionsToSplitLambda);
        findPartitionsToSplitLambda.addEventSource(SqsEventSource.Builder.create(findPartitionsToSplitQueue)
                .batchSize(instanceProperties.getInt(FIND_PARTITIONS_TO_SPLIT_BATCH_SIZE))
                .maxConcurrency(instanceProperties.getInt(FIND_PARTITIONS_TO_SPLIT_LAMBDA_CONCURRENCY_MAXIMUM))
                .build());
    }

    private void createSplitPartitionFunction(InstanceProperties instanceProperties, LambdaCode splitterJar, CoreStacks coreStacks, Map<String, String> environmentVariables) {
        String splitFunctionName = String.join("-", "sleeper",
                Utils.cleanInstanceId(instanceProperties), "partition-splitting-handler");

        // Lambda to split partitions (triggered by partition splitting job
        // arriving on partitionSplittingQueue)
        int concurrency = instanceProperties.getInt(SPLIT_PARTITIONS_RESERVED_CONCURRENCY);
        IFunction splitPartitionLambda = splitterJar.buildFunction(this, "SplitPartitionLambda", builder -> builder
                .functionName(splitFunctionName)
                .description("Triggered by an SQS event that contains a partition to split")
                .runtime(Runtime.JAVA_17)
                .memorySize(instanceProperties.getInt(SPLIT_PARTITIONS_LAMBDA_MEMORY_IN_MB))
                .timeout(Duration.seconds(instanceProperties.getInt(SPLIT_PARTITIONS_TIMEOUT_IN_SECONDS)))
                .reservedConcurrentExecutions(concurrency)
                .handler("sleeper.splitter.lambda.SplitPartitionLambda::handleRequest")
                .environment(environmentVariables)
                .logGroup(coreStacks.getLogGroupByFunctionName(splitFunctionName)));

        coreStacks.grantSplitPartitions(splitPartitionLambda);
        splitPartitionLambda.addEventSource(SqsEventSource.Builder.create(partitionSplittingJobQueue)
                .batchSize(1).maxConcurrency(concurrency).build());
    }

    public IQueue getJobQueue() {
        return partitionSplittingJobQueue;
    }
}
