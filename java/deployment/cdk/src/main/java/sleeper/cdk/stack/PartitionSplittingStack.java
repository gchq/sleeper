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
import software.amazon.awscdk.services.sqs.DeadLetterQueue;
import software.amazon.awscdk.services.sqs.IQueue;
import software.amazon.awscdk.services.sqs.Queue;
import software.constructs.Construct;

import sleeper.cdk.SleeperInstanceProps;
import sleeper.cdk.lambda.SleeperLambdaCode;
import sleeper.cdk.stack.core.LoggingStack.LogGroupRef;
import sleeper.cdk.util.Utils;
import sleeper.core.deploy.LambdaHandler;
import sleeper.core.deploy.SleeperScheduleRule;
import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.util.EnvironmentUtils;

import java.util.List;
import java.util.Map;

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
import static sleeper.core.properties.instance.PartitionSplittingProperty.FIND_PARTITIONS_TO_SPLIT_BATCH_SIZE;
import static sleeper.core.properties.instance.PartitionSplittingProperty.FIND_PARTITIONS_TO_SPLIT_LAMBDA_CONCURRENCY_MAXIMUM;
import static sleeper.core.properties.instance.PartitionSplittingProperty.FIND_PARTITIONS_TO_SPLIT_LAMBDA_CONCURRENCY_RESERVED;
import static sleeper.core.properties.instance.PartitionSplittingProperty.FIND_PARTITIONS_TO_SPLIT_LAMBDA_MEMORY_IN_MB;
import static sleeper.core.properties.instance.PartitionSplittingProperty.FIND_PARTITIONS_TO_SPLIT_TIMEOUT_IN_SECONDS;
import static sleeper.core.properties.instance.PartitionSplittingProperty.PARTITION_SPLITTING_TRIGGER_PERIOD_IN_MINUTES;
import static sleeper.core.properties.instance.PartitionSplittingProperty.SPLIT_PARTITIONS_LAMBDA_MEMORY_IN_MB;
import static sleeper.core.properties.instance.PartitionSplittingProperty.SPLIT_PARTITIONS_RESERVED_CONCURRENCY;
import static sleeper.core.properties.instance.PartitionSplittingProperty.SPLIT_PARTITIONS_TIMEOUT_IN_SECONDS;
import static sleeper.core.properties.instance.TableStateProperty.TABLE_BATCHING_LAMBDAS_MEMORY_IN_MB;
import static sleeper.core.properties.instance.TableStateProperty.TABLE_BATCHING_LAMBDAS_TIMEOUT_IN_SECONDS;

/**
 * Deploys resources to perform partition splitting. A CloudWatch rule will periodically trigger to check every Sleeper
 * table for partitions that need splitting, and split them.
 */
public class PartitionSplittingStack extends NestedStack {
    public static final String PARTITION_SPLITTING_QUEUE_URL = "PartitionSplittingQueueUrl";
    public static final String PARTITION_SPLITTING_DLQ_URL = "PartitionSplittingDLQUrl";
    private final Queue partitionSplittingJobQueue;
    private final Queue findPartitionsToSplitQueue;

    public PartitionSplittingStack(
            Construct scope, String id,
            SleeperInstanceProps props,
            SleeperCoreStacks coreStacks) {
        super(scope, id);
        InstanceProperties instanceProperties = props.getInstanceProperties();

        // Create queue for batching tables
        findPartitionsToSplitQueue = createBatchQueues(instanceProperties, coreStacks);
        // Create queue for partition splitting job definitions
        partitionSplittingJobQueue = createJobQueues(instanceProperties, coreStacks);

        // Partition splitting code
        SleeperLambdaCode lambdaCode = SleeperLambdaCode.atScope(this, instanceProperties, props.getArtefacts());
        Map<String, String> environmentVariables = EnvironmentUtils.createDefaultEnvironment(instanceProperties);

        // Lambda to batch tables and put requests on the batch SQS queue, to be consumed by FindPartitionsToSplit
        createTriggerFunction(props, lambdaCode, coreStacks, environmentVariables);

        // Lambda to look for partitions that need splitting (for each partition that
        // needs splitting it puts a definition of the splitting job onto a queue)
        createFindPartitionsToSplitFunction(instanceProperties, lambdaCode, coreStacks, environmentVariables);

        // Lambda to split partitions (triggered by partition splitting job
        // arriving on partitionSplittingQueue)
        createSplitPartitionFunction(instanceProperties, lambdaCode, coreStacks, environmentVariables);

        Utils.addTags(this, instanceProperties);
    }

    private Queue createBatchQueues(InstanceProperties instanceProperties, SleeperCoreStacks coreStacks) {
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
        coreStacks.alarmOnDeadLetters(this, "FindPartitionsToSplitAlarm", "finding partitions to split", findPartitionsToSplitDlq);
        return findPartitionsToSplitQueue;
    }

    private Queue createJobQueues(InstanceProperties instanceProperties, SleeperCoreStacks coreStacks) {
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

        coreStacks.alarmOnDeadLetters(this, "PartitionSplittingAlarm", "partition splitting", partitionSplittingJobDlq);

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

    private void createTriggerFunction(SleeperInstanceProps props, SleeperLambdaCode lambdaCode, SleeperCoreStacks coreStacks, Map<String, String> environmentVariables) {
        InstanceProperties instanceProperties = props.getInstanceProperties();
        String triggerFunctionName = String.join("-", "sleeper",
                Utils.cleanInstanceId(instanceProperties), "partition-splitting-trigger");
        IFunction triggerFunction = lambdaCode.buildFunction(this, LambdaHandler.FIND_PARTITIONS_TO_SPLIT_TRIGGER, "FindPartitionsToSplitTriggerLambda", builder -> builder
                .functionName(triggerFunctionName)
                .description("Creates batches of Sleeper tables to perform partition splitting for and puts them on a queue to be processed")
                .memorySize(instanceProperties.getInt(TABLE_BATCHING_LAMBDAS_MEMORY_IN_MB))
                .timeout(Duration.seconds(instanceProperties.getInt(TABLE_BATCHING_LAMBDAS_TIMEOUT_IN_SECONDS)))
                .environment(environmentVariables)
                .reservedConcurrentExecutions(1)
                .logGroup(coreStacks.getLogGroup(LogGroupRef.PARTITION_SPLITTING_TRIGGER)));
        // Cloudwatch rule to trigger this lambda
        Rule rule = Rule.Builder
                .create(this, "FindPartitionsToSplitPeriodicTrigger")
                .ruleName(SleeperScheduleRule.PARTITION_SPLITTING.buildRuleName(instanceProperties))
                .description(SleeperScheduleRule.PARTITION_SPLITTING.getDescription())
                .enabled(!props.isDeployPaused())
                .schedule(Schedule.rate(Duration.minutes(instanceProperties.getInt(PARTITION_SPLITTING_TRIGGER_PERIOD_IN_MINUTES))))
                .targets(List.of(new LambdaFunction(triggerFunction)))
                .build();
        instanceProperties.set(PARTITION_SPLITTING_TRIGGER_LAMBDA_FUNCTION, triggerFunction.getFunctionName());
        instanceProperties.set(PARTITION_SPLITTING_CLOUDWATCH_RULE, rule.getRuleName());

        coreStacks.grantReadTablesStatus(triggerFunction);
        findPartitionsToSplitQueue.grantSendMessages(triggerFunction);
        coreStacks.grantInvokeScheduled(triggerFunction, findPartitionsToSplitQueue);
    }

    private void createFindPartitionsToSplitFunction(InstanceProperties instanceProperties, SleeperLambdaCode lambdaCode, SleeperCoreStacks coreStacks, Map<String, String> environmentVariables) {
        String functionName = String.join("-", "sleeper",
                Utils.cleanInstanceId(instanceProperties), "partition-splitting-find-to-split");
        IFunction findPartitionsToSplitLambda = lambdaCode.buildFunction(this, LambdaHandler.FIND_PARTITIONS_TO_SPLIT, "FindPartitionsToSplitLambda", builder -> builder
                .functionName(functionName)
                .description("Scan the state stores of the provided tables looking for partitions that need splitting")
                .memorySize(instanceProperties.getInt(FIND_PARTITIONS_TO_SPLIT_LAMBDA_MEMORY_IN_MB))
                .timeout(Duration.seconds(instanceProperties.getInt(FIND_PARTITIONS_TO_SPLIT_TIMEOUT_IN_SECONDS)))
                .environment(environmentVariables)
                .reservedConcurrentExecutions(instanceProperties.getIntOrNull(FIND_PARTITIONS_TO_SPLIT_LAMBDA_CONCURRENCY_RESERVED))
                .logGroup(coreStacks.getLogGroup(LogGroupRef.PARTITION_SPLITTING_FIND_TO_SPLIT)));

        coreStacks.grantReadTablesMetadata(findPartitionsToSplitLambda);
        partitionSplittingJobQueue.grantSendMessages(findPartitionsToSplitLambda);
        findPartitionsToSplitLambda.addEventSource(SqsEventSource.Builder.create(findPartitionsToSplitQueue)
                .batchSize(instanceProperties.getInt(FIND_PARTITIONS_TO_SPLIT_BATCH_SIZE))
                .maxConcurrency(instanceProperties.getIntOrNull(FIND_PARTITIONS_TO_SPLIT_LAMBDA_CONCURRENCY_MAXIMUM))
                .build());
    }

    private void createSplitPartitionFunction(InstanceProperties instanceProperties, SleeperLambdaCode lambdaCode, SleeperCoreStacks coreStacks, Map<String, String> environmentVariables) {
        String splitFunctionName = String.join("-", "sleeper",
                Utils.cleanInstanceId(instanceProperties), "partition-splitting-handler");

        // Lambda to split partitions (triggered by partition splitting job
        // arriving on partitionSplittingQueue)
        Integer concurrency = instanceProperties.getIntOrNull(SPLIT_PARTITIONS_RESERVED_CONCURRENCY);
        IFunction splitPartitionLambda = lambdaCode.buildFunction(this, LambdaHandler.SPLIT_PARTITION, "SplitPartitionLambda", builder -> builder
                .functionName(splitFunctionName)
                .description("Triggered by an SQS event that contains a partition to split")
                .memorySize(instanceProperties.getInt(SPLIT_PARTITIONS_LAMBDA_MEMORY_IN_MB))
                .timeout(Duration.seconds(instanceProperties.getInt(SPLIT_PARTITIONS_TIMEOUT_IN_SECONDS)))
                .reservedConcurrentExecutions(concurrency)
                .environment(environmentVariables)
                .logGroup(coreStacks.getLogGroup(LogGroupRef.PARTITION_SPLITTING_HANDLER)));

        coreStacks.grantSplitPartitions(splitPartitionLambda);
        splitPartitionLambda.addEventSource(SqsEventSource.Builder.create(partitionSplittingJobQueue)
                .batchSize(1).maxConcurrency(concurrency).build());
    }

    public IQueue getJobQueue() {
        return partitionSplittingJobQueue;
    }
}
