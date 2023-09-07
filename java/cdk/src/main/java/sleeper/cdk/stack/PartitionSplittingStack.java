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

import software.amazon.awscdk.CfnOutput;
import software.amazon.awscdk.CfnOutputProps;
import software.amazon.awscdk.Duration;
import software.amazon.awscdk.NestedStack;
import software.amazon.awscdk.services.cloudwatch.Alarm;
import software.amazon.awscdk.services.cloudwatch.ComparisonOperator;
import software.amazon.awscdk.services.cloudwatch.MetricOptions;
import software.amazon.awscdk.services.cloudwatch.TreatMissingData;
import software.amazon.awscdk.services.cloudwatch.actions.SnsAction;
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
import sleeper.configuration.properties.instance.SystemDefinedInstanceProperty;

import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import static sleeper.cdk.Utils.shouldDeployPaused;
import static sleeper.configuration.properties.instance.CommonProperty.ID;
import static sleeper.configuration.properties.instance.CommonProperty.LOG_RETENTION_IN_DAYS;
import static sleeper.configuration.properties.instance.PartitionSplittingProperty.FIND_PARTITIONS_TO_SPLIT_LAMBDA_MEMORY_IN_MB;
import static sleeper.configuration.properties.instance.PartitionSplittingProperty.FIND_PARTITIONS_TO_SPLIT_TIMEOUT_IN_SECONDS;
import static sleeper.configuration.properties.instance.PartitionSplittingProperty.PARTITION_SPLITTING_PERIOD_IN_MINUTES;
import static sleeper.configuration.properties.instance.PartitionSplittingProperty.SPLIT_PARTITIONS_LAMBDA_MEMORY_IN_MB;
import static sleeper.configuration.properties.instance.PartitionSplittingProperty.SPLIT_PARTITIONS_TIMEOUT_IN_SECONDS;
import static sleeper.configuration.properties.instance.SystemDefinedInstanceProperty.CONFIG_BUCKET;
import static sleeper.configuration.properties.instance.SystemDefinedInstanceProperty.PARTITION_SPLITTING_CLOUDWATCH_RULE;
import static sleeper.configuration.properties.instance.SystemDefinedInstanceProperty.PARTITION_SPLITTING_DLQ_ARN;
import static sleeper.configuration.properties.instance.SystemDefinedInstanceProperty.PARTITION_SPLITTING_DLQ_URL;
import static sleeper.configuration.properties.instance.SystemDefinedInstanceProperty.PARTITION_SPLITTING_LAMBDA_FUNCTION;

/**
 * A {@link NestedStack} to look for partitions that need splitting and to split them.
 */
public class PartitionSplittingStack extends NestedStack {
    public static final String PARTITION_SPLITTING_QUEUE_URL = "PartitionSplittingQueueUrl";
    public static final String PARTITION_SPLITTING_DL_QUEUE_URL = "PartitionSplittingDLQueueUrl";
    private final Queue partitionSplittingQueue;
    private final Queue dlQueue;

    public PartitionSplittingStack(Construct scope,
                                   String id,
                                   InstanceProperties instanceProperties,
                                   BuiltJars jars,
                                   List<IBucket> dataBuckets,
                                   List<StateStoreStack> stateStoreStacks,
                                   Topic topic) {
        super(scope, id);

        // Config bucket
        IBucket configBucket = Bucket.fromBucketName(this, "ConfigBucket", instanceProperties.get(CONFIG_BUCKET));

        // Jars bucket
        IBucket jarsBucket = Bucket.fromBucketName(this, "JarsBucket", jars.bucketName());

        // Create queue for partition splitting job definitions
        this.dlQueue = Queue.Builder
                .create(this, "PartitionSplittingDeadLetterQueue")
                .queueName(instanceProperties.get(ID) + "-PartitionSplittingDLQueue")
                .build();
        DeadLetterQueue partitionSplittingDeadLetterQueue = DeadLetterQueue.builder()
                .maxReceiveCount(1)
                .queue(dlQueue)
                .build();
        this.partitionSplittingQueue = Queue.Builder
                .create(this, "PartitionSplittingQueue")
                .queueName(instanceProperties.get(ID) + "-PartitionSplittingQueue")
                .deadLetterQueue(partitionSplittingDeadLetterQueue)
                .visibilityTimeout(Duration.seconds(instanceProperties.getInt(SPLIT_PARTITIONS_TIMEOUT_IN_SECONDS))) // TODO Needs to be >= function timeout
                .build();
        instanceProperties.set(SystemDefinedInstanceProperty.PARTITION_SPLITTING_QUEUE_URL,
                partitionSplittingQueue.getQueueUrl());
        instanceProperties.set(SystemDefinedInstanceProperty.PARTITION_SPLITTING_QUEUE_ARN,
                partitionSplittingQueue.getQueueArn());
        instanceProperties.set(PARTITION_SPLITTING_DLQ_URL,
                partitionSplittingDeadLetterQueue.getQueue().getQueueUrl());
        instanceProperties.set(PARTITION_SPLITTING_DLQ_ARN,
                partitionSplittingDeadLetterQueue.getQueue().getQueueArn());

        // Add alarm to send message to SNS if there are any messages on the dead letter queue
        Alarm partitionSplittingAlarm = Alarm.Builder
                .create(this, "PartitionSplittingAlarm")
                .alarmDescription("Alarms if there are any messages on the dead letter queue for the partition splitting queue")
                .metric(dlQueue.metricApproximateNumberOfMessagesVisible()
                        .with(MetricOptions.builder().statistic("Sum").period(Duration.seconds(60)).build())
                )
                .comparisonOperator(ComparisonOperator.GREATER_THAN_THRESHOLD)
                .threshold(0)
                .evaluationPeriods(1)
                .datapointsToAlarm(1)
                .treatMissingData(TreatMissingData.IGNORE)
                .build();
        partitionSplittingAlarm.addAlarmAction(new SnsAction(topic));

        CfnOutputProps partitionSplittingQueueOutputProps = new CfnOutputProps.Builder()
                .value(partitionSplittingQueue.getQueueUrl())
                .build();
        new CfnOutput(this, PARTITION_SPLITTING_QUEUE_URL, partitionSplittingQueueOutputProps);

        CfnOutputProps partitionSplittingDLQueueOutputProps = new CfnOutputProps.Builder()
                .value(partitionSplittingDeadLetterQueue.getQueue().getQueueUrl())
                .build();
        new CfnOutput(this, PARTITION_SPLITTING_DL_QUEUE_URL, partitionSplittingDLQueueOutputProps);

        // Partition splitting code
        LambdaCode splitterJar = jars.lambdaCode(BuiltJar.PARTITION_SPLITTER, jarsBucket);

        // Lambda to look for partitions that need splitting (for each partition that
        // needs splitting it puts a definition of the splitting job onto a queue)
        Map<String, String> environmentVariables = Utils.createDefaultEnvironment(instanceProperties);

        String functionName = Utils.truncateTo64Characters(String.join("-", "sleeper",
                instanceProperties.get(ID).toLowerCase(Locale.ROOT), "find-partitions-to-split"));

        IFunction findPartitionsToSplitLambda = splitterJar.buildFunction(this, "FindPartitionsToSplitLambda", builder -> builder
                .functionName(functionName)
                .description("Scan DynamoDB looking for partitions that need splitting")
                .runtime(software.amazon.awscdk.services.lambda.Runtime.JAVA_11)
                .memorySize(instanceProperties.getInt(FIND_PARTITIONS_TO_SPLIT_LAMBDA_MEMORY_IN_MB))
                .timeout(Duration.seconds(instanceProperties.getInt(FIND_PARTITIONS_TO_SPLIT_TIMEOUT_IN_SECONDS)))
                .handler("sleeper.splitter.FindPartitionsToSplitLambda::eventHandler")
                .environment(environmentVariables)
                .reservedConcurrentExecutions(1)
                .logRetention(Utils.getRetentionDays(instanceProperties.getInt(LOG_RETENTION_IN_DAYS))));

        configBucket.grantRead(findPartitionsToSplitLambda);
        stateStoreStacks.forEach(stateStoreStack -> stateStoreStack.grantReadActiveFileMetadata(findPartitionsToSplitLambda));
        stateStoreStacks.forEach(stateStoreStack -> stateStoreStack.grantReadWritePartitionMetadata(findPartitionsToSplitLambda));

        // Grant this function permission to write to the SQS queue
        partitionSplittingQueue.grantSendMessages(findPartitionsToSplitLambda);

        // Cloudwatch rule to trigger this lambda
        Rule rule = Rule.Builder
                .create(this, "FindPartitionsToSplitPeriodicTrigger")
                .ruleName(SleeperScheduleRule.PARTITION_SPLITTING.buildRuleName(instanceProperties))
                .description("A rule to periodically trigger the lambda to look for partitions to split")
                .enabled(!shouldDeployPaused(this))
                .schedule(Schedule.rate(Duration.minutes(instanceProperties.getInt(PARTITION_SPLITTING_PERIOD_IN_MINUTES))))
                .targets(Collections.singletonList(new LambdaFunction(findPartitionsToSplitLambda)))
                .build();
        instanceProperties.set(PARTITION_SPLITTING_LAMBDA_FUNCTION, findPartitionsToSplitLambda.getFunctionName());
        instanceProperties.set(PARTITION_SPLITTING_CLOUDWATCH_RULE, rule.getRuleName());

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
                .handler("sleeper.splitter.SplitPartitionLambda::handleRequest")
                .environment(environmentVariables)
                .logRetention(Utils.getRetentionDays(instanceProperties.getInt(LOG_RETENTION_IN_DAYS))));

        // Add the queue as a source of events for this lambda
        SqsEventSourceProps eventSourceProps = SqsEventSourceProps.builder()
                .batchSize(1)
                .build();
        splitPartitionLambda.addEventSource(new SqsEventSource(partitionSplittingQueue, eventSourceProps));

        // Grant permission for this lambda to consume messages from the queue
        partitionSplittingQueue.grantConsumeMessages(splitPartitionLambda);

        // Grant this function permission to read config files and to read
        // from / write to the DynamoDB table
        configBucket.grantRead(splitPartitionLambda);
        dataBuckets.forEach(bucket -> bucket.grantRead(splitPartitionLambda));
        stateStoreStacks.forEach(stateStoreStack -> stateStoreStack.grantReadWritePartitionMetadata(splitPartitionLambda));

        Utils.addStackTagIfSet(this, instanceProperties);
    }

    public Queue getJobQueue() {
        return partitionSplittingQueue;
    }

    public Queue getDeadLetterQueue() {
        return dlQueue;
    }
}
