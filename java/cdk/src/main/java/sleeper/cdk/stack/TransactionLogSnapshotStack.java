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
import software.amazon.awscdk.services.sqs.Queue;
import software.constructs.Construct;

import sleeper.cdk.Utils;
import sleeper.cdk.jars.BuiltJar;
import sleeper.cdk.jars.BuiltJars;
import sleeper.cdk.jars.LambdaCode;
import sleeper.core.properties.deploy.SleeperScheduleRule;
import sleeper.core.properties.instance.InstanceProperties;

import java.util.List;

import static sleeper.cdk.Utils.createAlarmForDlq;
import static sleeper.cdk.Utils.createLambdaLogGroup;
import static sleeper.cdk.Utils.shouldDeployPaused;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.TRANSACTION_LOG_SNAPSHOT_CREATION_DLQ_ARN;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.TRANSACTION_LOG_SNAPSHOT_CREATION_DLQ_URL;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.TRANSACTION_LOG_SNAPSHOT_CREATION_QUEUE_ARN;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.TRANSACTION_LOG_SNAPSHOT_CREATION_QUEUE_URL;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.TRANSACTION_LOG_SNAPSHOT_CREATION_RULE;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.TRANSACTION_LOG_SNAPSHOT_DELETION_DLQ_ARN;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.TRANSACTION_LOG_SNAPSHOT_DELETION_DLQ_URL;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.TRANSACTION_LOG_SNAPSHOT_DELETION_QUEUE_ARN;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.TRANSACTION_LOG_SNAPSHOT_DELETION_QUEUE_URL;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.TRANSACTION_LOG_SNAPSHOT_DELETION_RULE;
import static sleeper.core.properties.instance.CommonProperty.JARS_BUCKET;
import static sleeper.core.properties.instance.CommonProperty.TABLE_BATCHING_LAMBDAS_MEMORY_IN_MB;
import static sleeper.core.properties.instance.CommonProperty.TABLE_BATCHING_LAMBDAS_TIMEOUT_IN_SECONDS;
import static sleeper.core.properties.instance.CommonProperty.TRANSACTION_LOG_SNAPSHOT_CREATION_BATCH_SIZE;
import static sleeper.core.properties.instance.CommonProperty.TRANSACTION_LOG_SNAPSHOT_CREATION_LAMBDA_CONCURRENCY_MAXIMUM;
import static sleeper.core.properties.instance.CommonProperty.TRANSACTION_LOG_SNAPSHOT_CREATION_LAMBDA_CONCURRENCY_RESERVED;
import static sleeper.core.properties.instance.CommonProperty.TRANSACTION_LOG_SNAPSHOT_CREATION_LAMBDA_PERIOD_IN_MINUTES;
import static sleeper.core.properties.instance.CommonProperty.TRANSACTION_LOG_SNAPSHOT_DELETION_BATCH_SIZE;
import static sleeper.core.properties.instance.CommonProperty.TRANSACTION_LOG_SNAPSHOT_DELETION_LAMBDA_CONCURRENCY_MAXIMUM;
import static sleeper.core.properties.instance.CommonProperty.TRANSACTION_LOG_SNAPSHOT_DELETION_LAMBDA_CONCURRENCY_RESERVED;
import static sleeper.core.properties.instance.CommonProperty.TRANSACTION_LOG_SNAPSHOT_DELETION_LAMBDA_PERIOD_IN_MINUTES;;

public class TransactionLogSnapshotStack extends NestedStack {

    public TransactionLogSnapshotStack(
            Construct scope, String id,
            InstanceProperties instanceProperties, BuiltJars jars, CoreStacks coreStacks,
            TransactionLogStateStoreStack transactionLogStateStoreStack,
            Topic topic, List<IMetric> errorMetrics) {
        super(scope, id);
        IBucket jarsBucket = Bucket.fromBucketName(this, "JarsBucket", instanceProperties.get(JARS_BUCKET));
        LambdaCode statestoreJar = jars.lambdaCode(BuiltJar.STATESTORE, jarsBucket);
        createSnapshotCreationLambda(instanceProperties, statestoreJar, coreStacks, transactionLogStateStoreStack, topic, errorMetrics);
        createSnapshotDeletionLambda(instanceProperties, statestoreJar, coreStacks, transactionLogStateStoreStack, topic, errorMetrics);
        Utils.addStackTagIfSet(this, instanceProperties);
    }

    private void createSnapshotCreationLambda(InstanceProperties instanceProperties, LambdaCode statestoreJar, CoreStacks coreStacks,
            TransactionLogStateStoreStack transactionLogStateStoreStack, Topic topic, List<IMetric> errorMetrics) {
        String instanceId = Utils.cleanInstanceId(instanceProperties);
        String triggerFunctionName = String.join("-", "sleeper", instanceId, "state-snapshot-creation-trigger");
        String creationFunctionName = String.join("-", "sleeper", instanceId, "state-snapshot-creation");
        IFunction snapshotCreationTrigger = statestoreJar.buildFunction(this, "TransactionLogSnapshotCreationTrigger", builder -> builder
                .functionName(triggerFunctionName)
                .description("Creates batches of Sleeper tables to create transaction log snapshots for and puts them on a queue to be processed")
                .runtime(Runtime.JAVA_11)
                .handler("sleeper.statestore.snapshot.TransactionLogSnapshotCreationTriggerLambda::handleRequest")
                .environment(Utils.createDefaultEnvironment(instanceProperties))
                .reservedConcurrentExecutions(1)
                .memorySize(instanceProperties.getInt(TABLE_BATCHING_LAMBDAS_MEMORY_IN_MB))
                .timeout(Duration.seconds(instanceProperties.getInt(TABLE_BATCHING_LAMBDAS_TIMEOUT_IN_SECONDS)))
                .logGroup(createLambdaLogGroup(this, "TransactionLogSnapshotCreationTriggerLogGroup", triggerFunctionName, instanceProperties)));
        IFunction snapshotCreationLambda = statestoreJar.buildFunction(this, "TransactionLogSnapshotCreation", builder -> builder
                .functionName(creationFunctionName)
                .description("Creates transaction log snapshots for tables")
                .runtime(Runtime.JAVA_11)
                .handler("sleeper.statestore.snapshot.TransactionLogSnapshotCreationLambda::handleRequest")
                .environment(Utils.createDefaultEnvironment(instanceProperties))
                .reservedConcurrentExecutions(instanceProperties.getInt(TRANSACTION_LOG_SNAPSHOT_CREATION_LAMBDA_CONCURRENCY_RESERVED))
                .memorySize(1024)
                .timeout(Duration.minutes(1))
                .logGroup(createLambdaLogGroup(this, "TransactionLogSnapshotCreationLogGroup", creationFunctionName, instanceProperties)));

        Rule rule = Rule.Builder.create(this, "TransactionLogSnapshotCreationSchedule")
                .ruleName(SleeperScheduleRule.TRANSACTION_LOG_SNAPSHOT_CREATION.buildRuleName(instanceProperties))
                .schedule(Schedule.rate(Duration.minutes(
                        instanceProperties.getLong(TRANSACTION_LOG_SNAPSHOT_CREATION_LAMBDA_PERIOD_IN_MINUTES))))
                .targets(List.of(new LambdaFunction(snapshotCreationTrigger)))
                .enabled(!shouldDeployPaused(this))
                .build();
        instanceProperties.set(TRANSACTION_LOG_SNAPSHOT_CREATION_RULE, rule.getRuleName());

        Queue deadLetterQueue = Queue.Builder
                .create(this, "TransactionLogSnapshotDeadLetterQueue")
                .queueName(String.join("-", "sleeper", instanceId, "TransactionLogSnapshotDLQ.fifo"))
                .fifo(true)
                .build();
        Queue queue = Queue.Builder
                .create(this, "TransactionLogSnapshotQueue")
                .queueName(String.join("-", "sleeper", instanceId, "TransactionLogSnapshotQ.fifo"))
                .deadLetterQueue(DeadLetterQueue.builder()
                        .maxReceiveCount(1)
                        .queue(deadLetterQueue)
                        .build())
                .fifo(true)
                .visibilityTimeout(Duration.seconds(70))
                .build();
        instanceProperties.set(TRANSACTION_LOG_SNAPSHOT_CREATION_QUEUE_URL, queue.getQueueUrl());
        instanceProperties.set(TRANSACTION_LOG_SNAPSHOT_CREATION_QUEUE_ARN, queue.getQueueArn());
        instanceProperties.set(TRANSACTION_LOG_SNAPSHOT_CREATION_DLQ_URL, deadLetterQueue.getQueueUrl());
        instanceProperties.set(TRANSACTION_LOG_SNAPSHOT_CREATION_DLQ_ARN, deadLetterQueue.getQueueArn());
        createAlarmForDlq(this, "TransactionLogSnapshotCreationAlarm",
                "Alarms if there are any messages on the dead letter queue for the transaction log snapshot creation queue",
                deadLetterQueue, topic);
        errorMetrics.add(Utils.createErrorMetric("Transaction Log Snapshot Creation Errors", deadLetterQueue, instanceProperties));
        queue.grantSendMessages(snapshotCreationTrigger);

        snapshotCreationLambda.addEventSource(SqsEventSource.Builder.create(queue)
                .batchSize(instanceProperties.getInt(TRANSACTION_LOG_SNAPSHOT_CREATION_BATCH_SIZE))
                .maxConcurrency(instanceProperties.getInt(TRANSACTION_LOG_SNAPSHOT_CREATION_LAMBDA_CONCURRENCY_MAXIMUM)).build());

        coreStacks.grantReadTablesStatus(snapshotCreationTrigger);
        coreStacks.grantInvokeScheduled(snapshotCreationTrigger, queue);
        coreStacks.grantReadTablesStatus(snapshotCreationLambda);
        transactionLogStateStoreStack.grantCreateSnapshots(snapshotCreationLambda);
    }

    private void createSnapshotDeletionLambda(InstanceProperties instanceProperties, LambdaCode statestoreJar, CoreStacks coreStacks,
            TransactionLogStateStoreStack transactionLogStateStoreStack, Topic topic, List<IMetric> errorMetrics) {
        String instanceId = Utils.cleanInstanceId(instanceProperties);
        String triggerFunctionName = String.join("-", "sleeper", instanceId, "state-snapshot-deletion-trigger");
        String deletionFunctionName = String.join("-", "sleeper", instanceId, "state-snapshot-deletion");
        IFunction snapshotDeletionTrigger = statestoreJar.buildFunction(this, "TransactionLogSnapshotDeletionTrigger", builder -> builder
                .functionName(triggerFunctionName)
                .description("Creates batches of Sleeper tables to delete old transaction log snapshots for and puts them on a queue to be processed")
                .runtime(Runtime.JAVA_11)
                .handler("sleeper.statestore.snapshot.TransactionLogSnapshotDeletionTriggerLambda::handleRequest")
                .environment(Utils.createDefaultEnvironment(instanceProperties))
                .reservedConcurrentExecutions(1)
                .memorySize(instanceProperties.getInt(TABLE_BATCHING_LAMBDAS_MEMORY_IN_MB))
                .timeout(Duration.seconds(instanceProperties.getInt(TABLE_BATCHING_LAMBDAS_TIMEOUT_IN_SECONDS)))
                .logGroup(createLambdaLogGroup(this, "TransactionLogSnapshotDeletionTriggerLogGroup", triggerFunctionName, instanceProperties)));
        IFunction snapshotDeletionLambda = statestoreJar.buildFunction(this, "TransactionLogSnapshotDeletion", builder -> builder
                .functionName(deletionFunctionName)
                .description("Deletes old transaction log snapshots for tables")
                .runtime(Runtime.JAVA_11)
                .handler("sleeper.statestore.snapshot.TransactionLogSnapshotDeletionLambda::handleRequest")
                .environment(Utils.createDefaultEnvironment(instanceProperties))
                .reservedConcurrentExecutions(instanceProperties.getInt(TRANSACTION_LOG_SNAPSHOT_DELETION_LAMBDA_CONCURRENCY_RESERVED))
                .memorySize(1024)
                .timeout(Duration.minutes(1))
                .logGroup(createLambdaLogGroup(this, "TransactionLogSnapshotDeletionLogGroup", deletionFunctionName, instanceProperties)));

        Rule rule = Rule.Builder.create(this, "TransactionLogSnapshotDeletionSchedule")
                .ruleName(SleeperScheduleRule.TRANSACTION_LOG_SNAPSHOT_DELETION.buildRuleName(instanceProperties))
                .schedule(Schedule.rate(Duration.minutes(
                        instanceProperties.getLong(TRANSACTION_LOG_SNAPSHOT_DELETION_LAMBDA_PERIOD_IN_MINUTES))))
                .targets(List.of(new LambdaFunction(snapshotDeletionTrigger)))
                .enabled(!shouldDeployPaused(this))
                .build();
        instanceProperties.set(TRANSACTION_LOG_SNAPSHOT_DELETION_RULE, rule.getRuleName());

        Queue deadLetterQueue = Queue.Builder
                .create(this, "TransactionLogSnapshotDeletionDeadLetterQueue")
                .queueName(String.join("-", "sleeper", instanceId, "TransactionLogSnapshotDeletionDLQ.fifo"))
                .fifo(true)
                .build();
        Queue queue = Queue.Builder
                .create(this, "TransactionLogSnapshotDeletionQueue")
                .queueName(String.join("-", "sleeper", instanceId, "TransactionLogSnapshotDeletionQ.fifo"))
                .deadLetterQueue(DeadLetterQueue.builder()
                        .maxReceiveCount(1)
                        .queue(deadLetterQueue)
                        .build())
                .fifo(true)
                .visibilityTimeout(Duration.seconds(70))
                .build();
        instanceProperties.set(TRANSACTION_LOG_SNAPSHOT_DELETION_QUEUE_URL, queue.getQueueUrl());
        instanceProperties.set(TRANSACTION_LOG_SNAPSHOT_DELETION_QUEUE_ARN, queue.getQueueArn());
        instanceProperties.set(TRANSACTION_LOG_SNAPSHOT_DELETION_DLQ_URL, deadLetterQueue.getQueueUrl());
        instanceProperties.set(TRANSACTION_LOG_SNAPSHOT_DELETION_DLQ_ARN, deadLetterQueue.getQueueArn());
        createAlarmForDlq(this, "TransactionLogSnapshotDeletionAlarm",
                "Alarms if there are any messages on the dead letter queue for the transaction log snapshot deletion queue",
                deadLetterQueue, topic);
        errorMetrics.add(Utils.createErrorMetric("Transaction Log Snapshot Deletion Errors", deadLetterQueue, instanceProperties));
        queue.grantSendMessages(snapshotDeletionTrigger);

        snapshotDeletionLambda.addEventSource(SqsEventSource.Builder.create(queue)
                .batchSize(instanceProperties.getInt(TRANSACTION_LOG_SNAPSHOT_DELETION_BATCH_SIZE))
                .maxConcurrency(instanceProperties.getInt(TRANSACTION_LOG_SNAPSHOT_DELETION_LAMBDA_CONCURRENCY_MAXIMUM)).build());

        coreStacks.grantReadTablesStatus(snapshotDeletionTrigger);
        coreStacks.grantInvokeScheduled(snapshotDeletionTrigger, queue);
        coreStacks.grantReadTablesStatus(snapshotDeletionLambda);
        transactionLogStateStoreStack.grantDeleteSnapshots(snapshotDeletionLambda);
    }
}
