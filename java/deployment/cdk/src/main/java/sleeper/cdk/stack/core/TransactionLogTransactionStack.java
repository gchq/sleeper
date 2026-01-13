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
package sleeper.cdk.stack.core;

import software.amazon.awscdk.Duration;
import software.amazon.awscdk.NestedStack;
import software.amazon.awscdk.services.events.Rule;
import software.amazon.awscdk.services.events.Schedule;
import software.amazon.awscdk.services.events.targets.LambdaFunction;
import software.amazon.awscdk.services.lambda.IFunction;
import software.amazon.awscdk.services.lambda.MetricType;
import software.amazon.awscdk.services.lambda.MetricsConfig;
import software.amazon.awscdk.services.lambda.StartingPosition;
import software.amazon.awscdk.services.lambda.eventsources.DynamoEventSource;
import software.amazon.awscdk.services.lambda.eventsources.SqsEventSource;
import software.amazon.awscdk.services.s3.IBucket;
import software.amazon.awscdk.services.sqs.DeadLetterQueue;
import software.amazon.awscdk.services.sqs.Queue;
import software.constructs.Construct;

import sleeper.cdk.SleeperInstanceProps;
import sleeper.cdk.jars.SleeperLambdaCode;
import sleeper.cdk.stack.SleeperCoreStacks;
import sleeper.cdk.stack.core.LoggingStack.LogGroupRef;
import sleeper.cdk.util.TrackDeadLetters;
import sleeper.cdk.util.Utils;
import sleeper.core.deploy.LambdaHandler;
import sleeper.core.deploy.SleeperScheduleRule;
import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.util.EnvironmentUtils;

import java.util.List;

import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.TRANSACTION_LOG_TRANSACTION_DELETION_DLQ_ARN;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.TRANSACTION_LOG_TRANSACTION_DELETION_DLQ_URL;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.TRANSACTION_LOG_TRANSACTION_DELETION_QUEUE_ARN;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.TRANSACTION_LOG_TRANSACTION_DELETION_QUEUE_URL;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.TRANSACTION_LOG_TRANSACTION_DELETION_RULE;
import static sleeper.core.properties.instance.TableStateProperty.TABLE_BATCHING_LAMBDAS_MEMORY_IN_MB;
import static sleeper.core.properties.instance.TableStateProperty.TABLE_BATCHING_LAMBDAS_TIMEOUT_IN_SECONDS;
import static sleeper.core.properties.instance.TableStateProperty.TRANSACTION_DELETION_BATCH_SIZE;
import static sleeper.core.properties.instance.TableStateProperty.TRANSACTION_DELETION_LAMBDA_CONCURRENCY_MAXIMUM;
import static sleeper.core.properties.instance.TableStateProperty.TRANSACTION_DELETION_LAMBDA_CONCURRENCY_RESERVED;
import static sleeper.core.properties.instance.TableStateProperty.TRANSACTION_DELETION_LAMBDA_PERIOD_IN_MINUTES;
import static sleeper.core.properties.instance.TableStateProperty.TRANSACTION_DELETION_LAMBDA_TIMEOUT_SECS;
import static sleeper.core.properties.instance.TableStateProperty.TRANSACTION_FOLLOWER_LAMBDA_CONCURRENCY_RESERVED;
import static sleeper.core.properties.instance.TableStateProperty.TRANSACTION_FOLLOWER_LAMBDA_MEMORY;
import static sleeper.core.properties.instance.TableStateProperty.TRANSACTION_FOLLOWER_LAMBDA_TIMEOUT_SECS;

public class TransactionLogTransactionStack extends NestedStack {
    public TransactionLogTransactionStack(
            Construct scope, String id,
            SleeperInstanceProps props, SleeperCoreStacks coreStacks,
            TransactionLogStateStoreStack transactionLogStateStoreStack,
            TrackDeadLetters trackDeadLetters) {
        super(scope, id);
        IBucket jarsBucket = props.getJars().createJarsBucketReference(this, "JarsBucket");
        SleeperLambdaCode lambdaCode = props.getJars().lambdaCode(jarsBucket);
        createFunctionToFollowTransactionLog(props.getInstanceProperties(), lambdaCode, coreStacks, transactionLogStateStoreStack);
        createTransactionDeletionLambda(props, lambdaCode, coreStacks, transactionLogStateStoreStack, trackDeadLetters);
        Utils.addTags(this, props.getInstanceProperties());
    }

    private void createTransactionDeletionLambda(SleeperInstanceProps props, SleeperLambdaCode lambdaCode,
            SleeperCoreStacks coreStacks, TransactionLogStateStoreStack transactionLogStateStoreStack,
            TrackDeadLetters trackDeadLetters) {
        InstanceProperties instanceProperties = props.getInstanceProperties();
        String instanceId = Utils.cleanInstanceId(instanceProperties);
        String triggerFunctionName = String.join("-", "sleeper", instanceId, "state-transaction-deletion-trigger");
        String deletionFunctionName = String.join("-", "sleeper", instanceId, "state-transaction-deletion");
        IFunction transactionDeletionTrigger = lambdaCode.buildFunction(this, LambdaHandler.TRANSACTION_DELETION_TRIGGER, "TransactionLogTransactionDeletionTrigger", builder -> builder
                .functionName(triggerFunctionName)
                .description("Creates batches of Sleeper tables to delete old transaction log transactions for and puts them on a queue to be processed")
                .environment(EnvironmentUtils.createDefaultEnvironment(instanceProperties))
                .reservedConcurrentExecutions(1)
                .memorySize(instanceProperties.getInt(TABLE_BATCHING_LAMBDAS_MEMORY_IN_MB))
                .timeout(Duration.seconds(instanceProperties.getInt(TABLE_BATCHING_LAMBDAS_TIMEOUT_IN_SECONDS)))
                .logGroup(coreStacks.getLogGroup(LogGroupRef.STATE_TRANSACTION_DELETION_TRIGGER)));
        IFunction transactionDeletionLambda = lambdaCode.buildFunction(this, LambdaHandler.TRANSACTION_DELETION, "TransactionLogTransactionDeletion", builder -> builder
                .functionName(deletionFunctionName)
                .description("Deletes old transaction log transactions for tables")
                .environment(EnvironmentUtils.createDefaultEnvironment(instanceProperties))
                .reservedConcurrentExecutions(instanceProperties.getIntOrNull(TRANSACTION_DELETION_LAMBDA_CONCURRENCY_RESERVED))
                .memorySize(1024)
                .timeout(Duration.seconds(instanceProperties.getInt(TRANSACTION_DELETION_LAMBDA_TIMEOUT_SECS)))
                .logGroup(coreStacks.getLogGroup(LogGroupRef.STATE_TRANSACTION_DELETION)));

        Rule rule = Rule.Builder.create(this, "TransactionLogTransactionDeletionSchedule")
                .ruleName(SleeperScheduleRule.TRANSACTION_LOG_TRANSACTION_DELETION.buildRuleName(instanceProperties))
                .description(SleeperScheduleRule.TRANSACTION_LOG_TRANSACTION_DELETION.getDescription())
                .schedule(Schedule.rate(Duration.minutes(
                        instanceProperties.getLong(TRANSACTION_DELETION_LAMBDA_PERIOD_IN_MINUTES))))
                .targets(List.of(new LambdaFunction(transactionDeletionTrigger)))
                .enabled(!props.isDeployPaused())
                .build();
        instanceProperties.set(TRANSACTION_LOG_TRANSACTION_DELETION_RULE, rule.getRuleName());

        Queue deadLetterQueue = Queue.Builder
                .create(this, "TransactionLogTransactionDeletionDeadLetterQueue")
                .queueName(String.join("-", "sleeper", instanceId, "TransactionLogTransactionDeletionDLQ.fifo"))
                .fifo(true)
                .build();
        Queue queue = Queue.Builder
                .create(this, "TransactionLogTransactionDeletionQueue")
                .queueName(String.join("-", "sleeper", instanceId, "TransactionLogTransactionDeletionQ.fifo"))
                .deadLetterQueue(DeadLetterQueue.builder()
                        .maxReceiveCount(1)
                        .queue(deadLetterQueue)
                        .build())
                .fifo(true)
                .visibilityTimeout(Duration.seconds(instanceProperties.getInt(TRANSACTION_DELETION_LAMBDA_TIMEOUT_SECS)))
                .build();
        instanceProperties.set(TRANSACTION_LOG_TRANSACTION_DELETION_QUEUE_URL, queue.getQueueUrl());
        instanceProperties.set(TRANSACTION_LOG_TRANSACTION_DELETION_QUEUE_ARN, queue.getQueueArn());
        instanceProperties.set(TRANSACTION_LOG_TRANSACTION_DELETION_DLQ_URL, deadLetterQueue.getQueueUrl());
        instanceProperties.set(TRANSACTION_LOG_TRANSACTION_DELETION_DLQ_ARN, deadLetterQueue.getQueueArn());
        trackDeadLetters.alarmOnDeadLetters(this, "TransactionLogTransactionDeletionAlarm", "transaction log transaction deletion", deadLetterQueue);
        queue.grantSendMessages(transactionDeletionTrigger);

        transactionDeletionLambda.addEventSource(SqsEventSource.Builder.create(queue)
                .batchSize(instanceProperties.getInt(TRANSACTION_DELETION_BATCH_SIZE))
                .maxConcurrency(instanceProperties.getIntOrNull(TRANSACTION_DELETION_LAMBDA_CONCURRENCY_MAXIMUM)).build());

        coreStacks.grantReadTablesStatus(transactionDeletionTrigger);
        coreStacks.grantInvokeScheduled(transactionDeletionTrigger, queue);
        coreStacks.grantReadTablesStatus(transactionDeletionLambda);
        transactionLogStateStoreStack.grantDeleteTransactions(transactionDeletionLambda);
    }

    private void createFunctionToFollowTransactionLog(
            InstanceProperties instanceProperties, SleeperLambdaCode lambdaCode, SleeperCoreStacks coreStacks, TransactionLogStateStoreStack transactionLogStateStoreStack) {
        String instanceId = Utils.cleanInstanceId(instanceProperties);
        String functionName = String.join("-", "sleeper", instanceId, "state-transaction-follower");
        IFunction lambda = lambdaCode.buildFunction(this, LambdaHandler.TRANSACTION_FOLLOWER, "TransactionLogFollower", builder -> builder
                .functionName(functionName)
                .description("Follows the state store transaction log to trigger updates, e.g. to job trackers")
                .environment(EnvironmentUtils.createDefaultEnvironment(instanceProperties))
                .reservedConcurrentExecutions(instanceProperties.getIntOrNull(TRANSACTION_FOLLOWER_LAMBDA_CONCURRENCY_RESERVED))
                .memorySize(instanceProperties.getInt(TRANSACTION_FOLLOWER_LAMBDA_MEMORY))
                .timeout(Duration.seconds(instanceProperties.getInt(TRANSACTION_FOLLOWER_LAMBDA_TIMEOUT_SECS)))
                .logGroup(coreStacks.getLogGroup(LogGroupRef.STATE_TRANSACTION_FOLLOWER)));

        lambda.addEventSource(DynamoEventSource.Builder.create(transactionLogStateStoreStack.getFilesLogTable())
                .startingPosition(StartingPosition.LATEST)
                .metricsConfig(MetricsConfig.builder()
                        .metrics(List.of(MetricType.EVENT_COUNT))
                        .build())
                .build());
        coreStacks.grantUpdateJobTrackersFromTransactionLog(lambda);
    }
}
