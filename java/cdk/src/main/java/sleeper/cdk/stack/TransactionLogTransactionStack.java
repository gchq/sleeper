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
import sleeper.configuration.properties.SleeperScheduleRule;
import sleeper.configuration.properties.instance.InstanceProperties;

import java.util.List;

import static sleeper.cdk.Utils.createAlarmForDlq;
import static sleeper.cdk.Utils.createLambdaLogGroup;
import static sleeper.cdk.Utils.shouldDeployPaused;
import static sleeper.configuration.properties.instance.CdkDefinedInstanceProperty.TRANSACTION_LOG_TRANSACTION_DELETION_DLQ_ARN;
import static sleeper.configuration.properties.instance.CdkDefinedInstanceProperty.TRANSACTION_LOG_TRANSACTION_DELETION_DLQ_URL;
import static sleeper.configuration.properties.instance.CdkDefinedInstanceProperty.TRANSACTION_LOG_TRANSACTION_DELETION_QUEUE_ARN;
import static sleeper.configuration.properties.instance.CdkDefinedInstanceProperty.TRANSACTION_LOG_TRANSACTION_DELETION_QUEUE_URL;
import static sleeper.configuration.properties.instance.CdkDefinedInstanceProperty.TRANSACTION_LOG_TRANSACTION_DELETION_RULE;
import static sleeper.configuration.properties.instance.CommonProperty.JARS_BUCKET;
import static sleeper.configuration.properties.instance.CommonProperty.TABLE_BATCHING_LAMBDAS_MEMORY_IN_MB;
import static sleeper.configuration.properties.instance.CommonProperty.TABLE_BATCHING_LAMBDAS_TIMEOUT_IN_SECONDS;
import static sleeper.configuration.properties.instance.CommonProperty.TRANSACTION_LOG_TRANSACTION_DELETION_BATCH_SIZE;
import static sleeper.configuration.properties.instance.CommonProperty.TRANSACTION_LOG_TRANSACTION_DELETION_LAMBDA_CONCURRENCY_MAXIMUM;
import static sleeper.configuration.properties.instance.CommonProperty.TRANSACTION_LOG_TRANSACTION_DELETION_LAMBDA_CONCURRENCY_RESERVED;
import static sleeper.configuration.properties.instance.CommonProperty.TRANSACTION_LOG_TRANSACTION_DELETION_LAMBDA_PERIOD_IN_MINUTES;

public class TransactionLogTransactionStack extends NestedStack {
    public TransactionLogTransactionStack(
            Construct scope, String id,
            InstanceProperties instanceProperties, BuiltJars jars, CoreStacks coreStacks,
            TransactionLogStateStoreStack transactionLogStateStoreStack,
            Topic topic, List<IMetric> errorMetrics) {
        super(scope, id);
        IBucket jarsBucket = Bucket.fromBucketName(this, "JarsBucket", instanceProperties.get(JARS_BUCKET));
        LambdaCode statestoreJar = jars.lambdaCode(BuiltJar.STATESTORE, jarsBucket);
        createTransactionDeletionLambda(instanceProperties, statestoreJar, coreStacks, transactionLogStateStoreStack, topic, errorMetrics);
        Utils.addStackTagIfSet(this, instanceProperties);
    }

    private void createTransactionDeletionLambda(InstanceProperties instanceProperties, LambdaCode statestoreJar,
            CoreStacks coreStacks, TransactionLogStateStoreStack transactionLogStateStoreStack,
            Topic topic, List<IMetric> errorMetrics) {
        String instanceId = Utils.cleanInstanceId(instanceProperties);
        String triggerFunctionName = String.join("-", "sleeper", instanceId, "state-transaction-deletion-trigger");
        String deletionFunctionName = String.join("-", "sleeper", instanceId, "state-transaction-deletion");
        IFunction transactionDeletionTrigger = statestoreJar.buildFunction(this, "TransactionLogTransactionDeletionTrigger", builder -> builder
                .functionName(triggerFunctionName)
                .description("Creates batches of Sleeper tables to delete old transaction log transactions for and puts them on a queue to be processed")
                .runtime(Runtime.JAVA_11)
                .handler("sleeper.statestore.transaction.TransactionLogTransactionDeletionTriggerLambda::handleRequest")
                .environment(Utils.createDefaultEnvironment(instanceProperties))
                .reservedConcurrentExecutions(instanceProperties.getInt(TRANSACTION_LOG_TRANSACTION_DELETION_LAMBDA_CONCURRENCY_RESERVED))
                .memorySize(instanceProperties.getInt(TABLE_BATCHING_LAMBDAS_MEMORY_IN_MB))
                .timeout(Duration.seconds(instanceProperties.getInt(TABLE_BATCHING_LAMBDAS_TIMEOUT_IN_SECONDS)))
                .logGroup(createLambdaLogGroup(this, "TransactionLogTransactionDeletionTriggerLogGroup", triggerFunctionName, instanceProperties)));
        IFunction transactionDeletionLambda = statestoreJar.buildFunction(this, "TransactionLogTransactionDeletion", builder -> builder
                .functionName(deletionFunctionName)
                .description("Deletes old transaction log transactions for tables")
                .runtime(Runtime.JAVA_11)
                .handler("sleeper.statestore.transaction.TransactionLogTransactionDeletionLambda::handleRequest")
                .environment(Utils.createDefaultEnvironment(instanceProperties))
                .memorySize(1024)
                .timeout(Duration.minutes(1))
                .logGroup(createLambdaLogGroup(this, "TransactionLogTransactionDeletionLogGroup", deletionFunctionName, instanceProperties)));

        Rule rule = Rule.Builder.create(this, "TransactionLogTransactionDeletionSchedule")
                .ruleName(SleeperScheduleRule.TRANSACTION_LOG_TRANSACTION_DELETION.buildRuleName(instanceProperties))
                .schedule(Schedule.rate(Duration.minutes(
                        instanceProperties.getLong(TRANSACTION_LOG_TRANSACTION_DELETION_LAMBDA_PERIOD_IN_MINUTES))))
                .targets(List.of(new LambdaFunction(transactionDeletionTrigger)))
                .enabled(!shouldDeployPaused(this))
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
                .visibilityTimeout(Duration.seconds(70))
                .build();
        instanceProperties.set(TRANSACTION_LOG_TRANSACTION_DELETION_QUEUE_URL, queue.getQueueUrl());
        instanceProperties.set(TRANSACTION_LOG_TRANSACTION_DELETION_QUEUE_ARN, queue.getQueueArn());
        instanceProperties.set(TRANSACTION_LOG_TRANSACTION_DELETION_DLQ_URL, deadLetterQueue.getQueueUrl());
        instanceProperties.set(TRANSACTION_LOG_TRANSACTION_DELETION_DLQ_ARN, deadLetterQueue.getQueueArn());
        createAlarmForDlq(this, "TransactionLogTransactionDeletionAlarm",
                "Alarms if there are any messages on the dead letter queue for the transaction log transaction deletion queue",
                deadLetterQueue, topic);
        errorMetrics.add(Utils.createErrorMetric("Transaction Log Transaction Deletion Errors", deadLetterQueue, instanceProperties));
        queue.grantSendMessages(transactionDeletionTrigger);

        transactionDeletionLambda.addEventSource(SqsEventSource.Builder.create(queue)
                .batchSize(instanceProperties.getInt(TRANSACTION_LOG_TRANSACTION_DELETION_BATCH_SIZE))
                .maxConcurrency(instanceProperties.getInt(TRANSACTION_LOG_TRANSACTION_DELETION_LAMBDA_CONCURRENCY_MAXIMUM)).build());

        coreStacks.grantReadTablesStatus(transactionDeletionTrigger);
        coreStacks.grantInvokeScheduled(transactionDeletionTrigger, queue);
        coreStacks.grantReadTablesStatus(transactionDeletionLambda);
        transactionLogStateStoreStack.grantDeleteTransactions(transactionDeletionLambda);
    }
}
