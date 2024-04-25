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

import static sleeper.cdk.Utils.createAlarmForDlq;
import static sleeper.cdk.Utils.createLambdaLogGroup;
import static sleeper.cdk.Utils.shouldDeployPaused;
import static sleeper.configuration.properties.instance.CdkDefinedInstanceProperty.TRANSACTION_LOG_SNAPSHOT_CREATION_DLQ_ARN;
import static sleeper.configuration.properties.instance.CdkDefinedInstanceProperty.TRANSACTION_LOG_SNAPSHOT_CREATION_DLQ_URL;
import static sleeper.configuration.properties.instance.CdkDefinedInstanceProperty.TRANSACTION_LOG_SNAPSHOT_CREATION_QUEUE_ARN;
import static sleeper.configuration.properties.instance.CdkDefinedInstanceProperty.TRANSACTION_LOG_SNAPSHOT_CREATION_QUEUE_URL;
import static sleeper.configuration.properties.instance.CdkDefinedInstanceProperty.TRANSACTION_LOG_SNAPSHOT_CREATION_RULE;
import static sleeper.configuration.properties.instance.CommonProperty.ID;
import static sleeper.configuration.properties.instance.CommonProperty.JARS_BUCKET;
import static sleeper.configuration.properties.instance.CommonProperty.TABLE_BATCHING_LAMBDAS_MEMORY_IN_MB;
import static sleeper.configuration.properties.instance.CommonProperty.TABLE_BATCHING_LAMBDAS_TIMEOUT_IN_SECONDS;;

public class TransactionLogSnapshotCreationStack extends NestedStack {

    public TransactionLogSnapshotCreationStack(
            Construct scope, String id,
            InstanceProperties instanceProperties, BuiltJars jars, CoreStacks coreStacks,
            Topic topic, List<IMetric> errorMetrics) {
        super(scope, id);
        IBucket jarsBucket = Bucket.fromBucketName(this, "JarsBucket", instanceProperties.get(JARS_BUCKET));
        LambdaCode statestoreJar = jars.lambdaCode(BuiltJar.STATESTORE_LAMBDA, jarsBucket);
        String triggerFunctionName = Utils.truncateTo64Characters(String.join("-", "sleeper",
                instanceProperties.get(ID).toLowerCase(Locale.ROOT), "transaction-log-snapshot-creation-trigger"));
        String creationFunctionName = Utils.truncateTo64Characters(String.join("-", "sleeper",
                instanceProperties.get(ID).toLowerCase(Locale.ROOT), "transaction-log-snapshot-creation"));
        IFunction snapshotCreationTrigger = statestoreJar.buildFunction(this, "TransactionLogSnapshotCreationTrigger", builder -> builder
                .functionName(triggerFunctionName)
                .description("Creates batches of Sleeper tables to create transaction log snapshots for and puts them on a queue to be processed")
                .runtime(Runtime.JAVA_11)
                .handler("sleeper.statestore.TransactionLogSnapshotCreationTriggerLambda::handleRequest")
                .environment(Utils.createDefaultEnvironment(instanceProperties))
                .memorySize(instanceProperties.getInt(TABLE_BATCHING_LAMBDAS_MEMORY_IN_MB))
                .timeout(Duration.seconds(instanceProperties.getInt(TABLE_BATCHING_LAMBDAS_TIMEOUT_IN_SECONDS)))
                .logGroup(createLambdaLogGroup(this, "TransactionLogSnapshotCreationTriggerLogGroup", triggerFunctionName, instanceProperties)));
        IFunction snapshotCreationLambda = statestoreJar.buildFunction(this, "TransactionLogSnapshotCreation", builder -> builder
                .functionName(creationFunctionName)
                .description("Creates transaction log snapshots for tables")
                .runtime(Runtime.JAVA_11)
                .handler("sleeper.statestore.TransactionLogSnapshotCreationLambda::handleRequest")
                .environment(Utils.createDefaultEnvironment(instanceProperties))
                .memorySize(1024)
                .timeout(Duration.minutes(1))
                .logGroup(createLambdaLogGroup(this, "TransactionLogSnapshotCreationLogGroup", creationFunctionName, instanceProperties)));

        coreStacks.grantReadTablesStatus(snapshotCreationTrigger);
        coreStacks.grantReadTablesMetadata(snapshotCreationLambda);
        Rule rule = Rule.Builder.create(this, "TransactionLogSnapshotCreationSchedule")
                .ruleName(SleeperScheduleRule.TRANSACTION_LOG_SNAPSHOT_CREATION.buildRuleName(instanceProperties))
                .schedule(Schedule.rate(Duration.minutes(1)))
                .targets(List.of(new LambdaFunction(snapshotCreationTrigger)))
                .enabled(!shouldDeployPaused(this))
                .build();
        instanceProperties.set(TRANSACTION_LOG_SNAPSHOT_CREATION_RULE, rule.getRuleName());

        String deadLetterQueueName = Utils.truncateTo64Characters(instanceProperties.get(ID) + "-TransactionLogSnapshotDLQ");
        Queue deadLetterQueue = Queue.Builder
                .create(this, "TransactionLogSnapshotDeadLetterQueue")
                .queueName(deadLetterQueueName)
                .build();
        String queueName = Utils.truncateTo64Characters(instanceProperties.get(ID) + "-TransactionLogSnapshotQ");
        Queue queue = Queue.Builder
                .create(this, "TransactionLogSnapshotQueue")
                .queueName(queueName)
                .deadLetterQueue(DeadLetterQueue.builder()
                        .maxReceiveCount(1)
                        .queue(deadLetterQueue)
                        .build())
                .visibilityTimeout(Duration.seconds(70))
                .build();
        instanceProperties.set(TRANSACTION_LOG_SNAPSHOT_CREATION_QUEUE_URL, queue.getQueueUrl());
        instanceProperties.set(TRANSACTION_LOG_SNAPSHOT_CREATION_QUEUE_ARN, queue.getQueueArn());
        instanceProperties.set(TRANSACTION_LOG_SNAPSHOT_CREATION_DLQ_URL, deadLetterQueue.getQueueUrl());
        instanceProperties.set(TRANSACTION_LOG_SNAPSHOT_CREATION_DLQ_ARN, deadLetterQueue.getQueueArn());
        createAlarmForDlq(this, "TransactionLogSnapshotCreationAlarm",
                "Alarms if there are any messages on the dead letter queue for the transaction log snapshot creation queue",
                deadLetterQueue, topic);
        errorMetrics.add(Utils.createErrorMetric("Transaction Log Snapshot Errors", deadLetterQueue, instanceProperties));
        queue.grantSendMessages(snapshotCreationTrigger);
        coreStacks.grantInvokeScheduled(snapshotCreationTrigger, queue);
        snapshotCreationLambda.addEventSource(new SqsEventSource(queue,
                SqsEventSourceProps.builder().batchSize(1).build()));
        coreStacks.grantReadTablesAndData(snapshotCreationLambda);

        Utils.addStackTagIfSet(this, instanceProperties);
    }
}
