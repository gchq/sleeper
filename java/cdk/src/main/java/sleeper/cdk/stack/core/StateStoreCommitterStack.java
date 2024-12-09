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
package sleeper.cdk.stack.core;

import software.amazon.awscdk.Duration;
import software.amazon.awscdk.NestedStack;
import software.amazon.awscdk.services.cloudwatch.IMetric;
import software.amazon.awscdk.services.iam.Effect;
import software.amazon.awscdk.services.iam.IGrantable;
import software.amazon.awscdk.services.iam.PolicyStatement;
import software.amazon.awscdk.services.lambda.IFunction;
import software.amazon.awscdk.services.lambda.eventsources.SqsEventSource;
import software.amazon.awscdk.services.logs.ILogGroup;
import software.amazon.awscdk.services.s3.Bucket;
import software.amazon.awscdk.services.s3.IBucket;
import software.amazon.awscdk.services.sns.Topic;
import software.amazon.awscdk.services.sqs.DeadLetterQueue;
import software.amazon.awscdk.services.sqs.DeduplicationScope;
import software.amazon.awscdk.services.sqs.FifoThroughputLimit;
import software.amazon.awscdk.services.sqs.Queue;
import software.constructs.Construct;

import sleeper.cdk.jars.BuiltJars;
import sleeper.cdk.jars.LambdaCode;
import sleeper.cdk.stack.compaction.CompactionStatusStoreResources;
import sleeper.cdk.stack.core.LoggingStack.LogGroupRef;
import sleeper.cdk.stack.ingest.IngestStatusStoreResources;
import sleeper.cdk.util.Utils;
import sleeper.core.deploy.LambdaHandler;
import sleeper.core.properties.instance.InstanceProperties;

import java.util.List;
import java.util.Map;

import static sleeper.cdk.util.Utils.createAlarmForDlq;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.STATESTORE_COMMITTER_DLQ_ARN;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.STATESTORE_COMMITTER_DLQ_URL;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.STATESTORE_COMMITTER_EVENT_SOURCE_ID;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.STATESTORE_COMMITTER_LOG_GROUP;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.STATESTORE_COMMITTER_QUEUE_ARN;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.STATESTORE_COMMITTER_QUEUE_URL;
import static sleeper.core.properties.instance.TableStateProperty.STATESTORE_COMMITTER_BATCH_SIZE;
import static sleeper.core.properties.instance.TableStateProperty.STATESTORE_COMMITTER_LAMBDA_CONCURRENCY_MAXIMUM;
import static sleeper.core.properties.instance.TableStateProperty.STATESTORE_COMMITTER_LAMBDA_CONCURRENCY_RESERVED;
import static sleeper.core.properties.instance.TableStateProperty.STATESTORE_COMMITTER_LAMBDA_MEMORY_IN_MB;
import static sleeper.core.properties.instance.TableStateProperty.STATESTORE_COMMITTER_LAMBDA_TIMEOUT_IN_SECONDS;

public class StateStoreCommitterStack extends NestedStack {
    private final InstanceProperties instanceProperties;
    private final Queue commitQueue;

    @SuppressWarnings("checkstyle:ParameterNumberCheck")
    public StateStoreCommitterStack(
            Construct scope,
            String id,
            InstanceProperties instanceProperties,
            BuiltJars jars,
            LoggingStack loggingStack,
            ConfigBucketStack configBucketStack,
            TableIndexStack tableIndexStack,
            StateStoreStacks stateStoreStacks,
            IngestStatusStoreResources ingestStatusStore,
            CompactionStatusStoreResources compactionStatusStore,
            ManagedPoliciesStack policiesStack,
            Topic topic,
            List<IMetric> errorMetrics) {
        super(scope, id);
        this.instanceProperties = instanceProperties;
        IBucket jarsBucket = Bucket.fromBucketName(this, "JarsBucket", jars.bucketName());
        LambdaCode lambdaCode = jars.lambdaCode(jarsBucket);

        commitQueue = sqsQueueForStateStoreCommitter(policiesStack, topic, errorMetrics);
        lambdaToCommitStateStoreUpdates(
                loggingStack, policiesStack, lambdaCode,
                configBucketStack, tableIndexStack, stateStoreStacks,
                compactionStatusStore, ingestStatusStore);
    }

    private Queue sqsQueueForStateStoreCommitter(ManagedPoliciesStack policiesStack, Topic topic, List<IMetric> errorMetrics) {
        String instanceId = Utils.cleanInstanceId(instanceProperties);
        Queue deadLetterQueue = Queue.Builder
                .create(this, "StateStoreCommitterDLQ")
                .queueName(String.join("-", "sleeper", instanceId, "StateStoreCommitterDLQ.fifo"))
                .fifo(true)
                .build();
        Queue queue = Queue.Builder
                .create(this, "StateStoreCommitterQueue")
                .queueName(String.join("-", "sleeper", instanceId, "StateStoreCommitterQ.fifo"))
                .deadLetterQueue(DeadLetterQueue.builder()
                        .maxReceiveCount(1)
                        .queue(deadLetterQueue)
                        .build())
                .fifo(true)
                .fifoThroughputLimit(FifoThroughputLimit.PER_MESSAGE_GROUP_ID)
                .deduplicationScope(DeduplicationScope.MESSAGE_GROUP)
                .visibilityTimeout(
                        Duration.seconds(instanceProperties.getInt(STATESTORE_COMMITTER_LAMBDA_TIMEOUT_IN_SECONDS)))
                .build();
        instanceProperties.set(STATESTORE_COMMITTER_QUEUE_URL, queue.getQueueUrl());
        instanceProperties.set(STATESTORE_COMMITTER_QUEUE_ARN, queue.getQueueArn());
        instanceProperties.set(STATESTORE_COMMITTER_DLQ_URL, deadLetterQueue.getQueueUrl());
        instanceProperties.set(STATESTORE_COMMITTER_DLQ_ARN, deadLetterQueue.getQueueArn());

        queue.grantSendMessages(policiesStack.getDirectIngestPolicyForGrants());
        queue.grantPurge(policiesStack.getPurgeQueuesPolicyForGrants());
        createAlarmForDlq(this, "StateStoreCommitterAlarm",
                "Alarms if there are any messages on the dead letter queue for the state store committer lambda",
                deadLetterQueue, topic);
        errorMetrics.add(Utils.createErrorMetric("State Store Committer Errors", deadLetterQueue, instanceProperties));
        return queue;
    }

    private void lambdaToCommitStateStoreUpdates(
            LoggingStack loggingStack, ManagedPoliciesStack policiesStack, LambdaCode lambdaCode,
            ConfigBucketStack configBucketStack, TableIndexStack tableIndexStack, StateStoreStacks stateStoreStacks,
            CompactionStatusStoreResources compactionStatusStore,
            IngestStatusStoreResources ingestStatusStore) {
        Map<String, String> environmentVariables = Utils.createDefaultEnvironment(instanceProperties);

        String functionName = String.join("-", "sleeper",
                Utils.cleanInstanceId(instanceProperties), "statestore-committer");
        ILogGroup logGroup = loggingStack.getLogGroup(LogGroupRef.STATESTORE_COMMITTER);
        instanceProperties.set(STATESTORE_COMMITTER_LOG_GROUP, logGroup.getLogGroupName());

        IFunction handlerFunction = lambdaCode.buildFunction(this, LambdaHandler.STATESTORE_COMMITTER, "StateStoreCommitter", builder -> builder
                .functionName(functionName)
                .description("Commits updates to the state store. Used to commit compaction and ingest jobs asynchronously.")
                .memorySize(instanceProperties.getInt(STATESTORE_COMMITTER_LAMBDA_MEMORY_IN_MB))
                .timeout(Duration.seconds(instanceProperties.getInt(STATESTORE_COMMITTER_LAMBDA_TIMEOUT_IN_SECONDS)))
                .environment(environmentVariables)
                .reservedConcurrentExecutions(instanceProperties.getIntOrNull(STATESTORE_COMMITTER_LAMBDA_CONCURRENCY_RESERVED))
                .logGroup(logGroup));

        SqsEventSource eventSource = SqsEventSource.Builder.create(commitQueue)
                .batchSize(instanceProperties.getInt(STATESTORE_COMMITTER_BATCH_SIZE))
                .maxConcurrency(instanceProperties.getIntOrNull(STATESTORE_COMMITTER_LAMBDA_CONCURRENCY_MAXIMUM))
                .build();
        handlerFunction.addEventSource(eventSource);
        instanceProperties.set(STATESTORE_COMMITTER_EVENT_SOURCE_ID, eventSource.getEventSourceMappingId());

        policiesStack.getEditStateStoreCommitterTriggerPolicyForGrants().addStatements(
                PolicyStatement.Builder.create()
                        .effect(Effect.ALLOW)
                        .actions(List.of("lambda:GetEventSourceMapping"))
                        .resources(List.of(eventSource.getEventSourceMappingArn()))
                        .build(),
                PolicyStatement.Builder.create()
                        .effect(Effect.ALLOW)
                        .actions(List.of("lambda:UpdateEventSourceMapping"))
                        .resources(List.of(eventSource.getEventSourceMappingArn()))
                        .build());
        logGroup.grantRead(policiesStack.getReportingPolicyForGrants());
        logGroup.grant(policiesStack.getReportingPolicyForGrants(), "logs:StartQuery", "logs:GetQueryResults");
        configBucketStack.grantRead(handlerFunction);
        tableIndexStack.grantRead(handlerFunction);
        stateStoreStacks.grantReadWriteAllFilesAndPartitions(handlerFunction);
        compactionStatusStore.grantWriteJobEvent(handlerFunction);
        ingestStatusStore.grantWriteJobEvent(handlerFunction);
    }

    public void grantSendCommits(IGrantable grantee) {
        commitQueue.grantSendMessages(grantee);
    }
}
