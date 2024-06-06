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
import software.amazon.awscdk.services.lambda.IFunction;
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
import sleeper.configuration.properties.instance.InstanceProperties;

import java.util.List;
import java.util.Map;

import static sleeper.cdk.Utils.createAlarmForDlq;
import static sleeper.cdk.Utils.createLambdaLogGroup;
import static sleeper.configuration.properties.instance.CdkDefinedInstanceProperty.STATESTORE_COMMITTER_DLQ_ARN;
import static sleeper.configuration.properties.instance.CdkDefinedInstanceProperty.STATESTORE_COMMITTER_DLQ_URL;
import static sleeper.configuration.properties.instance.CdkDefinedInstanceProperty.STATESTORE_COMMITTER_QUEUE_ARN;
import static sleeper.configuration.properties.instance.CdkDefinedInstanceProperty.STATESTORE_COMMITTER_QUEUE_URL;
import static sleeper.configuration.properties.instance.CommonProperty.STATESTORE_COMMITTER_BATCH_SIZE;
import static sleeper.configuration.properties.instance.CommonProperty.STATESTORE_COMMITTER_LAMBDA_MEMORY_IN_MB;
import static sleeper.configuration.properties.instance.CommonProperty.STATESTORE_COMMITTER_LAMBDA_TIMEOUT_IN_SECONDS;
import static software.amazon.awscdk.services.lambda.Runtime.JAVA_11;

public class StateStoreUpdateStack extends NestedStack {
    private final InstanceProperties instanceProperties;

    public StateStoreUpdateStack(
            Construct scope,
            String id,
            InstanceProperties instanceProperties,
            BuiltJars jars,
            CompactionStatusStoreResources compactionStatusStore,
            Topic topic,
            CoreStacks coreStacks,
            List<IMetric> errorMetrics) {
        super(scope, id);
        this.instanceProperties = instanceProperties;
        IBucket jarsBucket = Bucket.fromBucketName(this, "JarsBucket", jars.bucketName());
        LambdaCode jobCommitterJar = jars.lambdaCode(BuiltJar.STATESTORE_COMMITTER, jarsBucket);

        Queue queue = sqsQueueForStateStoreCommitter(topic, errorMetrics);
        lambdaToCommitStateStoreUpdates(coreStacks, topic, errorMetrics, jarsBucket, jobCommitterJar, queue, compactionStatusStore);
    }

    private Queue sqsQueueForStateStoreCommitter(Topic topic, List<IMetric> errorMetrics) {
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
                .visibilityTimeout(
                        Duration.seconds(instanceProperties.getInt(STATESTORE_COMMITTER_LAMBDA_TIMEOUT_IN_SECONDS)))
                .build();
        instanceProperties.set(STATESTORE_COMMITTER_QUEUE_URL, queue.getQueueUrl());
        instanceProperties.set(STATESTORE_COMMITTER_QUEUE_ARN, queue.getQueueArn());
        instanceProperties.set(STATESTORE_COMMITTER_DLQ_URL, deadLetterQueue.getQueueUrl());
        instanceProperties.set(STATESTORE_COMMITTER_DLQ_ARN, deadLetterQueue.getQueueArn());

        createAlarmForDlq(this, "StateStoreCommitterAlarm",
                "Alarms if there are any messages on the dead letter queue for the state store committer lambda",
                deadLetterQueue, topic);
        errorMetrics.add(Utils.createErrorMetric("State Store Committer Errors", deadLetterQueue, instanceProperties));
        return queue;
    }

    private void lambdaToCommitStateStoreUpdates(
            CoreStacks coreStacks, Topic topic, List<IMetric> errorMetrics,
            IBucket jarsBucket, LambdaCode jobCommitterJar, Queue jobCommitterQueue,
            CompactionStatusStoreResources compactionStatusStore) {
        Map<String, String> environmentVariables = Utils.createDefaultEnvironment(instanceProperties);

        String functionName = String.join("-", "sleeper",
                Utils.cleanInstanceId(instanceProperties), "compaction-job-committer");

        IFunction handlerFunction = jobCommitterJar.buildFunction(this, "CompactionJobCommitter", builder -> builder
                .functionName(functionName)
                .description("Applies the results of a compaction job to the state store and updates the status store.")
                .runtime(JAVA_11)
                .memorySize(instanceProperties.getInt(STATESTORE_COMMITTER_LAMBDA_MEMORY_IN_MB))
                .timeout(Duration.seconds(instanceProperties.getInt(STATESTORE_COMMITTER_LAMBDA_TIMEOUT_IN_SECONDS)))
                .handler("sleeper.committer.lambda.StateStoreCommitterLambda::handleRequest")
                .environment(environmentVariables)
                .logGroup(createLambdaLogGroup(this, "StateStoreCommitterLogGroup", functionName, instanceProperties)));

        handlerFunction.addEventSource(SqsEventSource.Builder.create(jobCommitterQueue)
                .batchSize(instanceProperties.getInt(STATESTORE_COMMITTER_BATCH_SIZE))
                .build());

        jobCommitterQueue.grantSendMessages(handlerFunction);
        coreStacks.grantRunCompactionJobs(handlerFunction);
        jarsBucket.grantRead(handlerFunction);
        compactionStatusStore.grantWriteJobEvent(handlerFunction);
    }
}
