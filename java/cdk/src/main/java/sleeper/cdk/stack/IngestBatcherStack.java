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

import software.amazon.awscdk.Duration;
import software.amazon.awscdk.NestedStack;
import software.amazon.awscdk.services.lambda.IFunction;
import software.amazon.awscdk.services.lambda.eventsources.SqsEventSource;
import software.amazon.awscdk.services.s3.Bucket;
import software.amazon.awscdk.services.s3.IBucket;
import software.amazon.awscdk.services.sqs.DeadLetterQueue;
import software.amazon.awscdk.services.sqs.Queue;
import software.constructs.Construct;

import sleeper.cdk.Utils;
import sleeper.cdk.jars.BuiltJar;
import sleeper.cdk.jars.BuiltJars;
import sleeper.cdk.jars.LambdaCode;
import sleeper.cdk.stack.bulkimport.EksBulkImportStack;
import sleeper.cdk.stack.bulkimport.EmrBulkImportStack;
import sleeper.cdk.stack.bulkimport.PersistentEmrBulkImportStack;
import sleeper.configuration.properties.InstanceProperties;

import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Stream;

import static sleeper.configuration.properties.SystemDefinedInstanceProperty.CONFIG_BUCKET;
import static sleeper.configuration.properties.SystemDefinedInstanceProperty.INGEST_BATCHER_SUBMIT_DLQ_URL;
import static sleeper.configuration.properties.SystemDefinedInstanceProperty.INGEST_BATCHER_SUBMIT_QUEUE_URL;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.ID;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.INGEST_BATCHER_JOB_CREATOR_MEMORY_IN_MB;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.INGEST_BATCHER_JOB_CREATOR_TIMEOUT_IN_SECONDS;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.INGEST_BATCHER_SUBMITTER_MEMORY_IN_MB;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.INGEST_BATCHER_SUBMITTER_TIMEOUT_IN_SECONDS;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.LOG_RETENTION_IN_DAYS;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.QUEUE_VISIBILITY_TIMEOUT_IN_SECONDS;

public class IngestBatcherStack extends NestedStack {

    private Queue submitQueue;
    private Queue submitDLQ;

    public IngestBatcherStack(
            Construct scope,
            String id,
            InstanceProperties instanceProperties,
            BuiltJars jars,
            IngestStack ingestStack,
            EmrBulkImportStack emrBulkImportStack,
            PersistentEmrBulkImportStack persistentEmrBulkImportStack,
            EksBulkImportStack eksBulkImportStack) {
        super(scope, id);

        // Queue to submit files to the batcher
        submitDLQ = Queue.Builder
                .create(this, "IngestBatcherSubmitDLQ")
                .queueName(Utils.truncateTo64Characters(instanceProperties.get(ID) + "-IngestBatcherSubmitDLQ"))
                .build();
        DeadLetterQueue ingestJobDeadLetterQueue = DeadLetterQueue.builder()
                .maxReceiveCount(1)
                .queue(submitDLQ)
                .build();
        submitQueue = Queue.Builder
                .create(this, "IngestBatcherSubmitQueue")
                .queueName(Utils.truncateTo64Characters(instanceProperties.get(ID) + "-IngestBatcherSubmitQ"))
                .deadLetterQueue(ingestJobDeadLetterQueue)
                .visibilityTimeout(Duration.seconds(instanceProperties.getInt(QUEUE_VISIBILITY_TIMEOUT_IN_SECONDS)))
                .build();
        instanceProperties.set(INGEST_BATCHER_SUBMIT_QUEUE_URL, submitQueue.getQueueUrl());
        instanceProperties.set(INGEST_BATCHER_SUBMIT_DLQ_URL, submitDLQ.getQueueUrl());

        // Lambdas to receive submitted files and create batches
        IBucket jarsBucket = Bucket.fromBucketName(this, "JarsBucket", jars.bucketName());
        IBucket configBucket = Bucket.fromBucketName(scope, "ConfigBucket", instanceProperties.get(CONFIG_BUCKET));
        LambdaCode submitterJar = jars.lambdaCode(BuiltJar.INGEST_BATCHER_SUBMITTER, jarsBucket);
        LambdaCode jobCreatorJar = jars.lambdaCode(BuiltJar.INGEST_BATCHER_JOB_CREATOR, jarsBucket);

        String submitterName = Utils.truncateTo64Characters(String.join("-", "sleeper",
                instanceProperties.get(ID).toLowerCase(Locale.ROOT), "submit-ingest-batcher"));
        String jobCreatorName = Utils.truncateTo64Characters(String.join("-", "sleeper",
                instanceProperties.get(ID).toLowerCase(Locale.ROOT), "batch-ingest-jobs"));

        Map<String, String> environmentVariables = Utils.createDefaultEnvironment(instanceProperties);

        IFunction submitterLambda = submitterJar.buildFunction(this, "SubmitToIngestBatcherLambda", builder -> builder
                .functionName(submitterName)
                .description("Triggered by an SQS event that contains a request to ingest a file")
                .runtime(software.amazon.awscdk.services.lambda.Runtime.JAVA_11)
                .memorySize(instanceProperties.getInt(INGEST_BATCHER_SUBMITTER_MEMORY_IN_MB))
                .timeout(Duration.seconds(instanceProperties.getInt(INGEST_BATCHER_SUBMITTER_TIMEOUT_IN_SECONDS)))
                .handler("sleeper.ingest.batcher.submitter.IngestBatcherSubmitterLambda::handleRequest")
                .environment(environmentVariables)
                .logRetention(Utils.getRetentionDays(instanceProperties.getInt(LOG_RETENTION_IN_DAYS)))
                .events(List.of(new SqsEventSource(submitQueue))));

        submitQueue.grantConsumeMessages(submitterLambda);
        configBucket.grantRead(submitterLambda);

        IFunction jobCreatorLambda = jobCreatorJar.buildFunction(this, "BatchIngestJobsLambda", builder -> builder
                .functionName(jobCreatorName)
                .description("Create ingest jobs by batching up submitted file ingest requests")
                .runtime(software.amazon.awscdk.services.lambda.Runtime.JAVA_11)
                .memorySize(instanceProperties.getInt(INGEST_BATCHER_JOB_CREATOR_MEMORY_IN_MB))
                .timeout(Duration.seconds(instanceProperties.getInt(INGEST_BATCHER_JOB_CREATOR_TIMEOUT_IN_SECONDS)))
                .handler("sleeper.ingest.batcher.job.creator.IngestBatcherJobCreatorLambda::eventHandler")
                .environment(environmentVariables)
                .reservedConcurrentExecutions(1)
                .logRetention(Utils.getRetentionDays(instanceProperties.getInt(LOG_RETENTION_IN_DAYS))));

        configBucket.grantRead(jobCreatorLambda);
        ingestQueues(ingestStack, emrBulkImportStack, persistentEmrBulkImportStack, eksBulkImportStack)
                .forEach(queue -> queue.grantSendMessages(jobCreatorLambda));
    }

    private static Stream<Queue> ingestQueues(IngestStack ingestStack,
                                              EmrBulkImportStack emrBulkImportStack,
                                              PersistentEmrBulkImportStack persistentEmrBulkImportStack,
                                              EksBulkImportStack eksBulkImportStack) {
        return Stream.of(
                        ingestQueue(ingestStack, IngestStack::getIngestJobQueue),
                        ingestQueue(emrBulkImportStack, EmrBulkImportStack::getBulkImportJobQueue),
                        ingestQueue(persistentEmrBulkImportStack, PersistentEmrBulkImportStack::getBulkImportJobQueue),
                        ingestQueue(eksBulkImportStack, EksBulkImportStack::getBulkImportJobQueue))
                .flatMap(Optional::stream);
    }

    private static <T> Optional<Queue> ingestQueue(T stack, Function<T, Queue> getter) {
        return Optional.ofNullable(stack).map(getter);
    }
}
