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
package sleeper.compaction.job.execution;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.ecs.EcsClient;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.SendMessageRequest;
import software.amazon.awssdk.transfer.s3.S3TransferManager;

import sleeper.common.job.EC2ContainerMetadata;
import sleeper.compaction.core.job.CompactionJobCommitterOrSendToLambda;
import sleeper.compaction.core.job.CompactionJobCommitterOrSendToLambda.BatchedCommitQueueSender;
import sleeper.compaction.core.job.CompactionJobCommitterOrSendToLambda.CommitQueueSender;
import sleeper.compaction.core.job.commit.CompactionCommitMessageSerDe;
import sleeper.compaction.core.task.CompactionTask;
import sleeper.compaction.core.task.StateStoreWaitForFiles;
import sleeper.compaction.tracker.job.CompactionJobTrackerFactory;
import sleeper.compaction.tracker.task.CompactionTaskTrackerFactory;
import sleeper.configuration.jars.S3UserJarsLoader;
import sleeper.configuration.properties.S3InstanceProperties;
import sleeper.configuration.properties.S3PropertiesReloader;
import sleeper.configuration.properties.S3TableProperties;
import sleeper.core.properties.PropertiesReloader;
import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.table.TablePropertiesProvider;
import sleeper.core.statestore.StateStoreProvider;
import sleeper.core.statestore.commit.StateStoreCommitRequestSerDe;
import sleeper.core.tracker.compaction.job.CompactionJobTracker;
import sleeper.core.tracker.compaction.task.CompactionTaskTracker;
import sleeper.core.util.LoggedDuration;
import sleeper.core.util.ObjectFactory;
import sleeper.core.util.ObjectFactoryException;
import sleeper.dynamodb.tools.DynamoDBUtils;
import sleeper.parquet.utils.HadoopConfigurationProvider;
import sleeper.sketches.store.S3SketchesStore;
import sleeper.statestore.StateStoreFactory;

import java.io.IOException;
import java.nio.file.Path;
import java.time.Instant;
import java.util.UUID;

import static sleeper.configuration.utils.AwsV2ClientHelper.buildAwsV2Client;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.COMPACTION_COMMIT_QUEUE_URL;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.STATESTORE_COMMITTER_QUEUE_URL;
import static sleeper.core.properties.instance.CompactionProperty.COMPACTION_ECS_LAUNCHTYPE;

/**
 * Runs a compaction task in ECS. Delegates the running of compaction jobs to {@link DefaultCompactionRunnerFactory},
 * and the processing of SQS messages to {@link SqsCompactionQueueHandler}.
 */
public class ECSCompactionTaskRunner {
    private static final Logger LOGGER = LoggerFactory.getLogger(ECSCompactionTaskRunner.class);

    private ECSCompactionTaskRunner() {
    }

    public static void main(String[] args) throws IOException, ObjectFactoryException {
        if (1 != args.length) {
            System.err.println("Error: must have 1 argument (config bucket), got " + args.length + " arguments (" + StringUtils.join(args, ',') + ")");
            System.exit(1);
        }
        String s3Bucket = args[0];

        Instant startTime = Instant.now();

        try (EcsClient ecsClient = buildAwsV2Client(EcsClient.builder());
                DynamoDbClient dynamoDBClient = buildAwsV2Client(DynamoDbClient.builder());
                SqsClient sqsClient = buildAwsV2Client(SqsClient.builder());
                S3Client s3Client = buildAwsV2Client(S3Client.builder());
                S3AsyncClient s3AsyncClient = buildAwsV2Client(S3AsyncClient.crtBuilder());
                S3TransferManager s3TransferManager = S3TransferManager.builder().s3Client(s3AsyncClient).build()) {
            InstanceProperties instanceProperties = S3InstanceProperties.loadFromBucket(s3Client, s3Bucket);

            // Log some basic data if running on EC2 inside ECS
            logEC2Metadata(instanceProperties, ecsClient);

            TablePropertiesProvider tablePropertiesProvider = S3TableProperties.createProvider(instanceProperties, s3Client, dynamoDBClient);
            PropertiesReloader propertiesReloader = S3PropertiesReloader.ifConfigured(s3Client, instanceProperties, tablePropertiesProvider);
            StateStoreProvider stateStoreProvider = StateStoreFactory.createProvider(instanceProperties, s3Client, dynamoDBClient);
            CompactionJobTracker jobTracker = CompactionJobTrackerFactory.getTracker(dynamoDBClient,
                    instanceProperties);
            CompactionTaskTracker taskTracker = CompactionTaskTrackerFactory.getTracker(dynamoDBClient,
                    instanceProperties);
            String taskId = UUID.randomUUID().toString();

            ObjectFactory objectFactory = new S3UserJarsLoader(instanceProperties, s3Client, Path.of("/tmp")).buildObjectFactory();

            DefaultCompactionRunnerFactory compactionSelector = new DefaultCompactionRunnerFactory(objectFactory,
                    HadoopConfigurationProvider.getConfigurationForECS(instanceProperties),
                    new S3SketchesStore(s3Client, s3TransferManager));

            StateStoreWaitForFiles waitForFiles = new StateStoreWaitForFiles(
                    tablePropertiesProvider, stateStoreProvider, jobTracker, DynamoDBUtils.retryOnThrottlingException());

            CompactionJobCommitterOrSendToLambda committerOrLambda = committerOrSendToLambda(
                    tablePropertiesProvider, stateStoreProvider, jobTracker, instanceProperties, sqsClient);
            CompactionTask task = new CompactionTask(instanceProperties, tablePropertiesProvider, propertiesReloader,
                    stateStoreProvider, new SqsCompactionQueueHandler(sqsClient, instanceProperties),
                    waitForFiles, committerOrLambda, jobTracker, taskTracker, compactionSelector, taskId);
            task.run();
        } finally {
            LOGGER.info("Total run time = {}", LoggedDuration.withFullOutput(startTime, Instant.now()));
        }
    }

    public static void logEC2Metadata(InstanceProperties instanceProperties, EcsClient ecsClient) {
        // Log some basic data if running on EC2 inside ECS
        if (instanceProperties.get(COMPACTION_ECS_LAUNCHTYPE).equalsIgnoreCase("EC2")) {
            try {
                if (ecsClient != null) {
                    EC2ContainerMetadata.retrieveContainerMetadata(ecsClient).ifPresent(info -> LOGGER.info(
                            "Task running on EC2 instance ID {} in AZ {} with ARN {} in cluster {} with status {}",
                            info.instanceID, info.az, info.instanceARN, info.clusterName, info.status));
                } else {
                    LOGGER.warn("ECS client is null");
                }
            } catch (IOException e) {
                LOGGER.warn("EC2 instance data not available", e);
            }
        }
    }

    public static CompactionJobCommitterOrSendToLambda committerOrSendToLambda(
            TablePropertiesProvider tablePropertiesProvider, StateStoreProvider stateStoreProvider,
            CompactionJobTracker jobTracker, InstanceProperties instanceProperties, SqsClient sqsClient) {
        return new CompactionJobCommitterOrSendToLambda(
                tablePropertiesProvider, stateStoreProvider, jobTracker,
                sendToSqs(instanceProperties, tablePropertiesProvider, sqsClient),
                sendToSqsBatched(instanceProperties, sqsClient));
    }

    private static CommitQueueSender sendToSqs(
            InstanceProperties instanceProperties, TablePropertiesProvider tablePropertiesProvider, SqsClient sqsClient) {
        return request -> {
            String queueUrl = instanceProperties.get(STATESTORE_COMMITTER_QUEUE_URL);
            String tableId = request.getTableId();
            sqsClient.sendMessage(SendMessageRequest.builder()
                    .queueUrl(queueUrl)
                    .messageDeduplicationId(UUID.randomUUID().toString())
                    .messageGroupId(tableId)
                    .messageBody(new StateStoreCommitRequestSerDe(tablePropertiesProvider).toJson(request))
                    .build());
        };
    }

    private static BatchedCommitQueueSender sendToSqsBatched(
            InstanceProperties instanceProperties, SqsClient sqsClient) {
        return request -> sqsClient.sendMessage(SendMessageRequest.builder()
                .queueUrl(instanceProperties.get(COMPACTION_COMMIT_QUEUE_URL))
                .messageBody(new CompactionCommitMessageSerDe().toJson(request))
                .build());
    }
}
