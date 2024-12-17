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
package sleeper.compaction.job.execution;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClientBuilder;
import com.amazonaws.services.sqs.model.SendMessageRequest;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.ecs.EcsClient;

import sleeper.compaction.core.job.commit.CompactionJobCommitRequestSerDe;
import sleeper.compaction.core.job.commit.CompactionJobCommitterOrSendToLambda;
import sleeper.compaction.core.job.commit.CompactionJobCommitterOrSendToLambda.CommitQueueSender;
import sleeper.compaction.core.task.CompactionTask;
import sleeper.compaction.core.task.CompactionTaskStatusStore;
import sleeper.compaction.core.task.StateStoreWaitForFiles;
import sleeper.compaction.status.store.job.CompactionJobTrackerFactory;
import sleeper.compaction.status.store.task.CompactionTaskStatusStoreFactory;
import sleeper.configuration.jars.S3UserJarsLoader;
import sleeper.configuration.properties.S3InstanceProperties;
import sleeper.configuration.properties.S3PropertiesReloader;
import sleeper.configuration.properties.S3TableProperties;
import sleeper.core.properties.PropertiesReloader;
import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.table.TablePropertiesProvider;
import sleeper.core.statestore.StateStoreProvider;
import sleeper.core.tracker.compaction.job.CompactionJobTracker;
import sleeper.core.util.LoggedDuration;
import sleeper.core.util.ObjectFactory;
import sleeper.core.util.ObjectFactoryException;
import sleeper.job.common.EC2ContainerMetadata;
import sleeper.parquet.utils.HadoopConfigurationProvider;
import sleeper.statestore.StateStoreFactory;

import java.io.IOException;
import java.time.Instant;
import java.util.UUID;

import static sleeper.compaction.job.execution.AwsV2ClientHelper.buildAwsV2Client;
import static sleeper.configuration.utils.AwsV1ClientHelper.buildAwsV1Client;
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
        AmazonDynamoDB dynamoDBClient = buildAwsV1Client(AmazonDynamoDBClientBuilder.standard());
        AmazonSQS sqsClient = buildAwsV1Client(AmazonSQSClientBuilder.standard());
        AmazonS3 s3Client = buildAwsV1Client(AmazonS3ClientBuilder.standard());

        try (EcsClient ecsClient = buildAwsV2Client(EcsClient.builder())) {
            InstanceProperties instanceProperties = S3InstanceProperties.loadFromBucket(s3Client, s3Bucket);

            // Log some basic data if running on EC2 inside ECS
            logEC2Metadata(instanceProperties, ecsClient);

            TablePropertiesProvider tablePropertiesProvider = S3TableProperties.createProvider(instanceProperties, s3Client, dynamoDBClient);
            PropertiesReloader propertiesReloader = S3PropertiesReloader.ifConfigured(s3Client, instanceProperties, tablePropertiesProvider);
            StateStoreProvider stateStoreProvider = StateStoreFactory.createProvider(instanceProperties, s3Client, dynamoDBClient,
                    HadoopConfigurationProvider.getConfigurationForECS(instanceProperties));
            CompactionJobTracker jobStatusStore = CompactionJobTrackerFactory.getStatusStore(dynamoDBClient,
                    instanceProperties);
            CompactionTaskStatusStore taskStatusStore = CompactionTaskStatusStoreFactory.getStatusStore(dynamoDBClient,
                    instanceProperties);
            String taskId = UUID.randomUUID().toString();

            ObjectFactory objectFactory = new S3UserJarsLoader(instanceProperties, s3Client, "/tmp").buildObjectFactory();

            DefaultCompactionRunnerFactory compactionSelector = new DefaultCompactionRunnerFactory(objectFactory,
                    HadoopConfigurationProvider.getConfigurationForECS(instanceProperties));

            StateStoreWaitForFiles waitForFiles = new StateStoreWaitForFiles(tablePropertiesProvider, stateStoreProvider, jobStatusStore);

            CompactionJobCommitterOrSendToLambda committerOrLambda = committerOrSendToLambda(
                    tablePropertiesProvider, stateStoreProvider, jobStatusStore, instanceProperties, sqsClient);
            CompactionTask task = new CompactionTask(instanceProperties, tablePropertiesProvider, propertiesReloader,
                    stateStoreProvider, new SqsCompactionQueueHandler(sqsClient, instanceProperties),
                    waitForFiles, committerOrLambda, jobStatusStore, taskStatusStore, compactionSelector, taskId);
            task.run();
        } finally {
            sqsClient.shutdown();
            LOGGER.info("Shut down sqsClient");
            dynamoDBClient.shutdown();
            LOGGER.info("Shut down dynamoDBClient");
            s3Client.shutdown();
            LOGGER.info("Shut down s3Client");
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
            CompactionJobTracker jobStatusStore, InstanceProperties instanceProperties, AmazonSQS sqsClient) {
        return new CompactionJobCommitterOrSendToLambda(
                tablePropertiesProvider, stateStoreProvider, jobStatusStore,
                sendToSqs(instanceProperties, sqsClient));
    }

    private static CommitQueueSender sendToSqs(InstanceProperties instanceProperties, AmazonSQS sqsClient) {
        return request -> {
            String queueUrl = instanceProperties.get(STATESTORE_COMMITTER_QUEUE_URL);
            String tableId = request.getJob().getTableId();
            sqsClient.sendMessage(new SendMessageRequest()
                    .withQueueUrl(queueUrl)
                    .withMessageDeduplicationId(UUID.randomUUID().toString())
                    .withMessageGroupId(tableId)
                    .withMessageBody(new CompactionJobCommitRequestSerDe().toJson(request)));
        };
    }
}
