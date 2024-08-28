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
import com.amazonaws.services.ecs.AmazonECS;
import com.amazonaws.services.ecs.AmazonECSClientBuilder;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClientBuilder;
import com.amazonaws.services.sqs.model.SendMessageRequest;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sleeper.compaction.job.CompactionJobStatusStore;
import sleeper.compaction.job.commit.CompactionJobCommitRequestSerDe;
import sleeper.compaction.job.commit.CompactionJobCommitterOrSendToLambda;
import sleeper.compaction.job.commit.CompactionJobCommitterOrSendToLambda.CommitQueueSender;
import sleeper.compaction.status.store.job.CompactionJobStatusStoreFactory;
import sleeper.compaction.status.store.task.CompactionTaskStatusStoreFactory;
import sleeper.compaction.task.CompactionTask;
import sleeper.compaction.task.CompactionTask.WaitForFileAssignment;
import sleeper.compaction.task.CompactionTaskStatusStore;
import sleeper.compaction.task.StateStoreWaitForFiles;
import sleeper.configuration.jars.ObjectFactory;
import sleeper.configuration.jars.ObjectFactoryException;
import sleeper.configuration.properties.PropertiesReloader;
import sleeper.configuration.properties.instance.InstanceProperties;
import sleeper.configuration.properties.table.TablePropertiesProvider;
import sleeper.core.util.LoggedDuration;
import sleeper.io.parquet.utils.HadoopConfigurationProvider;
import sleeper.job.common.EC2ContainerMetadata;
import sleeper.statestore.StateStoreFactory;
import sleeper.statestore.StateStoreProvider;

import java.io.IOException;
import java.time.Instant;
import java.util.UUID;

import static sleeper.configuration.properties.instance.CdkDefinedInstanceProperty.STATESTORE_COMMITTER_QUEUE_URL;
import static sleeper.configuration.properties.instance.CompactionProperty.COMPACTION_ECS_LAUNCHTYPE;
import static sleeper.configuration.utils.AwsV1ClientHelper.buildAwsV1Client;

/**
 * Runs a compaction task in ECS. Delegates the running of compaction jobs to {@link DefaultSelector},
 * and the processing of SQS messages to {@link SqsCompactionQueueHandler}.
 */
public class ECSCompactionTaskRunner {
    private static final Logger LOGGER = LoggerFactory.getLogger(ECSCompactionTaskRunner.class);

    private ECSCompactionTaskRunner() {
    }

    public static void main(String[] args) throws IOException, ObjectFactoryException, InterruptedException {
        if (1 != args.length) {
            System.err.println("Error: must have 1 argument (config bucket), got " + args.length + " arguments (" + StringUtils.join(args, ',') + ")");
            System.exit(1);
        }
        String s3Bucket = args[0];

        Instant startTime = Instant.now();
        AmazonDynamoDB dynamoDBClient = buildAwsV1Client(AmazonDynamoDBClientBuilder.standard());
        AmazonSQS sqsClient = buildAwsV1Client(AmazonSQSClientBuilder.standard());
        AmazonS3 s3Client = buildAwsV1Client(AmazonS3ClientBuilder.standard());
        AmazonECS ecsClient = buildAwsV1Client(AmazonECSClientBuilder.standard());

        try {
            InstanceProperties instanceProperties = new InstanceProperties();
            instanceProperties.loadFromS3(s3Client, s3Bucket);

            // Log some basic data if running on EC2 inside ECS
            logEC2Metadata(instanceProperties, ecsClient);

            TablePropertiesProvider tablePropertiesProvider = new TablePropertiesProvider(instanceProperties, s3Client, dynamoDBClient);
            PropertiesReloader propertiesReloader = PropertiesReloader.ifConfigured(s3Client, instanceProperties, tablePropertiesProvider);
            StateStoreProvider stateStoreProvider = StateStoreFactory.createProvider(instanceProperties, s3Client, dynamoDBClient,
                    HadoopConfigurationProvider.getConfigurationForECS(instanceProperties));
            CompactionJobStatusStore jobStatusStore = CompactionJobStatusStoreFactory.getStatusStore(dynamoDBClient,
                    instanceProperties);
            CompactionTaskStatusStore taskStatusStore = CompactionTaskStatusStoreFactory.getStatusStore(dynamoDBClient,
                    instanceProperties);
            String taskId = UUID.randomUUID().toString();

            ObjectFactory objectFactory = new ObjectFactory(instanceProperties, s3Client, "/tmp");

            DefaultSelector compactionSelector = new DefaultSelector(tablePropertiesProvider, stateStoreProvider, objectFactory,
                    HadoopConfigurationProvider.getConfigurationForECS(instanceProperties));

            WaitForFileAssignment waitForFiles = new StateStoreWaitForFiles(stateStoreProvider.byTableId(tablePropertiesProvider));

            CompactionJobCommitterOrSendToLambda committerOrLambda = committerOrSendToLambda(
                    tablePropertiesProvider, stateStoreProvider, jobStatusStore, instanceProperties, sqsClient);
            CompactionTask task = new CompactionTask(instanceProperties, propertiesReloader,
                    new SqsCompactionQueueHandler(sqsClient, instanceProperties),
                    waitForFiles, committerOrLambda, jobStatusStore, taskStatusStore, compactionSelector, taskId);
            task.run();
        } finally {
            sqsClient.shutdown();
            LOGGER.info("Shut down sqsClient");
            dynamoDBClient.shutdown();
            LOGGER.info("Shut down dynamoDBClient");
            s3Client.shutdown();
            LOGGER.info("Shut down s3Client");
            ecsClient.shutdown();
            LOGGER.info("Shut down ecsClient");
            LOGGER.info("Total run time = {}", LoggedDuration.withFullOutput(startTime, Instant.now()));
        }
    }

    public static void logEC2Metadata(InstanceProperties instanceProperties, AmazonECS ecsClient) {
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
            CompactionJobStatusStore jobStatusStore, InstanceProperties instanceProperties, AmazonSQS sqsClient) {
        return new CompactionJobCommitterOrSendToLambda(
                tablePropertiesProvider, stateStoreProvider.byTableId(tablePropertiesProvider), jobStatusStore,
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
