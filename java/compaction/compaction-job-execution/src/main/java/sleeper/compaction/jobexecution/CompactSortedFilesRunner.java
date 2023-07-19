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
package sleeper.compaction.jobexecution;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.ecs.AmazonECS;
import com.amazonaws.services.ecs.AmazonECSClientBuilder;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClientBuilder;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.ReceiveMessageRequest;
import com.amazonaws.services.sqs.model.ReceiveMessageResult;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sleeper.compaction.job.CompactionJob;
import sleeper.compaction.job.CompactionJobSerDe;
import sleeper.compaction.job.CompactionJobStatusStore;
import sleeper.compaction.status.store.job.CompactionJobStatusStoreFactory;
import sleeper.compaction.status.store.task.CompactionTaskStatusStoreFactory;
import sleeper.compaction.task.CompactionTaskFinishedStatus;
import sleeper.compaction.task.CompactionTaskStatus;
import sleeper.compaction.task.CompactionTaskStatusStore;
import sleeper.compaction.task.CompactionTaskType;
import sleeper.configuration.jars.ObjectFactory;
import sleeper.configuration.jars.ObjectFactoryException;
import sleeper.configuration.properties.instance.InstanceProperties;
import sleeper.configuration.properties.table.TableProperties;
import sleeper.configuration.properties.table.TablePropertiesProvider;
import sleeper.core.iterator.IteratorException;
import sleeper.core.record.process.RecordsProcessedSummary;
import sleeper.job.common.CommonJobUtils;
import sleeper.job.common.action.ActionException;
import sleeper.job.common.action.DeleteMessageAction;
import sleeper.job.common.action.MessageReference;
import sleeper.job.common.action.thread.PeriodicActionRunnable;
import sleeper.statestore.StateStore;
import sleeper.statestore.StateStoreProvider;
import sleeper.utils.HadoopConfigurationProvider;

import java.io.IOException;
import java.time.Instant;
import java.util.UUID;

import static sleeper.configuration.properties.instance.CompactionProperty.COMPACTION_ECS_LAUNCHTYPE;
import static sleeper.configuration.properties.instance.CompactionProperty.COMPACTION_KEEP_ALIVE_PERIOD_IN_SECONDS;
import static sleeper.configuration.properties.instance.CompactionProperty.COMPACTION_QUEUE_VISIBILITY_TIMEOUT_IN_SECONDS;
import static sleeper.configuration.properties.instance.SystemDefinedInstanceProperty.COMPACTION_JOB_QUEUE_URL;
import static sleeper.configuration.properties.instance.SystemDefinedInstanceProperty.SPLITTING_COMPACTION_JOB_QUEUE_URL;

/**
 * Retrieves compaction {@link CompactionJob}s from an SQS queue, and executes
 * them. It delegates the actual execution of the job to an instance of
 * {@link CompactSortedFiles}. It passes a
 * {@link sleeper.job.common.action.ChangeMessageVisibilityTimeoutAction} to
 * that class so that the message on the SQS queue can be kept alive whilst the job
 * is executing. It also handles deletion of the message when the job is completed.
 */
public class CompactSortedFilesRunner {
    private static final Logger LOGGER = LoggerFactory.getLogger(CompactSortedFilesRunner.class);

    private final InstanceProperties instanceProperties;
    private final ObjectFactory objectFactory;
    private final TablePropertiesProvider tablePropertiesProvider;
    private final StateStoreProvider stateStoreProvider;
    private final CompactionJobStatusStore jobStatusStore;
    private final CompactionTaskStatusStore taskStatusStore;
    private final String taskId;
    private final CompactionJobSerDe compactionJobSerDe;
    private final String sqsJobQueueUrl;
    private final AmazonSQS sqsClient;
    private final AmazonECS ecsClient;
    private final CompactionTaskType type;
    private final int keepAliveFrequency;
    private final int maxMessageRetrieveAttempts;
    private final int waitTimeSeconds;

    @SuppressWarnings("checkstyle:parameternumber")
    public CompactSortedFilesRunner(
            InstanceProperties instanceProperties,
            ObjectFactory objectFactory,
            TablePropertiesProvider tablePropertiesProvider,
            StateStoreProvider stateStoreProvider,
            CompactionJobStatusStore jobStatusStore,
            CompactionTaskStatusStore taskStatusStore,
            String taskId,
            String sqsJobQueueUrl,
            AmazonSQS sqsClient,
            AmazonECS ecsClient,
            CompactionTaskType type,
            int maxMessageRetrieveAttempts,
            int waitTimeSeconds) {
        this.instanceProperties = instanceProperties;
        this.objectFactory = objectFactory;
        this.tablePropertiesProvider = tablePropertiesProvider;
        this.stateStoreProvider = stateStoreProvider;
        this.jobStatusStore = jobStatusStore;
        this.taskStatusStore = taskStatusStore;
        this.taskId = taskId;
        this.compactionJobSerDe = new CompactionJobSerDe(tablePropertiesProvider);
        this.sqsJobQueueUrl = sqsJobQueueUrl;
        this.keepAliveFrequency = instanceProperties.getInt(COMPACTION_KEEP_ALIVE_PERIOD_IN_SECONDS);
        this.sqsClient = sqsClient;
        this.ecsClient = ecsClient;
        this.type = type;
        this.maxMessageRetrieveAttempts = maxMessageRetrieveAttempts;
        this.waitTimeSeconds = waitTimeSeconds;
    }

    public CompactSortedFilesRunner(
            InstanceProperties instanceProperties,
            ObjectFactory objectFactory,
            TablePropertiesProvider tablePropertiesProvider,
            StateStoreProvider stateStoreProvider,
            CompactionJobStatusStore jobStatusStore,
            CompactionTaskStatusStore taskStatusStore,
            String taskId,
            String sqsJobQueueUrl,
            AmazonSQS sqsClient,
            AmazonECS ecsClient,
            CompactionTaskType type) {
        this(instanceProperties, objectFactory, tablePropertiesProvider, stateStoreProvider,
                jobStatusStore, taskStatusStore, taskId, sqsJobQueueUrl, sqsClient, ecsClient, type, 3, 20);
    }

    public void run() throws InterruptedException, IOException, ActionException {
        Instant startTime = Instant.now();
        CompactionTaskStatus.Builder taskStatusBuilder = CompactionTaskStatus
                .builder().taskId(taskId).type(type).startTime(startTime);
        LOGGER.info("Starting task {}", taskId);

        // Log some basic data if running on EC2 inside ECS
        if (instanceProperties.get(COMPACTION_ECS_LAUNCHTYPE).equalsIgnoreCase("EC2")) {
            try {
                if (this.ecsClient != null) {
                    CommonJobUtils.retrieveContainerMetadata(ecsClient).ifPresent(info -> {
                        LOGGER.info("Task running on EC2 instance ID {} in AZ {} with ARN {} in cluster {} with status {}",
                                info.instanceID, info.az, info.instanceARN, info.clusterName, info.status);
                    });
                } else {
                    LOGGER.warn("ECS client is null");
                }
            } catch (IOException e) {
                LOGGER.warn("EC2 instance data not available", e);
            }
        }

        taskStatusStore.taskStarted(taskStatusBuilder.build());
        CompactionTaskFinishedStatus.Builder taskFinishedBuilder = CompactionTaskFinishedStatus.builder();
        long totalNumberOfMessagesProcessed = 0L;
        int numConsecutiveTimesNoMessages = 0;
        while (numConsecutiveTimesNoMessages < maxMessageRetrieveAttempts) {
            ReceiveMessageRequest receiveMessageRequest = new ReceiveMessageRequest(sqsJobQueueUrl)
                    .withMaxNumberOfMessages(1)
                    .withWaitTimeSeconds(waitTimeSeconds); // Must be >= 0 and <= 20
            ReceiveMessageResult receiveMessageResult = sqsClient.receiveMessage(receiveMessageRequest);
            if (receiveMessageResult.getMessages().isEmpty()) {
                LOGGER.info("Received no messages in {} seconds", waitTimeSeconds);
                numConsecutiveTimesNoMessages++;
                Thread.sleep(10000L);
            } else {
                Message message = receiveMessageResult.getMessages().get(0);
                LOGGER.info("Received message: {}", message);
                CompactionJob compactionJob = compactionJobSerDe.deserialiseFromString(message.getBody());
                LOGGER.info("CompactionJob is: {}", compactionJob);
                try {
                    taskFinishedBuilder.addJobSummary(compact(compactionJob, message));
                } catch (IOException | IteratorException e) {
                    LOGGER.error("Exception running compactionJob", e);
                    return;
                }
                totalNumberOfMessagesProcessed++;
                numConsecutiveTimesNoMessages = 0;
            }
        }
        LOGGER.info("Returning from run() method in CompactSortedFilesRunner as no messages received in {} seconds",
                (numConsecutiveTimesNoMessages * 30));
        LOGGER.info("Total number of messages processed = {}", totalNumberOfMessagesProcessed);

        Instant finishTime = Instant.now();
        double runTimeInSeconds = (finishTime.toEpochMilli() - startTime.toEpochMilli()) / 1000.0;
        LOGGER.info("CompactSortedFilesRunner total run time = {}", runTimeInSeconds);

        CompactionTaskStatus taskFinished = taskStatusBuilder.finished(finishTime, taskFinishedBuilder).build();
        taskStatusStore.taskFinished(taskFinished);
    }

    private RecordsProcessedSummary compact(CompactionJob compactionJob, Message message)
            throws IOException, IteratorException, ActionException {
        MessageReference messageReference = new MessageReference(sqsClient, sqsJobQueueUrl,
                "Compaction job " + compactionJob.getId(), message.getReceiptHandle());
        // Create background thread to keep messages alive
        PeriodicActionRunnable keepAliveRunnable = new PeriodicActionRunnable(
                messageReference.changeVisibilityTimeoutAction(
                        instanceProperties.getInt(COMPACTION_QUEUE_VISIBILITY_TIMEOUT_IN_SECONDS)),
                keepAliveFrequency);
        keepAliveRunnable.start();
        LOGGER.info("Compaction job {}: Created background thread to keep SQS messages alive (period is {} seconds)",
                compactionJob.getId(), keepAliveFrequency);

        TableProperties tableProperties = tablePropertiesProvider.getTableProperties(compactionJob.getTableName());
        StateStore stateStore = stateStoreProvider.getStateStore(tableProperties);
        CompactSortedFiles compactSortedFiles = new CompactSortedFiles(instanceProperties, tableProperties, objectFactory,
                compactionJob, stateStore, jobStatusStore, taskId);
        RecordsProcessedSummary summary = compactSortedFiles.compact();

        // Delete message from queue
        DeleteMessageAction deleteAction = messageReference.deleteAction();
        deleteAction.call();

        LOGGER.info("Compaction job {}: Stopping background thread to keep SQS messages alive",
                compactionJob.getId());
        keepAliveRunnable.stop();
        return summary;
    }

    public static void main(String[] args)
            throws InterruptedException, IOException, ObjectFactoryException, ActionException {
        if (2 != args.length) {
            System.err.println("Error: must have 2 arguments (config bucket and compaction type (compaction or splittingcompaction)), got "
                    + args.length
                    + " arguments (" + StringUtils.join(args, ',') + ")");
            System.exit(1);
        }

        AmazonDynamoDB dynamoDBClient = AmazonDynamoDBClientBuilder.defaultClient();
        AmazonSQS sqsClient = AmazonSQSClientBuilder.defaultClient();
        AmazonS3 s3Client = AmazonS3ClientBuilder.defaultClient();
        AmazonECS ecsClient = AmazonECSClientBuilder.defaultClient();

        String s3Bucket = args[0];
        InstanceProperties instanceProperties = new InstanceProperties();
        instanceProperties.loadFromS3(s3Client, s3Bucket);

        TablePropertiesProvider tablePropertiesProvider = new TablePropertiesProvider(s3Client, instanceProperties);
        StateStoreProvider stateStoreProvider = new StateStoreProvider(dynamoDBClient, instanceProperties,
                HadoopConfigurationProvider.getConfigurationForECS(instanceProperties));
        CompactionJobStatusStore jobStatusStore = CompactionJobStatusStoreFactory.getStatusStore(dynamoDBClient,
                instanceProperties);
        CompactionTaskStatusStore taskStatusStore = CompactionTaskStatusStoreFactory.getStatusStore(dynamoDBClient,
                instanceProperties);

        String sqsJobQueueUrl;
        String typeStr = args[1];
        CompactionTaskType type;
        if (typeStr.equals("compaction")) {
            type = CompactionTaskType.COMPACTION;
            sqsJobQueueUrl = instanceProperties.get(COMPACTION_JOB_QUEUE_URL);
        } else if (typeStr.equals("splittingcompaction")) {
            type = CompactionTaskType.SPLITTING;
            sqsJobQueueUrl = instanceProperties.get(SPLITTING_COMPACTION_JOB_QUEUE_URL);
        } else {
            throw new RuntimeException("Invalid type: got " + typeStr + ", should be 'compaction' or 'splittingcompaction'");
        }

        ObjectFactory objectFactory = new ObjectFactory(instanceProperties, s3Client, "/tmp");
        CompactSortedFilesRunner runner = new CompactSortedFilesRunner(
                instanceProperties, objectFactory,
                tablePropertiesProvider,
                stateStoreProvider,
                jobStatusStore,
                taskStatusStore,
                UUID.randomUUID().toString(),
                sqsJobQueueUrl,
                sqsClient,
                ecsClient,
                type);
        runner.run();

        sqsClient.shutdown();
        LOGGER.info("Shut down sqsClient");
        dynamoDBClient.shutdown();
        LOGGER.info("Shut down dynamoDBClient");
        s3Client.shutdown();
        LOGGER.info("Shut down s3Client");
        ecsClient.shutdown();
        LOGGER.info("Shut down ecsClient");
    }
}
