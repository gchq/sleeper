/*
 * Copyright 2022 Crown Copyright
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
import sleeper.compaction.status.job.DynamoDBCompactionJobStatusStore;
import sleeper.compaction.status.task.DynamoDBCompactionTaskStatusStore;
import sleeper.compaction.task.CompactionTaskFinishedStatus;
import sleeper.compaction.task.CompactionTaskStatus;
import sleeper.compaction.task.CompactionTaskStatusStore;
import sleeper.configuration.jars.ObjectFactory;
import sleeper.configuration.jars.ObjectFactoryException;
import sleeper.configuration.properties.InstanceProperties;
import sleeper.configuration.properties.table.TableProperties;
import sleeper.configuration.properties.table.TablePropertiesProvider;
import sleeper.core.iterator.IteratorException;
import sleeper.core.record.process.RecordsProcessedSummary;
import sleeper.io.parquet.record.SchemaConverter;
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

import static sleeper.configuration.properties.SystemDefinedInstanceProperty.COMPACTION_JOB_QUEUE_URL;
import static sleeper.configuration.properties.SystemDefinedInstanceProperty.SPLITTING_COMPACTION_JOB_QUEUE_URL;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.COMPACTION_KEEP_ALIVE_PERIOD_IN_SECONDS;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.COMPACTION_QUEUE_VISIBILITY_TIMEOUT_IN_SECONDS;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.MAXIMUM_CONNECTIONS_TO_S3;
import static sleeper.configuration.properties.table.TableProperty.COMPRESSION_CODEC;
import static sleeper.configuration.properties.table.TableProperty.PAGE_SIZE;
import static sleeper.configuration.properties.table.TableProperty.ROW_GROUP_SIZE;

/**
 * Retrieves compaction {@link CompactionJob}s from an SQS queue, and executes them. It
 * delegates the actual execution of the job to an instance of {@link CompactSortedFiles}.
 * It passes a {@link sleeper.job.common.action.ChangeMessageVisibilityTimeoutAction} to that class so that the message on the
 * SQS queue can be kept alive whilst the job is executing. It also handles
 * deletion of the message when the job is completed.
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
    private final int maxConnectionsToS3;
    private final int keepAliveFrequency;
    private final int maxMessageRetrieveAttempts;
    private final int waitTimeSeconds;

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
        this.maxConnectionsToS3 = instanceProperties.getInt(MAXIMUM_CONNECTIONS_TO_S3);
        this.keepAliveFrequency = instanceProperties.getInt(COMPACTION_KEEP_ALIVE_PERIOD_IN_SECONDS);
        this.sqsClient = sqsClient;
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
            AmazonSQS sqsClient) {
        this(instanceProperties, objectFactory, tablePropertiesProvider, stateStoreProvider, jobStatusStore, taskStatusStore, taskId, sqsJobQueueUrl, sqsClient, 3, 20);
    }

    public void run() throws InterruptedException, IOException, ActionException {

        Instant startTime = Instant.now();
        CompactionTaskStatus.Builder taskStatusBuilder = CompactionTaskStatus
                .builder().taskId(taskId).started(startTime);
        LOGGER.info("Starting task {}", taskId);
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

        CompactionTaskStatus taskFinished = taskStatusBuilder.finished(taskFinishedBuilder, finishTime).build();
        taskStatusStore.taskFinished(taskFinished);
    }

    private RecordsProcessedSummary compact(CompactionJob compactionJob, Message message) throws IOException, IteratorException, ActionException {
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
        CompactSortedFiles compactSortedFiles = new CompactSortedFiles(instanceProperties, objectFactory,
                tableProperties.getSchema(), SchemaConverter.getSchema(tableProperties.getSchema()), compactionJob,
                stateStore, jobStatusStore, tableProperties.getInt(ROW_GROUP_SIZE), tableProperties.getInt(PAGE_SIZE),
                tableProperties.get(COMPRESSION_CODEC), taskId);
        RecordsProcessedSummary summary = compactSortedFiles.compact();

        // Delete message from queue
        DeleteMessageAction deleteAction = messageReference.deleteAction();
        deleteAction.call();

        LOGGER.info("Compaction job {}: Stopping background thread to keep SQS messages alive",
                compactionJob.getId());
        keepAliveRunnable.stop();
        return summary;
    }

    public static void main(String[] args) throws InterruptedException, IOException, ObjectFactoryException, ActionException {
        if (2 != args.length) {
            System.err.println("Error: must have 2 arguments (config bucket and compaction type (compaction or splittingcompaction)), got "
                    + args.length
                    + " arguments (" + StringUtils.join(args, ',') + ")");
            System.exit(1);
        }

        AmazonDynamoDB dynamoDBClient = AmazonDynamoDBClientBuilder.defaultClient();
        AmazonSQS sqsClient = AmazonSQSClientBuilder.defaultClient();
        AmazonS3 s3Client = AmazonS3ClientBuilder.defaultClient();

        String s3Bucket = args[0];
        InstanceProperties instanceProperties = new InstanceProperties();
        instanceProperties.loadFromS3(s3Client, s3Bucket);

        TablePropertiesProvider tablePropertiesProvider = new TablePropertiesProvider(s3Client, instanceProperties);
        StateStoreProvider stateStoreProvider = new StateStoreProvider(dynamoDBClient, instanceProperties, HadoopConfigurationProvider.getConfigurationForECS(instanceProperties));
        CompactionJobStatusStore jobStatusStore = DynamoDBCompactionJobStatusStore.from(dynamoDBClient, instanceProperties);
        CompactionTaskStatusStore taskStatusStore = DynamoDBCompactionTaskStatusStore.from(dynamoDBClient, instanceProperties);

        String sqsJobQueueUrl;
        String type = args[1];
        if (type.equals("compaction")) {
            sqsJobQueueUrl = instanceProperties.get(COMPACTION_JOB_QUEUE_URL);
        } else if (type.equals("splittingcompaction")) {
            sqsJobQueueUrl = instanceProperties.get(SPLITTING_COMPACTION_JOB_QUEUE_URL);
        } else {
            throw new RuntimeException("Invalid type: got " + type + ", should be 'compaction' or 'splittingcompaction'");
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
                sqsClient);
        runner.run();

        sqsClient.shutdown();
        LOGGER.info("Shut down sqsClient");
        dynamoDBClient.shutdown();
        LOGGER.info("Shut down dynamoDBClient");
        s3Client.shutdown();
        LOGGER.info("Shut down s3Client");
    }
}
