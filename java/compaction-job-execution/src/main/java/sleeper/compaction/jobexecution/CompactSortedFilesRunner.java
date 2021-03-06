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

import sleeper.job.common.action.DeleteMessageAction;
import sleeper.job.common.action.ActionException;
import sleeper.job.common.action.ChangeMessageVisibilityTimeoutAction;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClientBuilder;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.ReceiveMessageRequest;
import com.amazonaws.services.sqs.model.ReceiveMessageResult;
import java.io.IOException;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sleeper.compaction.job.CompactionJob;
import sleeper.compaction.job.CompactionJobSerDe;
import sleeper.job.common.action.thread.PeriodicActionRunnable;
import sleeper.configuration.jars.ObjectFactory;
import sleeper.configuration.jars.ObjectFactoryException;
import sleeper.configuration.properties.InstanceProperties;
import static sleeper.configuration.properties.SystemDefinedInstanceProperty.COMPACTION_JOB_QUEUE_URL;
import static sleeper.configuration.properties.SystemDefinedInstanceProperty.SPLITTING_COMPACTION_JOB_QUEUE_URL;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.COMPACTION_KEEP_ALIVE_PERIOD_IN_SECONDS;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.COMPACTION_QUEUE_VISIBILITY_TIMEOUT_IN_SECONDS;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.MAXIMUM_CONNECTIONS_TO_S3;
import sleeper.configuration.properties.table.TableProperties;
import sleeper.configuration.properties.table.TablePropertiesProvider;
import static sleeper.configuration.properties.table.TableProperty.COMPRESSION_CODEC;
import static sleeper.configuration.properties.table.TableProperty.PAGE_SIZE;
import static sleeper.configuration.properties.table.TableProperty.ROW_GROUP_SIZE;
import sleeper.core.iterator.IteratorException;
import sleeper.io.parquet.record.SchemaConverter;
import sleeper.statestore.StateStore;
import sleeper.table.util.StateStoreProvider;
import sleeper.utils.HadoopConfigurationProvider;

/**
 * Retrieves compaction {@link CompactionJob}s from an SQS queue, and executes them. It
 * delegates the actual execution of the job to an instance of {@link CompactSortedFiles}.
 * It passes a {@link ChangeMessageVisibilityTimeoutAction} to that class so that the message on the
 * SQS queue can be kept alive whilst the job is executing. It also handles
 * deletion of the message when the job is completed.
 */
public class CompactSortedFilesRunner {
    private static final Logger LOGGER = LoggerFactory.getLogger(CompactSortedFilesRunner.class);
    
    private final InstanceProperties instanceProperties;
    private final ObjectFactory objectFactory;
    private final TablePropertiesProvider tablePropertiesProvider;
    private final StateStoreProvider stateStoreProvider;
    private final CompactionJobSerDe compactionJobSerDe;
    private final String sqsJobQueueUrl;
    private final AmazonSQS sqsClient;
    private final int maxConnectionsToS3;
    private final int keepAliveFrequency;
    private final int maxMessageRetrieveAttempts;
    private final int waitTimeSeconds;

    public CompactSortedFilesRunner(InstanceProperties instanceProperties,
                                    ObjectFactory objectFactory,
                                    TablePropertiesProvider tablePropertiesProvider,
                                    StateStoreProvider stateStoreProvider,
                                    String sqsJobQueueUrl,
                                    AmazonSQS sqsClient,
                                    int maxMessageRetrieveAttempts,
                                    int waitTimeSeconds) {
        this.instanceProperties = instanceProperties;
        this.objectFactory = objectFactory;
        this.tablePropertiesProvider = tablePropertiesProvider;
        this.stateStoreProvider = stateStoreProvider;
        this.compactionJobSerDe = new CompactionJobSerDe(tablePropertiesProvider);
        this.sqsJobQueueUrl = sqsJobQueueUrl;
        this.maxConnectionsToS3 = instanceProperties.getInt(MAXIMUM_CONNECTIONS_TO_S3);
        this.keepAliveFrequency = instanceProperties.getInt(COMPACTION_KEEP_ALIVE_PERIOD_IN_SECONDS);
        this.sqsClient = sqsClient;
        this.maxMessageRetrieveAttempts = maxMessageRetrieveAttempts;
        this.waitTimeSeconds = waitTimeSeconds;
    }
    
    public CompactSortedFilesRunner(InstanceProperties instanceProperties,
                                    ObjectFactory objectFactory,
                                    TablePropertiesProvider tablePropertiesProvider,
                                    StateStoreProvider stateStoreProvider,
                                    String sqsJobQueueUrl,
                                    AmazonSQS sqsClient) {
        this(instanceProperties, objectFactory, tablePropertiesProvider, stateStoreProvider, sqsJobQueueUrl, sqsClient, 3, 20);
    }

    public void run() throws InterruptedException, IOException, ActionException {
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
                    compact(compactionJob, message);
                } catch (IOException | IteratorException e) {
                    LOGGER.error("Exception running compaction compactionJob", e);
                    return;
                }
                totalNumberOfMessagesProcessed++;
                numConsecutiveTimesNoMessages = 0;
            }
        }
        LOGGER.info("Returning from run() method in CompactSortedFilesRunner as no messages received in {} seconds",
                (numConsecutiveTimesNoMessages * 30));
        LOGGER.info("Total number of messages processed = " + totalNumberOfMessagesProcessed);
    }

    private void compact(CompactionJob compactionJob, Message message) throws IOException, IteratorException, ActionException {
        // Create background thread to keep messages alive
        ChangeMessageVisibilityTimeoutAction changeMessageVisibilityAction = new ChangeMessageVisibilityTimeoutAction(sqsClient,
                sqsJobQueueUrl, "Compaction job " + compactionJob.getId(), message.getReceiptHandle(),
                instanceProperties.getInt(COMPACTION_QUEUE_VISIBILITY_TIMEOUT_IN_SECONDS));
        PeriodicActionRunnable keepAliveRunnable = new PeriodicActionRunnable(changeMessageVisibilityAction, keepAliveFrequency);
        keepAliveRunnable.start();
        LOGGER.info("Compaction job {}: Created background thread to keep SQS messages alive (period is {} seconds)",
                compactionJob.getId(), keepAliveFrequency);

        TableProperties tableProperties = tablePropertiesProvider.getTableProperties(compactionJob.getTableName());
        StateStore stateStore = stateStoreProvider.getStateStore(tableProperties);
        CompactSortedFiles compactSortedFiles = new CompactSortedFiles(instanceProperties, objectFactory,
                tableProperties.getSchema(), SchemaConverter.getSchema(tableProperties.getSchema()), compactionJob,
                stateStore, tableProperties.getInt(ROW_GROUP_SIZE), tableProperties.getInt(PAGE_SIZE),
                tableProperties.get(COMPRESSION_CODEC), maxConnectionsToS3, keepAliveFrequency);
        compactSortedFiles.compact();
        
        // Delete message from queue
        DeleteMessageAction deleteAction = new DeleteMessageAction(sqsClient, sqsJobQueueUrl, compactionJob.getId(), message.getReceiptHandle());
        deleteAction.call();
        
        LOGGER.info("Compaction job {}: Stopping background thread to keep SQS messages alive",
                compactionJob.getId());
        keepAliveRunnable.stop();
    }
    
    public static void main(String[] args) throws InterruptedException, IOException, ObjectFactoryException, ActionException {
        if (2 != args.length) {
            System.err.println("Error: must have 2 arguments (config bucket and compaction type (compaction or splittingcompaction)), got "
                    + args.length
                    + " arguments (" + StringUtils.join(args, ',') + ")");
            System.exit(1);
        }

        long startTime = System.currentTimeMillis();
        AmazonDynamoDB dynamoDBClient = AmazonDynamoDBClientBuilder.defaultClient();
        AmazonSQS sqsClient = AmazonSQSClientBuilder.defaultClient();
        AmazonS3 s3Client = AmazonS3ClientBuilder.defaultClient();

        String s3Bucket = args[0];
        InstanceProperties instanceProperties = new InstanceProperties();
        instanceProperties.loadFromS3(s3Client, s3Bucket);

        TablePropertiesProvider tablePropertiesProvider = new TablePropertiesProvider(s3Client, instanceProperties);
        StateStoreProvider stateStoreProvider = new StateStoreProvider(dynamoDBClient, instanceProperties, HadoopConfigurationProvider.getConfigurationForECS(instanceProperties));

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
                sqsJobQueueUrl,
                sqsClient);
        runner.run();

        sqsClient.shutdown();
        LOGGER.info("Shut down sqsClient");
        dynamoDBClient.shutdown();
        LOGGER.info("Shut down dynamoDBClient");
        s3Client.shutdown();
        LOGGER.info("Shut down s3Client");
        long finishTime = System.currentTimeMillis();
        double runTimeInSeconds = (finishTime - startTime) / 1000.0;
        LOGGER.info("CompactSortedFilesRunner total run time = " + runTimeInSeconds);
    }
}
