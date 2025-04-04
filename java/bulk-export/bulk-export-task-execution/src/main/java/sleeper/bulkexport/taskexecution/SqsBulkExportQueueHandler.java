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
package sleeper.bulkexport.taskexecution;

import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.ReceiveMessageRequest;
import com.amazonaws.services.sqs.model.ReceiveMessageResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sleeper.bulkexport.core.model.BulkExportLeafPartitionQuery;
import sleeper.bulkexport.core.model.BulkExportLeafPartitionQuerySerDe;
import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.table.TablePropertiesProvider;
import sleeper.job.common.action.ActionException;
import sleeper.job.common.action.MessageReference;
import sleeper.job.common.action.thread.PeriodicActionRunnable;

import java.io.IOException;
import java.util.Optional;

import static sleeper.core.properties.instance.BulkExportProperty.BULK_EXPORT_JOB_FAILED_VISIBILITY_TIMEOUT_IN_SECONDS;
import static sleeper.core.properties.instance.BulkExportProperty.BULK_EXPORT_KEEP_ALIVE_PERIOD_IN_SECONDS;
import static sleeper.core.properties.instance.BulkExportProperty.BULK_EXPORT_QUEUE_VISIBILITY_TIMEOUT_IN_SECONDS;
import static sleeper.core.properties.instance.BulkExportProperty.BULK_EXPORT_TASK_WAIT_TIME_IN_SECONDS;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.LEAF_PARTITION_BULK_EXPORT_QUEUE_URL;

/**
 * This class is used to handle the SQS queue for bulk export jobs.
 * It receives messages from the queue and deserialises them into
 * BulkExportLeafPartitionQuery objects.
 */
public class SqsBulkExportQueueHandler {
    private static final Logger LOGGER = LoggerFactory.getLogger(SqsBulkExportQueueHandler.class);

    private final AmazonSQS sqsClient;
    private final InstanceProperties instanceProperties;
    private final TablePropertiesProvider tablePropertiesProvider;

    public SqsBulkExportQueueHandler(AmazonSQS sqsClient, TablePropertiesProvider tablePropertiesProvider, InstanceProperties instanceProperties) {
        this.sqsClient = sqsClient;
        this.instanceProperties = instanceProperties;
        this.tablePropertiesProvider = tablePropertiesProvider;
    }

    /**
     * Receive a BulkExportLeafPartitionQuery message from the SQS queue and returns
     * a SqsMessageHandle that contains the deserialised message.
     *
     * @return an Optional containing the SqsMessageHandle if a message was
     *         received, or an empty Optional if no message was received
     * @throws IOException if there was an error deserialising the message
     */
    public Optional<SqsMessageHandle> receiveMessage() throws IOException {
        int waitTimeSeconds = instanceProperties.getInt(BULK_EXPORT_TASK_WAIT_TIME_IN_SECONDS);
        int keepAliveFrequency = instanceProperties.getInt(BULK_EXPORT_KEEP_ALIVE_PERIOD_IN_SECONDS);
        String sqsJobQueueUrl = instanceProperties.get(LEAF_PARTITION_BULK_EXPORT_QUEUE_URL);

        ReceiveMessageRequest receiveMessageRequest = new ReceiveMessageRequest(sqsJobQueueUrl)
                .withMaxNumberOfMessages(1)
                .withWaitTimeSeconds(waitTimeSeconds); // Must be >= 0 and <= 20
        ReceiveMessageResult receiveMessageResult = sqsClient.receiveMessage(receiveMessageRequest);
        if (receiveMessageResult.getMessages().isEmpty()) {
            LOGGER.info("Received no messages in {} seconds", waitTimeSeconds);
            return Optional.empty();
        } else {
            Message message = receiveMessageResult.getMessages().get(0);
            LOGGER.info("Received message: {}", message);
            BulkExportLeafPartitionQuerySerDe serDe = new BulkExportLeafPartitionQuerySerDe(tablePropertiesProvider);
            BulkExportLeafPartitionQuery bulkExportQuery = serDe.fromJson(message.getBody());
            LOGGER.info("Bulk Export bulkExportQuery {}: Deserialised message: {}", bulkExportQuery.getSubExportId(), bulkExportQuery);
            MessageReference messageReference = new MessageReference(sqsClient, sqsJobQueueUrl,
                    "Bulk Export job " + bulkExportQuery.getExportId(), message.getReceiptHandle());

            // Create background thread to keep messages alive
            PeriodicActionRunnable keepAliveRunnable = new PeriodicActionRunnable(
                    messageReference.changeVisibilityTimeoutAction(
                            instanceProperties.getInt(BULK_EXPORT_QUEUE_VISIBILITY_TIMEOUT_IN_SECONDS)),
                    keepAliveFrequency);
            keepAliveRunnable.start();
            LOGGER.info("Bulk Export job {}: Created background thread to keep SQS messages alive (period is {} seconds)",
                    bulkExportQuery.getSubExportId(), keepAliveFrequency);

            return Optional.of(new SqsMessageHandle(bulkExportQuery, messageReference, keepAliveRunnable));
        }
    }

    /**
     * This class is used to handle the SQS message for a bulk export job.
     * It contains the job, the message reference, and a background thread to keep the message alive.
     * The message is deleted from the queue when the job is complete.
     * The message is returned to the queue if the job fails.
     */
    public class SqsMessageHandle {
        private final BulkExportLeafPartitionQuery job;
        private final MessageReference message;
        private final PeriodicActionRunnable keepAliveRunnable;

        SqsMessageHandle(
                BulkExportLeafPartitionQuery job, MessageReference message, PeriodicActionRunnable keepAliveRunnable) {
            this.job = job;
            this.message = message;
            this.keepAliveRunnable = keepAliveRunnable;
        }

        public BulkExportLeafPartitionQuery getJob() {
            return job;
        }

        /**
         * Delete the message from the queue.
         */
        public void deleteFromQueue() {
            // Delete message from queue
            LOGGER.info("Bulk Export job {}: Deleting message from queue", job.getSubExportId());
            try {
                message.deleteAction().call();
            } catch (ActionException e) {
                throw new RuntimeException(e);
            }
        }

        /**
         * Return the message to the queue.
         */
        public void returnToQueue() {
            LOGGER.info("Bulk Export job {}: Returning message to queue", job.getSubExportId());
            int visibilityTimeout = instanceProperties.getInt(BULK_EXPORT_JOB_FAILED_VISIBILITY_TIMEOUT_IN_SECONDS);
            String sqsJobQueueUrl = instanceProperties.get(LEAF_PARTITION_BULK_EXPORT_QUEUE_URL);
            sqsClient.changeMessageVisibility(sqsJobQueueUrl, message.getReceiptHandle(), visibilityTimeout);
        }

        /**
         * Stop the background thread to keep the message alive.
         */
        public void close() {
            LOGGER.info("Bulk Export job {}: Stopping background thread to keep SQS messages alive", job.getSubExportId());
            if (keepAliveRunnable != null) {
                keepAliveRunnable.stop();
            }
        }
    }

}
