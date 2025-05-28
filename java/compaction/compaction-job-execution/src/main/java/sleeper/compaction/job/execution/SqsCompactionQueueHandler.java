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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.ChangeMessageVisibilityRequest;
import software.amazon.awssdk.services.sqs.model.Message;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageRequest;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageResponse;

import sleeper.common.jobv2.action.ActionException;
import sleeper.common.jobv2.action.MessageReference;
import sleeper.common.jobv2.action.thread.PeriodicActionRunnable;
import sleeper.compaction.core.job.CompactionJob;
import sleeper.compaction.core.job.CompactionJobSerDe;
import sleeper.compaction.core.task.CompactionTask;
import sleeper.compaction.core.task.CompactionTask.MessageHandle;
import sleeper.core.properties.instance.InstanceProperties;

import java.io.IOException;
import java.util.Optional;

import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.COMPACTION_JOB_QUEUE_URL;
import static sleeper.core.properties.instance.CompactionProperty.COMPACTION_JOB_FAILED_VISIBILITY_TIMEOUT_IN_SECONDS;
import static sleeper.core.properties.instance.CompactionProperty.COMPACTION_KEEP_ALIVE_PERIOD_IN_SECONDS;
import static sleeper.core.properties.instance.CompactionProperty.COMPACTION_QUEUE_VISIBILITY_TIMEOUT_IN_SECONDS;
import static sleeper.core.properties.instance.CompactionProperty.COMPACTION_TASK_WAIT_TIME_IN_SECONDS;

public class SqsCompactionQueueHandler implements CompactionTask.MessageReceiver {
    private static final Logger LOGGER = LoggerFactory.getLogger(SqsCompactionQueueHandler.class);

    private final SqsClient sqsClient;
    private final InstanceProperties instanceProperties;

    public SqsCompactionQueueHandler(SqsClient sqsClient, InstanceProperties instanceProperties) {
        this.sqsClient = sqsClient;
        this.instanceProperties = instanceProperties;
    }

    public Optional<MessageHandle> receiveMessage() throws IOException {
        int waitTimeSeconds = instanceProperties.getInt(COMPACTION_TASK_WAIT_TIME_IN_SECONDS);
        int keepAliveFrequency = instanceProperties.getInt(COMPACTION_KEEP_ALIVE_PERIOD_IN_SECONDS);
        String sqsJobQueueUrl = instanceProperties.get(COMPACTION_JOB_QUEUE_URL);
        ReceiveMessageRequest receiveMessageRequest = ReceiveMessageRequest.builder()
                .queueUrl(sqsJobQueueUrl)
                .maxNumberOfMessages(1)
                .waitTimeSeconds(waitTimeSeconds).build();
        ReceiveMessageResponse receiveMessageResult = sqsClient.receiveMessage(receiveMessageRequest);
        if (receiveMessageResult.messages().isEmpty()) {
            LOGGER.info("Received no messages in {} seconds", waitTimeSeconds);
            return Optional.empty();
        } else {
            Message message = receiveMessageResult.messages().get(0);
            LOGGER.info("Received message: {}", message);
            CompactionJob compactionJob = new CompactionJobSerDe().fromJson(message.body());
            MessageReference messageReference = new MessageReference(sqsClient, sqsJobQueueUrl,
                    "Compaction job " + compactionJob.getId(), message.receiptHandle());

            // Create background thread to keep messages alive
            PeriodicActionRunnable keepAliveRunnable = new PeriodicActionRunnable(
                    messageReference.changeVisibilityTimeoutAction(
                            instanceProperties.getInt(COMPACTION_QUEUE_VISIBILITY_TIMEOUT_IN_SECONDS)),
                    keepAliveFrequency);
            keepAliveRunnable.start();
            LOGGER.info("Compaction job {}: Created background thread to keep SQS messages alive (period is {} seconds)",
                    compactionJob.getId(), keepAliveFrequency);

            return Optional.of(new SqsMessageHandle(compactionJob, messageReference, keepAliveRunnable));
        }
    }

    private class SqsMessageHandle implements MessageHandle {
        private final CompactionJob job;
        private final MessageReference message;
        private final PeriodicActionRunnable keepAliveRunnable;

        SqsMessageHandle(CompactionJob job, MessageReference message, PeriodicActionRunnable keepAliveRunnable) {
            this.job = job;
            this.message = message;
            this.keepAliveRunnable = keepAliveRunnable;
        }

        @Override
        public CompactionJob getJob() {
            return job;
        }

        @Override
        public void deleteFromQueue() {
            // Delete message from queue
            LOGGER.info("Compaction job {}: Deleting message from queue", job.getId());
            try {
                message.deleteAction().call();
            } catch (ActionException e) {
                throw new RuntimeException(e);
            }
        }

        @Override
        public void returnToQueue() {
            LOGGER.info("Compaction job {}: Returning message to queue", job.getId());
            int visibilityTimeout = instanceProperties.getInt(COMPACTION_JOB_FAILED_VISIBILITY_TIMEOUT_IN_SECONDS);
            String sqsJobQueueUrl = instanceProperties.get(COMPACTION_JOB_QUEUE_URL);
            sqsClient.changeMessageVisibility(ChangeMessageVisibilityRequest.builder()
                    .queueUrl(sqsJobQueueUrl)
                    .receiptHandle(message.getReceiptHandle())
                    .visibilityTimeout(visibilityTimeout).build());
        }

        @Override
        public void close() {
            LOGGER.info("Compaction job {}: Stopping background thread to keep SQS messages alive", job.getId());
            if (keepAliveRunnable != null) {
                keepAliveRunnable.stop();
            }
        }
    }

}
