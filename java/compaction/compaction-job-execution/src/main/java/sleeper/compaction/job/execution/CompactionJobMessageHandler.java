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

import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.ReceiveMessageRequest;
import com.amazonaws.services.sqs.model.ReceiveMessageResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sleeper.compaction.job.CompactionJob;
import sleeper.compaction.job.CompactionJobSerDe;
import sleeper.configuration.properties.instance.InstanceProperties;

import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.function.Supplier;

import static sleeper.configuration.properties.instance.CdkDefinedInstanceProperty.COMPACTION_JOB_QUEUE_URL;
import static sleeper.configuration.properties.instance.CompactionProperty.COMPACTION_JOB_FAILED_VISIBILITY_TIMEOUT_IN_SECONDS;
import static sleeper.configuration.properties.instance.CompactionProperty.COMPACTION_TASK_DELAY_BEFORE_RETRY_IN_SECONDS;
import static sleeper.configuration.properties.instance.CompactionProperty.COMPACTION_TASK_MAX_CONSECUTIVE_FAILURES;
import static sleeper.configuration.properties.instance.CompactionProperty.COMPACTION_TASK_MAX_TIME_IN_SECONDS;
import static sleeper.configuration.properties.instance.CompactionProperty.COMPACTION_TASK_WAIT_TIME_IN_SECONDS;

public class CompactionJobMessageHandler {
    private static final Logger LOGGER = LoggerFactory.getLogger(CompactionJobMessageHandler.class);

    private final AmazonSQS sqsClient;
    private final String sqsJobQueueUrl;
    private final Supplier<Instant> timeSupplier;
    private final int maxConsecutiveFailures;
    private final int waitTimeSeconds;
    private final int maxTimeInSeconds;
    private final int visibilityTimeout;
    private final int delayBeforeRetry;
    private final CompactionJobMessageConsumer messageConsumer;

    public CompactionJobMessageHandler(AmazonSQS sqsClient, InstanceProperties instanceProperties, Supplier<Instant> timeSupplier, CompactionJobMessageConsumer messageConsumer) {
        visibilityTimeout = instanceProperties.getInt(COMPACTION_JOB_FAILED_VISIBILITY_TIMEOUT_IN_SECONDS);
        waitTimeSeconds = instanceProperties.getInt(COMPACTION_TASK_WAIT_TIME_IN_SECONDS);
        maxTimeInSeconds = instanceProperties.getInt(COMPACTION_TASK_MAX_TIME_IN_SECONDS);
        maxConsecutiveFailures = instanceProperties.getInt(COMPACTION_TASK_MAX_CONSECUTIVE_FAILURES);
        delayBeforeRetry = instanceProperties.getInt(COMPACTION_TASK_DELAY_BEFORE_RETRY_IN_SECONDS);
        sqsJobQueueUrl = instanceProperties.get(COMPACTION_JOB_QUEUE_URL);
        this.sqsClient = sqsClient;
        this.timeSupplier = timeSupplier;
        this.messageConsumer = messageConsumer;
    }

    public Result run(Instant startTime) throws InterruptedException, IOException {
        Instant maxTime = startTime.plus(Duration.ofSeconds(maxTimeInSeconds));
        int numConsecutiveFailures = 0;
        Instant currentTime = timeSupplier.get();
        long totalNumberOfMessagesProcessed = 0;
        while (numConsecutiveFailures < maxConsecutiveFailures && currentTime.isBefore(maxTime)) {
            ReceiveMessageRequest receiveMessageRequest = new ReceiveMessageRequest(sqsJobQueueUrl)
                    .withMaxNumberOfMessages(1)
                    .withWaitTimeSeconds(waitTimeSeconds); // Must be >= 0 and <= 20
            ReceiveMessageResult receiveMessageResult = sqsClient.receiveMessage(receiveMessageRequest);
            if (receiveMessageResult.getMessages().isEmpty()) {
                LOGGER.info("Received no messages in {} seconds. Waiting {} seconds before trying again",
                        waitTimeSeconds, delayBeforeRetry);
                numConsecutiveFailures++;
                Thread.sleep(delayBeforeRetry * 1000L);
            } else {
                Message message = receiveMessageResult.getMessages().get(0);
                LOGGER.info("Received message: {}", message);
                CompactionJob compactionJob = CompactionJobSerDe.deserialiseFromString(message.getBody());
                LOGGER.info("CompactionJob is: {}", compactionJob);
                try {
                    messageConsumer.consume(compactionJob, message);
                    totalNumberOfMessagesProcessed++;
                    numConsecutiveFailures = 0;
                } catch (Exception e) {
                    LOGGER.error("Failed processing compaction job, putting job back on queue", e);
                    numConsecutiveFailures++;
                    sqsClient.changeMessageVisibility(sqsJobQueueUrl, message.getReceiptHandle(), visibilityTimeout);
                }
            }
            currentTime = timeSupplier.get();
        }
        return new Result(totalNumberOfMessagesProcessed, currentTime.isAfter(maxTime), numConsecutiveFailures >= maxConsecutiveFailures);
    }

    @FunctionalInterface
    interface CompactionJobMessageConsumer {
        void consume(CompactionJob job, Message message) throws Exception;
    }

    public class Result {
        private long totalMessagesProcessed;
        private boolean maxTimeExceeded;
        private boolean maxConsecutiveFailuresReached;

        public Result(long totalMessagesProcessed, boolean maxTimeExceeded, boolean maxConsecutiveFailureReached) {
            super();
            this.totalMessagesProcessed = totalMessagesProcessed;
            this.maxTimeExceeded = maxTimeExceeded;
            this.maxConsecutiveFailuresReached = maxConsecutiveFailureReached;
        }

        public long getTotalMessagesProcessed() {
            return totalMessagesProcessed;
        }

        public boolean hasMaxTimeExceeded() {
            return maxTimeExceeded;
        }

        public boolean hasMaxConsecutiveFailuresBeenReached() {
            return maxConsecutiveFailuresReached;
        }
    }
}
