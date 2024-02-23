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

import com.amazonaws.services.sqs.model.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sleeper.compaction.job.CompactionJob;
import sleeper.configuration.properties.instance.InstanceProperties;
import sleeper.core.util.LoggedDuration;

import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.Optional;
import java.util.function.Supplier;

import static sleeper.configuration.properties.instance.CompactionProperty.COMPACTION_TASK_MAX_CONSECUTIVE_FAILURES;
import static sleeper.configuration.properties.instance.CompactionProperty.COMPACTION_TASK_MAX_TIME_IN_SECONDS;

public class CompactionTask {
    private static final Logger LOGGER = LoggerFactory.getLogger(CompactionTask.class);

    private final Supplier<Instant> timeSupplier;
    private final int maxConsecutiveFailures;
    private final int maxTimeInSeconds;
    private final MessageReceiver messageReceiver;
    private final MessageConsumer messageConsumer;
    private final FailedJobHandler failedJobHandler;

    public CompactionTask(InstanceProperties instanceProperties, Supplier<Instant> timeSupplier,
            MessageReceiver messageReceiver, MessageConsumer messageConsumer, FailedJobHandler failedJobHandler) {
        maxTimeInSeconds = instanceProperties.getInt(COMPACTION_TASK_MAX_TIME_IN_SECONDS);
        maxConsecutiveFailures = instanceProperties.getInt(COMPACTION_TASK_MAX_CONSECUTIVE_FAILURES);
        this.timeSupplier = timeSupplier;
        this.messageReceiver = messageReceiver;
        this.messageConsumer = messageConsumer;
        this.failedJobHandler = failedJobHandler;
    }

    public void runAt(Instant startTime) throws InterruptedException, IOException {
        Instant maxTime = startTime.plus(Duration.ofSeconds(maxTimeInSeconds));
        int numConsecutiveFailures = 0;
        Instant currentTime = startTime;
        long totalNumberOfMessagesProcessed = 0;
        while (numConsecutiveFailures < maxConsecutiveFailures && currentTime.isBefore(maxTime)) {
            Optional<JobAndMessage> jobAndMessageOpt = messageReceiver.receiveMessage();
            if (!jobAndMessageOpt.isPresent()) {
                numConsecutiveFailures++;
            } else {
                JobAndMessage jobAndMessage = jobAndMessageOpt.get();
                LOGGER.info("CompactionJob is: {}", jobAndMessage.job);
                try {
                    messageConsumer.consume(jobAndMessage);
                    totalNumberOfMessagesProcessed++;
                    numConsecutiveFailures = 0;
                } catch (Exception e) {
                    LOGGER.error("Failed processing compaction job, putting job back on queue", e);
                    numConsecutiveFailures++;
                    failedJobHandler.onFailure(jobAndMessage);
                }
            }
            currentTime = timeSupplier.get();
        }
        if (numConsecutiveFailures >= maxConsecutiveFailures) {
            LOGGER.info("Terminating compaction task as {} consecutive failures exceeds maximum of {}",
                    numConsecutiveFailures, maxConsecutiveFailures);
        } else {
            LOGGER.info("Terminating compaction task as it ran for {}, exceeeding maximum of {}",
                    LoggedDuration.withFullOutput(startTime, currentTime),
                    LoggedDuration.withFullOutput(Duration.ofSeconds(maxTimeInSeconds)));
        }
        LOGGER.info("Total number of messages processed = {}", totalNumberOfMessagesProcessed);
    }

    @FunctionalInterface
    interface MessageReceiver {
        Optional<JobAndMessage> receiveMessage() throws InterruptedException, IOException;
    }

    @FunctionalInterface
    interface MessageConsumer {
        void consume(JobAndMessage jobAndMessage) throws Exception;
    }

    @FunctionalInterface
    interface FailedJobHandler {
        void onFailure(JobAndMessage jobAndMessage);
    }

    static class JobAndMessage {
        private CompactionJob job;
        private Message message;

        JobAndMessage(CompactionJob job, Message message) {
            this.job = job;
            this.message = message;
        }

        public CompactionJob getJob() {
            return job;
        }

        public Message getMessage() {
            return message;
        }
    }
}
