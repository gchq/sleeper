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
import static sleeper.configuration.properties.instance.CompactionProperty.COMPACTION_TASK_MAX_IDLE_TIME_IN_SECONDS;

public class CompactionTask {
    private static final Logger LOGGER = LoggerFactory.getLogger(CompactionTask.class);

    private final Supplier<Instant> timeSupplier;
    private final int maxConsecutiveFailures;
    private final Duration maxIdleTime;
    private final MessageReceiver messageReceiver;
    private final MessageConsumer messageConsumer;

    public CompactionTask(InstanceProperties instanceProperties, Supplier<Instant> timeSupplier,
            MessageReceiver messageReceiver, MessageConsumer messageConsumer) {
        maxIdleTime = Duration.ofSeconds(instanceProperties.getInt(COMPACTION_TASK_MAX_IDLE_TIME_IN_SECONDS));
        maxConsecutiveFailures = instanceProperties.getInt(COMPACTION_TASK_MAX_CONSECUTIVE_FAILURES);
        this.timeSupplier = timeSupplier;
        this.messageReceiver = messageReceiver;
        this.messageConsumer = messageConsumer;
    }

    public void runAt(Instant startTime) throws InterruptedException, IOException {
        Instant lastActiveTime = startTime;
        int numConsecutiveFailures = 0;
        long totalNumberOfMessagesProcessed = 0;
        while (numConsecutiveFailures < maxConsecutiveFailures) {
            Optional<MessageHandle> messageOpt = messageReceiver.receiveMessage();
            if (!messageOpt.isPresent()) {
                Duration runTime = Duration.between(lastActiveTime, timeSupplier.get());
                if (runTime.compareTo(maxIdleTime) >= 0) {
                    LOGGER.info("Terminating compaction task as it was idle for {}, exceeding maximum of {}",
                            LoggedDuration.withFullOutput(runTime),
                            LoggedDuration.withFullOutput(maxIdleTime));
                    return;
                } else {
                    continue;
                }
            }
            try (MessageHandle message = messageOpt.get()) {
                CompactionJob job = message.getJob();
                LOGGER.info("CompactionJob is: {}", job);
                try {
                    messageConsumer.consume(job);
                    message.completed();
                    totalNumberOfMessagesProcessed++;
                    numConsecutiveFailures = 0;
                    lastActiveTime = timeSupplier.get();
                } catch (Exception e) {
                    LOGGER.error("Failed processing compaction job, putting job back on queue", e);
                    numConsecutiveFailures++;
                    message.failed();
                }
            }
        }
        if (numConsecutiveFailures >= maxConsecutiveFailures) {
            LOGGER.info("Terminating compaction task as {} consecutive failures exceeds maximum of {}",
                    numConsecutiveFailures, maxConsecutiveFailures);
        }
        LOGGER.info("Total number of messages processed = {}", totalNumberOfMessagesProcessed);
    }

    @FunctionalInterface
    interface MessageReceiver {
        Optional<MessageHandle> receiveMessage() throws InterruptedException, IOException;
    }

    @FunctionalInterface
    interface MessageConsumer {
        void consume(CompactionJob jobAndMessage) throws Exception;
    }

    @FunctionalInterface
    interface FailedJobHandler {
        void onFailure(JobAndMessage jobAndMessage);
    }

    interface MessageHandle extends AutoCloseable {
        CompactionJob getJob();

        void completed();

        void failed();

        void close();
    }

    static class JobAndMessage implements MessageHandle {
        private final CompactionJob job;
        private final FailedJobHandler failedJobHandler;

        JobAndMessage(CompactionJob job, FailedJobHandler failedJobHandler) {
            this.job = job;
            this.failedJobHandler = failedJobHandler;
        }

        public CompactionJob getJob() {
            return job;
        }

        public void close() {
        }

        public void completed() {
        }

        public void failed() {
            failedJobHandler.onFailure(this);
        }
    }
}
