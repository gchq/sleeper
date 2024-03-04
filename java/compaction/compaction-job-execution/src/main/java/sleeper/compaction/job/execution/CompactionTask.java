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
import sleeper.compaction.task.CompactionTaskFinishedStatus;
import sleeper.compaction.task.CompactionTaskStatus;
import sleeper.compaction.task.CompactionTaskStatusStore;
import sleeper.configuration.properties.instance.InstanceProperties;
import sleeper.core.record.process.RecordsProcessedSummary;
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
    private final CompactionRunner compactor;
    private final CompactionTaskStatusStore taskStatusStore;
    private final String taskId;

    public CompactionTask(InstanceProperties instanceProperties, Supplier<Instant> timeSupplier,
            MessageReceiver messageReceiver, CompactionRunner compactor, CompactionTaskStatusStore taskStore, String taskId) {
        maxIdleTime = Duration.ofSeconds(instanceProperties.getInt(COMPACTION_TASK_MAX_IDLE_TIME_IN_SECONDS));
        maxConsecutiveFailures = instanceProperties.getInt(COMPACTION_TASK_MAX_CONSECUTIVE_FAILURES);
        this.timeSupplier = timeSupplier;
        this.messageReceiver = messageReceiver;
        this.compactor = compactor;
        this.taskStatusStore = taskStore;
        this.taskId = taskId;
    }

    public void runAt(Instant startTime) throws InterruptedException, IOException {
        Instant lastActiveTime = startTime;
        CompactionTaskStatus.Builder taskStatusBuilder = CompactionTaskStatus.builder().taskId(taskId).startTime(startTime);
        LOGGER.info("Starting task {}", taskId);
        taskStatusStore.taskStarted(taskStatusBuilder.build());
        CompactionTaskFinishedStatus.Builder taskFinishedBuilder = CompactionTaskFinishedStatus.builder();
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
                    break;
                } else {
                    continue;
                }
            }
            try (MessageHandle message = messageOpt.get()) {
                CompactionJob job = message.getJob();
                LOGGER.info("CompactionJob is: {}", job);
                try {
                    taskFinishedBuilder.addJobSummary(compactor.compact(job));
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
        Instant finishTime = timeSupplier.get();
        LOGGER.info("CompactSortedFilesRunner total run time = {}", LoggedDuration.withFullOutput(startTime, finishTime));

        CompactionTaskStatus taskFinished = taskStatusBuilder.finished(finishTime, taskFinishedBuilder).build();
        taskStatusStore.taskFinished(taskFinished);
    }

    @FunctionalInterface
    interface MessageReceiver {
        Optional<MessageHandle> receiveMessage() throws InterruptedException, IOException;
    }

    @FunctionalInterface
    interface CompactionRunner {
        RecordsProcessedSummary compact(CompactionJob job) throws Exception;
    }

    interface MessageHandle extends AutoCloseable {
        CompactionJob getJob();

        void completed();

        void failed();

        void close();
    }
}
