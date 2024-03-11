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
import sleeper.compaction.job.CompactionJobStatusStore;
import sleeper.compaction.task.CompactionTaskFinishedStatus;
import sleeper.compaction.task.CompactionTaskStatus;
import sleeper.compaction.task.CompactionTaskStatusStore;
import sleeper.configuration.properties.PropertiesReloader;
import sleeper.configuration.properties.instance.InstanceProperties;
import sleeper.core.record.process.RecordsProcessed;
import sleeper.core.record.process.RecordsProcessedSummary;
import sleeper.core.util.LoggedDuration;

import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.Optional;
import java.util.function.Supplier;

import static sleeper.configuration.properties.instance.CompactionProperty.COMPACTION_TASK_MAX_CONSECUTIVE_FAILURES;
import static sleeper.configuration.properties.instance.CompactionProperty.COMPACTION_TASK_MAX_IDLE_TIME_IN_SECONDS;
import static sleeper.core.metrics.MetricsLogger.METRICS_LOGGER;

/**
 * Runs a compaction task, updating the {@link CompactionTaskStatusStore} with progress of the task.
 */
public class CompactionTask {
    private static final Logger LOGGER = LoggerFactory.getLogger(CompactionTask.class);

    private final Supplier<Instant> timeSupplier;
    private final int maxConsecutiveFailures;
    private final Duration maxIdleTime;
    private final MessageReceiver messageReceiver;
    private final CompactionRunner compactor;
    private final CompactionJobStatusStore jobStatusStore;
    private final CompactionTaskStatusStore taskStatusStore;
    private final String taskId;
    private final PropertiesReloader propertiesReloader;
    private int numConsecutiveFailures = 0;
    private int totalNumberOfMessagesProcessed = 0;

    public CompactionTask(InstanceProperties instanceProperties, PropertiesReloader propertiesReloader,
            Supplier<Instant> timeSupplier, MessageReceiver messageReceiver, CompactionRunner compactor,
            CompactionJobStatusStore jobStatusStore, CompactionTaskStatusStore taskStore, String taskId) {
        maxIdleTime = Duration.ofSeconds(instanceProperties.getInt(COMPACTION_TASK_MAX_IDLE_TIME_IN_SECONDS));
        maxConsecutiveFailures = instanceProperties.getInt(COMPACTION_TASK_MAX_CONSECUTIVE_FAILURES);
        this.propertiesReloader = propertiesReloader;
        this.timeSupplier = timeSupplier;
        this.messageReceiver = messageReceiver;
        this.compactor = compactor;
        this.jobStatusStore = jobStatusStore;
        this.taskStatusStore = taskStore;
        this.taskId = taskId;
    }

    public void run() throws InterruptedException, IOException {
        Instant startTime = timeSupplier.get();
        CompactionTaskStatus.Builder taskStatusBuilder = CompactionTaskStatus.builder().taskId(taskId).startTime(startTime);
        LOGGER.info("Starting task {}", taskId);
        taskStatusStore.taskStarted(taskStatusBuilder.build());
        CompactionTaskFinishedStatus.Builder taskFinishedBuilder = CompactionTaskFinishedStatus.builder();
        Instant finishTime = handleMessages(startTime, taskFinishedBuilder);
        if (numConsecutiveFailures >= maxConsecutiveFailures) {
            LOGGER.info("Terminating compaction task as {} consecutive failures exceeds maximum of {}",
                    numConsecutiveFailures, maxConsecutiveFailures);
        }
        LOGGER.info("Total number of messages processed = {}", totalNumberOfMessagesProcessed);
        LOGGER.info("Total run time = {}", LoggedDuration.withFullOutput(startTime, finishTime));

        CompactionTaskStatus taskFinished = taskStatusBuilder.finished(finishTime, taskFinishedBuilder).build();
        taskStatusStore.taskFinished(taskFinished);
    }

    public Instant handleMessages(Instant startTime, CompactionTaskFinishedStatus.Builder taskFinishedBuilder) throws InterruptedException, IOException {
        Instant lastActiveTime = startTime;
        while (numConsecutiveFailures < maxConsecutiveFailures) {
            Optional<MessageHandle> messageOpt = messageReceiver.receiveMessage();
            if (!messageOpt.isPresent()) {
                Instant currentTime = timeSupplier.get();
                Duration runTime = Duration.between(lastActiveTime, currentTime);
                if (runTime.compareTo(maxIdleTime) >= 0) {
                    LOGGER.info("Terminating compaction task as it was idle for {}, exceeding maximum of {}",
                            LoggedDuration.withFullOutput(runTime),
                            LoggedDuration.withFullOutput(maxIdleTime));
                    return currentTime;
                } else {
                    continue;
                }
            }
            try (MessageHandle message = messageOpt.get()) {
                CompactionJob job = message.getJob();
                try {
                    RecordsProcessedSummary summary = compact(job);
                    taskFinishedBuilder.addJobSummary(summary);
                    message.completed();
                    totalNumberOfMessagesProcessed++;
                    numConsecutiveFailures = 0;
                    lastActiveTime = summary.getFinishTime();
                } catch (Exception e) {
                    LOGGER.error("Failed processing compaction job, putting job back on queue", e);
                    numConsecutiveFailures++;
                    message.failed();
                }
            }
        }
        return timeSupplier.get();
    }

    private RecordsProcessedSummary compact(CompactionJob job) throws Exception {
        Instant jobStartTime = timeSupplier.get();
        LOGGER.info("Compaction job {}: compaction called at {}", job.getId(), jobStartTime);
        jobStatusStore.jobStarted(job, jobStartTime, taskId);
        propertiesReloader.reloadIfNeeded();
        RecordsProcessed recordsProcessed = compactor.compact(job);
        Instant jobFinishTime = timeSupplier.get();
        RecordsProcessedSummary summary = new RecordsProcessedSummary(recordsProcessed, jobStartTime, jobFinishTime);
        jobStatusStore.jobFinished(job, summary, taskId);
        logMetrics(job, summary);
        return summary;
    }

    private void logMetrics(CompactionJob job, RecordsProcessedSummary summary) {
        LOGGER.info("Compaction job {}: finished at {}", job.getId(), summary.getFinishTime());
        METRICS_LOGGER.info("Compaction job {}: compaction run time = {}", job.getId(), summary.getDurationInSeconds());
        METRICS_LOGGER.info("Compaction job {}: compaction read {} records at {} per second", job.getId(),
                summary.getRecordsRead(), String.format("%.1f", summary.getRecordsReadPerSecond()));
        METRICS_LOGGER.info("Compaction job {}: compaction wrote {} records at {} per second", job.getId(),
                summary.getRecordsWritten(), String.format("%.1f", summary.getRecordsWrittenPerSecond()));
        jobStatusStore.jobFinished(job, summary, taskId);
    }

    @FunctionalInterface
    interface MessageReceiver {
        Optional<MessageHandle> receiveMessage() throws InterruptedException, IOException;
    }

    @FunctionalInterface
    interface CompactionRunner {
        RecordsProcessed compact(CompactionJob job) throws Exception;
    }

    interface MessageHandle extends AutoCloseable {
        CompactionJob getJob();

        void completed();

        void failed();

        void close();
    }
}
