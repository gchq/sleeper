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

package sleeper.compaction.task;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sleeper.compaction.job.CompactionJob;
import sleeper.compaction.job.CompactionJobStatusStore;
import sleeper.compaction.job.CompactionRunner;
import sleeper.compaction.job.commit.CompactionJobCommitterOrSendToLambda;
import sleeper.configuration.properties.PropertiesReloader;
import sleeper.configuration.properties.instance.InstanceProperties;
import sleeper.core.record.process.ProcessRunTime;
import sleeper.core.record.process.RecordsProcessed;
import sleeper.core.record.process.RecordsProcessedSummary;
import sleeper.core.util.LoggedDuration;

import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.Optional;
import java.util.UUID;
import java.util.function.Consumer;
import java.util.function.Supplier;

import static sleeper.compaction.job.status.CompactionJobFailedEvent.compactionJobFailed;
import static sleeper.compaction.job.status.CompactionJobFinishedEvent.compactionJobFinished;
import static sleeper.compaction.job.status.CompactionJobStartedEvent.compactionJobStarted;
import static sleeper.configuration.properties.instance.CompactionProperty.COMPACTION_TASK_DELAY_BEFORE_RETRY_IN_SECONDS;
import static sleeper.configuration.properties.instance.CompactionProperty.COMPACTION_TASK_MAX_CONSECUTIVE_FAILURES;
import static sleeper.configuration.properties.instance.CompactionProperty.COMPACTION_TASK_MAX_IDLE_TIME_IN_SECONDS;
import static sleeper.core.metrics.MetricsLogger.METRICS_LOGGER;

/**
 * Runs a compaction task. Executes jobs from a queue, updating the status stores with progress of the task.
 */
public class CompactionTask {
    private static final Logger LOGGER = LoggerFactory.getLogger(CompactionTask.class);

    private final InstanceProperties instanceProperties;
    private final PropertiesReloader propertiesReloader;
    private final Consumer<Duration> sleepForTime;
    private final MessageReceiver messageReceiver;
    private final CompactionAlgorithmSelector selector;
    private final CompactionJobStatusStore jobStatusStore;
    private final CompactionTaskStatusStore taskStatusStore;
    private final CompactionJobCommitterOrSendToLambda jobCommitter;
    private final String taskId;
    private final Supplier<String> jobRunIdSupplier;
    private final Supplier<Instant> timeSupplier;
    private final WaitForFileAssignment waitForFiles;

    public CompactionTask(InstanceProperties instanceProperties, PropertiesReloader propertiesReloader,
            MessageReceiver messageReceiver, WaitForFileAssignment waitForFiles,
            CompactionJobCommitterOrSendToLambda jobCommitter, CompactionJobStatusStore jobStore,
            CompactionTaskStatusStore taskStore, CompactionAlgorithmSelector selector, String taskId) {
        this(instanceProperties, propertiesReloader, messageReceiver, waitForFiles, jobCommitter,
                jobStore, taskStore, selector, taskId, () -> UUID.randomUUID().toString(), Instant::now, threadSleep());
    }

    public CompactionTask(
            InstanceProperties instanceProperties,
            PropertiesReloader propertiesReloader,
            MessageReceiver messageReceiver, WaitForFileAssignment waitForFiles,
            CompactionJobCommitterOrSendToLambda jobCommitter,
            CompactionJobStatusStore jobStore, CompactionTaskStatusStore taskStore, CompactionAlgorithmSelector selector,
            String taskId, Supplier<String> jobRunIdSupplier, Supplier<Instant> timeSupplier, Consumer<Duration> sleepForTime) {
        this.instanceProperties = instanceProperties;
        this.propertiesReloader = propertiesReloader;
        this.timeSupplier = timeSupplier;
        this.sleepForTime = sleepForTime;
        this.messageReceiver = messageReceiver;
        this.selector = selector;
        this.jobStatusStore = jobStore;
        this.taskStatusStore = taskStore;
        this.taskId = taskId;
        this.jobRunIdSupplier = jobRunIdSupplier;
        this.jobCommitter = jobCommitter;
        this.waitForFiles = waitForFiles;
    }

    public void run() throws IOException {
        Instant startTime = timeSupplier.get();
        CompactionTaskStatus.Builder taskStatusBuilder = CompactionTaskStatus.builder().taskId(taskId).startTime(startTime);
        LOGGER.info("Starting task {}", taskId);
        taskStatusStore.taskStarted(taskStatusBuilder.build());
        CompactionTaskFinishedStatus.Builder taskFinishedBuilder = CompactionTaskFinishedStatus.builder();
        Instant finishTime = handleMessages(startTime, taskFinishedBuilder);
        CompactionTaskStatus taskFinished = taskStatusBuilder.finished(finishTime, taskFinishedBuilder).build();
        LOGGER.info("Total number of messages processed = {}", taskFinished.getJobRuns());
        LOGGER.info("Total run time = {}", LoggedDuration.withFullOutput(startTime, finishTime));

        taskStatusStore.taskFinished(taskFinished);
    }

    public Instant handleMessages(Instant startTime, CompactionTaskFinishedStatus.Builder taskFinishedBuilder) throws IOException {
        Instant lastActiveTime = startTime;
        Duration maxIdleTime = Duration.ofSeconds(instanceProperties.getInt(COMPACTION_TASK_MAX_IDLE_TIME_IN_SECONDS));
        int maxConsecutiveFailures = instanceProperties.getInt(COMPACTION_TASK_MAX_CONSECUTIVE_FAILURES);
        Duration delayBeforeRetry = Duration.ofSeconds(instanceProperties.getInt(COMPACTION_TASK_DELAY_BEFORE_RETRY_IN_SECONDS));
        int numConsecutiveFailures = 0;
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
                    if (!delayBeforeRetry.isZero()) {
                        LOGGER.info("Received no messages, waiting {} before trying again",
                                LoggedDuration.withFullOutput(delayBeforeRetry));
                        sleepForTime.accept(delayBeforeRetry);
                    }
                    continue;
                }
            }
            try (MessageHandle message = messageOpt.get()) {
                CompactionJob job = message.getJob();
                String jobRunId = jobRunIdSupplier.get();
                Instant jobStartTime = timeSupplier.get();
                try {
                    waitForFiles.wait(job);
                    RecordsProcessedSummary summary = compact(job, jobRunId, jobStartTime);
                    taskFinishedBuilder.addJobSummary(summary);
                    message.completed();
                    numConsecutiveFailures = 0;
                    lastActiveTime = summary.getFinishTime();
                } catch (Exception e) {
                    Instant jobFinishTime = timeSupplier.get();
                    jobStatusStore.jobFailed(compactionJobFailed(job,
                            new ProcessRunTime(jobStartTime, jobFinishTime))
                            .failure(e).taskId(taskId).jobRunId(jobRunId).build());
                    LOGGER.error("Failed processing compaction job, putting job back on queue", e);
                    numConsecutiveFailures++;
                    message.failed();
                }
            }
        }
        LOGGER.info("Terminating compaction task as {} consecutive failures exceeds maximum of {}",
                numConsecutiveFailures, maxConsecutiveFailures);
        return timeSupplier.get();
    }

    private RecordsProcessedSummary compact(CompactionJob job, String jobRunId, Instant jobStartTime) throws Exception {
        LOGGER.info("Compaction job {}: compaction called at {}", job.getId(), jobStartTime);
        jobStatusStore.jobStarted(compactionJobStarted(job, jobStartTime).taskId(taskId).jobRunId(jobRunId).build());
        propertiesReloader.reloadIfNeeded();
        CompactionRunner compactor = this.selector.chooseCompactor(job);
        RecordsProcessed recordsProcessed = compactor.compact(job);
        Instant jobFinishTime = timeSupplier.get();
        RecordsProcessedSummary summary = new RecordsProcessedSummary(recordsProcessed, jobStartTime, jobFinishTime);
        jobCommitter.commit(job, compactionJobFinished(job, summary).taskId(taskId).jobRunId(jobRunId));
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
    }

    @FunctionalInterface
    public interface MessageReceiver {
        Optional<MessageHandle> receiveMessage() throws IOException;
    }

    @FunctionalInterface
    public interface WaitForFileAssignment {
        void wait(CompactionJob job) throws InterruptedException;
    }

    public interface MessageHandle extends AutoCloseable {
        CompactionJob getJob();

        void completed();

        void failed();

        void close();
    }

    private static Consumer<Duration> threadSleep() {
        return time -> {
            try {
                Thread.sleep(time.toMillis());
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException(e);
            }
        };
    }
}
