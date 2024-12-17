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

package sleeper.compaction.core.task;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sleeper.compaction.core.job.CompactionJob;
import sleeper.compaction.core.job.CompactionJobStatusStore;
import sleeper.compaction.core.job.CompactionRunner;
import sleeper.compaction.core.job.commit.CompactionJobCommitterOrSendToLambda;
import sleeper.core.partition.Partition;
import sleeper.core.properties.PropertiesReloader;
import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.table.TableProperties;
import sleeper.core.properties.table.TablePropertiesProvider;
import sleeper.core.record.process.ProcessRunTime;
import sleeper.core.record.process.RecordsProcessed;
import sleeper.core.record.process.RecordsProcessedSummary;
import sleeper.core.statestore.StateStore;
import sleeper.core.statestore.StateStoreProvider;
import sleeper.core.statestore.exception.FileNotFoundException;
import sleeper.core.statestore.exception.FileReferenceAssignedToJobException;
import sleeper.core.statestore.exception.FileReferenceNotAssignedToJobException;
import sleeper.core.statestore.exception.FileReferenceNotFoundException;
import sleeper.core.table.TableNotFoundException;
import sleeper.core.util.LoggedDuration;

import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.Optional;
import java.util.UUID;
import java.util.function.Consumer;
import java.util.function.Supplier;

import static sleeper.core.metrics.MetricsLogger.METRICS_LOGGER;
import static sleeper.core.properties.instance.CompactionProperty.COMPACTION_TASK_DELAY_BEFORE_RETRY_IN_SECONDS;
import static sleeper.core.properties.instance.CompactionProperty.COMPACTION_TASK_MAX_CONSECUTIVE_FAILURES;
import static sleeper.core.properties.instance.CompactionProperty.COMPACTION_TASK_MAX_IDLE_TIME_IN_SECONDS;
import static sleeper.core.properties.instance.CompactionProperty.COMPACTION_TASK_WAIT_FOR_INPUT_FILE_ASSIGNMENT;

/**
 * Runs a compaction task. Executes jobs from a queue, updating the status stores with progress of the task.
 */
public class CompactionTask {
    private static final Logger LOGGER = LoggerFactory.getLogger(CompactionTask.class);

    private final InstanceProperties instanceProperties;
    private final TablePropertiesProvider tablePropertiesProvider;
    private final PropertiesReloader propertiesReloader;
    private final StateStoreProvider stateStoreProvider;
    private final Consumer<Duration> sleepForTime;
    private final MessageReceiver messageReceiver;
    private final CompactionRunnerFactory selector;
    private final CompactionJobStatusStore jobStatusStore;
    private final CompactionTaskStatusStore taskStatusStore;
    private final CompactionJobCommitterOrSendToLambda jobCommitter;
    private final String taskId;
    private final Supplier<String> jobRunIdSupplier;
    private final Supplier<Instant> timeSupplier;
    private final StateStoreWaitForFiles waitForFiles;

    public CompactionTask(InstanceProperties instanceProperties, TablePropertiesProvider tablePropertiesProvider,
            PropertiesReloader propertiesReloader, StateStoreProvider stateStoreProvider,
            MessageReceiver messageReceiver, StateStoreWaitForFiles waitForFiles,
            CompactionJobCommitterOrSendToLambda jobCommitter, CompactionJobStatusStore jobStore,
            CompactionTaskStatusStore taskStore, CompactionRunnerFactory selector, String taskId) {
        this(instanceProperties, tablePropertiesProvider, propertiesReloader, stateStoreProvider,
                messageReceiver, waitForFiles, jobCommitter,
                jobStore, taskStore, selector, taskId,
                () -> UUID.randomUUID().toString(), Instant::now, threadSleep());
    }

    @SuppressWarnings("checkstyle:ParameterNumberCheck")
    public CompactionTask(
            InstanceProperties instanceProperties,
            TablePropertiesProvider tablePropertiesProvider,
            PropertiesReloader propertiesReloader,
            StateStoreProvider stateStoreProvider,
            MessageReceiver messageReceiver, StateStoreWaitForFiles waitForFiles,
            CompactionJobCommitterOrSendToLambda jobCommitter,
            CompactionJobStatusStore jobStore, CompactionTaskStatusStore taskStore, CompactionRunnerFactory selector,
            String taskId, Supplier<String> jobRunIdSupplier, Supplier<Instant> timeSupplier, Consumer<Duration> sleepForTime) {
        this.instanceProperties = instanceProperties;
        this.tablePropertiesProvider = tablePropertiesProvider;
        this.propertiesReloader = propertiesReloader;
        this.stateStoreProvider = stateStoreProvider;
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

    private Instant handleMessages(Instant startTime, CompactionTaskFinishedStatus.Builder taskFinishedBuilder) throws IOException {
        IdleTimeTracker idleTimeTracker = new IdleTimeTracker(startTime);
        ConsecutiveFailuresTracker consecutiveFailuresTracker = new ConsecutiveFailuresTracker(instanceProperties);
        while (consecutiveFailuresTracker.hasNotMetMaximumFailures()) {
            Optional<MessageHandle> messageOpt = messageReceiver.receiveMessage();
            if (!messageOpt.isPresent()) {
                Instant currentTime = timeSupplier.get();
                if (idleTimeTracker.isLookForNextMessage(currentTime)) {
                    continue;
                } else {
                    return currentTime;
                }
            }
            try (MessageHandle message = messageOpt.get()) {
                String jobRunId = jobRunIdSupplier.get();
                if (prepareCompactionMessage(jobRunId, message, consecutiveFailuresTracker)) {
                    processCompactionMessage(jobRunId, taskFinishedBuilder, message, idleTimeTracker, consecutiveFailuresTracker);
                }
            }
        }
        return timeSupplier.get();
    }

    private boolean prepareCompactionMessage(String jobRunId, MessageHandle message, ConsecutiveFailuresTracker failureTracker) {
        try {
            propertiesReloader.reloadIfNeeded();
            if (instanceProperties.getBoolean(COMPACTION_TASK_WAIT_FOR_INPUT_FILE_ASSIGNMENT)) {
                waitForFiles.wait(message.getJob(), taskId, jobRunId);
            }
            return true;
        } catch (FileReferenceNotFoundException | FileReferenceAssignedToJobException e) {
            LOGGER.error("Found invalid job while waiting for input file assignment, deleting from queue", e);
            message.deleteFromQueue();
            return false;
        } catch (Exception e) {
            LOGGER.error("Failed preparing compaction job, putting job back on queue", e);
            failureTracker.incrementConsecutiveFailures();
            message.returnToQueue();
            return false;
        }
    }

    private void processCompactionMessage(
            String jobRunId, CompactionTaskFinishedStatus.Builder builder, MessageHandle message,
            IdleTimeTracker idleTimeTracker, ConsecutiveFailuresTracker failureTracker) {
        Instant jobStartTime = timeSupplier.get();
        try {
            RecordsProcessedSummary summary;
            try {
                summary = compact(message.getJob(), jobRunId, jobStartTime);
            } catch (TableNotFoundException e) {
                LOGGER.warn("Found compaction job for non-existent table, ignoring: {}", message.getJob());
                message.deleteFromQueue();
                throw e;
            } catch (Exception e) {
                LOGGER.error("Failed processing compaction job, putting job back on queue", e);
                failureTracker.incrementConsecutiveFailures();
                message.returnToQueue();
                throw e;
            }
            commitCompaction(jobRunId, builder, message, idleTimeTracker, failureTracker, summary);
        } catch (Exception e) {
            Instant jobFinishTime = timeSupplier.get();
            jobStatusStore.jobFailed(message.getJob()
                    .failedEventBuilder(new ProcessRunTime(jobStartTime, jobFinishTime))
                    .failure(e).taskId(taskId).jobRunId(jobRunId).build());
        }
    }

    private void commitCompaction(
            String jobRunId, CompactionTaskFinishedStatus.Builder builder, MessageHandle message,
            IdleTimeTracker idleTimeTracker, ConsecutiveFailuresTracker failureTracker, RecordsProcessedSummary summary) throws Exception {
        CompactionJob job = message.getJob();
        try {
            jobCommitter.commit(job, job.finishedEventBuilder(summary).taskId(taskId).jobRunId(jobRunId).build());
            logMetrics(job, summary);
            builder.addJobSummary(summary);
            message.deleteFromQueue();
            failureTracker.resetConsecutiveFailures();
            idleTimeTracker.setLastActiveTime(summary.getFinishTime());
        } catch (Exception e) {
            if (e.getCause() instanceof FileNotFoundException || e.getCause() instanceof FileReferenceNotAssignedToJobException) {
                LOGGER.error("Found invalid job while committing to state store, deleting from queue", e);
                message.deleteFromQueue();
            } else {
                LOGGER.error("Failed committing compaction job, putting job back on queue", e);
                failureTracker.incrementConsecutiveFailures();
                message.returnToQueue();
            }
            throw e;
        }
    }

    private RecordsProcessedSummary compact(CompactionJob job, String jobRunId, Instant jobStartTime) throws Exception {
        LOGGER.info("Compaction job {}: compaction called at {}", job.getId(), jobStartTime);
        jobStatusStore.jobStarted(job.startedEventBuilder(jobStartTime).taskId(taskId).jobRunId(jobRunId).build());
        TableProperties tableProperties = tablePropertiesProvider.getById(job.getTableId());
        CompactionRunner compactor = selector.createCompactor(job, tableProperties);
        StateStore stateStore = stateStoreProvider.getStateStore(tableProperties);
        Partition partition = stateStore.getPartition(job.getPartitionId());
        RecordsProcessed recordsProcessed = compactor.compact(job, tableProperties, partition);
        Instant jobFinishTime = timeSupplier.get();
        RecordsProcessedSummary summary = new RecordsProcessedSummary(recordsProcessed, jobStartTime, jobFinishTime);
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

    public interface MessageHandle extends AutoCloseable {
        CompactionJob getJob();

        void deleteFromQueue();

        void returnToQueue();

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

    private class IdleTimeTracker {

        private final Duration maxIdleTime;
        private final Duration delayBeforeRetry;
        private Instant lastActiveTime;

        private IdleTimeTracker(Instant startTime) {
            this.maxIdleTime = Duration.ofSeconds(instanceProperties.getInt(COMPACTION_TASK_MAX_IDLE_TIME_IN_SECONDS));
            this.delayBeforeRetry = Duration.ofSeconds(instanceProperties.getInt(COMPACTION_TASK_DELAY_BEFORE_RETRY_IN_SECONDS));
            this.lastActiveTime = startTime;
        }

        private boolean isLookForNextMessage(Instant currentTime) {
            Duration runTime = Duration.between(lastActiveTime, currentTime);
            if (runTime.compareTo(maxIdleTime) >= 0) {
                LOGGER.info("Terminating compaction task as it was idle for {}, exceeding maximum of {}",
                        LoggedDuration.withFullOutput(runTime),
                        LoggedDuration.withFullOutput(maxIdleTime));
                return false;
            } else {
                if (!delayBeforeRetry.isZero()) {
                    LOGGER.info("Received no messages, waiting {} before trying again",
                            LoggedDuration.withFullOutput(delayBeforeRetry));
                    sleepForTime.accept(delayBeforeRetry);
                }
                return true;
            }
        }

        private void setLastActiveTime(Instant currentTime) {
            lastActiveTime = currentTime;
        }

    }

    private static class ConsecutiveFailuresTracker {

        private final int maxConsecutiveFailures;
        private int numConsecutiveFailures;

        private ConsecutiveFailuresTracker(InstanceProperties instanceProperties) {
            this.maxConsecutiveFailures = instanceProperties.getInt(COMPACTION_TASK_MAX_CONSECUTIVE_FAILURES);
        }

        private void incrementConsecutiveFailures() {
            numConsecutiveFailures++;
        }

        private void resetConsecutiveFailures() {
            numConsecutiveFailures = 0;
        }

        private boolean hasNotMetMaximumFailures() {
            if (numConsecutiveFailures < maxConsecutiveFailures) {
                return true;
            } else {
                LOGGER.info("Terminating compaction task as {} consecutive failures exceeds maximum of {}",
                        numConsecutiveFailures, maxConsecutiveFailures);
                return false;
            }
        }
    }
}
