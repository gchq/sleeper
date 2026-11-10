/*
 * Copyright 2022-2026 Crown Copyright
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
import sleeper.compaction.core.job.CompactionJobCommitterOrSendToLambda;
import sleeper.compaction.core.job.CompactionRequest;
import sleeper.compaction.core.job.CompactionRunner;
import sleeper.core.partition.Partition;
import sleeper.core.properties.PropertiesReloader;
import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.model.CompactionProgressCheckSettings;
import sleeper.core.properties.table.TableProperties;
import sleeper.core.properties.table.TablePropertiesProvider;
import sleeper.core.statestore.StateStore;
import sleeper.core.statestore.StateStoreProvider;
import sleeper.core.statestore.exception.FileNotFoundException;
import sleeper.core.statestore.exception.FileReferenceAssignedToJobException;
import sleeper.core.statestore.exception.FileReferenceNotAssignedToJobException;
import sleeper.core.statestore.exception.FileReferenceNotFoundException;
import sleeper.core.table.TableNotFoundException;
import sleeper.core.tracker.compaction.job.CompactionJobTracker;
import sleeper.core.tracker.compaction.task.CompactionTaskFinishedStatus;
import sleeper.core.tracker.compaction.task.CompactionTaskStatus;
import sleeper.core.tracker.compaction.task.CompactionTaskTracker;
import sleeper.core.tracker.job.run.JobRunSummary;
import sleeper.core.tracker.job.run.RowsProcessed;
import sleeper.core.util.LoggedDuration;
import sleeper.core.util.ThreadSleep;

import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.DoubleSupplier;
import java.util.function.LongConsumer;
import java.util.function.Supplier;

import static sleeper.core.metrics.MetricsLogger.METRICS_LOGGER;
import static sleeper.core.properties.instance.CompactionProperty.COMPACTION_TASK_DELAY_BEFORE_RETRY_IN_SECONDS;
import static sleeper.core.properties.instance.CompactionProperty.COMPACTION_TASK_MAX_ALIVE_JITTER_IN_MINUTES;
import static sleeper.core.properties.instance.CompactionProperty.COMPACTION_TASK_MAX_ALIVE_TIME_IN_MINUTES;
import static sleeper.core.properties.instance.CompactionProperty.COMPACTION_TASK_MAX_CONSECUTIVE_FAILURES;
import static sleeper.core.properties.instance.CompactionProperty.COMPACTION_TASK_MAX_IDLE_TIME_IN_SECONDS;
import static sleeper.core.properties.instance.CompactionProperty.COMPACTION_TASK_WAIT_FOR_INPUT_FILE_ASSIGNMENT;

/**
 * Runs a compaction task. Executes jobs from a queue, updating the job and task trackers with progress.
 */
public class CompactionTask {
    private static final Logger LOGGER = LoggerFactory.getLogger(CompactionTask.class);

    private final InstanceProperties instanceProperties;
    private final TablePropertiesProvider tablePropertiesProvider;
    private final PropertiesReloader propertiesReloader;
    private final StateStoreProvider stateStoreProvider;
    private final MessageReceiver messageReceiver;
    private final StateStoreWaitForFiles waitForFiles;
    private final CompactionJobCommitterOrSendToLambda jobCommitter;
    private final CompactionJobTracker jobTracker;
    private final CompactionTaskTracker taskTracker;
    private final CompactionRunnerFactory runnerFactory;
    private final String taskId;
    private final Supplier<String> jobRunIdSupplier;
    private final Supplier<Instant> timeSupplier;
    private final ThreadSleep threadSleep;
    private final Duration maxAliveTimeDuration;
    private final LongConsumer progressTracker;
    private final CompactionWatchdogFactory watchdogFactory;

    private CompactionTask(Builder builder) {
        instanceProperties = Objects.requireNonNull(builder.instanceProperties, "instanceProperties must not be null");
        tablePropertiesProvider = Objects.requireNonNull(builder.tablePropertiesProvider, "tablePropertiesProvider must not be null");
        propertiesReloader = Objects.requireNonNull(builder.propertiesReloader, "propertiesReloader must not be null");
        stateStoreProvider = Objects.requireNonNull(builder.stateStoreProvider, "stateStoreProvider must not be null");
        messageReceiver = Objects.requireNonNull(builder.messageReceiver, "messageReceiver must not be null");
        waitForFiles = Objects.requireNonNull(builder.waitForFiles, "waitForFiles must not be null");
        jobCommitter = Objects.requireNonNull(builder.jobCommitter, "jobCommitter must not be null");
        jobTracker = Objects.requireNonNull(builder.jobTracker, "jobTracker must not be null");
        taskTracker = Objects.requireNonNull(builder.taskTracker, "taskTracker must not be null");
        runnerFactory = Objects.requireNonNull(builder.runnerFactory, "runnerFactory must not be null");
        taskId = Objects.requireNonNull(builder.taskId, "taskId must not be null");
        watchdogFactory = Objects.requireNonNull(builder.watchdogFactory, "watchdogFactory must not be null");
        jobRunIdSupplier = Objects.requireNonNullElse(builder.jobRunIdSupplier, () -> UUID.randomUUID().toString());
        timeSupplier = Objects.requireNonNullElse(builder.timeSupplier, Instant::now);
        threadSleep = Objects.requireNonNullElse(builder.threadSleep, Thread::sleep);
        progressTracker = Objects.requireNonNullElse(builder.progressTracker, rows -> {
        });
        maxAliveTimeDuration = generateMaxAliveTimeWithJitter(
                Objects.requireNonNullElse(builder.randomJitterFunction, Math::random));
    }

    public static Builder builder() {
        return new Builder();
    }

    public void run() throws IOException {
        Instant startTime = timeSupplier.get();
        CompactionTaskStatus.Builder taskStatusBuilder = CompactionTaskStatus.builder().taskId(taskId).startTime(startTime);
        LOGGER.info("Starting task {}", taskId);
        taskTracker.taskStarted(taskStatusBuilder.build());
        CompactionTaskFinishedStatus.Builder taskFinishedBuilder = CompactionTaskFinishedStatus.builder();
        Instant finishTime;
        try {
            finishTime = handleMessages(startTime, taskStatusBuilder, taskFinishedBuilder);
        } catch (CompactionStalledException e) {
            // Task finished status was already reported by the watchdog thread when the stall was detected
            LOGGER.error("Compaction task {} stopped after a compaction stalled", taskId, e);
            return;
        }
        CompactionTaskStatus taskFinished = taskStatusBuilder.finished(finishTime, taskFinishedBuilder).build();
        LOGGER.info("Total number of messages processed = {}", taskFinished.getJobRuns());
        LOGGER.info("Total run time = {}", LoggedDuration.withFullOutput(startTime, finishTime));

        taskTracker.taskFinished(taskFinished);
    }

    private Instant handleMessages(Instant startTime, CompactionTaskStatus.Builder taskStatusBuilder,
            CompactionTaskFinishedStatus.Builder taskFinishedBuilder) throws IOException {
        IdleTimeTracker idleTimeTracker = new IdleTimeTracker(startTime);
        ConsecutiveFailuresTracker consecutiveFailuresTracker = new ConsecutiveFailuresTracker(instanceProperties);
        while (consecutiveFailuresTracker.hasNotMetMaximumFailures() && hasNotReachedMaxAliveTime(startTime)) {
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
                    processCompactionMessage(jobRunId, taskStatusBuilder, taskFinishedBuilder, message, idleTimeTracker, consecutiveFailuresTracker);
                }
            }
            if (Thread.currentThread().isInterrupted()) {
                LOGGER.warn("Compaction task interrupted, terminating");
                break;
            }
        }
        return timeSupplier.get();
    }

    private boolean hasNotReachedMaxAliveTime(Instant startTime) {
        Duration aliveTime = Duration.between(startTime, timeSupplier.get());
        return aliveTime.compareTo(maxAliveTimeDuration) < 0;
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
            if (e instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
            LOGGER.error("Failed preparing compaction job, putting job back on queue", e);
            failureTracker.incrementConsecutiveFailures();
            message.returnToQueue();
            return false;
        }
    }

    private void processCompactionMessage(
            String jobRunId, CompactionTaskStatus.Builder taskStatusBuilder, CompactionTaskFinishedStatus.Builder builder,
            MessageHandle message, IdleTimeTracker idleTimeTracker, ConsecutiveFailuresTracker failureTracker) {
        Instant jobStartTime = timeSupplier.get();
        try {
            JobRunSummary summary;
            try {
                summary = compact(message, jobRunId, jobStartTime, taskStatusBuilder, builder);
            } catch (CompactionStalledException e) {
                throw e;
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
        } catch (CompactionStalledException e) {
            throw e;
        } catch (Exception e) {
            jobTracker.jobFailed(message.getJob()
                    .failedEventBuilder(timeSupplier.get())
                    .failure(e).taskId(taskId).jobRunId(jobRunId).build());
        }
    }

    private void commitCompaction(
            String jobRunId, CompactionTaskFinishedStatus.Builder builder, MessageHandle message,
            IdleTimeTracker idleTimeTracker, ConsecutiveFailuresTracker failureTracker, JobRunSummary summary) throws Exception {
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

    private JobRunSummary compact(
            MessageHandle message, String jobRunId, Instant jobStartTime,
            CompactionTaskStatus.Builder taskStatusBuilder, CompactionTaskFinishedStatus.Builder taskFinishedBuilder) throws Exception {
        CompactionJob job = message.getJob();
        LOGGER.info("Compaction job {}: compaction called at {}", job.getId(), jobStartTime);
        jobTracker.jobStarted(job.startedEventBuilder(jobStartTime).taskId(taskId).jobRunId(jobRunId).build());
        TableProperties tableProperties = tablePropertiesProvider.getById(job.getTableId());
        CompactionRunner compactor = runnerFactory.createCompactor(job, tableProperties);
        StateStore stateStore = stateStoreProvider.getStateStore(tableProperties);
        Partition partition = stateStore.getPartition(job.getPartitionId());
        CompactionRequest.Builder request = CompactionRequest.builder()
                .job(job)
                .tableProperties(tableProperties)
                .region(partition.getRegion());
        RowsProcessed rowsProcessed = compactWithWatchdog(compactor, request, message, jobRunId, jobStartTime, taskStatusBuilder, taskFinishedBuilder);
        Instant jobFinishTime = timeSupplier.get();
        JobRunSummary summary = new JobRunSummary(rowsProcessed, jobStartTime, jobFinishTime);
        return summary;
    }

    @SuppressWarnings("checkstyle:ParameterNumberCheck")
    private RowsProcessed compactWithWatchdog(
            CompactionRunner compactor, CompactionRequest.Builder request, MessageHandle message,
            String jobRunId, Instant jobStartTime,
            CompactionTaskStatus.Builder taskStatusBuilder, CompactionTaskFinishedStatus.Builder taskFinishedBuilder) throws Exception {
        CompactionJob job = message.getJob();
        CompactionProgressCheckSettings settings = CompactionProgressCheckSettings.from(instanceProperties);
        CompactionProgressMonitor monitor = watchdogFactory.createMonitor();
        AtomicReference<CompactionStall> stalled = new AtomicReference<>();
        RowsProcessed rowsProcessed;
        try (CompactionWatchdog watchdog = watchdogFactory.createWatchdog(job.getId(), monitor, settings,
                stall -> {
                    stalled.set(stall);
                    handleStall(stall, message, jobRunId, jobStartTime, taskStatusBuilder, taskFinishedBuilder);
                })) {
            watchdog.start();
            rowsProcessed = compactor.compact(request.progressCallback(monitor.andThen(progressTracker)).build());
        }
        if (stalled.get() != null) {
            throw new CompactionStalledException(job.getId(), stalled.get());
        }
        return rowsProcessed;
    }

    // Called on the watchdog thread while the compaction may still be running, so this does not interact with the
    // thread running the compaction. Each step is attempted even if an earlier one fails. The watchdog terminates the
    // task after this.
    @SuppressWarnings("checkstyle:ParameterNumberCheck")
    private void handleStall(
            CompactionStall stall, MessageHandle message, String jobRunId, Instant jobStartTime,
            CompactionTaskStatus.Builder taskStatusBuilder, CompactionTaskFinishedStatus.Builder taskFinishedBuilder) {
        CompactionJob job = message.getJob();
        LOGGER.error("Compaction job {}: compaction stalled, putting job back on queue and terminating task {}. {}",
                job.getId(), taskId, stall.description());
        attempt("record job failure", () -> jobTracker.jobFailed(job
                .failedEventBuilder(stall.detectedTime())
                .failureReasons(List.of("Compaction stalled: " + stall.description()))
                .taskId(taskId).jobRunId(jobRunId).build()));
        attempt("stop keep alive", message::close);
        attempt("return job to queue", message::returnToQueue);
        attempt("record task finished", () -> taskTracker.taskFinished(
                taskStatusBuilder.finished(stall.detectedTime(), taskFinishedBuilder).build()));
        LOGGER.info("Compaction job {}: ran for {} before stall was detected",
                job.getId(), LoggedDuration.withFullOutput(jobStartTime, stall.detectedTime()));
    }

    private static void attempt(String description, Runnable action) {
        try {
            action.run();
        } catch (RuntimeException e) {
            LOGGER.error("Failed to {} after compaction stalled", description, e);
        }
    }

    private void logMetrics(CompactionJob job, JobRunSummary summary) {
        LOGGER.info("Compaction job {}: finished at {}", job.getId(), summary.getFinishTime());
        METRICS_LOGGER.info("Compaction job {}: compaction run time = {}", job.getId(), summary.getDurationInSeconds());
        METRICS_LOGGER.info("Compaction job {}: compaction read {} rows at {} per second", job.getId(),
                summary.getRowsRead(), String.format("%.1f", summary.getRowsReadPerSecond()));
        METRICS_LOGGER.info("Compaction job {}: compaction wrote {} rows at {} per second", job.getId(),
                summary.getRowsWritten(), String.format("%.1f", summary.getRowsWrittenPerSecond()));
    }

    private Duration generateMaxAliveTimeWithJitter(DoubleSupplier randomJitterFunction) {
        Duration maxAlive = Duration.ofMinutes(instanceProperties.getLong(COMPACTION_TASK_MAX_ALIVE_TIME_IN_MINUTES));
        Duration maxJitter = Duration.ofMinutes(instanceProperties.getLong(COMPACTION_TASK_MAX_ALIVE_JITTER_IN_MINUTES));
        Duration jitter = Duration.ofMillis((long) (randomJitterFunction.getAsDouble() * maxJitter.toMillis()));
        return maxAlive.minus(jitter);
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
                    threadSleep.waitForDurationWrappingInterrupt(delayBeforeRetry);
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

    /**
     * Builds a compaction task. The job run ID supplier, time supplier, thread sleep, random jitter function and
     * progress tracker are optional and default to production behaviour. All other fields must be set.
     */
    public static class Builder {
        private InstanceProperties instanceProperties;
        private TablePropertiesProvider tablePropertiesProvider;
        private PropertiesReloader propertiesReloader;
        private StateStoreProvider stateStoreProvider;
        private MessageReceiver messageReceiver;
        private StateStoreWaitForFiles waitForFiles;
        private CompactionJobCommitterOrSendToLambda jobCommitter;
        private CompactionJobTracker jobTracker;
        private CompactionTaskTracker taskTracker;
        private CompactionRunnerFactory runnerFactory;
        private String taskId;
        private CompactionWatchdogFactory watchdogFactory;
        private Supplier<String> jobRunIdSupplier;
        private Supplier<Instant> timeSupplier;
        private ThreadSleep threadSleep;
        private DoubleSupplier randomJitterFunction;
        private LongConsumer progressTracker;

        private Builder() {
        }

        public Builder instanceProperties(InstanceProperties instanceProperties) {
            this.instanceProperties = instanceProperties;
            return this;
        }

        public Builder tablePropertiesProvider(TablePropertiesProvider tablePropertiesProvider) {
            this.tablePropertiesProvider = tablePropertiesProvider;
            return this;
        }

        public Builder propertiesReloader(PropertiesReloader propertiesReloader) {
            this.propertiesReloader = propertiesReloader;
            return this;
        }

        public Builder stateStoreProvider(StateStoreProvider stateStoreProvider) {
            this.stateStoreProvider = stateStoreProvider;
            return this;
        }

        public Builder messageReceiver(MessageReceiver messageReceiver) {
            this.messageReceiver = messageReceiver;
            return this;
        }

        public Builder waitForFiles(StateStoreWaitForFiles waitForFiles) {
            this.waitForFiles = waitForFiles;
            return this;
        }

        public Builder jobCommitter(CompactionJobCommitterOrSendToLambda jobCommitter) {
            this.jobCommitter = jobCommitter;
            return this;
        }

        public Builder jobTracker(CompactionJobTracker jobTracker) {
            this.jobTracker = jobTracker;
            return this;
        }

        public Builder taskTracker(CompactionTaskTracker taskTracker) {
            this.taskTracker = taskTracker;
            return this;
        }

        public Builder runnerFactory(CompactionRunnerFactory runnerFactory) {
            this.runnerFactory = runnerFactory;
            return this;
        }

        public Builder taskId(String taskId) {
            this.taskId = taskId;
            return this;
        }

        public Builder watchdogFactory(CompactionWatchdogFactory watchdogFactory) {
            this.watchdogFactory = watchdogFactory;
            return this;
        }

        public Builder jobRunIdSupplier(Supplier<String> jobRunIdSupplier) {
            this.jobRunIdSupplier = jobRunIdSupplier;
            return this;
        }

        public Builder timeSupplier(Supplier<Instant> timeSupplier) {
            this.timeSupplier = timeSupplier;
            return this;
        }

        public Builder threadSleep(ThreadSleep threadSleep) {
            this.threadSleep = threadSleep;
            return this;
        }

        public Builder randomJitterFunction(DoubleSupplier randomJitterFunction) {
            this.randomJitterFunction = randomJitterFunction;
            return this;
        }

        public Builder progressTracker(LongConsumer progressTracker) {
            this.progressTracker = progressTracker;
            return this;
        }

        public CompactionTask build() {
            return new CompactionTask(this);
        }
    }
}
