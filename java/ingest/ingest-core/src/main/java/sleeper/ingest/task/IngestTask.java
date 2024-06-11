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
package sleeper.ingest.task;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sleeper.core.record.process.ProcessRunTime;
import sleeper.core.record.process.RecordsProcessedSummary;
import sleeper.core.util.LoggedDuration;
import sleeper.ingest.IngestResult;
import sleeper.ingest.job.IngestJob;
import sleeper.ingest.job.IngestJobHandler;
import sleeper.ingest.job.commit.IngestJobCommitRequest;
import sleeper.ingest.job.commit.IngestJobCommitter;
import sleeper.ingest.job.status.IngestJobStatusStore;
import sleeper.ingest.task.IngestJobCommitterOrSendToLambda.CommitQueueSender;
import sleeper.ingest.task.IngestJobCommitterOrSendToLambda.TableCommitConfig;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.function.Supplier;

import static sleeper.ingest.job.status.IngestJobFailedEvent.ingestJobFailed;
import static sleeper.ingest.job.status.IngestJobStartedEvent.ingestJobStarted;

/**
 * Runs an ingest task. Executes jobs from a queue, updating the status stores with progress of the task.
 */
public class IngestTask {
    public static final Logger LOGGER = LoggerFactory.getLogger(IngestTask.class);
    private final Supplier<Instant> timeSupplier;
    private final MessageReceiver messageReceiver;
    private final IngestJobHandler ingester;
    private final IngestJobStatusStore jobStatusStore;
    private final IngestTaskStatusStore taskStatusStore;
    private final IngestJobCommitterOrSendToLambda jobCommitter;
    private final String taskId;
    private int totalNumberOfMessagesProcessed = 0;

    public IngestTask(Supplier<Instant> timeSupplier, MessageReceiver messageReceiver, IngestJobHandler ingester,
            TableCommitConfig tableCommitConfig, IngestJobCommitter jobCommitter, CommitQueueSender asyncCommitter,
            IngestJobStatusStore jobStatusStore, IngestTaskStatusStore taskStore, String taskId) {
        this(timeSupplier, messageReceiver, ingester,
                new IngestJobCommitterOrSendToLambda(tableCommitConfig, jobCommitter, asyncCommitter),
                jobStatusStore, taskStore, taskId);
    }

    public IngestTask(Supplier<Instant> timeSupplier, MessageReceiver messageReceiver, IngestJobHandler ingester,
            IngestJobCommitterOrSendToLambda jobCommitter,
            IngestJobStatusStore jobStatusStore, IngestTaskStatusStore taskStore, String taskId) {
        this.timeSupplier = timeSupplier;
        this.messageReceiver = messageReceiver;
        this.ingester = ingester;
        this.jobStatusStore = jobStatusStore;
        this.taskStatusStore = taskStore;
        this.taskId = taskId;
        this.jobCommitter = jobCommitter;
    }

    /**
     * Executes jobs from a queue, updating the status stores with progress of the task.
     */
    public void run() {
        Instant startTime = timeSupplier.get();
        IngestTaskStatus.Builder taskStatusBuilder = IngestTaskStatus.builder().taskId(taskId).startTime(startTime);
        LOGGER.info("Starting task {}", taskId);
        taskStatusStore.taskStarted(taskStatusBuilder.build());
        IngestTaskFinishedStatus.Builder taskFinishedBuilder = IngestTaskFinishedStatus.builder();
        Instant finishTime = handleMessages(startTime, taskFinishedBuilder);
        LOGGER.info("Total number of messages processed = {}", totalNumberOfMessagesProcessed);
        LOGGER.info("Total run time = {}", LoggedDuration.withFullOutput(startTime, finishTime));

        IngestTaskStatus taskFinished = taskStatusBuilder.finished(finishTime, taskFinishedBuilder).build();
        taskStatusStore.taskFinished(taskFinished);
    }

    /**
     * Receives messages, deserialising each message to an ingest job, then runs it. Updates the ingest job status
     * store with progress on the job. These actions are repeated until no more messages are found.
     *
     * @param  startTime           the start time
     * @param  taskFinishedBuilder the ingest task finished builder
     * @return                     the finish time
     */
    private Instant handleMessages(Instant startTime, IngestTaskFinishedStatus.Builder taskFinishedBuilder) {
        while (true) {
            Optional<MessageHandle> messageOpt = messageReceiver.receiveMessage();
            if (!messageOpt.isPresent()) {
                LOGGER.info("Terminating ingest task as no messages were received");
                return timeSupplier.get();
            }
            try (MessageHandle message = messageOpt.get()) {
                IngestJob job = message.getJob();
                LOGGER.info("IngestJob is: {}", job);
                Instant jobStartTime = timeSupplier.get();
                try {
                    jobStatusStore.jobStarted(ingestJobStarted(taskId, job, jobStartTime));
                    IngestResult result = ingester.ingest(job);
                    LOGGER.info("{} records were written", result.getRecordsWritten());
                    Instant jobFinishTime = timeSupplier.get();
                    RecordsProcessedSummary summary = new RecordsProcessedSummary(result.asRecordsProcessed(), jobStartTime, jobFinishTime);
                    jobCommitter.commit(new IngestJobCommitRequest(job, taskId, result.getFileReferenceList(), summary));
                    taskFinishedBuilder.addJobSummary(summary);
                    message.completed(summary);
                    totalNumberOfMessagesProcessed++;
                } catch (Exception e) {
                    Instant jobFinishTime = timeSupplier.get();
                    jobStatusStore.jobFailed(ingestJobFailed(job, new ProcessRunTime(jobStartTime, jobFinishTime))
                            .taskId(taskId)
                            .failureReasons(getFailureReasons(e))
                            .build());
                    LOGGER.error("Failed processing ingest job, terminating task", e);
                    message.failed();
                    return timeSupplier.get();
                }
            }
        }
    }

    private static List<String> getFailureReasons(Exception e) {
        List<String> reasons = new ArrayList<>();
        Throwable failure = e;
        while (failure != null) {
            reasons.add(failure.getMessage());
            failure = failure.getCause();
        }
        return reasons;
    }

    /**
     * Receives ingest job messages. This is so that the message can be deleted or
     * returned to the queue, depending on whether the ingest job succeeds or fails.
     */
    @FunctionalInterface
    public interface MessageReceiver {
        /**
         * Receives a message.
         *
         * @return a {@link MessageHandle}, or an empty optional if there are no messages
         */
        Optional<MessageHandle> receiveMessage();
    }

    /**
     * A message containing an ingest job. Used to control the message visibility of an SQS message.
     */
    public interface MessageHandle extends AutoCloseable {
        /**
         * Reads the ingest job from the message.
         *
         * @return an {@link IngestJob}
         */
        IngestJob getJob();

        /**
         * Called when a job completes successfully. This will delete the message from the queue, and may run further
         * reporting on the job.
         *
         * @param summary the records processed summary for the finished job
         */
        void completed(RecordsProcessedSummary summary);

        /**
         * Called when a job fails. This will return the message to the queue to be retried.
         */
        void failed();

        /**
         * Called when this message handle is closed. This will stop the ingest task from updating SQS to keep the
         * message assigned to the task. This is called whether the job succeeds or fails.
         */
        void close();
    }
}
