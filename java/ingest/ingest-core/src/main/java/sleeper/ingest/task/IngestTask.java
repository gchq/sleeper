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
import sleeper.ingest.job.status.IngestJobStatusStore;

import java.time.Instant;
import java.util.Optional;
import java.util.function.Supplier;

import static sleeper.ingest.job.status.IngestJobFailedEvent.ingestJobFailed;
import static sleeper.ingest.job.status.IngestJobFinishedEvent.ingestJobFinished;
import static sleeper.ingest.job.status.IngestJobStartedEvent.ingestJobStarted;

/**
 * Runs an ingest task. Executes jobs from a queue, updating the status stores with progress of the task.
 */
public class IngestTask {
    public static final Logger LOGGER = LoggerFactory.getLogger(IngestTask.class);
    private final Supplier<String> jobRunIdSupplier;
    private final Supplier<Instant> timeSupplier;
    private final MessageReceiver messageReceiver;
    private final IngestJobHandler ingester;
    private final IngestJobStatusStore jobStatusStore;
    private final IngestTaskStatusStore taskStatusStore;
    private final String taskId;
    private final IngestTaskStatus.Builder taskStatusBuilder;
    private final IngestTaskFinishedStatus.Builder taskFinishedBuilder = IngestTaskFinishedStatus.builder();
    private int totalNumberOfMessagesProcessed = 0;

    public IngestTask(Supplier<String> jobRunIdSupplier, Supplier<Instant> timeSupplier,
            MessageReceiver messageReceiver, IngestJobHandler ingester,
            IngestJobStatusStore jobStatusStore, IngestTaskStatusStore taskStore, String taskId) {
        this.jobRunIdSupplier = jobRunIdSupplier;
        this.timeSupplier = timeSupplier;
        this.messageReceiver = messageReceiver;
        this.ingester = ingester;
        this.jobStatusStore = jobStatusStore;
        this.taskStatusStore = taskStore;
        this.taskId = taskId;
        this.taskStatusBuilder = IngestTaskStatus.builder().taskId(taskId);
    }

    /**
     * Executes jobs from a queue, updating the status stores with progress of the task.
     */
    public void run() {
        start();
        while (true) {
            if (!handleOneMessage()) {
                break;
            }
        }
        finish();
    }

    /**
     * Records the start of the task.
     */
    public void start() {
        LOGGER.info("Starting task {}", taskId);
        taskStatusStore.taskStarted(taskStatusBuilder.startTime(timeSupplier.get()).build());
    }

    /**
     * Records the finish of the task.
     */
    public void finish() {
        IngestTaskStatus taskFinished = taskStatusBuilder.finished(timeSupplier.get(), taskFinishedBuilder).build();
        taskStatusStore.taskFinished(taskFinished);
        LOGGER.info("Total number of messages processed = {}", totalNumberOfMessagesProcessed);
        LOGGER.info("Total run time = {}", LoggedDuration.withFullOutput(taskFinished.getDuration()));
    }

    /**
     * Run an ingest job if there is one on the queue.
     *
     * @return true if a job was completed, false otherwise
     */
    public boolean handleOneMessage() {
        Optional<MessageHandle> messageOpt = messageReceiver.receiveMessage();
        if (!messageOpt.isPresent()) {
            LOGGER.info("Terminating ingest task as no messages were received");
            return false;
        }
        try (MessageHandle message = messageOpt.get()) {
            IngestJob job = message.getJob();
            LOGGER.info("IngestJob is: {}", job);
            String jobRunId = jobRunIdSupplier.get();
            Instant jobStartTime = timeSupplier.get();
            try {
                jobStatusStore.jobStarted(ingestJobStarted(job, jobStartTime)
                        .taskId(taskId).jobRunId(jobRunId).startOfRun(true).build());
                IngestResult result = ingester.ingest(job, jobRunId);
                LOGGER.info("{} records were written", result.getRecordsWritten());
                Instant jobFinishTime = timeSupplier.get();
                RecordsProcessedSummary summary = new RecordsProcessedSummary(result.asRecordsProcessed(), jobStartTime, jobFinishTime);
                jobStatusStore.jobFinished(ingestJobFinished(job, summary)
                        .taskId(taskId).jobRunId(jobRunId)
                        .committedBySeparateFileUpdates(true)
                        .fileReferencesAddedByJob(result.getFileReferenceList())
                        .build());
                taskFinishedBuilder.addJobSummary(summary);
                message.completed(summary);
                totalNumberOfMessagesProcessed++;
                return true;
            } catch (Exception e) {
                LOGGER.error("Failed processing ingest job, terminating task", e);
                Instant jobFinishTime = timeSupplier.get();
                jobStatusStore.jobFailed(ingestJobFailed(job, new ProcessRunTime(jobStartTime, jobFinishTime))
                        .taskId(taskId).jobRunId(jobRunId).failure(e)
                        .build());
                message.failed();
                return false;
            }
        }
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
