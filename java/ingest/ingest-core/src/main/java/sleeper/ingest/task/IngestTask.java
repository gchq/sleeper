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

import sleeper.core.record.process.RecordsProcessedSummary;
import sleeper.core.util.LoggedDuration;
import sleeper.ingest.IngestResult;
import sleeper.ingest.job.IngestJob;
import sleeper.ingest.job.IngestJobHandler;
import sleeper.ingest.job.status.IngestJobStatusStore;

import java.time.Instant;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.function.Supplier;

import static sleeper.ingest.job.status.IngestJobFinishedEvent.ingestJobFinished;
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
    private final String taskId;
    private int totalNumberOfMessagesProcessed = 0;

    public IngestTask(Supplier<Instant> timeSupplier, MessageReceiver messageReceiver, IngestJobHandler ingester,
            IngestJobStatusStore jobStatusStore, IngestTaskStatusStore taskStore, String taskId) {
        this.timeSupplier = timeSupplier;
        this.messageReceiver = messageReceiver;
        this.ingester = ingester;
        this.jobStatusStore = jobStatusStore;
        this.taskStatusStore = taskStore;
        this.taskId = taskId;
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
        Instant finishTime = handleMessages(startTime, taskFinishedBuilder::addJobSummary);
        LOGGER.info("Total number of messages processed = {}", totalNumberOfMessagesProcessed);
        LOGGER.info("Total run time = {}", LoggedDuration.withFullOutput(startTime, finishTime));

        IngestTaskStatus taskFinished = taskStatusBuilder.finished(finishTime, taskFinishedBuilder).build();
        taskStatusStore.taskFinished(taskFinished);
    }

    /**
     * Receives a message, then deserialises and runs the ingest job. Updates the ingest job status store with progress
     * on the job.
     *
     * @param  startTime       the start time
     * @param  summaryConsumer a consumer for finished job summaries
     * @return                 the finish time
     */
    public Instant handleMessages(Instant startTime, Consumer<RecordsProcessedSummary> summaryConsumer) {
        while (true) {
            Optional<MessageHandle> messageOpt = messageReceiver.receiveMessage();
            if (!messageOpt.isPresent()) {
                LOGGER.info("Terminating ingest task as no messages were received");
                return timeSupplier.get();
            }
            try (MessageHandle message = messageOpt.get()) {
                IngestJob job = message.getJob();
                LOGGER.info("IngestJob is: {}", job);
                try {
                    Instant jobStartTime = timeSupplier.get();
                    jobStatusStore.jobStarted(ingestJobStarted(taskId, job, jobStartTime));
                    IngestResult result = ingester.ingest(job);
                    LOGGER.info("{} records were written", result.getRecordsWritten());
                    Instant jobFinishTime = timeSupplier.get();
                    RecordsProcessedSummary summary = new RecordsProcessedSummary(result.asRecordsProcessed(), jobStartTime, jobFinishTime);
                    jobStatusStore.jobFinished(ingestJobFinished(taskId, job, summary));
                    summaryConsumer.accept(summary);
                    message.completed(summary);
                    totalNumberOfMessagesProcessed++;
                } catch (Exception e) {
                    LOGGER.error("Failed processing ingest job, terminating task", e);
                    message.failed();
                    return timeSupplier.get();
                }
            }
        }
    }

    /**
     * An interface for receiving message handles.
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
     * An interface for a message. Used to control the message visibility of an SQS message.
     */
    public interface MessageHandle extends AutoCloseable {
        /**
         * Gets the ingest job this message handle is linked to.
         *
         * @return an {@link IngestJob}
         */
        IngestJob getJob();

        /**
         * Called when a job completes successfully.
         *
         * @param summary the records processed summary for the finished job
         */
        void completed(RecordsProcessedSummary summary);

        /**
         * Called when a job fails.
         */
        void failed();

        /**
         * Called when this message handle is closed.
         */
        void close();
    }
}
