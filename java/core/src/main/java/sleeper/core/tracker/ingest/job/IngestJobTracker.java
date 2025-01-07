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

package sleeper.core.tracker.ingest.job;

import sleeper.core.tracker.ingest.job.update.IngestJobAddedFilesEvent;
import sleeper.core.tracker.ingest.job.update.IngestJobFailedEvent;
import sleeper.core.tracker.ingest.job.update.IngestJobFinishedEvent;
import sleeper.core.tracker.ingest.job.update.IngestJobStartedEvent;
import sleeper.core.tracker.ingest.job.update.IngestJobValidatedEvent;

import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Stores the status of ingest jobs.
 */
public interface IngestJobTracker {
    IngestJobTracker NONE = new IngestJobTracker() {
    };

    /**
     * Stores an ingest job validated event.
     *
     * @param event the event
     */
    default void jobValidated(IngestJobValidatedEvent event) {
    }

    /**
     * Stores an ingest job started event.
     *
     * @param event the event
     */
    default void jobStarted(IngestJobStartedEvent event) {
    }

    /**
     * Stores an ingest job added files event.
     *
     * @param event the event
     */
    default void jobAddedFiles(IngestJobAddedFilesEvent event) {
    }

    /**
     * Stores an ingest job finished event.
     *
     * @param event the event
     */
    default void jobFinished(IngestJobFinishedEvent event) {
    }

    /**
     * Stores an ingest job failed event.
     *
     * @param event the event
     */
    default void jobFailed(IngestJobFailedEvent event) {
    }

    /**
     * Streams all ingest job statuses that belong to a table.
     *
     * @param  tableId the table ID
     * @return         a stream of ingest job statuses
     */
    default Stream<IngestJobStatus> streamAllJobs(String tableId) {
        throw new UnsupportedOperationException("Instance has no ingest job status store");
    }

    /**
     * Gets a list of all ingest job statuses that belong to a table.
     *
     * @param  tableId the table ID
     * @return         a list of ingest job statuses
     */
    default List<IngestJobStatus> getAllJobs(String tableId) {
        return streamAllJobs(tableId).collect(Collectors.toList());
    }

    /**
     * Gets a list of all ingest job statuses that have not finished.
     *
     * @param  tableId the table ID
     * @return         a list of ingest job statuses that have not finished
     */
    default List<IngestJobStatus> getUnfinishedJobs(String tableId) {
        return streamAllJobs(tableId)
                .filter(IngestJobStatus::isUnfinishedOrAnyRunInProgress)
                .collect(Collectors.toList());
    }

    /**
     * Gets a list of all ingest job statuses that are assigned to a specific task.
     *
     * @param  tableId the table ID
     * @param  taskId  the task ID
     * @return         a list of all ingest job statuses assigned to the task
     */
    default List<IngestJobStatus> getJobsByTaskId(String tableId, String taskId) {
        return streamAllJobs(tableId)
                .filter(job -> job.isTaskIdAssigned(taskId))
                .collect(Collectors.toList());
    }

    /**
     * Gets a list of ingest job statuses that have occurred within a time window.
     *
     * @param  tableId   the table ID
     * @param  startTime the time window start
     * @param  endTime   the time window end
     * @return           a list of ingest job statuses that have occurred within the time window
     */
    default List<IngestJobStatus> getJobsInTimePeriod(String tableId, Instant startTime, Instant endTime) {
        return streamAllJobs(tableId)
                .filter(job -> job.isInPeriod(startTime, endTime))
                .collect(Collectors.toList());
    }

    /**
     * Gets an ingest job status by the ingest job ID.
     *
     * @param  jobId the ingest job ID
     * @return       an optional containing the ingest job status, or an empty optional if the ingest job status does
     *               not exist
     */
    default Optional<IngestJobStatus> getJob(String jobId) {
        throw new UnsupportedOperationException("Instance has no ingest job status store");
    }

    /**
     * Gets a list of all ingest job statuses that have failed validation.
     *
     * @return a list of all ingest job statuses that have failed validation
     */
    default List<IngestJobStatus> getInvalidJobs() {
        throw new UnsupportedOperationException("Instance has no ingest job status store");
    }
}
