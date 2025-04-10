/*
 * Copyright 2022-2025 Crown Copyright
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
package sleeper.core.tracker.compaction.job;

import sleeper.core.tracker.compaction.job.query.CompactionJobStatus;
import sleeper.core.tracker.compaction.job.update.CompactionJobCommittedEvent;
import sleeper.core.tracker.compaction.job.update.CompactionJobCreatedEvent;
import sleeper.core.tracker.compaction.job.update.CompactionJobFailedEvent;
import sleeper.core.tracker.compaction.job.update.CompactionJobFinishedEvent;
import sleeper.core.tracker.compaction.job.update.CompactionJobStartedEvent;

import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * This interface is used to track and update the status of jobs.
 */
public interface CompactionJobTracker {
    CompactionJobTracker NONE = new CompactionJobTracker() {
    };

    /**
     * This method marks a job as created from a CompactionJobCreatedEvent.
     *
     * @param event CompactionJobCreatedEvent.
     */
    default void jobCreated(CompactionJobCreatedEvent event) {
    }

    /**
     * This method marks a job as started from a CompactionJobStartedEvent.
     *
     * @param event CompactionJobStartedEvent.
     */
    default void jobStarted(CompactionJobStartedEvent event) {
    }

    /**
     * This method marks a job as finished from a CompactionJobFinishedEvent.
     *
     * @param event CompactionJobFinishedEvent.
     */
    default void jobFinished(CompactionJobFinishedEvent event) {
    }

    /**
     * This method marks a job as committed from a CompactionJobCommittedEvent.
     *
     * @param event CompactionJobCommittedEvent.
     */
    default void jobCommitted(CompactionJobCommittedEvent event) {
    }

    /**
     * This method marks a job as failed from a CompactionJobFailedEvent.
     *
     * @param event CompactionJobFailedEvent.
     */
    default void jobFailed(CompactionJobFailedEvent event) {
    }

    /**
     * This method takes in a jobId and returns the optional current status of it.
     *
     * @param  jobId The jobId to get the status of.
     * @return       Optional CompactionJobStatus.
     */
    default Optional<CompactionJobStatus> getJob(String jobId) {
        throw new UnsupportedOperationException("Instance has no compaction job tracker");
    }

    /**
     * This method returns a stream of CompactionJobStatuses for a given tableId.
     *
     * @param  tableId String.
     * @return         Stream of CompactionJobStatuses.
     */
    default Stream<CompactionJobStatus> streamAllJobs(String tableId) {
        throw new UnsupportedOperationException("Instance has no compaction job tracker");
    }

    /**
     * This method returns a list of CompactionJobStatuses for a given tableId.
     *
     * @param  tableId String.
     * @return         List of CompactionJobStatuses.
     */
    default List<CompactionJobStatus> getAllJobs(String tableId) {
        return streamAllJobs(tableId).collect(Collectors.toList());
    }

    /**
     * This method returns a list of CompactionJobStatuses for a given tableId that haven't finished.
     *
     * @param  tableId String.
     * @return         List of CompactionJobStatuses.
     */
    default List<CompactionJobStatus> getUnfinishedJobs(String tableId) {
        return streamAllJobs(tableId)
                .filter(CompactionJobStatus::isUnstartedOrInProgress)
                .collect(Collectors.toList());
    }

    /**
     * This method returns a list of CompactionJobStatus for a given tableId and taskId.
     *
     * @param  tableId String
     * @param  taskId  String
     * @return         List of CompactionJobStatuses.
     */
    default List<CompactionJobStatus> getJobsByTaskId(String tableId, String taskId) {
        return streamAllJobs(tableId)
                .filter(job -> job.isTaskIdAssigned(taskId))
                .collect(Collectors.toList());
    }

    /**
     * This method returns a list of CompactionJobStatuses for a given tableId in a provided timeframe.
     *
     * @param  tableId   String.
     * @param  startTime Instant.
     * @param  endTime   Instant.
     * @return           List of CompactionJobStatuses.
     */
    default List<CompactionJobStatus> getJobsInTimePeriod(String tableId, Instant startTime, Instant endTime) {
        return streamAllJobs(tableId)
                .filter(job -> job.isInPeriod(startTime, endTime))
                .collect(Collectors.toList());
    }
}
