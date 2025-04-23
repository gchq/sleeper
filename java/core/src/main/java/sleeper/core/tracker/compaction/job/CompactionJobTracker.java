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
 * Tracks and reports on the status of compaction jobs. This stores a history of events for each job. This is used for
 * reporting and to check the status of a job.
 */
public interface CompactionJobTracker {
    CompactionJobTracker NONE = new CompactionJobTracker() {
    };

    /**
     * Stores an event when a compaction job has been created, and is due to run.
     *
     * @param event the event
     */
    default void jobCreated(CompactionJobCreatedEvent event) {
    }

    /**
     * Stores an event when a compaction job has been picked up by a task and started.
     *
     * @param event the event
     */
    default void jobStarted(CompactionJobStartedEvent event) {
    }

    /**
     * Stores an event when a compaction job has been run to completion. The output file will have been written, but the
     * job may not yet have been committed to the state store.
     *
     * @param event the event
     */
    default void jobFinished(CompactionJobFinishedEvent event) {
    }

    /**
     * Stores an event when a compaction job has been committed to the state store. This means future queries against
     * the Sleeper table can read the file that was written as the output of the compaction, and will soon stop reading
     * the input files.
     *
     * @param event the event
     */
    default void jobCommitted(CompactionJobCommittedEvent event) {
    }

    /**
     * Stores an event when a compaction job has failed. It may or not be retried depending on the failure.
     *
     * @param event the event
     */
    default void jobFailed(CompactionJobFailedEvent event) {
    }

    /**
     * Retrieves the currently tracked status of a job. This will be derived from all events that have been tracked for
     * that job.
     *
     * @param  jobId                         the job ID
     * @return                               the status of the job, if it has been tracked
     * @throws UnsupportedOperationException if the compaction job tracker is disabled for this Sleeper instance
     */
    default Optional<CompactionJobStatus> getJob(String jobId) {
        throw new UnsupportedOperationException("Instance has no compaction job tracker");
    }

    /**
     * Retrieves the status of all currently tracked compaction jobs for a Sleeper table. This includes all jobs that
     * have tracked events, unless those events have expired and are no longer held in the tracker.
     *
     * @param  tableId                       the internal Sleeper table ID
     * @return                               a stream of job statuses
     * @throws UnsupportedOperationException if the compaction job tracker is disabled for this Sleeper instance
     */
    default Stream<CompactionJobStatus> streamAllJobs(String tableId) {
        throw new UnsupportedOperationException("Instance has no compaction job tracker");
    }

    /**
     * Retrieves the status of all currently tracked compaction jobs for a Sleeper table. This includes all jobs that
     * have tracked events, unless those events have expired and are no longer held in the tracker.
     *
     * @param  tableId                       the internal Sleeper table ID
     * @return                               the job statuses
     * @throws UnsupportedOperationException if the compaction job tracker is disabled for this Sleeper instance
     */
    default List<CompactionJobStatus> getAllJobs(String tableId) {
        return streamAllJobs(tableId).toList();
    }

    /**
     * Retrieves the status of compaction jobs that are tracked but not finished, for a Sleeper table.
     *
     * @param  tableId                       the internal Sleeper table ID
     * @return                               the unfinished job statuses
     * @throws UnsupportedOperationException if the compaction job tracker is disabled for this Sleeper instance
     */
    default List<CompactionJobStatus> getUnfinishedJobs(String tableId) {
        return streamAllJobs(tableId)
                .filter(CompactionJobStatus::isUnstartedOrInProgress)
                .toList();
    }

    /**
     * Retrieves the status of compaction jobs that have been received by a given compaction task, for a Sleeper table.
     *
     * @param  tableId                       the internal Sleeper table ID
     * @param  taskId                        the compaction task ID
     * @return                               the job statuses
     * @throws UnsupportedOperationException if the compaction job tracker is disabled for this Sleeper instance
     */
    default List<CompactionJobStatus> getJobsByTaskId(String tableId, String taskId) {
        return streamAllJobs(tableId)
                .filter(job -> job.isTaskIdAssigned(taskId))
                .collect(Collectors.toList());
    }

    /**
     * Retrieves the status of compaction jobs that have events in a given time period, for a Sleeper table.
     *
     * @param  tableId                       the internal Sleeper table ID
     * @param  startTime                     the start of the period
     * @param  endTime                       the end of the period
     * @return                               the job statuses
     * @throws UnsupportedOperationException if the compaction job tracker is disabled for this Sleeper instance
     */
    default List<CompactionJobStatus> getJobsInTimePeriod(String tableId, Instant startTime, Instant endTime) {
        return streamAllJobs(tableId)
                .filter(job -> job.isInPeriod(startTime, endTime))
                .collect(Collectors.toList());
    }
}
