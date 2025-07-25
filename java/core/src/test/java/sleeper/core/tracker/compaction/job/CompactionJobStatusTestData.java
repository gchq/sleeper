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

import sleeper.core.tracker.compaction.job.query.CompactionJobCommittedStatus;
import sleeper.core.tracker.compaction.job.query.CompactionJobCreatedStatus;
import sleeper.core.tracker.compaction.job.query.CompactionJobFinishedStatus;
import sleeper.core.tracker.compaction.job.query.CompactionJobStartedStatus;
import sleeper.core.tracker.compaction.job.query.CompactionJobStatus;
import sleeper.core.tracker.compaction.job.update.CompactionJobCreatedEvent;
import sleeper.core.tracker.job.run.JobRun;
import sleeper.core.tracker.job.run.JobRunSummary;
import sleeper.core.tracker.job.run.JobRunTime;
import sleeper.core.tracker.job.run.JobRuns;
import sleeper.core.tracker.job.run.RowsProcessed;
import sleeper.core.tracker.job.status.JobRunFailedStatus;
import sleeper.core.tracker.job.status.JobStatusUpdate;
import sleeper.core.tracker.job.status.TestJobStatusUpdateRecords;

import java.time.Instant;
import java.util.List;

import static sleeper.core.tracker.compaction.job.CompactionJobEventTestData.defaultCompactionJobCreatedEvent;
import static sleeper.core.tracker.job.run.JobRunSummaryTestHelper.summary;
import static sleeper.core.tracker.job.run.JobRunTestData.jobRunOnTask;
import static sleeper.core.tracker.job.status.JobStatusUpdateTestHelper.defaultUpdateTime;
import static sleeper.core.tracker.job.status.TestJobStatusUpdateRecords.records;

/**
 * Generates test data for compaction job statuses.
 */
public class CompactionJobStatusTestData {
    private CompactionJobStatusTestData() {
    }

    /**
     * Creates a status object for a compaction job that has been created but not started.
     *
     * @param  createdTime     the time the job should be set as created at
     * @param  runsLatestFirst the runs of the job, ordered from latest to earliest
     * @return                 the status
     */
    public static CompactionJobStatus compactionJobCreated(Instant createdTime, JobRun... runsLatestFirst) {
        return compactionJobCreated(defaultCompactionJobCreatedEvent(), createdTime, runsLatestFirst);
    }

    /**
     * Creates a status object for a compaction job that has been created but not started.
     *
     * @param  event           the created event to derive the status from
     * @param  createdTime     the time the job should be set as created at
     * @param  runsLatestFirst the runs of the job, ordered from latest to earliest
     * @return                 the status
     */
    public static CompactionJobStatus compactionJobCreated(CompactionJobCreatedEvent event, Instant createdTime, JobRun... runsLatestFirst) {
        return CompactionJobStatus.builder()
                .jobId(event.getJobId())
                .createdStatus(CompactionJobCreatedStatus.from(event, createdTime))
                .jobRuns(JobRuns.latestFirst(List.of(runsLatestFirst)))
                .build();
    }

    /**
     * Creates an object describing a run of a compaction job that has started.
     *
     * @param  taskId    the ID of the task that started the job
     * @param  startTime the time the task started the run
     * @return           the job run
     */
    public static JobRun startedCompactionRun(String taskId, Instant startTime) {
        return jobRunOnTask(taskId, compactionStartedStatus(startTime));
    }

    /**
     * Creates an object describing a run of a compaction job that has finished but has not been committed to the state
     * store.
     *
     * @param  taskId  the ID of the task that ran the job
     * @param  summary a summary of what happened during the run
     * @return         the job run
     */
    public static JobRun uncommittedCompactionRun(String taskId, JobRunSummary summary) {
        return jobRunOnTask(taskId,
                compactionStartedStatus(summary.getStartTime()),
                compactionFinishedStatus(summary));
    }

    /**
     * Creates an object describing a run of a compaction job that has finished and been committed to the state store.
     *
     * @param  taskId     the ID of the task that ran the job
     * @param  summary    a summary of what happened during the run
     * @param  commitTime the time the job was committed to the state store
     * @return            the job run
     */
    public static JobRun finishedCompactionRun(String taskId, JobRunSummary summary, Instant commitTime) {
        return jobRunOnTask(taskId,
                compactionStartedStatus(summary.getStartTime()),
                compactionFinishedStatus(summary),
                compactionCommittedStatus(commitTime));
    }

    /**
     * Creates an object describing a run of a compaction job that has failed.
     *
     * @param  taskId         the ID of the task that ran the job
     * @param  runTime        a summary of the time taken during the run
     * @param  failureReasons the reasons the run failed
     * @return                the job run
     */
    public static JobRun failedCompactionRun(String taskId, JobRunTime runTime, List<String> failureReasons) {
        return jobRunOnTask(taskId,
                compactionStartedStatus(runTime.getStartTime()),
                compactionFailedStatus(runTime.getFinishTime(), failureReasons));
    }

    /**
     * Creates an object describing a run of a compaction job that has finished, but failed to commit to the state
     * store.
     *
     * @param  taskId         the ID of the task that ran the job
     * @param  startTime      the time the run started
     * @param  finishTime     the time the run finished
     * @param  failureTime    the time the job failed to commit
     * @param  failureReasons the reasons the run failed
     * @return                the job run
     */
    public static JobRun failedCompactionRun(String taskId, Instant startTime, Instant finishTime, Instant failureTime, List<String> failureReasons) {
        return jobRunOnTask(taskId,
                compactionStartedStatus(startTime),
                compactionFinishedStatus(summary(startTime, finishTime, 10L, 10L)),
                compactionFailedStatus(failureTime, failureReasons));
    }

    /**
     * Creates a status update for when a compaction job was created.
     *
     * @param  createdTime     the time the job was created
     * @param  partitionId     the ID of the partition the job was created in
     * @param  inputFilesCount the number of files the job was planned to process
     * @return                 the status update
     */
    public static CompactionJobCreatedStatus compactionCreatedStatus(Instant createdTime, String partitionId, int inputFilesCount) {
        return CompactionJobCreatedStatus.builder()
                .updateTime(createdTime)
                .partitionId(partitionId)
                .inputFilesCount(inputFilesCount)
                .build();
    }

    /**
     * Creates a status update for when a compaction job was started in a task.
     *
     * @param  startTime the time the job started
     * @return           the status update
     */
    public static CompactionJobStartedStatus compactionStartedStatus(Instant startTime) {
        return CompactionJobStartedStatus.startAndUpdateTime(startTime, defaultUpdateTime(startTime));
    }

    /**
     * Creates a status update for when a compaction job was finished in a task.
     *
     * @param  summary a summary of what happened during the run
     * @return         the status update
     */
    public static CompactionJobFinishedStatus compactionFinishedStatus(JobRunSummary summary) {
        return compactionFinishedStatus(summary.getFinishTime(), summary.getRowsProcessed());
    }

    /**
     * Creates a status update for when a compaction job was finished in a task.
     *
     * @param  finishTime    the time the job was finished
     * @param  rowsProcessed a description of the number of rows processed during the run
     * @return               the status update
     */
    public static CompactionJobFinishedStatus compactionFinishedStatus(Instant finishTime, RowsProcessed rowsProcessed) {
        return CompactionJobFinishedStatus.builder()
                .updateTime(defaultUpdateTime(finishTime))
                .finishTime(finishTime)
                .rowsProcessed(rowsProcessed)
                .build();
    }

    /**
     * Creates a status update for when a finished compaction job was committed to the state store.
     *
     * @param  committedTime the time the state store was updated
     * @return               the status update
     */
    public static CompactionJobCommittedStatus compactionCommittedStatus(Instant committedTime) {
        return CompactionJobCommittedStatus.commitAndUpdateTime(committedTime, defaultUpdateTime(committedTime));
    }

    /**
     * Creates a status update for when a run of a compaction job failed.
     *
     * @param  failureTime    the time of the failure
     * @param  failureReasons the reasons the job failed
     * @return                the status update
     */
    public static JobRunFailedStatus compactionFailedStatus(Instant failureTime, List<String> failureReasons) {
        return JobRunFailedStatus.builder()
                .updateTime(defaultUpdateTime(failureTime))
                .failureTime(failureTime)
                .failureReasons(failureReasons)
                .build();
    }

    /**
     * Creates a status object for a compaction job with a single run, based on its status updates.
     *
     * @param  updates the status updates
     * @return         the status
     */
    public static CompactionJobStatus jobStatusFromSingleRunUpdates(JobStatusUpdate... updates) {
        return jobStatusFrom(records().singleRunUpdates(updates));
    }

    /**
     * Creates status objects for compaction jobs, based on status updates.
     *
     * @param  updates the status updates
     * @return         the status objects
     */
    public static List<CompactionJobStatus> jobStatusListFromUpdates(
            TestJobStatusUpdateRecords.TaskUpdates... updates) {
        return CompactionJobStatus.listFrom(records().fromUpdates(updates).stream());
    }

    /**
     * Creates a status object for a compaction job based on its status updates.
     *
     * @param  records               the status updates
     * @return                       the status
     * @throws IllegalStateException if more than one compaction job is found
     */
    public static CompactionJobStatus jobStatusFrom(TestJobStatusUpdateRecords records) {
        List<CompactionJobStatus> built = CompactionJobStatus.listFrom(records.stream());
        if (built.size() != 1) {
            throw new IllegalStateException("Expected one compaction job");
        }
        return built.get(0);
    }
}
