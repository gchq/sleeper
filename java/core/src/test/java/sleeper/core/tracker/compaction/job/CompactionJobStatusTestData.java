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
import sleeper.core.tracker.job.run.RecordsProcessed;
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
 * This class provides and number of methods for generating test data CompactionJobStatuses.
 */
public class CompactionJobStatusTestData {
    private CompactionJobStatusTestData() {
    }

    /**
     * This method builds a CompactionJobStatus in the created status with a default CompactionJobCreatedEvent.
     *
     * @param  createdTime     Instant time the job should be set as created at.
     * @param  runsLatestFirst Nullable list of JobRuns to be used in the created job
     * @return                 CompactionJobStatus built with the given values.
     */
    public static CompactionJobStatus compactionJobCreated(Instant createdTime, JobRun... runsLatestFirst) {
        return compactionJobCreated(defaultCompactionJobCreatedEvent(), createdTime, runsLatestFirst);
    }

    /**
     * This method builds a CompactionJobStatus in the created status.
     *
     * @param  event           CompactionJobCreatedEvent used to give the job status a job id.
     * @param  createdTime     Instant time the job should be set as created at.
     * @param  runsLatestFirst Nullable list of JobRuns to be used in the created job
     * @return                 CompactionJobStatus built with the given values.
     */
    public static CompactionJobStatus compactionJobCreated(CompactionJobCreatedEvent event, Instant createdTime, JobRun... runsLatestFirst) {
        return CompactionJobStatus.builder()
                .jobId(event.getJobId())
                .createdStatus(CompactionJobCreatedStatus.from(event, createdTime))
                .jobRuns(JobRuns.latestFirst(List.of(runsLatestFirst)))
                .build();
    }

    /**
     * This method creates a number of job statuses and then a started compaction jobRun from those statuses.
     *
     * @param  taskId    String id of the task to be used in the JobRun
     * @param  startTime Instant start time of the job run.
     * @return           JobRun built with the given values.
     */
    public static JobRun startedCompactionRun(String taskId, Instant startTime) {
        return jobRunOnTask(taskId, compactionStartedStatus(startTime));
    }

    /**
     * This method creates a number of job statuses and then a uncommitted compaction jobRun from those statuses.
     *
     * @param  taskId  String id of the task to be used in the JobRun.
     * @param  summary JobRunSummary summary to provide the start time and summary of the finished job.
     * @return         JobRun built with the given values.
     */
    public static JobRun uncommittedCompactionRun(String taskId, JobRunSummary summary) {
        return jobRunOnTask(taskId,
                compactionStartedStatus(summary.getStartTime()),
                compactionFinishedStatus(summary));
    }

    /**
     * This method creates a number of job statuses and then a finished compaction jobRun from those statuses.
     *
     * @param  taskId     String id of the task to be used in the JobRun.
     * @param  summary    JobRunSummary summary to provide the start time and summary of the finished job.
     * @param  commitTime Instant time the job was committed at.
     * @return            JobRun built with the given values.
     */
    public static JobRun finishedCompactionRun(String taskId, JobRunSummary summary, Instant commitTime) {
        return jobRunOnTask(taskId,
                compactionStartedStatus(summary.getStartTime()),
                compactionFinishedStatus(summary),
                compactionCommittedStatus(commitTime));
    }

    /**
     * This method creates a number of job statuses and then a failed compaction jobRun from those statuses.
     *
     * @param  taskId         String id of the task to be used in the JobRun.
     * @param  runTime        Instant time the task took to run.
     * @param  failureReasons List of Strings that detail the reasons for the failure.
     * @return                JobRun built with the given values.
     */
    public static JobRun failedCompactionRun(String taskId, JobRunTime runTime, List<String> failureReasons) {
        return jobRunOnTask(taskId,
                compactionStartedStatus(runTime.getStartTime()),
                compactionFailedStatus(runTime.getFinishTime(), failureReasons));
    }

    /**
     * This method creates a number of job statuses and then a failed compaction jobRun from those statuses.
     *
     * @param  taskId         String id of the task to be used in the JobRun.
     * @param  startTime      Instant time the job started at.
     * @param  finishTime     Instant time the job finished at.
     * @param  failureTime    Instant ime the job was marked as a failure at.
     * @param  failureReasons List of Strings that detail the reasons for the failure.
     * @return                JobRun built with the given values.
     */
    public static JobRun failedCompactionRun(String taskId, Instant startTime, Instant finishTime, Instant failureTime, List<String> failureReasons) {
        return jobRunOnTask(taskId,
                compactionStartedStatus(startTime),
                compactionFinishedStatus(summary(startTime, finishTime, 10L, 10L)),
                compactionFailedStatus(failureTime, failureReasons));
    }

    /**
     * This method builds a CompactionJobCreatedStatus based of given createdTime, partitionId and inputFilesCount.
     *
     * @param  createdTime     Instant time the job was created at.
     * @param  partitionId     String id of the partition.
     * @param  inputFilesCount int the number of files in the input.
     * @return                 CompactionJobCreatedStatus built with the given values.
     */
    public static CompactionJobCreatedStatus compactionCreatedStatus(Instant createdTime, String partitionId, int inputFilesCount) {
        return CompactionJobCreatedStatus.builder()
                .updateTime(createdTime)
                .partitionId(partitionId)
                .inputFilesCount(inputFilesCount)
                .build();
    }

    /**
     * This method builds a CompactionJobStartedStatus based of a given startTime and default update time.
     *
     * @param  startTime Instant the start time of the job and input for creating the updated time.
     * @return           CompactionJobFinishedStatus built with the given values.
     */
    public static CompactionJobStartedStatus compactionStartedStatus(Instant startTime) {
        return CompactionJobStartedStatus.startAndUpdateTime(startTime, defaultUpdateTime(startTime));
    }

    /**
     * This method builds a CompactionJobFinishedStatus based of a givem summary.
     *
     * @param  summary JobRunSummary summary to provide the finish time and summary of the finished job.
     * @return         CompactionJobFinishedStatus built with the given values.
     */
    public static CompactionJobFinishedStatus compactionFinishedStatus(JobRunSummary summary) {
        return compactionFinishedStatus(summary.getFinishTime(), summary.getRecordsProcessed());
    }

    /**
     * This method builds a CompactionJobFinishedStatus based of a given finishTime and recordsProcessed.
     *
     * @param  finishTime       Instant time the job finished at.
     * @param  recordsProcessed RecordsProcessed to be detailed in the Finished Status.
     * @return                  CompactionJobFinishedStatus built with the given values.
     */
    public static CompactionJobFinishedStatus compactionFinishedStatus(Instant finishTime, RecordsProcessed recordsProcessed) {
        return CompactionJobFinishedStatus.builder()
                .updateTime(defaultUpdateTime(finishTime))
                .finishTime(finishTime)
                .recordsProcessed(recordsProcessed)
                .build();
    }

    /**
     * This method created a new CompactionJobCommittedStatus with the given committed time and a default update time.
     *
     * @param  committedTime Instant time the job was committed at.
     * @return               CompactionJobCommittedStatus built with the given values.
     */
    public static CompactionJobCommittedStatus compactionCommittedStatus(Instant committedTime) {
        return CompactionJobCommittedStatus.commitAndUpdateTime(committedTime, defaultUpdateTime(committedTime));
    }

    /**
     * This method builds a JobRunFailedStatus from the given failure time and failure reasons.
     *
     * @param  failureTime    Instant time the job failed at.
     * @param  failureReasons List of Strings that detail the reasons for the failure.
     * @return                JobRunFailedStatus built with the given values.
     */
    public static JobRunFailedStatus compactionFailedStatus(Instant failureTime, List<String> failureReasons) {
        return JobRunFailedStatus.builder()
                .updateTime(defaultUpdateTime(failureTime))
                .failureTime(failureTime)
                .failureReasons(failureReasons)
                .build();
    }

    /**
     * This method returns a CompactionJobStatus for a single run update.
     *
     * @param  updates Nullable array of JobStatusUpdates to be used to generate the status.
     * @return         CompactionJobStatus built with the given values.
     */
    public static CompactionJobStatus jobStatusFromSingleRunUpdates(JobStatusUpdate... updates) {
        return jobStatusFrom(records().singleRunUpdates(updates));
    }

    /**
     * This method returns a list of CompactionJobStatuses from provided updates.
     *
     * @param  updates Nullable array of TestJobStatusUpdateRecords TaskUpdates to be used to generate the status.
     * @return         List of CompactionJobStatus built with the given values.
     */
    public static List<CompactionJobStatus> jobStatusListFromUpdates(
            TestJobStatusUpdateRecords.TaskUpdates... updates) {
        return CompactionJobStatus.listFrom(records().fromUpdates(updates).stream());
    }

    /**
     * This method returns a CompactionJobStaus for a provided record.
     * If more than one CompactionJobStatus is found it will throw an IllegalStateException.
     *
     * @param  records               TestJobStatusUpdateRecords to find the CompactionJobStatusFrom
     * @return                       CompactionJobStatus if only one found.
     * @throws IllegalStateException if more that one status found for record.
     */
    public static CompactionJobStatus jobStatusFrom(TestJobStatusUpdateRecords records) {
        List<CompactionJobStatus> built = CompactionJobStatus.listFrom(records.stream());
        if (built.size() != 1) {
            throw new IllegalStateException("Expected single status");
        }
        return built.get(0);
    }
}
