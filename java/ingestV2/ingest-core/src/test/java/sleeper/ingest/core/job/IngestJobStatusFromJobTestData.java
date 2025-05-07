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
package sleeper.ingest.core.job;

import sleeper.core.tracker.ingest.job.IngestJobStatusTestData;
import sleeper.core.tracker.ingest.job.query.IngestJobAcceptedStatus;
import sleeper.core.tracker.ingest.job.query.IngestJobFinishedStatus;
import sleeper.core.tracker.ingest.job.query.IngestJobRejectedStatus;
import sleeper.core.tracker.ingest.job.query.IngestJobStartedStatus;
import sleeper.core.tracker.ingest.job.query.IngestJobStatus;
import sleeper.core.tracker.job.run.JobRun;
import sleeper.core.tracker.job.run.JobRunSummary;
import sleeper.core.tracker.job.run.JobRunTime;
import sleeper.core.tracker.job.status.JobRunFailedStatus;

import java.time.Instant;
import java.util.List;

import static sleeper.core.tracker.ingest.job.IngestJobStatusTestData.ingestFinishedStatusUncommitted;
import static sleeper.core.tracker.job.run.JobRunTestData.jobRunOnTask;
import static sleeper.core.tracker.job.run.JobRunTestData.validationRun;
import static sleeper.core.tracker.job.status.JobStatusUpdateTestHelper.defaultUpdateTime;

/**
 * A helper for creating ingest job statuses for tests.
 */
public class IngestJobStatusFromJobTestData {

    private IngestJobStatusFromJobTestData() {
    }

    /**
     * Creates an ingest job status.
     *
     * @param  job  the ingest job
     * @param  runs the process runs
     * @return      an {@link IngestJobStatus}
     */
    public static IngestJobStatus ingestJobStatus(IngestJob job, JobRun... runs) {
        return IngestJobStatusTestData.ingestJobStatus(job.getId(), runs);
    }

    /**
     * Creates an ingest job status for a job that has started.
     *
     * @param  job       the ingest job
     * @param  taskId    the ingest task ID
     * @param  startTime the start time
     * @return           an {@link IngestJobStatus}
     */
    public static IngestJobStatus startedIngestJob(IngestJob job, String taskId, Instant startTime) {
        return IngestJobStatusTestData.ingestJobStatus(job.getId(), startedIngestRun(job, taskId, startTime));
    }

    /**
     * Creates an ingest job status for a job that has finished.
     *
     * @param  job                  the ingest job
     * @param  taskId               the ingest task ID
     * @param  summary              the records processed summary
     * @param  numFilesWrittenByJob the number of files written by the job
     * @return                      an {@link IngestJobStatus}
     */
    public static IngestJobStatus finishedIngestJob(IngestJob job, String taskId, JobRunSummary summary, int numFilesWrittenByJob) {
        return ingestJobStatus(job, finishedIngestRun(job, taskId, summary, numFilesWrittenByJob));
    }

    /**
     * Creates an ingest job status for a job that has finished, but has not yet been committed to the state store.
     *
     * @param  job     the ingest job
     * @param  taskId  the ingest task ID
     * @param  summary the records processed summary
     * @return         an {@link IngestJobStatus}
     */
    public static IngestJobStatus finishedIngestJobUncommitted(IngestJob job, String taskId, JobRunSummary summary) {
        return finishedIngestJobUncommitted(job, taskId, summary, 1);
    }

    /**
     * Creates an ingest job status for a job that has finished, but has not yet been committed to the state store.
     *
     * @param  job                  the ingest job
     * @param  taskId               the ingest task ID
     * @param  summary              the records processed summary
     * @param  numFilesWrittenByJob the number of files written by the job
     * @return                      an {@link IngestJobStatus}
     */
    public static IngestJobStatus finishedIngestJobUncommitted(IngestJob job, String taskId, JobRunSummary summary, int numFilesWrittenByJob) {
        return ingestJobStatus(job, finishedIngestRunUncommitted(job, taskId, summary, numFilesWrittenByJob));
    }

    /**
     * Creates an ingest job status for a job that has failed.
     *
     * @param  job            the ingest job
     * @param  taskId         the ingest task ID
     * @param  runTime        the process run time
     * @param  failureReasons a list of failure reasons
     * @return                an {@link IngestJobStatus}
     */
    public static IngestJobStatus failedIngestJob(IngestJob job, String taskId, JobRunTime runTime, List<String> failureReasons) {
        return ingestJobStatus(job, failedIngestRun(job, taskId, runTime, failureReasons));
    }

    /**
     * Creates a process run for an ingest job that was validated and started.
     *
     * @param  job            the ingest job
     * @param  taskId         the ingest task ID
     * @param  validationTime the validation time
     * @param  startTime      the start time
     * @return                a {@link JobRun}
     */
    public static JobRun acceptedRunWhichStarted(
            IngestJob job, String taskId, Instant validationTime, Instant startTime) {
        return jobRunOnTask(taskId,
                IngestJobAcceptedStatus.from(job.getFileCount(),
                        validationTime, defaultUpdateTime(validationTime)),
                IngestJobStartedStatus.builder()
                        .inputFileCount(job.getFileCount())
                        .startTime(startTime).updateTime(defaultUpdateTime(startTime)).build());
    }

    /**
     * Creates a process run for an ingest job that was validated and finished.
     *
     * @param  job            the ingest job
     * @param  taskId         the ingest task ID
     * @param  validationTime the validation time
     * @param  summary        the records processed summary
     * @return                a {@link JobRun}
     */
    public static JobRun acceptedRunWhichFinished(
            IngestJob job, String taskId, Instant validationTime, JobRunSummary summary, int numFilesWrittenByJob) {
        return jobRunOnTask(taskId,
                IngestJobAcceptedStatus.from(job.getFileCount(),
                        validationTime, defaultUpdateTime(validationTime)),
                IngestJobStartedStatus.builder()
                        .inputFileCount(job.getFileCount())
                        .startTime(summary.getStartTime())
                        .updateTime(defaultUpdateTime(summary.getStartTime())).build(),
                IngestJobFinishedStatus.builder()
                        .updateTime(defaultUpdateTime(summary.getFinishTime()))
                        .finishTime(summary.getFinishTime())
                        .recordsProcessed(summary.getRecordsProcessed())
                        .numFilesWrittenByJob(numFilesWrittenByJob).build());
    }

    /**
     * Creates a process run for an ingest job that was validated and failed.
     *
     * @param  job            the ingest job
     * @param  taskId         the ingest task ID
     * @param  validationTime the validation time
     * @param  runTime        the process run time
     * @param  failureReasons a list of failure reasons
     * @return                a {@link JobRun}
     */
    public static JobRun acceptedRunWhichFailed(
            IngestJob job, String taskId, Instant validationTime, JobRunTime runTime, List<String> failureReasons) {
        return jobRunOnTask(taskId,
                IngestJobAcceptedStatus.from(job.getFileCount(),
                        validationTime, defaultUpdateTime(validationTime)),
                IngestJobStartedStatus.builder()
                        .inputFileCount(job.getFileCount())
                        .startTime(runTime.getStartTime())
                        .updateTime(defaultUpdateTime(runTime.getStartTime())).build(),
                JobRunFailedStatus.builder()
                        .updateTime(defaultUpdateTime(runTime.getFinishTime()))
                        .failureTime(runTime.getFinishTime())
                        .failureReasons(failureReasons)
                        .build());
    }

    /**
     * Creates a process run for an ingest job that was validated.
     *
     * @param  job            the ingest job
     * @param  validationTime the validation time
     * @return                a {@link JobRun}
     */
    public static JobRun acceptedRun(IngestJob job, Instant validationTime) {
        return validationRun(
                IngestJobAcceptedStatus.from(job.getFileCount(), validationTime, defaultUpdateTime(validationTime)));
    }

    /**
     * Creates a process run for an ingest job that failed to validate.
     *
     * @param  job            the ingest job
     * @param  validationTime the validation time
     * @param  reasons        the reasons
     * @return                a {@link JobRun}
     */
    public static JobRun rejectedRun(IngestJob job, Instant validationTime, String... reasons) {
        return rejectedRun(job, null, validationTime, List.of(reasons));
    }

    /**
     * Creates a process run for an ingest job that failed to validate.
     *
     * @param  job            the ingest job
     * @param  validationTime the validation time
     * @param  reasons        the list of reasons
     * @return                a {@link JobRun}
     */
    public static JobRun rejectedRun(IngestJob job, Instant validationTime, List<String> reasons) {
        return rejectedRun(job, null, validationTime, reasons);
    }

    /**
     * Creates a process run for an ingest job that failed to validate.
     *
     * @param  job            the ingest job
     * @param  jsonMessage    the JSON string used in ingest job deserialisation
     * @param  validationTime the validation time
     * @param  reasons        the list of reasons
     * @return                a {@link JobRun}
     */
    public static JobRun rejectedRun(IngestJob job, String jsonMessage, Instant validationTime, List<String> reasons) {
        return validationRun(IngestJobRejectedStatus.builder()
                .validationTime(validationTime)
                .updateTime(defaultUpdateTime(validationTime))
                .reasons(reasons)
                .jsonMessage(jsonMessage)
                .inputFileCount(job.getFileCount())
                .build());
    }

    /**
     * Creates a process run for an ingest job that started.
     *
     * @param  job       the ingest job
     * @param  taskId    the ingest task ID
     * @param  startTime the start time
     * @return           a {@link JobRun}
     */
    public static JobRun startedIngestRun(IngestJob job, String taskId, Instant startTime) {
        return jobRunOnTask(taskId, ingestStartedStatus(job, startTime));
    }

    /**
     * Creates a process run for an ingest job that finished.
     *
     * @param  job     the ingest job
     * @param  taskId  the ingest task ID
     * @param  summary the records processed summary
     * @return         a {@link JobRun}
     */
    public static JobRun finishedIngestRun(
            IngestJob job, String taskId, JobRunSummary summary) {
        return finishedIngestRun(job, taskId, summary, 1);
    }

    /**
     * Creates a process run for an ingest job that finished.
     *
     * @param  job                  the ingest job
     * @param  taskId               the ingest task ID
     * @param  summary              the records processed summary
     * @param  numFilesWrittenByJob the number of files written by the job
     * @return                      a {@link JobRun}
     */
    public static JobRun finishedIngestRun(
            IngestJob job, String taskId, JobRunSummary summary, int numFilesWrittenByJob) {
        return jobRunOnTask(taskId,
                ingestStartedStatus(job, summary.getStartTime()),
                IngestJobFinishedStatus.builder()
                        .updateTime(defaultUpdateTime(summary.getFinishTime()))
                        .finishTime(summary.getFinishTime())
                        .recordsProcessed(summary.getRecordsProcessed())
                        .numFilesWrittenByJob(numFilesWrittenByJob)
                        .build());
    }

    /**
     * Creates a process run for an ingest job that finished, but has not yet been committed to the state store.
     *
     * @param  job                  the ingest job
     * @param  taskId               the ingest task ID
     * @param  summary              the records processed summary
     * @param  numFilesWrittenByJob the number of files written by the job
     * @return                      a {@link JobRun}
     */
    public static JobRun finishedIngestRunUncommitted(
            IngestJob job, String taskId, JobRunSummary summary, int numFilesWrittenByJob) {
        return jobRunOnTask(taskId,
                ingestStartedStatus(job, summary.getStartTime()),
                ingestFinishedStatusUncommitted(summary, numFilesWrittenByJob));
    }

    /**
     * Creates a process run for an ingest job that failed.
     *
     * @param  job            the ingest job
     * @param  taskId         the ingest task ID
     * @param  runTime        the process run time
     * @param  failureReasons a list of failure reasons
     * @return                a {@link JobRun}
     */
    public static JobRun failedIngestRun(
            IngestJob job, String taskId, JobRunTime runTime, List<String> failureReasons) {
        return jobRunOnTask(taskId,
                ingestStartedStatus(job, runTime.getStartTime()),
                JobRunFailedStatus.builder()
                        .updateTime(defaultUpdateTime(runTime.getFinishTime()))
                        .failureTime(runTime.getFinishTime())
                        .failureReasons(failureReasons)
                        .build());
    }

    /**
     * Creates a process run for an ingest job that passed validation then failed to start.
     *
     * @param  job            the ingest job
     * @param  validationTime the validation time
     * @param  failureTime    the failure time
     * @param  failureReasons a list of failure reasons
     * @return                a {@link JobRun}
     */
    public static JobRun acceptedAndFailedToStartIngestRun(
            IngestJob job, Instant validationTime, Instant failureTime, List<String> failureReasons) {
        return validationRun(
                IngestJobAcceptedStatus.from(job.getFileCount(), validationTime, defaultUpdateTime(validationTime)),
                JobRunFailedStatus.builder()
                        .updateTime(defaultUpdateTime(failureTime))
                        .failureTime(failureTime)
                        .failureReasons(failureReasons)
                        .build());
    }

    /**
     * Creates an ingest job accepted status update.
     *
     * @param  job            the ingest job
     * @param  validationTime the validation time
     * @return                an ingest job accepted status
     */
    public static IngestJobAcceptedStatus ingestAcceptedStatus(IngestJob job, Instant validationTime) {
        return IngestJobAcceptedStatus.from(job.getFileCount(), validationTime, defaultUpdateTime(validationTime));
    }

    /**
     * Creates an ingest job started status update.
     *
     * @param  job       the ingest job
     * @param  startTime the start time
     * @return           an ingest job started status
     */
    public static IngestJobStartedStatus ingestStartedStatus(IngestJob job, Instant startTime) {
        return IngestJobStatusTestData.ingestStartedStatus(job.getFileCount(), startTime, defaultUpdateTime(startTime));
    }

    /**
     * Creates an ingest job started status update.
     *
     * @param  job       the ingest job
     * @param  startTime the start time
     * @return           an ingest job started status
     */
    public static IngestJobStartedStatus validatedIngestStartedStatus(IngestJob job, Instant startTime) {
        return IngestJobStatusTestData.validatedIngestStartedStatus(startTime, job.getFileCount());
    }

}
