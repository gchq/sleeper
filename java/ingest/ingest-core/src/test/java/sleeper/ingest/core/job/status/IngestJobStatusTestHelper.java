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
package sleeper.ingest.core.job.status;

import sleeper.core.record.process.ProcessRunTime;
import sleeper.core.record.process.RecordsProcessedSummary;
import sleeper.core.record.process.status.ProcessFailedStatus;
import sleeper.core.record.process.status.ProcessRun;
import sleeper.core.record.process.status.ProcessRuns;
import sleeper.core.record.process.status.TestProcessStatusUpdateRecords;
import sleeper.ingest.core.job.IngestJob;

import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static sleeper.core.record.process.status.ProcessStatusUpdateTestHelper.defaultUpdateTime;

/**
 * A helper for creating ingest job statuses for tests.
 */
public class IngestJobStatusTestHelper {

    private IngestJobStatusTestHelper() {
    }

    /**
     * Creates an ingest job status.
     *
     * @param  jobId the ingest job ID
     * @param  runs  the process runs
     * @return       an {@link IngestJobStatus}
     */
    public static IngestJobStatus jobStatus(String jobId, ProcessRun... runs) {
        return jobStatus(IngestJob.builder().id(jobId).build(), runs);
    }

    /**
     * Creates an ingest job status.
     *
     * @param  job  the ingest job
     * @param  runs the process runs
     * @return      an {@link IngestJobStatus}
     */
    public static IngestJobStatus jobStatus(IngestJob job, ProcessRun... runs) {
        return IngestJobStatus.builder()
                .jobId(job.getId())
                .jobRuns(ProcessRuns.latestFirst(Arrays.asList(runs)))
                .build();
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
        return jobStatus(job, startedIngestRun(job, taskId, startTime));
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
    public static IngestJobStatus finishedIngestJob(IngestJob job, String taskId, RecordsProcessedSummary summary, int numFilesWrittenByJob) {
        return jobStatus(job, finishedIngestRun(job, taskId, summary, numFilesWrittenByJob));
    }

    /**
     * Creates an ingest job status for a job that has finished, but has not yet been committed to the state store.
     *
     * @param  job     the ingest job
     * @param  taskId  the ingest task ID
     * @param  summary the records processed summary
     * @return         an {@link IngestJobStatus}
     */
    public static IngestJobStatus finishedIngestJobUncommitted(IngestJob job, String taskId, RecordsProcessedSummary summary) {
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
    public static IngestJobStatus finishedIngestJobUncommitted(IngestJob job, String taskId, RecordsProcessedSummary summary, int numFilesWrittenByJob) {
        return jobStatus(job, finishedIngestRunUncommitted(job, taskId, summary, numFilesWrittenByJob));
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
    public static IngestJobStatus failedIngestJob(IngestJob job, String taskId, ProcessRunTime runTime, List<String> failureReasons) {
        return jobStatus(job, failedIngestRun(job, taskId, runTime, failureReasons));
    }

    /**
     * Creates a process run for an ingest job that was validated and started.
     *
     * @param  job            the ingest job
     * @param  taskId         the ingest task ID
     * @param  validationTime the validation time
     * @param  startTime      the start time
     * @return                a {@link ProcessRun}
     */
    public static ProcessRun acceptedRunWhichStarted(
            IngestJob job, String taskId, Instant validationTime, Instant startTime) {
        return ProcessRun.builder()
                .taskId(taskId)
                .startedStatus(IngestJobAcceptedStatus.from(job,
                        validationTime, defaultUpdateTime(validationTime)))
                .statusUpdate(
                        IngestJobStartedStatus.withStartOfRun(false)
                                .inputFileCount(job.getFiles().size())
                                .startTime(startTime).updateTime(defaultUpdateTime(startTime)).build())
                .build();
    }

    /**
     * Creates a process run for an ingest job that was validated and finished.
     *
     * @param  job            the ingest job
     * @param  taskId         the ingest task ID
     * @param  validationTime the validation time
     * @param  summary        the records processed summary
     * @return                a {@link ProcessRun}
     */
    public static ProcessRun acceptedRunWhichFinished(
            IngestJob job, String taskId, Instant validationTime, RecordsProcessedSummary summary, int numFilesWrittenByJob) {
        return ProcessRun.builder()
                .taskId(taskId)
                .startedStatus(IngestJobAcceptedStatus.from(job,
                        validationTime, defaultUpdateTime(validationTime)))
                .statusUpdate(
                        IngestJobStartedStatus.withStartOfRun(false)
                                .inputFileCount(job.getFiles().size())
                                .startTime(summary.getStartTime())
                                .updateTime(defaultUpdateTime(summary.getStartTime())).build())
                .finishedStatus(IngestJobFinishedStatus
                        .updateTimeAndSummary(defaultUpdateTime(summary.getFinishTime()), summary)
                        .numFilesWrittenByJob(numFilesWrittenByJob).build())
                .build();
    }

    /**
     * Creates a process run for an ingest job that was validated and failed.
     *
     * @param  job            the ingest job
     * @param  taskId         the ingest task ID
     * @param  validationTime the validation time
     * @param  runTime        the process run time
     * @param  failureReasons a list of failure reasons
     * @return                a {@link ProcessRun}
     */
    public static ProcessRun acceptedRunWhichFailed(
            IngestJob job, String taskId, Instant validationTime, ProcessRunTime runTime, List<String> failureReasons) {
        return ProcessRun.builder()
                .taskId(taskId)
                .startedStatus(IngestJobAcceptedStatus.from(job,
                        validationTime, defaultUpdateTime(validationTime)))
                .statusUpdate(
                        IngestJobStartedStatus.withStartOfRun(false)
                                .inputFileCount(job.getFiles().size())
                                .startTime(runTime.getStartTime())
                                .updateTime(defaultUpdateTime(runTime.getStartTime())).build())
                .finishedStatus(ProcessFailedStatus
                        .timeAndReasons(defaultUpdateTime(runTime.getFinishTime()), runTime, failureReasons))
                .build();
    }

    /**
     * Creates a process run for an ingest job that was validated.
     *
     * @param  job            the ingest job
     * @param  validationTime the validation time
     * @return                a {@link ProcessRun}
     */
    public static ProcessRun acceptedRun(IngestJob job, Instant validationTime) {
        return ProcessRun.builder()
                .startedStatus(IngestJobAcceptedStatus.from(job,
                        validationTime, defaultUpdateTime(validationTime)))
                .build();
    }

    /**
     * Creates a process run for an ingest job that was validated and picked up by an ingest task, but has not started
     * yet.
     *
     * @param  job            the ingest job
     * @param  taskId         the ingest task ID
     * @param  validationTime the validation time
     * @return                a {@link ProcessRun}
     */
    public static ProcessRun acceptedRunOnTask(IngestJob job, String taskId, Instant validationTime) {
        return ProcessRun.builder()
                .taskId(taskId)
                .startedStatus(IngestJobAcceptedStatus.from(job, validationTime,
                        defaultUpdateTime(validationTime)))
                .build();
    }

    /**
     * Creates a process run for an ingest job that failed to validate.
     *
     * @param  job            the ingest job
     * @param  validationTime the validation time
     * @param  reasons        the reasons
     * @return                a {@link ProcessRun}
     */
    public static ProcessRun rejectedRun(IngestJob job, Instant validationTime, String... reasons) {
        return rejectedRun(job, null, validationTime, List.of(reasons));
    }

    /**
     * Creates a process run for an ingest job that failed to validate.
     *
     * @param  jobId          the ingest job ID
     * @param  jsonMessage    the JSON string used in ingest job deserialisation
     * @param  validationTime the validation time
     * @param  reasons        the reasons
     * @return                a {@link ProcessRun}
     */
    public static ProcessRun rejectedRun(String jobId, String jsonMessage, Instant validationTime, String... reasons) {
        return rejectedRun(IngestJob.builder().id(jobId).build(), jsonMessage, validationTime, List.of(reasons));
    }

    /**
     * Creates a process run for an ingest job that failed to validate.
     *
     * @param  job            the ingest job
     * @param  validationTime the validation time
     * @param  reasons        the list of reasons
     * @return                a {@link ProcessRun}
     */
    public static ProcessRun rejectedRun(IngestJob job, Instant validationTime, List<String> reasons) {
        return rejectedRun(job, null, validationTime, reasons);
    }

    /**
     * Creates a process run for an ingest job that failed to validate.
     *
     * @param  job            the ingest job
     * @param  jsonMessage    the JSON string used in ingest job deserialisation
     * @param  validationTime the validation time
     * @param  reasons        the list of reasons
     * @return                a {@link ProcessRun}
     */
    public static ProcessRun rejectedRun(IngestJob job, String jsonMessage, Instant validationTime, List<String> reasons) {
        return ProcessRun.builder()
                .startedStatus(IngestJobRejectedStatus.builder()
                        .validationTime(validationTime)
                        .updateTime(defaultUpdateTime(validationTime))
                        .reasons(reasons)
                        .jsonMessage(jsonMessage)
                        .job(job)
                        .build())
                .build();
    }

    /**
     * Creates a process run for an ingest job that started.
     *
     * @param  job       the ingest job
     * @param  taskId    the ingest task ID
     * @param  startTime the start time
     * @return           a {@link ProcessRun}
     */
    public static ProcessRun startedIngestRun(IngestJob job, String taskId, Instant startTime) {
        return ProcessRun.started(taskId,
                ingestStartedStatus(job, startTime, defaultUpdateTime(startTime)));
    }

    /**
     * Creates a process run for an ingest job that finished.
     *
     * @param  job     the ingest job
     * @param  taskId  the ingest task ID
     * @param  summary the records processed summary
     * @return         a {@link ProcessRun}
     */
    public static ProcessRun finishedIngestRun(
            IngestJob job, String taskId, RecordsProcessedSummary summary) {
        return finishedIngestRun(job, taskId, summary, 1);
    }

    /**
     * Creates a process run for an ingest job that finished.
     *
     * @param  job                  the ingest job
     * @param  taskId               the ingest task ID
     * @param  summary              the records processed summary
     * @param  numFilesWrittenByJob the number of files written by the job
     * @return                      a {@link ProcessRun}
     */
    public static ProcessRun finishedIngestRun(
            IngestJob job, String taskId, RecordsProcessedSummary summary, int numFilesWrittenByJob) {
        return ProcessRun.finished(taskId,
                ingestStartedStatus(job, summary.getStartTime(), defaultUpdateTime(summary.getStartTime())),
                IngestJobFinishedStatus.updateTimeAndSummary(defaultUpdateTime(summary.getFinishTime()), summary)
                        .numFilesWrittenByJob(numFilesWrittenByJob).build());
    }

    /**
     * Creates a process run for an ingest job that finished, but has not yet been committed to the state store.
     *
     * @param  job                  the ingest job
     * @param  taskId               the ingest task ID
     * @param  summary              the records processed summary
     * @param  numFilesWrittenByJob the number of files written by the job
     * @return                      a {@link ProcessRun}
     */
    public static ProcessRun finishedIngestRunUncommitted(
            IngestJob job, String taskId, RecordsProcessedSummary summary, int numFilesWrittenByJob) {
        return ProcessRun.finished(taskId,
                ingestStartedStatus(job, summary.getStartTime(), defaultUpdateTime(summary.getStartTime())),
                ingestFinishedStatusUncommitted(job, summary, numFilesWrittenByJob));
    }

    /**
     * Creates a process run for an ingest job that failed.
     *
     * @param  job            the ingest job
     * @param  taskId         the ingest task ID
     * @param  runTime        the process run time
     * @param  failureReasons a list of failure reasons
     * @return                a {@link ProcessRun}
     */
    public static ProcessRun failedIngestRun(
            IngestJob job, String taskId, ProcessRunTime runTime, List<String> failureReasons) {
        return ProcessRun.finished(taskId,
                ingestStartedStatus(job, runTime.getStartTime(), defaultUpdateTime(runTime.getStartTime())),
                ProcessFailedStatus.timeAndReasons(defaultUpdateTime(runTime.getFinishTime()), runTime, failureReasons));
    }

    /**
     * Creates a process run for an ingest job that passed validation then failed to start.
     *
     * @param  job            the ingest job
     * @param  validationTime the validation time
     * @param  failureTime    the failure time
     * @param  failureReasons a list of failure reasons
     * @return                a {@link ProcessRun}
     */
    public static ProcessRun acceptedAndFailedToStartIngestRun(
            IngestJob job, Instant validationTime, Instant failureTime, List<String> failureReasons) {
        return ProcessRun.builder()
                .startedStatus(IngestJobAcceptedStatus.from(job,
                        validationTime, defaultUpdateTime(validationTime)))
                .finishedStatus(ProcessFailedStatus.timeAndReasons(
                        defaultUpdateTime(failureTime), new ProcessRunTime(failureTime, Duration.ZERO), failureReasons))
                .build();
    }

    /**
     * Creates a list of ingest job statuses from a stream of process update records.
     *
     * @param  records the {@link TestProcessStatusUpdateRecords} object
     * @return         a list of ingest job statuses
     */
    public static List<IngestJobStatus> jobStatusListFrom(TestProcessStatusUpdateRecords records) {
        return IngestJobStatus.streamFrom(records.stream()).collect(Collectors.toList());
    }

    /**
     * Creates one ingest job status from a stream of process update records.
     *
     * @param  records               the {@link TestProcessStatusUpdateRecords} object
     * @return                       an ingest job status
     * @throws IllegalStateException if there was not exactly 1 job
     */
    public static IngestJobStatus singleJobStatusFrom(TestProcessStatusUpdateRecords records) {
        List<IngestJobStatus> jobs = jobStatusListFrom(records);
        if (jobs.size() != 1) {
            throw new IllegalStateException("Expected single job, found " + jobs.size());
        }
        return jobs.get(0);
    }

    /**
     * Creates an ingest job accepted status.
     *
     * @param  job            the ingest job
     * @param  validationTime the validation time
     * @return                an ingest job accepted status
     */
    public static IngestJobAcceptedStatus ingestAcceptedStatus(IngestJob job, Instant validationTime) {
        return IngestJobAcceptedStatus.from(job, validationTime, defaultUpdateTime(validationTime));
    }

    /**
     * Creates an ingest job started status.
     *
     * @param  job       the ingest job
     * @param  startTime the start time
     * @return           an ingest job started status
     */
    public static IngestJobStartedStatus ingestStartedStatus(IngestJob job, Instant startTime) {
        return ingestStartedStatus(job, startTime, defaultUpdateTime(startTime));
    }

    /**
     * Creates an ingest job started status.
     *
     * @param  job        the ingest job
     * @param  startTime  the start time
     * @param  updateTime the update time
     * @return            an ingest job started status
     */
    public static IngestJobStartedStatus ingestStartedStatus(IngestJob job, Instant startTime, Instant updateTime) {
        return IngestJobStartedStatus.withStartOfRun(true).job(job)
                .startTime(startTime).updateTime(updateTime)
                .build();
    }

    /**
     * Creates an ingest job started status.
     *
     * @param  job       the ingest job
     * @param  startTime the start time
     * @return           an ingest job started status
     */
    public static IngestJobStartedStatus validatedIngestStartedStatus(IngestJob job, Instant startTime) {
        return IngestJobStartedStatus.withStartOfRun(false).job(job)
                .startTime(startTime).updateTime(defaultUpdateTime(startTime))
                .build();
    }

    /**
     * Creates an ingest job file added status.
     *
     * @param  writtenTime the written time
     * @param  fileCount   the number of files added
     * @return             an ingest job started status
     */
    public static IngestJobAddedFilesStatus ingestAddedFilesStatus(Instant writtenTime, int fileCount) {
        return IngestJobAddedFilesStatus.builder().writtenTime(writtenTime).updateTime(defaultUpdateTime(writtenTime)).fileCount(fileCount).build();
    }

    /**
     * Creates an ingest job finished status where files are committed.
     *
     * @param  job                  the ingest job
     * @param  summary              the summary
     * @param  numFilesWrittenByJob the number of files written by the job
     * @return                      an ingest job started status
     */
    public static IngestJobFinishedStatus ingestFinishedStatus(IngestJob job, RecordsProcessedSummary summary, int numFilesWrittenByJob) {
        return IngestJobFinishedStatus.updateTimeAndSummary(defaultUpdateTime(summary.getFinishTime()), summary)
                .committedBySeparateFileUpdates(false)
                .numFilesWrittenByJob(numFilesWrittenByJob)
                .build();
    }

    /**
     * Creates an ingest job finished status where files are uncommitted.
     *
     * @param  job                  the ingest job
     * @param  summary              the summary
     * @param  numFilesWrittenByJob the number of files written by the job
     * @return                      an ingest job started status
     */
    public static IngestJobFinishedStatus ingestFinishedStatusUncommitted(IngestJob job, RecordsProcessedSummary summary, int numFilesWrittenByJob) {
        return IngestJobFinishedStatus.updateTimeAndSummary(defaultUpdateTime(summary.getFinishTime()), summary)
                .committedBySeparateFileUpdates(true)
                .numFilesWrittenByJob(numFilesWrittenByJob)
                .build();
    }

}
