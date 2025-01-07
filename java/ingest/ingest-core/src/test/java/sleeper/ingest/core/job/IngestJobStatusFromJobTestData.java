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
package sleeper.ingest.core.job;

import sleeper.core.record.process.ProcessRunTime;
import sleeper.core.record.process.RecordsProcessedSummary;
import sleeper.core.record.process.status.ProcessFailedStatus;
import sleeper.core.record.process.status.ProcessRun;
import sleeper.core.tracker.ingest.job.IngestJobStatus;
import sleeper.core.tracker.ingest.job.IngestJobStatusTestData;
import sleeper.core.tracker.ingest.job.query.IngestJobAcceptedStatus;
import sleeper.core.tracker.ingest.job.query.IngestJobFinishedStatus;
import sleeper.core.tracker.ingest.job.query.IngestJobRejectedStatus;
import sleeper.core.tracker.ingest.job.query.IngestJobStartedStatus;

import java.time.Duration;
import java.time.Instant;
import java.util.List;

import static sleeper.core.record.process.ProcessRunTestData.finishedRun;
import static sleeper.core.record.process.ProcessRunTestData.startedRun;
import static sleeper.core.record.process.ProcessRunTestData.validationRun;
import static sleeper.core.record.process.status.ProcessStatusUpdateTestHelper.defaultUpdateTime;
import static sleeper.core.tracker.ingest.job.IngestJobStatusTestData.ingestFinishedStatusUncommitted;

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
    public static IngestJobStatus ingestJobStatus(IngestJob job, ProcessRun... runs) {
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
    public static IngestJobStatus finishedIngestJob(IngestJob job, String taskId, RecordsProcessedSummary summary, int numFilesWrittenByJob) {
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
    public static IngestJobStatus failedIngestJob(IngestJob job, String taskId, ProcessRunTime runTime, List<String> failureReasons) {
        return ingestJobStatus(job, failedIngestRun(job, taskId, runTime, failureReasons));
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
                .startedStatus(IngestJobAcceptedStatus.from(job.getFileCount(),
                        validationTime, defaultUpdateTime(validationTime)))
                .statusUpdate(
                        IngestJobStartedStatus.withStartOfRun(false)
                                .inputFileCount(job.getFileCount())
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
                .startedStatus(IngestJobAcceptedStatus.from(job.getFileCount(),
                        validationTime, defaultUpdateTime(validationTime)))
                .statusUpdate(
                        IngestJobStartedStatus.withStartOfRun(false)
                                .inputFileCount(job.getFileCount())
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
                .startedStatus(IngestJobAcceptedStatus.from(job.getFileCount(),
                        validationTime, defaultUpdateTime(validationTime)))
                .statusUpdate(
                        IngestJobStartedStatus.withStartOfRun(false)
                                .inputFileCount(job.getFileCount())
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
                .startedStatus(IngestJobAcceptedStatus.from(job.getFileCount(),
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
                .startedStatus(IngestJobAcceptedStatus.from(job.getFileCount(), validationTime,
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
     * @return           a {@link ProcessRun}
     */
    public static ProcessRun startedIngestRun(IngestJob job, String taskId, Instant startTime) {
        return startedRun(taskId, ingestStartedStatus(job, startTime));
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
        return finishedRun(taskId,
                ingestStartedStatus(job, summary.getStartTime()),
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
        return finishedRun(taskId,
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
     * @return                a {@link ProcessRun}
     */
    public static ProcessRun failedIngestRun(
            IngestJob job, String taskId, ProcessRunTime runTime, List<String> failureReasons) {
        return finishedRun(taskId,
                ingestStartedStatus(job, runTime.getStartTime()),
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
                .startedStatus(IngestJobAcceptedStatus.from(job.getFileCount(),
                        validationTime, defaultUpdateTime(validationTime)))
                .finishedStatus(ProcessFailedStatus.timeAndReasons(
                        defaultUpdateTime(failureTime), new ProcessRunTime(failureTime, Duration.ZERO), failureReasons))
                .build();
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
        return IngestJobStatusTestData.validatedIngestStartedStatus(job.getFileCount(), startTime);
    }

}
