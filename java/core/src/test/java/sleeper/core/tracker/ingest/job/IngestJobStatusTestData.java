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

import sleeper.core.tracker.ingest.job.query.IngestJobAcceptedStatus;
import sleeper.core.tracker.ingest.job.query.IngestJobAddedFilesStatus;
import sleeper.core.tracker.ingest.job.query.IngestJobFinishedStatus;
import sleeper.core.tracker.ingest.job.query.IngestJobRejectedStatus;
import sleeper.core.tracker.ingest.job.query.IngestJobStartedStatus;
import sleeper.core.tracker.ingest.job.query.IngestJobValidatedStatus;
import sleeper.core.tracker.ingest.job.update.IngestJobEvent;
import sleeper.core.tracker.job.JobRunSummary;
import sleeper.core.tracker.job.JobRunTime;
import sleeper.core.tracker.job.status.ProcessRun;
import sleeper.core.tracker.job.status.ProcessRuns;
import sleeper.core.tracker.job.status.TestProcessStatusUpdateRecords;

import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static sleeper.core.tracker.job.ProcessRunTestData.finishedRun;
import static sleeper.core.tracker.job.ProcessRunTestData.startedRun;
import static sleeper.core.tracker.job.status.ProcessStatusUpdateTestHelper.defaultUpdateTime;
import static sleeper.core.tracker.job.status.ProcessStatusUpdateTestHelper.failedStatus;

/**
 * A helper for creating ingest job statuses for tests.
 */
public class IngestJobStatusTestData {

    private IngestJobStatusTestData() {
    }

    /**
     * Creates an ingest job status.
     *
     * @param  jobId the ingest job ID
     * @param  runs  the process runs
     * @return       an {@link IngestJobStatus}
     */
    public static IngestJobStatus ingestJobStatus(String jobId, ProcessRun... runs) {
        return IngestJobStatus.builder()
                .jobId(jobId)
                .jobRuns(ProcessRuns.latestFirst(Arrays.asList(runs)))
                .build();
    }

    /**
     * Creates an ingest job status.
     *
     * @param  job  an event for the job this status is for
     * @param  runs the process runs
     * @return      an {@link IngestJobStatus}
     */
    public static IngestJobStatus ingestJobStatus(IngestJobEvent job, ProcessRun... runs) {
        return ingestJobStatus(job.getJobId(), runs);
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
        return ProcessRun.builder()
                .startedStatus(IngestJobRejectedStatus.builder()
                        .validationTime(validationTime)
                        .updateTime(defaultUpdateTime(validationTime))
                        .reasons(List.of(reasons))
                        .jsonMessage(jsonMessage)
                        .build())
                .build();
    }

    /**
     * Creates a process run for an ingest job that started.
     *
     * @param  taskId    the ingest task ID
     * @param  startTime the start time
     * @return           a {@link ProcessRun}
     */
    public static ProcessRun startedIngestRun(String taskId, Instant startTime) {
        return startedRun(taskId,
                ingestStartedStatus(1, startTime, defaultUpdateTime(startTime)));
    }

    /**
     * Creates a process run for an ingest job that finished.
     *
     * @param  taskId  the ingest task ID
     * @param  summary the records processed summary
     * @return         a {@link ProcessRun}
     */
    public static ProcessRun finishedIngestRun(String taskId, JobRunSummary summary) {
        return finishedRun(taskId,
                ingestStartedStatus(summary.getStartTime()),
                ingestFinishedStatus(summary));
    }

    /**
     * Creates a process run for an ingest job that finished.
     *
     * @param  taskId               the ingest task ID
     * @param  summary              the records processed summary
     * @param  numFilesWrittenByJob the number of files written by the job
     * @return                      a {@link ProcessRun}
     */
    public static ProcessRun finishedIngestRun(String taskId, JobRunSummary summary, int numFilesWrittenByJob) {
        return finishedRun(taskId,
                ingestStartedStatus(summary.getStartTime()),
                ingestFinishedStatus(summary, numFilesWrittenByJob));
    }

    /**
     * Creates a process run for an ingest job that finished.
     *
     * @param  taskId  the ingest task ID
     * @param  summary the records processed summary
     * @return         a {@link ProcessRun}
     */
    public static ProcessRun finishedIngestRunUncommitted(String taskId, JobRunSummary summary) {
        return finishedRun(taskId,
                ingestStartedStatus(summary.getStartTime()),
                ingestFinishedStatusUncommitted(summary));
    }

    /**
     * Creates a process run for an ingest job that failed.
     *
     * @param  taskId    the ingest task ID
     * @param  startTime the time the job started
     * @param  failTime  the time the job failed
     * @param  reasons   the reasons the job failed
     * @return           a {@link ProcessRun}
     */
    public static ProcessRun failedIngestRun(String taskId, Instant startTime, Instant failTime, List<String> reasons) {
        return finishedRun(taskId,
                ingestStartedStatus(startTime),
                failedStatus(new JobRunTime(startTime, failTime), reasons));
    }

    /**
     * Creates a process run for an ingest job that failed.
     *
     * @param  taskId    the ingest task ID
     * @param  startTime the time the job started
     * @param  duration  the duration after which the job failed
     * @param  reasons   the reasons the job failed
     * @return           a {@link ProcessRun}
     */
    public static ProcessRun failedIngestRun(String taskId, Instant startTime, Duration duration, List<String> reasons) {
        return finishedRun(taskId,
                ingestStartedStatus(startTime),
                failedStatus(new JobRunTime(startTime, duration), reasons));
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
     * @param  validationTime the validation time
     * @return                an ingest job accepted status
     */
    public static IngestJobAcceptedStatus ingestAcceptedStatus(Instant validationTime) {
        return IngestJobAcceptedStatus.from(1, validationTime, defaultUpdateTime(validationTime));
    }

    /**
     * Creates an ingest job accepted status.
     *
     * @param  validationTime the validation time
     * @param  inputFileCount the number of input files in the job
     * @return                an ingest job accepted status
     */
    public static IngestJobAcceptedStatus ingestAcceptedStatus(Instant validationTime, int inputFileCount) {
        return IngestJobAcceptedStatus.from(inputFileCount, validationTime, defaultUpdateTime(validationTime));
    }

    /**
     * Creates an ingest job started status.
     *
     * @param  startTime the start time
     * @return           an ingest job started status
     */
    public static IngestJobStartedStatus ingestStartedStatus(Instant startTime) {
        return ingestStartedStatus(1, startTime, defaultUpdateTime(startTime));
    }

    /**
     * Creates an ingest job started status.
     *
     * @param  startTime the start time
     * @param  fileCount the number of input files in the job
     * @return           an ingest job started status
     */
    public static IngestJobStartedStatus ingestStartedStatus(Instant startTime, int fileCount) {
        return ingestStartedStatus(fileCount, startTime, defaultUpdateTime(startTime));
    }

    /**
     * Creates an ingest job started status.
     *
     * @param  fileCount  the number of input files in the job
     * @param  startTime  the start time
     * @param  updateTime the update time
     * @return            an ingest job started status
     */
    public static IngestJobStartedStatus ingestStartedStatus(int fileCount, Instant startTime, Instant updateTime) {
        return IngestJobStartedStatus.withStartOfRun(true).inputFileCount(fileCount)
                .startTime(startTime).updateTime(updateTime)
                .build();
    }

    /**
     * Creates an ingest job started status.
     *
     * @param  startTime  the start time
     * @param  updateTime the update time
     * @return            an ingest job started status
     */
    public static IngestJobStartedStatus ingestStartedStatus(Instant startTime, Instant updateTime) {
        return ingestStartedStatus(1, startTime, updateTime);
    }

    /**
     * Creates an ingest job started status.
     *
     * @param  startTime the start time
     * @return           an ingest job started status
     */
    public static IngestJobStartedStatus validatedIngestStartedStatus(Instant startTime) {
        return IngestJobStartedStatus.withStartOfRun(false).inputFileCount(1)
                .startTime(startTime).updateTime(defaultUpdateTime(startTime))
                .build();
    }

    /**
     * Creates an ingest job started status.
     *
     * @param  inputFileCount the number of input files in the job
     * @param  startTime      the start time
     * @return                an ingest job started status
     */
    public static IngestJobStartedStatus validatedIngestStartedStatus(int inputFileCount, Instant startTime) {
        return IngestJobStartedStatus.withStartOfRun(false).inputFileCount(inputFileCount)
                .startTime(startTime).updateTime(defaultUpdateTime(startTime))
                .build();
    }

    /**
     * Creates an ingest job rejected status.
     *
     * @param  validationTime the validation time
     * @param  jsonMessage    the JSON of the message that was rejected
     * @param  reasons        the list of reasons the job was rejected
     * @param  inputFileCount the number of input files in the job
     * @return                an ingest job started status
     */
    public static IngestJobValidatedStatus ingestRejectedStatus(Instant validationTime, String jsonMessage, List<String> reasons, int inputFileCount) {
        return IngestJobRejectedStatus.builder()
                .validationTime(validationTime)
                .updateTime(defaultUpdateTime(validationTime))
                .reasons(reasons)
                .jsonMessage(jsonMessage)
                .inputFileCount(inputFileCount)
                .build();
    }

    /**
     * Creates an ingest job rejected status.
     *
     * @param  validationTime the validation time
     * @param  reasons        the list of reasons the job was rejected
     * @param  inputFileCount the number of input files in the job
     * @return                an ingest job started status
     */
    public static IngestJobValidatedStatus ingestRejectedStatus(Instant validationTime, List<String> reasons, int inputFileCount) {
        return ingestRejectedStatus(validationTime, null, reasons, inputFileCount);
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
     * @param  summary the summary
     * @return         an ingest job started status
     */
    public static IngestJobFinishedStatus ingestFinishedStatus(JobRunSummary summary) {
        return ingestFinishedStatus(summary, 1);
    }

    /**
     * Creates an ingest job finished status where files are committed.
     *
     * @param  summary              the summary
     * @param  numFilesWrittenByJob the number of files written by the job
     * @return                      an ingest job started status
     */
    public static IngestJobFinishedStatus ingestFinishedStatus(JobRunSummary summary, int numFilesWrittenByJob) {
        return IngestJobFinishedStatus.updateTimeAndSummary(defaultUpdateTime(summary.getFinishTime()), summary)
                .committedBySeparateFileUpdates(false)
                .numFilesWrittenByJob(numFilesWrittenByJob)
                .build();
    }

    /**
     * Creates an ingest job finished status where files are uncommitted.
     *
     * @param  summary the summary
     * @return         an ingest job started status
     */
    public static IngestJobFinishedStatus ingestFinishedStatusUncommitted(JobRunSummary summary) {
        return ingestFinishedStatusUncommitted(summary, 1);
    }

    /**
     * Creates an ingest job finished status where files are uncommitted.
     *
     * @param  summary              the summary
     * @param  numFilesWrittenByJob the number of files written by the job
     * @return                      an ingest job started status
     */
    public static IngestJobFinishedStatus ingestFinishedStatusUncommitted(JobRunSummary summary, int numFilesWrittenByJob) {
        return IngestJobFinishedStatus.updateTimeAndSummary(defaultUpdateTime(summary.getFinishTime()), summary)
                .committedBySeparateFileUpdates(true)
                .numFilesWrittenByJob(numFilesWrittenByJob)
                .build();
    }

}
