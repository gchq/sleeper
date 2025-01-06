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
import sleeper.core.record.process.status.ProcessRun;
import sleeper.core.tracker.ingest.job.IngestJobStatus;
import sleeper.core.tracker.ingest.job.query.IngestJobAcceptedStatus;
import sleeper.core.tracker.ingest.job.query.IngestJobStartedStatus;
import sleeper.ingest.core.job.IngestJob;

import java.time.Instant;
import java.util.List;

import static sleeper.core.record.process.status.ProcessStatusUpdateTestHelper.defaultUpdateTime;

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
        return IngestJobStatusTestHelper.ingestJobStatus(job.getId(), runs);
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
        return IngestJobStatusTestHelper.ingestJobStatus(job.getId(), IngestJobStatusTestHelper.startedIngestRun(job, taskId, startTime));
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
        return ingestJobStatus(job, IngestJobStatusTestHelper.finishedIngestRun(job, taskId, summary, numFilesWrittenByJob));
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
        return ingestJobStatus(job, IngestJobStatusTestHelper.finishedIngestRunUncommitted(job, taskId, summary, numFilesWrittenByJob));
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
        return ingestJobStatus(job, IngestJobStatusTestHelper.failedIngestRun(job, taskId, runTime, failureReasons));
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
        return IngestJobStatusTestHelper.ingestStartedStatus(job.getFileCount(), startTime, defaultUpdateTime(startTime));
    }

    /**
     * Creates an ingest job started status update.
     *
     * @param  job       the ingest job
     * @param  startTime the start time
     * @return           an ingest job started status
     */
    public static IngestJobStartedStatus validatedIngestStartedStatus(IngestJob job, Instant startTime) {
        return IngestJobStatusTestHelper.validatedIngestStartedStatus(job.getFileCount(), startTime);
    }

}
