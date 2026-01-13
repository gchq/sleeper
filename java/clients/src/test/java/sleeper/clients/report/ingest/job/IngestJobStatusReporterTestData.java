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

package sleeper.clients.report.ingest.job;

import sleeper.core.tracker.ingest.job.query.IngestJobAcceptedStatus;
import sleeper.core.tracker.ingest.job.query.IngestJobRejectedStatus;
import sleeper.core.tracker.ingest.job.query.IngestJobStatus;
import sleeper.core.tracker.job.run.JobRunTime;
import sleeper.core.tracker.job.run.RowsProcessed;
import sleeper.ingest.core.job.IngestJob;

import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static sleeper.clients.report.StatusReporterTestHelper.job;
import static sleeper.clients.report.StatusReporterTestHelper.task;
import static sleeper.core.tracker.ingest.job.IngestJobStatusTestData.ingestAcceptedStatus;
import static sleeper.core.tracker.ingest.job.IngestJobStatusTestData.ingestAddedFilesStatus;
import static sleeper.core.tracker.ingest.job.IngestJobStatusTestData.ingestFinishedStatusUncommitted;
import static sleeper.core.tracker.job.run.JobRunSummaryTestHelper.summary;
import static sleeper.core.tracker.job.run.JobRunTestData.jobRunOnTask;
import static sleeper.core.tracker.job.status.JobStatusUpdateTestHelper.defaultUpdateTime;
import static sleeper.ingest.core.job.IngestJobStatusFromJobTestData.acceptedRun;
import static sleeper.ingest.core.job.IngestJobStatusFromJobTestData.acceptedRunWhichStarted;
import static sleeper.ingest.core.job.IngestJobStatusFromJobTestData.failedIngestJob;
import static sleeper.ingest.core.job.IngestJobStatusFromJobTestData.failedIngestRun;
import static sleeper.ingest.core.job.IngestJobStatusFromJobTestData.finishedIngestJob;
import static sleeper.ingest.core.job.IngestJobStatusFromJobTestData.finishedIngestJobUncommitted;
import static sleeper.ingest.core.job.IngestJobStatusFromJobTestData.finishedIngestRun;
import static sleeper.ingest.core.job.IngestJobStatusFromJobTestData.ingestJobStatus;
import static sleeper.ingest.core.job.IngestJobStatusFromJobTestData.ingestStartedStatus;
import static sleeper.ingest.core.job.IngestJobStatusFromJobTestData.rejectedRun;
import static sleeper.ingest.core.job.IngestJobStatusFromJobTestData.startedIngestJob;
import static sleeper.ingest.core.job.IngestJobStatusFromJobTestData.startedIngestRun;
import static sleeper.ingest.core.job.IngestJobStatusFromJobTestData.validatedIngestStartedStatus;

/**
 * Creates example data for testing reports on ingest jobs.
 */
public class IngestJobStatusReporterTestData {
    private IngestJobStatusReporterTestData() {
    }

    /**
     * Creates a record of messages on the standard ingest job queue, with no messages on any other ingest queues.
     *
     * @param  messages the number of standard ingest job messages
     * @return          the queue message counts
     */
    public static IngestQueueMessages ingestMessageCount(int messages) {
        return IngestQueueMessages.builder().ingestMessages(messages).build();
    }

    /**
     * Creates example data for ingest jobs from the job tracker with a mix of statuses where the jobs have not yet
     * finished.
     *
     * @return the job statuses
     */
    public static List<IngestJobStatus> mixedUnfinishedJobStatuses() {
        IngestJob job1 = createJob(1, 1);
        Instant startTime1 = Instant.parse("2022-09-18T13:34:12.001Z");

        IngestJob job2 = createJob(2, 2);
        Instant startTime2 = Instant.parse("2022-09-19T13:34:12.001Z");

        IngestJob job3 = createJob(3, 3);
        Instant startTime3 = Instant.parse("2022-09-20T13:34:12.001Z");

        IngestJob job4 = createJob(4, 4);
        Instant startTime4 = Instant.parse("2022-09-21T13:34:12.001Z");

        IngestJob job5 = createJob(5, 5);
        Instant startTime5 = Instant.parse("2022-09-22T13:34:12.001Z");

        return Arrays.asList(
                ingestJobStatus(job5, jobRunOnTask(task(3),
                        ingestStartedStatus(job5, startTime5),
                        ingestAddedFilesStatus(startTime5.plus(Duration.ofMinutes(1)), 2))),
                ingestJobStatus(job4, jobRunOnTask(task(3),
                        ingestStartedStatus(job4, startTime4),
                        ingestFinishedStatusUncommitted(summary(startTime4, Duration.ofMinutes(1), 600, 300), 1))),
                ingestJobStatus(job3, failedIngestRun(job3, task(2),
                        new JobRunTime(startTime3, Duration.ofSeconds(30)),
                        List.of("Unexpected failure", "Some IO problem"))),
                ingestJobStatus(job2, startedIngestRun(job2, task(1), startTime2)),
                ingestJobStatus(job1, acceptedRun(job1, startTime1)));
    }

    /**
     * Creates example data for ingest jobs from the job tracker with a mix of different statuses.
     *
     * @return the job statuses
     */
    public static List<IngestJobStatus> mixedJobStatuses() {
        IngestJob job1 = createJob(1, 1);
        Instant startTime1 = Instant.parse("2022-09-21T13:34:12.001Z");

        IngestJob job2 = createJob(2, 2);
        Instant startTime2 = Instant.parse("2022-09-22T13:34:12.001Z");

        IngestJob job3 = createJob(3, 3);
        Instant startTime3 = Instant.parse("2022-09-23T13:34:12.001Z");

        IngestJob job4 = createJob(4, 4);
        Instant startTime4 = Instant.parse("2022-09-24T13:34:12.001Z");

        IngestJob job5 = createJob(5, 5);
        Instant startTime5 = Instant.parse("2022-09-25T13:34:12.001Z");

        IngestJob job6 = createJob(6, 6);
        Instant startTime6 = Instant.parse("2022-09-26T13:34:12.001Z");

        IngestJob job7 = createJob(7, 7);
        Instant startTime7 = Instant.parse("2022-09-27T13:34:12.001Z");

        return Arrays.asList(
                ingestJobStatus(job7, jobRunOnTask(task(3),
                        ingestStartedStatus(job7, startTime7),
                        ingestAddedFilesStatus(startTime7.plus(Duration.ofSeconds(55)), 2),
                        ingestFinishedStatusUncommitted(summary(startTime7, Duration.ofMinutes(1), 600, 300), 2))),
                ingestJobStatus(job6, jobRunOnTask(task(3),
                        ingestStartedStatus(job6, startTime6),
                        ingestAddedFilesStatus(startTime6.plus(Duration.ofMinutes(1)), 1))),
                finishedIngestJobUncommitted(job5, task(3), summary(startTime5, Duration.ofMinutes(1), 600, 300), 3),
                finishedIngestJob(job4, task(2), summary(startTime4, Duration.ofMinutes(1), 600, 300), 2),
                startedIngestJob(job3, task(2), startTime3),
                finishedIngestJob(job2, task(1), summary(startTime2, Duration.ofMinutes(1), 600, 300), 1),
                failedIngestJob(job1, task(1),
                        new JobRunTime(startTime1, Duration.ofMinutes(1)),
                        List.of("Something went wrong", "More details")));
    }

    /**
     * Creates example data for a single ingest job from the job tracker that has been run several times.
     *
     * @return the job status list with a single job
     */
    public static List<IngestJobStatus> jobWithMultipleRuns() {
        IngestJob job = createJob(1, 1);

        return Collections.singletonList(ingestJobStatus(job,
                startedIngestRun(job, task(1), Instant.parse("2022-10-12T10:02:00.001Z")),
                finishedIngestRun(job, task(2), summary(Instant.parse("2022-10-12T10:01:15.001Z"), Duration.ofSeconds(30), 300, 200), 2),
                finishedIngestRun(job, task(1), summary(Instant.parse("2022-10-12T10:01:00.001Z"), Duration.ofSeconds(20), 300, 200), 1)));
    }

    /**
     * Creates example data for ingest jobs from the job tracker with a variety of values for their statistics. This
     * includes the duration of the job, and the number of rows read and written.
     *
     * @return the job statuses
     */
    public static List<IngestJobStatus> jobsWithLargeAndDecimalStatistics() {
        Instant startTime1 = Instant.parse("2022-10-13T10:00:10Z");
        Instant startTime2 = Instant.parse("2022-10-13T12:01:10Z");
        Instant startTime3 = Instant.parse("2022-10-13T14:01:10Z");
        Instant startTime4 = Instant.parse("2022-10-13T14:02:10Z");

        return Arrays.asList(
                finishedIngestJob(createJob(4, 1), "task-id", summary(startTime4, Duration.ofMillis(123_456), 1_234_000L, 1_234_000L), 4),
                finishedIngestJob(createJob(3, 1), "task-id", summary(startTime3, Duration.ofMinutes(1), 1_000_600L, 500_300L), 3),
                finishedIngestJob(createJob(2, 1), "task-id", summary(startTime2, Duration.ofHours(2), 1_000_600L, 500_300L), 2),
                finishedIngestJob(createJob(1, 1), "task-id", summary(startTime1, Duration.ofMillis(123_123), 600_000, 300_000), 1));
    }

    /**
     * Creates example data for jobs from the ingest job tracker with a single bulk import job that has been accepted
     * but not yet started.
     *
     * @return the job status list with a single job
     */
    public static List<IngestJobStatus> acceptedJob() {
        IngestJob job = createJob(1, 2);
        return List.of(ingestJobStatus(job,
                acceptedRun(job, Instant.parse("2023-06-05T17:20:00Z"))));
    }

    /**
     * Creates example data for jobs from the ingest job tracker with a single bulk import job that has been accepted
     * and started, but not yet finished.
     *
     * @return the job status list with a single job
     */
    public static List<IngestJobStatus> acceptedJobWhichStarted() {
        IngestJob job = createJob(1, 2);
        return List.of(ingestJobStatus(job,
                acceptedRunWhichStarted(job, "test-task",
                        Instant.parse("2023-06-05T17:20:00Z"),
                        Instant.parse("2023-06-05T18:20:00Z"))));
    }

    /**
     * Creates example data for jobs from the ingest job tracker with a single bulk import job that has been rejected
     * with a single reason for the rejection.
     *
     * @return the job status list with a single job
     */
    public static List<IngestJobStatus> rejectedJobWithOneReason() {
        List<String> reasons = List.of("Test validation reason");
        IngestJob job = createJob(1, 2);
        return List.of(ingestJobStatus(job,
                rejectedRun(job, Instant.parse("2023-06-05T17:20:00Z"), reasons)));
    }

    /**
     * Creates example data for jobs from the ingest job tracker with a single bulk import job that has been rejected
     * with multiple reasons for the rejection.
     *
     * @return the job status list with a single job
     */
    public static List<IngestJobStatus> rejectedJobWithMultipleReasons() {
        List<String> reasons = List.of("Test validation reason 1", "Test validation reason 2", "Test validation reason 3");
        IngestJob job = createJob(1, 2);
        return List.of(ingestJobStatus(job,
                rejectedRun(job, Instant.parse("2023-06-05T17:20:00Z"), reasons)));
    }

    /**
     * Creates example data for jobs from the ingest job tracker with a single bulk import job that has fully completed.
     *
     * @return the job status list with a single job
     */
    public static List<IngestJobStatus> finishedBulkImportJob() {
        IngestJob job8 = createJob(8, 8);
        Instant startTime8 = Instant.parse("2022-09-28T13:34:12.001Z");
        return List.of(ingestJobStatus(job8, jobRunOnTask("bulk-import-cluster-8",
                ingestAcceptedStatus(startTime8, 8),
                validatedIngestStartedStatus(job8, startTime8.plus(Duration.ofMinutes(5))),
                ingestFinishedStatusUncommitted(startTime8.plus(Duration.ofMinutes(10)), 1, new RowsProcessed(3000, 1500)),
                ingestAddedFilesStatus(startTime8.plus(Duration.ofMinutes(11)), 1))));
    }

    /**
     * Creates an example ingest or bulk import job. This can be used to create further example data for testing
     * reports.
     *
     * @param  jobNum         a number identifying this job as distinct from others in the test
     * @param  inputFileCount the number of input files in the job (will be named test1.parquet, test2.parquet, etc.)
     * @return                the job
     */
    public static IngestJob createJob(int jobNum, int inputFileCount) {
        return IngestJob.builder()
                .id(job(jobNum))
                .files(IntStream.range(1, inputFileCount + 1)
                        .mapToObj(f -> String.format("test%1d.parquet", f))
                        .collect(Collectors.toList()))
                .build();
    }

    /**
     * Creates an example status update when a bulk import job was accepted in the bulk import starter.
     *
     * @param  job            the bulk import job, as an ingest job
     * @param  validationTime the time the job was accepted in the bulk import starter
     * @return                the status update
     */
    public static IngestJobAcceptedStatus acceptedStatusUpdate(IngestJob job, Instant validationTime) {
        return IngestJobAcceptedStatus.from(job.getFileCount(), validationTime, defaultUpdateTime(validationTime));
    }

    /**
     * Creates an example status update when a bulk import job was rejected in the bulk import starter. Usually this
     * means a validation failure.
     *
     * @param  job            the bulk import job, as an ingest job
     * @param  validationTime the time the job was rejected in the bulk import starter
     * @return                the status update
     */
    public static IngestJobRejectedStatus rejectedStatusUpdate(IngestJob job, Instant validationTime) {
        return rejectedStatusUpdate(job, validationTime, null);
    }

    /**
     * Creates an example status update when a bulk import job was rejected in the bulk import starter. Usually this
     * means a validation failure.
     *
     * @param  job            the bulk import job, as an ingest job
     * @param  validationTime the time the job was rejected in the bulk import starter
     * @param  jsonMessage    the JSON body of the message that was rejected
     * @return                the status update
     */
    public static IngestJobRejectedStatus rejectedStatusUpdate(IngestJob job, Instant validationTime, String jsonMessage) {
        return IngestJobRejectedStatus.builder().inputFileCount(job.getFileCount())
                .validationTime(validationTime).updateTime(defaultUpdateTime(validationTime))
                .reasons(List.of("Test validation reason"))
                .jsonMessage(jsonMessage)
                .build();
    }
}
