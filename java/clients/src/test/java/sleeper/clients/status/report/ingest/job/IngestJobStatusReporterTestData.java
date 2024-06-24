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

package sleeper.clients.status.report.ingest.job;

import sleeper.core.record.process.ProcessRunTime;
import sleeper.ingest.job.IngestJob;
import sleeper.ingest.job.status.IngestJobAcceptedStatus;
import sleeper.ingest.job.status.IngestJobRejectedStatus;
import sleeper.ingest.job.status.IngestJobStatus;

import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static sleeper.clients.status.report.StatusReporterTestHelper.job;
import static sleeper.clients.status.report.StatusReporterTestHelper.task;
import static sleeper.core.record.process.RecordsProcessedSummaryTestHelper.summary;
import static sleeper.core.record.process.status.ProcessStatusUpdateTestHelper.defaultUpdateTime;
import static sleeper.ingest.job.status.IngestJobStatusTestHelper.acceptedRun;
import static sleeper.ingest.job.status.IngestJobStatusTestHelper.acceptedRunWhichStarted;
import static sleeper.ingest.job.status.IngestJobStatusTestHelper.failedIngestJob;
import static sleeper.ingest.job.status.IngestJobStatusTestHelper.failedIngestRun;
import static sleeper.ingest.job.status.IngestJobStatusTestHelper.finishedIngestJob;
import static sleeper.ingest.job.status.IngestJobStatusTestHelper.finishedIngestRun;
import static sleeper.ingest.job.status.IngestJobStatusTestHelper.jobStatus;
import static sleeper.ingest.job.status.IngestJobStatusTestHelper.rejectedRun;
import static sleeper.ingest.job.status.IngestJobStatusTestHelper.startedIngestJob;
import static sleeper.ingest.job.status.IngestJobStatusTestHelper.startedIngestRun;

public class IngestJobStatusReporterTestData {
    private IngestJobStatusReporterTestData() {
    }

    public static IngestQueueMessages ingestMessageCount(int messages) {
        return IngestQueueMessages.builder().ingestMessages(messages).build();
    }

    public static List<IngestJobStatus> mixedUnfinishedJobStatuses() {
        IngestJob job1 = createJob(1, 1);
        Instant startTime1 = Instant.parse("2022-09-18T13:34:12.001Z");

        IngestJob job2 = createJob(2, 2);
        Instant startTime2 = Instant.parse("2022-09-19T13:34:12.001Z");

        IngestJob job3 = createJob(3, 3);
        Instant startTime3 = Instant.parse("2022-09-20T13:34:12.001Z");

        return Arrays.asList(
                jobStatus(job3, failedIngestRun(job3, task(2),
                        new ProcessRunTime(startTime3, Duration.ofSeconds(30)),
                        List.of("Unexpected failure", "Some IO problem"))),
                jobStatus(job2, startedIngestRun(job2, task(1), startTime2)),
                jobStatus(job1, acceptedRun(job1, startTime1)));
    }

    public static List<IngestJobStatus> mixedJobStatuses() {
        IngestJob job1 = createJob(2, 1);
        Instant startTime1 = Instant.parse("2022-09-18T13:34:12.001Z");

        IngestJob job2 = createJob(3, 2);
        Instant startTime2 = Instant.parse("2022-09-19T13:34:12.001Z");

        IngestJob job3 = createJob(5, 3);
        Instant startTime3 = Instant.parse("2022-09-21T13:34:12.001Z");

        IngestJob job4 = createJob(6, 4);
        Instant startTime4 = Instant.parse("2022-09-22T13:34:12.001Z");

        return Arrays.asList(
                finishedIngestJob(job4, task(2), summary(startTime4, Duration.ofMinutes(1), 600, 300), 2),
                startedIngestJob(job3, task(2), startTime3),
                finishedIngestJob(job2, task(1), summary(startTime2, Duration.ofMinutes(1), 600, 300), 1),
                failedIngestJob(job1, task(1),
                        new ProcessRunTime(startTime1, Duration.ofMinutes(1)),
                        List.of("Something went wrong", "More details")));
    }

    public static List<IngestJobStatus> jobWithMultipleRuns() {
        IngestJob job = createJob(2, 1);

        return Collections.singletonList(jobStatus(job,
                startedIngestRun(job, task(1), Instant.parse("2022-10-12T10:02:00.001Z")),
                finishedIngestRun(job, task(2), summary(Instant.parse("2022-10-12T10:01:15.001Z"), Duration.ofSeconds(30), 300, 200), 2),
                finishedIngestRun(job, task(1), summary(Instant.parse("2022-10-12T10:01:00.001Z"), Duration.ofSeconds(20), 300, 200), 1)));
    }

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

    public static List<IngestJobStatus> acceptedJob() {
        IngestJob job = createJob(1, 2);
        return List.of(jobStatus(job,
                acceptedRun(job, Instant.parse("2023-06-05T17:20:00Z"))));
    }

    public static List<IngestJobStatus> acceptedJobWhichStarted() {
        IngestJob job = createJob(1, 2);
        return List.of(jobStatus(job,
                acceptedRunWhichStarted(job, "test-task",
                        Instant.parse("2023-06-05T17:20:00Z"),
                        Instant.parse("2023-06-05T18:20:00Z"))));
    }

    public static List<IngestJobStatus> rejectedJobWithOneReason() {
        List<String> reasons = List.of("Test validation reason");
        IngestJob job = createJob(1, 2);
        return List.of(jobStatus(job,
                rejectedRun(job, Instant.parse("2023-06-05T17:20:00Z"), reasons)));
    }

    public static List<IngestJobStatus> rejectedJobWithMultipleReasons() {
        List<String> reasons = List.of("Test validation reason 1", "Test validation reason 2", "Test validation reason 3");
        IngestJob job = createJob(1, 2);
        return List.of(jobStatus(job,
                rejectedRun(job, Instant.parse("2023-06-05T17:20:00Z"), reasons)));
    }

    public static IngestJob createJob(int jobNum, int inputFileCount) {
        return IngestJob.builder()
                .id(job(jobNum))
                .files(IntStream.range(1, inputFileCount + 1)
                        .mapToObj(f -> String.format("test%1d.parquet", f))
                        .collect(Collectors.toList()))
                .build();
    }

    public static IngestJobAcceptedStatus acceptedStatusUpdate(IngestJob job, Instant validationTime) {
        return IngestJobAcceptedStatus.from(job, validationTime, defaultUpdateTime(validationTime));
    }

    public static IngestJobRejectedStatus rejectedStatusUpdate(IngestJob job, Instant validationTime) {
        return rejectedStatusUpdate(job, validationTime, null);
    }

    public static IngestJobRejectedStatus rejectedStatusUpdate(IngestJob job, Instant validationTime, String jsonMessage) {
        return IngestJobRejectedStatus.builder().job(job)
                .validationTime(validationTime).updateTime(defaultUpdateTime(validationTime))
                .reasons(List.of("Test validation reason"))
                .jsonMessage(jsonMessage)
                .build();
    }
}
