/*
 * Copyright 2022-2023 Crown Copyright
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

import sleeper.clients.status.report.StatusReporterTestHelper;
import sleeper.ingest.job.IngestJob;
import sleeper.ingest.job.status.IngestJobStatus;

import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static sleeper.core.record.process.RecordsProcessedSummaryTestData.summary;
import static sleeper.ingest.job.status.IngestJobStatusTestData.finishedIngestJob;
import static sleeper.ingest.job.status.IngestJobStatusTestData.finishedIngestRun;
import static sleeper.ingest.job.status.IngestJobStatusTestData.jobStatus;
import static sleeper.ingest.job.status.IngestJobStatusTestData.startedIngestJob;
import static sleeper.ingest.job.status.IngestJobStatusTestData.startedIngestRun;

public class IngestJobStatusReporterTestData {
    private IngestJobStatusReporterTestData() {
    }

    public static IngestQueueMessages ingestMessageCount(int messages) {
        return IngestQueueMessages.builder().ingestMessages(messages).build();
    }

    public static List<IngestJobStatus> mixedUnfinishedJobStatuses() {
        return mixedJobStatuses().stream()
                .filter(status -> !status.isFinished())
                .collect(Collectors.toList());
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
                finishedIngestJob(job4, StatusReporterTestHelper.task(2), summary(startTime4, Duration.ofMinutes(1), 600, 300)),
                startedIngestJob(job3, StatusReporterTestHelper.task(2), startTime3),
                finishedIngestJob(job2, StatusReporterTestHelper.task(1), summary(startTime2, Duration.ofMinutes(1), 600, 300)),
                startedIngestJob(job1, StatusReporterTestHelper.task(1), startTime1));
    }

    public static List<IngestJobStatus> jobWithMultipleRuns() {
        IngestJob job = createJob(2, 1);

        return Collections.singletonList(jobStatus(job,
                startedIngestRun(job, StatusReporterTestHelper.task(1), Instant.parse("2022-10-12T10:02:00.001Z")),
                finishedIngestRun(job, StatusReporterTestHelper.task(2), summary(Instant.parse("2022-10-12T10:01:15.001Z"), Duration.ofSeconds(30), 300, 200)),
                finishedIngestRun(job, StatusReporterTestHelper.task(1), summary(Instant.parse("2022-10-12T10:01:00.001Z"), Duration.ofSeconds(20), 300, 200))));
    }

    public static List<IngestJobStatus> jobsWithLargeAndDecimalStatistics() {
        Instant startTime1 = Instant.parse("2022-10-13T10:00:10Z");
        Instant startTime2 = Instant.parse("2022-10-13T12:01:10Z");
        Instant startTime3 = Instant.parse("2022-10-13T14:01:10Z");
        Instant startTime4 = Instant.parse("2022-10-13T14:02:10Z");

        return Arrays.asList(
                finishedIngestJob(createJob(4, 1), "task-id", summary(startTime4, Duration.ofMillis(123_456), 1_234_000L, 1_234_000L)),
                finishedIngestJob(createJob(3, 1), "task-id", summary(startTime3, Duration.ofMinutes(1), 1_000_600L, 500_300L)),
                finishedIngestJob(createJob(2, 1), "task-id", summary(startTime2, Duration.ofHours(2), 1_000_600L, 500_300L)),
                finishedIngestJob(createJob(1, 1), "task-id", summary(startTime1, Duration.ofMillis(123_123), 600_000, 300_000)));
    }

    private static IngestJob createJob(int jobNum, int inputFileCount) {
        return IngestJob.builder()
                .id(StatusReporterTestHelper.job(jobNum))
                .files(IntStream.range(1, inputFileCount + 1)
                        .mapToObj(f -> String.format("test%1d.parquet", f))
                        .collect(Collectors.toList()))
                .build();
    }
}
