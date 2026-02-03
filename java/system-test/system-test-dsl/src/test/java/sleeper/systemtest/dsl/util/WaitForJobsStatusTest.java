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

package sleeper.systemtest.dsl.util;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import sleeper.compaction.core.job.CompactionJob;
import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.table.TableProperties;
import sleeper.core.table.TableStatus;
import sleeper.core.tracker.compaction.job.InMemoryCompactionJobTracker;
import sleeper.core.tracker.ingest.job.InMemoryIngestJobTracker;
import sleeper.core.tracker.ingest.job.IngestJobTracker;
import sleeper.ingest.core.job.IngestJob;

import java.time.Duration;
import java.time.Instant;
import java.util.Collection;
import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.core.properties.testutils.InstancePropertiesTestHelper.createTestInstanceProperties;
import static sleeper.core.properties.testutils.TablePropertiesTestHelper.createTestTableProperties;
import static sleeper.core.schema.SchemaTestHelper.createSchemaWithKey;
import static sleeper.core.tracker.job.run.JobRunSummaryTestHelper.summary;
import static sleeper.core.tracker.job.status.JobStatusUpdateTestHelper.defaultUpdateTime;
import static sleeper.ingest.core.job.IngestJobTestData.createJobWithTableAndFiles;

public class WaitForJobsStatusTest {

    InstanceProperties instanceProperties = createTestInstanceProperties();
    TableProperties tableProperties = createTestTableProperties(instanceProperties, createSchemaWithKey("key"));
    TableStatus table = tableProperties.getStatus();
    InMemoryCompactionJobTracker compactionTracker = new InMemoryCompactionJobTracker();
    IngestJobTracker ingestTracker = new InMemoryIngestJobTracker();
    int maxFailureReasons = 10;

    @Test
    void shouldReportSeveralBulkImportJobs() {
        // Given
        IngestJob acceptedJob = createJobWithTableAndFiles("accepted-job", table, "test.parquet", "test2.parquet");
        IngestJob startedJob = createJobWithTableAndFiles("started-job", table, "test3.parquet", "test4.parquet");
        IngestJob finishedJob = createJobWithTableAndFiles("finished-job", table, "test3.parquet", "test4.parquet");
        ingestTracker.jobValidated(acceptedJob.acceptedEventBuilder(Instant.parse("2022-09-22T13:33:10Z")).jobRunId("accepted-run").build());
        ingestTracker.jobValidated(startedJob.acceptedEventBuilder(Instant.parse("2022-09-22T13:33:11Z")).jobRunId("started-run").build());
        ingestTracker.jobValidated(finishedJob.acceptedEventBuilder(Instant.parse("2022-09-22T13:33:12Z")).jobRunId("finished-run").build());
        ingestTracker.jobStarted(startedJob.startedAfterValidationEventBuilder(Instant.parse("2022-09-22T13:33:31Z")).jobRunId("started-run").taskId("started-task").build());
        ingestTracker.jobStarted(finishedJob.startedAfterValidationEventBuilder(Instant.parse("2022-09-22T13:33:32Z")).jobRunId("finished-run").taskId("finished-task").build());
        ingestTracker.jobFinished(finishedJob.finishedEventBuilder(
                summary(Instant.parse("2022-09-22T13:33:32Z"), Instant.parse("2022-09-22T13:35:32Z"), 100L, 100L))
                .jobRunId("finished-run").taskId("finished-task").numFilesWrittenByJob(2).build());

        // When
        WaitForJobsStatus status = ingestStatus(
                List.of("accepted-job", "started-job", "finished-job"),
                Instant.parse("2022-09-22T13:34:00Z"));

        // Then
        assertThat(status).hasToString("{\n" +
                "  \"countByFurthestStatus\": {\n" +
                "    \"ACCEPTED\": 1,\n" +
                "    \"FINISHED\": 1,\n" +
                "    \"IN_PROGRESS\": 1\n" +
                "  },\n" +
                "  \"numUnfinished\": 2,\n" +
                "  \"firstInProgressStartTime\": \"2022-09-22T13:33:10Z\",\n" +
                "  \"longestInProgressDuration\": \"PT50S\"\n" +
                "}");
        assertThat(status.areAllJobsFinished()).isFalse();
    }

    @Test
    void shouldReportSeveralCompactionJobs() {
        // Given
        CompactionJob createdJob = compactionJob("created-job", "1.parquet", "2.parquet");
        CompactionJob startedJob = compactionJob("started-job", "x.parquet", "y.parquet");
        CompactionJob uncommittedJob = compactionJob("uncommitted-job", "alpha.parquet", "beta.parquet");
        CompactionJob finishedJob = compactionJob("finished-job", "first.parquet", "second.parquet");
        compactionTracker.fixUpdateTime(Instant.parse("2023-09-18T14:47:00Z"));
        jobsCreated(createdJob, startedJob, uncommittedJob, finishedJob);
        compactionTracker.fixUpdateTime(Instant.parse("2023-09-18T14:48:03Z"));
        compactionTracker.jobStarted(startedJob.startedEventBuilder(Instant.parse("2023-09-18T14:48:00Z")).taskId("started-task").jobRunId("started-run").build());
        compactionTracker.jobStarted(uncommittedJob.startedEventBuilder(Instant.parse("2023-09-18T14:48:01Z")).taskId("finished-task-1").jobRunId("finished-run-1").build());
        compactionTracker.jobStarted(finishedJob.startedEventBuilder(Instant.parse("2023-09-18T14:48:02Z")).taskId("finished-task-2").jobRunId("finished-run-2").build());
        compactionTracker.fixUpdateTime(Instant.parse("2023-09-18T14:48:05Z"));
        compactionTracker.jobFinished(uncommittedJob.finishedEventBuilder(
                summary(Instant.parse("2023-09-18T14:48:01Z"), Instant.parse("2023-09-18T14:50:01Z"), 100L, 100L))
                .taskId("finished-task-1").jobRunId("finished-run-1").build());
        compactionTracker.jobFinished(finishedJob.finishedEventBuilder(
                summary(Instant.parse("2023-09-18T14:48:02Z"), Instant.parse("2023-09-18T14:50:02Z"), 100L, 100L))
                .taskId("finished-task-2").jobRunId("finished-run-2").build());
        compactionTracker.fixUpdateTime(Instant.parse("2023-09-18T14:50:10Z"));
        compactionTracker.jobCommitted(finishedJob.committedEventBuilder(Instant.parse("2023-09-18T14:50:06Z")).taskId("finished-task-2").jobRunId("finished-run-2").build());
        // When
        WaitForJobsStatus status = allCompactionsStatus(Instant.parse("2023-09-18T14:50:01Z"));

        // Then
        assertThat(status).hasToString("{\n" +
                "  \"countByFurthestStatus\": {\n" +
                "    \"CREATED\": 1,\n" +
                "    \"FINISHED\": 1,\n" +
                "    \"IN_PROGRESS\": 1,\n" +
                "    \"UNCOMMITTED\": 1\n" +
                "  },\n" +
                "  \"numUnstarted\": 1,\n" +
                "  \"numUnfinished\": 3,\n" +
                "  \"firstInProgressStartTime\": \"2023-09-18T14:48:00Z\",\n" +
                "  \"longestInProgressDuration\": \"PT2M1S\"\n" +
                "}");
        assertThat(status.areAllJobsFinished()).isFalse();
    }

    @Test
    void shouldReportCompactionJobWithUnfinishedRunThenFinishedRun() {
        // Given
        CompactionJob jobRunTwice = compactionJob("finished-job", "5.parquet", "6.parquet");
        addCreatedJob(jobRunTwice, Instant.parse("2023-09-18T14:47:00Z"));
        // First run
        addUnfinishedRun(jobRunTwice, Instant.parse("2023-09-18T14:48:00Z"), "test-task");
        // Second run
        addFinishedRun(jobRunTwice, Instant.parse("2023-09-18T14:49:00Z"), Instant.parse("2023-09-18T14:51:00Z"), "test-task");

        // When
        WaitForJobsStatus status = compactionStatus(
                List.of("finished-job"),
                Instant.parse("2023-09-18T14:50:00Z"));

        // Then
        assertThat(status).hasToString("{\n" +
                "  \"countByFurthestStatus\": {\n" +
                "    \"FINISHED\": 1\n" +
                "  },\n" +
                "  \"numUnfinished\": 0\n" +
                "}");
        assertThat(status.areAllJobsFinished()).isTrue();
    }

    @Test
    void shouldReportCompactionJobWithFinishedRunThenUnfinishedRun() {
        // Given
        CompactionJob jobRunTwice = compactionJob("finished-job", "5.parquet", "6.parquet");
        addCreatedJob(jobRunTwice, Instant.parse("2023-09-18T14:47:00Z"));
        // First run
        addFinishedRun(jobRunTwice, Instant.parse("2023-09-18T14:48:00Z"), Instant.parse("2023-09-18T14:50:00Z"), "task-1");
        // Second run
        addUnfinishedRun(jobRunTwice, Instant.parse("2023-09-18T14:51:00Z"), "task-2");

        // When
        WaitForJobsStatus status = compactionStatus(
                List.of("finished-job"),
                Instant.parse("2023-09-18T14:52:00Z"));

        // Then
        assertThat(status).hasToString("{\n" +
                "  \"countByFurthestStatus\": {\n" +
                "    \"FINISHED\": 1\n" +
                "  },\n" +
                "  \"numUnfinished\": 0\n" +
                "}");
        assertThat(status.areAllJobsFinished()).isTrue();
    }

    @Test
    void shouldReportTwoCompactionJobsOneFinishedWithTwoRunsThenOneInProgress() {
        // Given
        CompactionJob finishedJob = compactionJob("finished-job", "5.parquet", "6.parquet");
        CompactionJob inProgressJob = compactionJob("unfinished-job", "7.parquet", "8.parquet");
        addCreatedJob(finishedJob, Instant.parse("2023-09-18T14:46:00Z"));
        addCreatedJob(inProgressJob, Instant.parse("2023-09-18T14:46:00Z"));
        // First run
        addUnfinishedRun(finishedJob, Instant.parse("2023-09-18T14:47:00Z"), "test-task");
        // Second run
        addFinishedRun(finishedJob, Instant.parse("2023-09-18T14:48:00Z"), Instant.parse("2023-09-18T14:50:00Z"), "test-task");

        // First run
        addUnfinishedRun(inProgressJob, Instant.parse("2023-09-18T14:51:00Z"), "test-task");

        // When
        WaitForJobsStatus status = compactionStatus(
                List.of("finished-job", "unfinished-job"),
                Instant.parse("2023-09-18T14:52:00Z"));

        // Then
        assertThat(status).hasToString("{\n" +
                "  \"countByFurthestStatus\": {\n" +
                "    \"FINISHED\": 1,\n" +
                "    \"IN_PROGRESS\": 1\n" +
                "  },\n" +
                "  \"numUnfinished\": 1,\n" +
                "  \"firstInProgressStartTime\": \"2023-09-18T14:51:00Z\",\n" +
                "  \"longestInProgressDuration\": \"PT1M\"\n" +
                "}");
        assertThat(status.areAllJobsFinished()).isFalse();
    }

    @Test
    void shouldReportIngestJobUnfinishedWhenUncommitted() {
        // Given
        IngestJob job = createJobWithTableAndFiles("test-job", table, "test.parquet");
        ingestTracker.jobStarted(job.startedEventBuilder(Instant.parse("2024-06-27T09:40:00Z")).jobRunId("test-run").taskId("test-task").build());
        ingestTracker.jobFinished(job.finishedEventBuilder(summary(Instant.parse("2024-06-27T09:40:00Z"), Duration.ofMinutes(2), 100L, 100L))
                .jobRunId("test-run").taskId("test-task").numFilesWrittenByJob(2)
                .committedBySeparateFileUpdates(true).build());

        // When
        WaitForJobsStatus status = ingestStatus(
                List.of("test-job"),
                Instant.parse("2024-06-27T09:43:00Z"));

        // Then
        assertThat(status).hasToString("{\n" +
                "  \"countByFurthestStatus\": {\n" +
                "    \"UNCOMMITTED\": 1\n" +
                "  },\n" +
                "  \"numUnfinished\": 1,\n" +
                "  \"firstInProgressStartTime\": \"2024-06-27T09:40:00Z\",\n" +
                "  \"longestInProgressDuration\": \"PT3M\"\n" +
                "}");
        assertThat(status.areAllJobsFinished()).isFalse();
    }

    @Test
    void shouldReportIngestJobUnstarted() {
        // When
        WaitForJobsStatus status = ingestStatus(
                List.of("test-job"),
                Instant.parse("2024-06-27T09:43:00Z"));

        // Then
        assertThat(status).hasToString("{\n" +
                "  \"countByFurthestStatus\": {\n" +
                "    \"NONE\": 1\n" +
                "  },\n" +
                "  \"numUnstarted\": 1,\n" +
                "  \"numUnfinished\": 1\n" +
                "}");
        assertThat(status.areAllJobsFinished()).isFalse();
    }

    @Test
    void shouldReportMultipleIngestJobsUnstarted() {
        // When
        WaitForJobsStatus status = ingestStatus(
                List.of("job-1", "job-2"),
                Instant.parse("2024-06-27T09:43:00Z"));

        // Then
        assertThat(status).hasToString("{\n" +
                "  \"countByFurthestStatus\": {\n" +
                "    \"NONE\": 2\n" +
                "  },\n" +
                "  \"numUnstarted\": 2,\n" +
                "  \"numUnfinished\": 2\n" +
                "}");
        assertThat(status.areAllJobsFinished()).isFalse();
    }

    @Test
    void shouldReportIngestJobStartedAndUnstarted() {
        // Given
        IngestJob job = createJobWithTableAndFiles("job-1", table, "test.parquet");
        ingestTracker.jobStarted(job.startedEventBuilder(Instant.parse("2024-06-27T09:40:00Z")).jobRunId("test-run").taskId("test-task").build());

        // When
        WaitForJobsStatus status = ingestStatus(
                List.of("job-1", "job-2"),
                Instant.parse("2024-06-27T09:43:00Z"));

        // Then
        assertThat(status).hasToString("{\n" +
                "  \"countByFurthestStatus\": {\n" +
                "    \"IN_PROGRESS\": 1,\n" +
                "    \"NONE\": 1\n" +
                "  },\n" +
                "  \"numUnstarted\": 1,\n" +
                "  \"numUnfinished\": 2,\n" +
                "  \"firstInProgressStartTime\": \"2024-06-27T09:40:00Z\",\n" +
                "  \"longestInProgressDuration\": \"PT3M\"\n" +
                "}");
        assertThat(status.areAllJobsFinished()).isFalse();
    }

    @Nested
    @DisplayName("Display failure reasons")
    class FailureReasons {

        @Test
        void shouldReportFailureReason() {
            // Given
            IngestJob job = createJobWithTableAndFiles("job-1", table, "test.parquet");
            ingestTracker.jobStarted(job.startedEventBuilder(Instant.parse("2024-06-27T09:40:00Z")).jobRunId("test-run").taskId("test-task").build());
            ingestTracker.jobFailed(job.failedEventBuilder(Instant.parse("2024-06-27T09:40:01Z")).jobRunId("test-run").taskId("test-task")
                    .failureReasons(List.of("Some failure")).build());

            // When
            WaitForJobsStatus status = ingestStatus(
                    List.of("job-1"),
                    Instant.parse("2024-06-27T09:43:00Z"));

            // Then
            assertThat(status).hasToString("{\n" +
                    "  \"countByFurthestStatus\": {\n" +
                    "    \"FAILED\": 1\n" +
                    "  },\n" +
                    "  \"failureReasons\": [\n" +
                    "    \"Some failure\"\n" +
                    "  ],\n" +
                    "  \"numUnfinished\": 1,\n" +
                    "  \"firstInProgressStartTime\": \"2024-06-27T09:40:00Z\",\n" +
                    "  \"longestInProgressDuration\": \"PT3M\"\n" +
                    "}");
            assertThat(status.areAllJobsFinished()).isFalse();
        }

        @Test
        void shouldRestrictNumberOfFailureReasonsInOneJob() {
            // Given
            maxFailureReasons = 2;
            IngestJob job = createJobWithTableAndFiles("job-1", table, "test.parquet");
            ingestTracker.jobStarted(job.startedEventBuilder(Instant.parse("2024-06-27T09:40:00Z")).jobRunId("test-run").taskId("test-task").build());
            ingestTracker.jobFailed(job.failedEventBuilder(Instant.parse("2024-06-27T09:40:01Z")).jobRunId("test-run").taskId("test-task")
                    .failureReasons(List.of("Failure 1", "Failure 2", "Failure 3")).build());

            // When
            WaitForJobsStatus status = ingestStatus(
                    List.of("job-1"),
                    Instant.parse("2024-06-27T09:43:00Z"));

            // Then
            assertThat(status).hasToString("{\n" +
                    "  \"countByFurthestStatus\": {\n" +
                    "    \"FAILED\": 1\n" +
                    "  },\n" +
                    "  \"failureReasons\": [\n" +
                    "    \"Failure 1\",\n" +
                    "    \"Failure 2\"\n" +
                    "  ],\n" +
                    "  \"numUnfinished\": 1,\n" +
                    "  \"firstInProgressStartTime\": \"2024-06-27T09:40:00Z\",\n" +
                    "  \"longestInProgressDuration\": \"PT3M\"\n" +
                    "}");
            assertThat(status.areAllJobsFinished()).isFalse();
        }

        @Test
        void shouldRestrictNumberOfFailureReasonsAcrossJobs() {
            // Given
            maxFailureReasons = 2;
            IngestJob job1 = createJobWithTableAndFiles("job-1", table, "test.parquet");
            ingestTracker.jobStarted(job1.startedEventBuilder(Instant.parse("2024-06-27T09:40:00Z")).jobRunId("run-1").taskId("test-task").build());
            ingestTracker.jobFailed(job1.failedEventBuilder(Instant.parse("2024-06-27T09:40:01Z")).jobRunId("run-1").taskId("test-task")
                    .failureReasons(List.of("Failure 1")).build());
            IngestJob job2 = createJobWithTableAndFiles("job-2", table, "test.parquet");
            ingestTracker.jobStarted(job2.startedEventBuilder(Instant.parse("2024-06-27T09:40:00Z")).jobRunId("run-2").taskId("test-task").build());
            ingestTracker.jobFailed(job2.failedEventBuilder(Instant.parse("2024-06-27T09:40:01Z")).jobRunId("run-2").taskId("test-task")
                    .failureReasons(List.of("Failure 2", "Failure 3")).build());

            // When
            WaitForJobsStatus status = ingestStatus(
                    List.of("job-1", "job-2"),
                    Instant.parse("2024-06-27T09:43:00Z"));

            // Then
            assertThat(status).hasToString("{\n" +
                    "  \"countByFurthestStatus\": {\n" +
                    "    \"FAILED\": 2\n" +
                    "  },\n" +
                    "  \"failureReasons\": [\n" +
                    "    \"Failure 1\",\n" +
                    "    \"Failure 2\"\n" +
                    "  ],\n" +
                    "  \"numUnfinished\": 2,\n" +
                    "  \"firstInProgressStartTime\": \"2024-06-27T09:40:00Z\",\n" +
                    "  \"longestInProgressDuration\": \"PT3M\"\n" +
                    "}");
            assertThat(status.areAllJobsFinished()).isFalse();
        }

        @Test
        void shouldDisplayFailureReasonsWhenJobRetried() {
            // Given
            IngestJob job = createJobWithTableAndFiles("test-job", table, "test.parquet");
            ingestTracker.jobStarted(job.startedEventBuilder(Instant.parse("2024-06-27T09:40:00Z"))
                    .jobRunId("fail-run").taskId("test-task").build());
            ingestTracker.jobFailed(job.failedEventBuilder(Instant.parse("2024-06-27T09:40:01Z"))
                    .jobRunId("fail-run").taskId("test-task")
                    .failureReasons(List.of("Some failure")).build());
            ingestTracker.jobStarted(job.startedEventBuilder(Instant.parse("2024-06-27T09:41:00Z"))
                    .jobRunId("retry-run").taskId("test-task").build());
            ingestTracker.jobFinished(job.finishedEventBuilder(summary(Instant.parse("2024-06-27T09:41:00Z"), Duration.ofMinutes(2), 100L, 100L))
                    .jobRunId("retry-run").taskId("test-task").numFilesWrittenByJob(2).build());

            // When
            WaitForJobsStatus status = ingestStatus(
                    List.of("test-job"),
                    Instant.parse("2024-06-27T09:44:00Z"));

            // Then
            assertThat(status).hasToString("{\n" +
                    "  \"countByFurthestStatus\": {\n" +
                    "    \"FINISHED\": 1\n" +
                    "  },\n" +
                    "  \"failureReasons\": [\n" +
                    "    \"Some failure\"\n" +
                    "  ],\n" +
                    "  \"numUnfinished\": 0\n" +
                    "}");
        }
    }

    private WaitForJobsStatus compactionStatus(Collection<String> jobIds, Instant now) {
        return WaitForJobsStatus.atTime(now)
                .maxFailureReasons(maxFailureReasons)
                .reportById(jobIds, WaitForJobsStatus.streamCompactionJobs(compactionTracker, List.of(tableProperties)))
                .build();
    }

    private WaitForJobsStatus allCompactionsStatus(Instant now) {
        return WaitForJobsStatus.atTime(now)
                .maxFailureReasons(maxFailureReasons)
                .report(WaitForJobsStatus.streamCompactionJobs(compactionTracker, List.of(tableProperties)))
                .build();
    }

    private WaitForJobsStatus ingestStatus(Collection<String> jobIds, Instant now) {
        return WaitForJobsStatus.atTime(now)
                .maxFailureReasons(maxFailureReasons)
                .reportById(jobIds, WaitForJobsStatus.streamIngestJobs(ingestTracker, List.of(tableProperties)))
                .build();
    }

    private CompactionJob compactionJob(String id, String... files) {
        return CompactionJob.builder()
                .tableId(table.getTableUniqueId())
                .jobId(id)
                .inputFiles(List.of(files))
                .outputFile(id + "/outputFile")
                .partitionId(id + "-partition").build();
    }

    private void addCreatedJob(CompactionJob job, Instant createdTime) {
        compactionTracker.fixUpdateTime(defaultUpdateTime(createdTime));
        compactionTracker.jobCreated(job.createCreatedEvent());
    }

    private void jobsCreated(CompactionJob... jobs) {
        for (CompactionJob job : jobs) {
            compactionTracker.jobCreated(job.createCreatedEvent());
        }
    }

    private void addUnfinishedRun(CompactionJob job, Instant startTime, String taskId) {
        compactionTracker.fixUpdateTime(defaultUpdateTime(startTime));
        compactionTracker.jobStarted(job.startedEventBuilder(startTime).taskId(taskId).jobRunId(UUID.randomUUID().toString()).build());
    }

    private void addFinishedRun(CompactionJob job, Instant startTime, Instant finishTime, String taskId) {
        String jobRunId = UUID.randomUUID().toString();
        compactionTracker.fixUpdateTime(defaultUpdateTime(startTime));
        compactionTracker.jobStarted(job.startedEventBuilder(startTime).taskId(taskId).jobRunId(jobRunId).build());

        compactionTracker.fixUpdateTime(defaultUpdateTime(finishTime));
        compactionTracker.jobFinished(job.finishedEventBuilder(
                summary(startTime, finishTime, 100L, 100L))
                .taskId(taskId).jobRunId(jobRunId).build());
        Instant commitTime = finishTime.plus(Duration.ofMinutes(1));
        compactionTracker.fixUpdateTime(defaultUpdateTime(commitTime));
        compactionTracker.jobCommitted(job.committedEventBuilder(commitTime).taskId(taskId).jobRunId(jobRunId).build());
    }
}
