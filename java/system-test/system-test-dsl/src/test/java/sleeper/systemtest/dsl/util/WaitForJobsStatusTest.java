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

package sleeper.systemtest.dsl.util;

import org.junit.jupiter.api.Test;

import sleeper.compaction.job.CompactionJob;
import sleeper.compaction.testutils.InMemoryCompactionJobStatusStore;
import sleeper.core.table.TableStatus;
import sleeper.core.table.TableStatusTestHelper;
import sleeper.ingest.job.IngestJob;
import sleeper.ingest.job.status.InMemoryIngestJobStatusStore;
import sleeper.ingest.job.status.IngestJobStatusStore;

import java.time.Duration;
import java.time.Instant;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.compaction.job.status.CompactionJobCommittedEvent.compactionJobCommitted;
import static sleeper.compaction.job.status.CompactionJobFinishedEvent.compactionJobFinished;
import static sleeper.compaction.job.status.CompactionJobStartedEvent.compactionJobStarted;
import static sleeper.core.record.process.RecordsProcessedSummaryTestHelper.summary;
import static sleeper.core.record.process.status.ProcessStatusUpdateTestHelper.defaultUpdateTime;
import static sleeper.ingest.job.IngestJobTestData.createJobWithTableAndFiles;
import static sleeper.ingest.job.status.IngestJobFinishedEvent.ingestJobFinished;
import static sleeper.ingest.job.status.IngestJobStartedEvent.ingestJobStarted;
import static sleeper.ingest.job.status.IngestJobStartedEvent.validatedIngestJobStarted;
import static sleeper.ingest.job.status.IngestJobValidatedEvent.ingestJobAccepted;

public class WaitForJobsStatusTest {

    private final TableStatus table = TableStatusTestHelper.uniqueIdAndName("test-table-id", "test-table");
    private final InMemoryCompactionJobStatusStore store = new InMemoryCompactionJobStatusStore();

    @Test
    void shouldReportSeveralBulkImportJobs() {
        // Given
        IngestJobStatusStore store = new InMemoryIngestJobStatusStore();
        IngestJob acceptedJob = createJobWithTableAndFiles("accepted-job", table, "test.parquet", "test2.parquet");
        IngestJob startedJob = createJobWithTableAndFiles("started-job", table, "test3.parquet", "test4.parquet");
        IngestJob finishedJob = createJobWithTableAndFiles("finished-job", table, "test3.parquet", "test4.parquet");
        store.jobValidated(ingestJobAccepted(acceptedJob, Instant.parse("2022-09-22T13:33:10Z")).jobRunId("accepted-run").build());
        store.jobValidated(ingestJobAccepted(startedJob, Instant.parse("2022-09-22T13:33:11Z")).jobRunId("started-run").build());
        store.jobValidated(ingestJobAccepted(finishedJob, Instant.parse("2022-09-22T13:33:12Z")).jobRunId("finished-run").build());
        store.jobStarted(validatedIngestJobStarted(startedJob, Instant.parse("2022-09-22T13:33:31Z")).jobRunId("started-run").taskId("started-task").build());
        store.jobStarted(validatedIngestJobStarted(finishedJob, Instant.parse("2022-09-22T13:33:32Z")).jobRunId("finished-run").taskId("finished-task").build());
        store.jobFinished(ingestJobFinished(finishedJob,
                summary(Instant.parse("2022-09-22T13:33:32Z"), Instant.parse("2022-09-22T13:35:32Z"), 100L, 100L))
                .jobRunId("finished-run").taskId("finished-task").numFilesWrittenByJob(2).build());

        // When
        WaitForJobsStatus status = WaitForJobsStatus.forIngest(store,
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
        CompactionJob startedJob = compactionJob("started-job", "3.parquet", "4.parquet");
        CompactionJob uncommittedJob = compactionJob("uncommitted-job", "5.parquet", "6.parquet");
        CompactionJob finishedJob = compactionJob("finished-job", "7.parquet", "8.parquet");
        store.fixUpdateTime(Instant.parse("2023-09-18T14:47:00Z"));
        store.jobCreated(createdJob);
        store.jobCreated(startedJob);
        store.jobCreated(uncommittedJob);
        store.jobCreated(finishedJob);
        store.fixUpdateTime(Instant.parse("2023-09-18T14:48:03Z"));
        store.jobStarted(compactionJobStarted(startedJob, Instant.parse("2023-09-18T14:48:00Z")).taskId("started-task").build());
        store.jobStarted(compactionJobStarted(uncommittedJob, Instant.parse("2023-09-18T14:48:01Z")).taskId("finished-task-1").build());
        store.jobStarted(compactionJobStarted(finishedJob, Instant.parse("2023-09-18T14:48:02Z")).taskId("finished-task-2").build());
        store.fixUpdateTime(Instant.parse("2023-09-18T14:48:05Z"));
        store.jobFinished(compactionJobFinished(uncommittedJob,
                summary(Instant.parse("2023-09-18T14:48:01Z"), Instant.parse("2023-09-18T14:50:01Z"), 100L, 100L))
                .taskId("finished-task-1").build());
        store.jobFinished(compactionJobFinished(finishedJob,
                summary(Instant.parse("2023-09-18T14:48:02Z"), Instant.parse("2023-09-18T14:50:02Z"), 100L, 100L))
                .taskId("finished-task-2").build());
        store.fixUpdateTime(Instant.parse("2023-09-18T14:50:10Z"));
        store.jobCommitted(compactionJobCommitted(finishedJob, Instant.parse("2023-09-18T14:50:06Z")).taskId("finished-task-2").build());
        // When
        WaitForJobsStatus status = WaitForJobsStatus.forCompaction(store,
                List.of("created-job", "started-job", "uncommitted-job", "finished-job"),
                Instant.parse("2023-09-18T14:50:01Z"));

        // Then
        assertThat(status).hasToString("{\n" +
                "  \"countByFurthestStatus\": {\n" +
                "    \"FINISHED\": 1,\n" +
                "    \"IN_PROGRESS\": 1,\n" +
                "    \"PENDING\": 1,\n" +
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
        WaitForJobsStatus status = WaitForJobsStatus.forCompaction(store,
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
        WaitForJobsStatus status = WaitForJobsStatus.forCompaction(store,
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
        WaitForJobsStatus status = WaitForJobsStatus.forCompaction(store,
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
        IngestJobStatusStore store = new InMemoryIngestJobStatusStore();
        IngestJob job = createJobWithTableAndFiles("test-job", table, "test.parquet");
        store.jobStarted(ingestJobStarted(job, Instant.parse("2024-06-27T09:40:00Z")).jobRunId("test-run").taskId("test-task").build());
        store.jobFinished(ingestJobFinished(job, summary(Instant.parse("2024-06-27T09:40:00Z"), Duration.ofMinutes(2), 100L, 100L))
                .jobRunId("test-run").taskId("test-task").numFilesWrittenByJob(2)
                .committedBySeparateFileUpdates(true).build());

        // When
        WaitForJobsStatus status = WaitForJobsStatus.forIngest(store,
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

    private CompactionJob compactionJob(String id, String... files) {
        return CompactionJob.builder()
                .tableId("test-table-id")
                .jobId(id)
                .inputFiles(List.of(files))
                .outputFile(id + "/outputFile")
                .partitionId(id + "-partition").build();
    }

    private void addCreatedJob(CompactionJob job, Instant createdTime) {
        store.fixUpdateTime(defaultUpdateTime(createdTime));
        store.jobCreated(job);
    }

    private void addUnfinishedRun(CompactionJob job, Instant startTime, String taskId) {
        store.fixUpdateTime(defaultUpdateTime(startTime));
        store.jobStarted(compactionJobStarted(job, startTime).taskId(taskId).build());
    }

    private void addFinishedRun(CompactionJob job, Instant startTime, Instant finishTime, String taskId) {
        store.fixUpdateTime(defaultUpdateTime(startTime));
        store.jobStarted(compactionJobStarted(job, startTime).taskId(taskId).build());

        store.fixUpdateTime(defaultUpdateTime(finishTime));
        store.jobFinished(compactionJobFinished(job,
                summary(startTime, finishTime, 100L, 100L))
                .taskId(taskId).build());
        Instant commitTime = finishTime.plus(Duration.ofMinutes(1));
        store.fixUpdateTime(defaultUpdateTime(commitTime));
        store.jobCommitted(compactionJobCommitted(job, commitTime).taskId(taskId).build());
    }
}
