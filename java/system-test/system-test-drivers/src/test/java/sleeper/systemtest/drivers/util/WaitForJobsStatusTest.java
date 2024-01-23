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

package sleeper.systemtest.drivers.util;

import org.junit.jupiter.api.Test;

import sleeper.compaction.job.CompactionJob;
import sleeper.compaction.testutils.CompactionJobStatusStoreInMemory;
import sleeper.core.table.TableIdentity;
import sleeper.ingest.job.IngestJob;
import sleeper.ingest.job.status.IngestJobStatusStore;
import sleeper.ingest.job.status.WriteToMemoryIngestJobStatusStore;

import java.time.Duration;
import java.time.Instant;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.core.record.process.RecordsProcessedSummaryTestData.summary;
import static sleeper.core.record.process.status.TestRunStatusUpdates.defaultUpdateTime;
import static sleeper.ingest.job.IngestJobTestData.createJobWithTableAndFiles;
import static sleeper.ingest.job.status.IngestJobFinishedEvent.ingestJobFinished;
import static sleeper.ingest.job.status.IngestJobStartedEvent.validatedIngestJobStarted;
import static sleeper.ingest.job.status.IngestJobValidatedEvent.ingestJobAccepted;

public class WaitForJobsStatusTest {

    private final TableIdentity tableId = TableIdentity.uniqueIdAndName("test-table-id", "test-table");
    private final CompactionJobStatusStoreInMemory store = new CompactionJobStatusStoreInMemory();

    @Test
    void shouldReportSeveralBulkImportJobs() {
        // Given
        IngestJobStatusStore store = new WriteToMemoryIngestJobStatusStore();
        IngestJob acceptedJob = createJobWithTableAndFiles("accepted-job", tableId, "test.parquet", "test2.parquet");
        IngestJob startedJob = createJobWithTableAndFiles("started-job", tableId, "test3.parquet", "test4.parquet");
        IngestJob finishedJob = createJobWithTableAndFiles("finished-job", tableId, "test3.parquet", "test4.parquet");
        store.jobValidated(ingestJobAccepted(acceptedJob, Instant.parse("2022-09-22T13:33:10Z")).jobRunId("accepted-run").build());
        store.jobValidated(ingestJobAccepted(startedJob, Instant.parse("2022-09-22T13:33:11Z")).jobRunId("started-run").build());
        store.jobValidated(ingestJobAccepted(finishedJob, Instant.parse("2022-09-22T13:33:12Z")).jobRunId("finished-run").build());
        store.jobStarted(validatedIngestJobStarted(startedJob, Instant.parse("2022-09-22T13:33:31Z")).jobRunId("started-run").taskId("started-task").build());
        store.jobStarted(validatedIngestJobStarted(finishedJob, Instant.parse("2022-09-22T13:33:32Z")).jobRunId("finished-run").taskId("finished-task").build());
        store.jobFinished(ingestJobFinished(finishedJob, summary(Instant.parse("2022-09-22T13:33:32Z"), Duration.ofMinutes(2), 100L, 100L)).jobRunId("finished-run").taskId("finished-task").build());

        // When
        WaitForJobsStatus status = WaitForJobsStatus.forIngest(store,
                List.of("accepted-job", "started-job", "finished-job"),
                Instant.parse("2022-09-22T13:34:00Z"));

        // Then
        assertThat(status).hasToString("{\n" +
                "  \"countByLastStatus\": {\n" +
                "    \"IngestJobAcceptedStatus\": 1,\n" +
                "    \"IngestJobStartedStatus\": 1,\n" +
                "    \"ProcessFinishedStatus\": 1\n" +
                "  },\n" +
                "  \"numUnfinished\": 2,\n" +
                "  \"firstInProgressStartTime\": \"2022-09-22T13:33:10Z\",\n" +
                "  \"longestInProgressDuration\": \"PT50S\"\n" +
                "}");
    }

    @Test
    void shouldReportSeveralCompactionJobs() {
        // Given
        CompactionJob createdJob = compactionJob("created-job", "1.parquet", "2.parquet");
        CompactionJob startedJob = compactionJob("started-job", "3.parquet", "4.parquet");
        CompactionJob finishedJob = compactionJob("finished-job", "5.parquet", "6.parquet");
        store.fixUpdateTime(Instant.parse("2023-09-18T14:47:00Z"));
        store.jobCreated(createdJob);
        store.jobCreated(startedJob);
        store.jobCreated(finishedJob);
        store.fixUpdateTime(Instant.parse("2023-09-18T14:48:02Z"));
        store.jobStarted(finishedJob, Instant.parse("2023-09-18T14:48:00Z"), "finished-task");
        store.jobStarted(startedJob, Instant.parse("2023-09-18T14:48:01Z"), "started-task");
        store.fixUpdateTime(Instant.parse("2023-09-18T14:49:01Z"));
        store.jobFinished(finishedJob, summary(Instant.parse("2023-09-18T14:48:00Z"), Duration.ofMinutes(2), 100L, 100L), "finished-task");

        // When
        WaitForJobsStatus status = WaitForJobsStatus.forCompaction(store,
                List.of("created-job", "started-job", "finished-job"),
                Instant.parse("2023-09-18T14:50:01Z"));

        // Then
        assertThat(status).hasToString("{\n" +
                "  \"countByLastStatus\": {\n" +
                "    \"CompactionJobStartedStatus\": 1,\n" +
                "    \"None\": 1,\n" +
                "    \"ProcessFinishedStatus\": 1\n" +
                "  },\n" +
                "  \"numUnstarted\": 1,\n" +
                "  \"numUnfinished\": 2,\n" +
                "  \"firstInProgressStartTime\": \"2023-09-18T14:48:01Z\",\n" +
                "  \"longestInProgressDuration\": \"PT2M\"\n" +
                "}");
    }

    @Test
    void shouldReportCompactionJobWithUnfinishedRunThenFinishedRun() {
        // Given
        CompactionJob jobRunTwice = compactionJob("finished-job", "5.parquet", "6.parquet");
        addCreatedJob(jobRunTwice, Instant.parse("2023-09-18T14:47:00Z"));
        // First run
        addUnfinishedRun(jobRunTwice, Instant.parse("2023-09-18T14:48:00Z"), "test-task");
        // Second run
        addFinishedRun(jobRunTwice, Instant.parse("2023-09-18T14:49:00Z"), Duration.ofMinutes(2), "test-task");

        // When
        WaitForJobsStatus status = WaitForJobsStatus.forCompaction(store,
                List.of("finished-job"),
                Instant.parse("2023-09-18T14:50:00Z"));

        // Then
        assertThat(status).hasToString("{\n" +
                "  \"countByLastStatus\": {\n" +
                "    \"ProcessFinishedStatus\": 1\n" +
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
        addFinishedRun(jobRunTwice, Instant.parse("2023-09-18T14:48:00Z"), Duration.ofMinutes(2), "task-1");
        // Second run
        addUnfinishedRun(jobRunTwice, Instant.parse("2023-09-18T14:51:00Z"), "task-2");

        // When
        WaitForJobsStatus status = WaitForJobsStatus.forCompaction(store,
                List.of("finished-job"),
                Instant.parse("2023-09-18T14:52:00Z"));

        // Then
        assertThat(status).hasToString("{\n" +
                "  \"countByLastStatus\": {\n" +
                "    \"ProcessFinishedStatus\": 1\n" +
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
        addFinishedRun(finishedJob, Instant.parse("2023-09-18T14:48:00Z"), Duration.ofMinutes(2), "test-task");

        // First run
        addUnfinishedRun(inProgressJob, Instant.parse("2023-09-18T14:51:00Z"), "test-task");

        // When
        WaitForJobsStatus status = WaitForJobsStatus.forCompaction(store,
                List.of("finished-job", "unfinished-job"),
                Instant.parse("2023-09-18T14:52:00Z"));

        // Then
        assertThat(status).hasToString("{\n" +
                "  \"countByLastStatus\": {\n" +
                "    \"CompactionJobStartedStatus\": 1,\n" +
                "    \"ProcessFinishedStatus\": 1\n" +
                "  },\n" +
                "  \"numUnfinished\": 1,\n" +
                "  \"firstInProgressStartTime\": \"2023-09-18T14:51:00Z\",\n" +
                "  \"longestInProgressDuration\": \"PT1M\"\n" +
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
        store.jobStarted(job, startTime, taskId);
    }

    private void addFinishedRun(CompactionJob job, Instant startTime, Duration duration, String taskId) {
        store.fixUpdateTime(defaultUpdateTime(startTime));
        store.jobStarted(job, startTime, taskId);
        Instant finishTime = startTime.plus(duration);
        store.fixUpdateTime(defaultUpdateTime(finishTime));
        store.jobFinished(job, summary(startTime, finishTime, 100L, 100L), taskId);
    }
}
