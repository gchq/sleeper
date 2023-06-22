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
package sleeper.ingest.job.status;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import sleeper.core.record.process.RecordsProcessed;
import sleeper.core.record.process.RecordsProcessedSummary;
import sleeper.ingest.job.IngestJob;

import java.time.Duration;
import java.time.Instant;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static sleeper.ingest.job.IngestJobTestData.createJobWithTableAndFiles;
import static sleeper.ingest.job.status.IngestJobStartedData.ingestJobStarted;
import static sleeper.ingest.job.status.IngestJobStatusTestData.acceptedRun;
import static sleeper.ingest.job.status.IngestJobStatusTestData.acceptedRunWhichStarted;
import static sleeper.ingest.job.status.IngestJobStatusTestData.finishedIngestRun;
import static sleeper.ingest.job.status.IngestJobStatusTestData.jobStatus;
import static sleeper.ingest.job.status.IngestJobStatusTestData.rejectedRun;
import static sleeper.ingest.job.status.IngestJobStatusTestData.startedIngestRun;

public class WriteToMemoryIngestJobStatusStoreTest {

    private final WriteToMemoryIngestJobStatusStore store = new WriteToMemoryIngestJobStatusStore();

    @Test
    public void shouldReturnOneStartedJobWithNoFiles() {
        String tableName = "test-table";
        String taskId = "test-task";
        Instant startTime = Instant.parse("2022-09-22T12:00:14.000Z");
        IngestJob job = createJobWithTableAndFiles("test-job", tableName);

        store.jobStarted(ingestJobStarted(taskId, job, startTime));
        assertThat(store.getAllJobs(tableName)).containsExactly(
                jobStatus(job, startedIngestRun(job, taskId, startTime)));
    }

    @Test
    public void shouldReturnOneStartedJobWithFiles() {
        String tableName = "test-table";
        String taskId = "test-task";
        Instant startTime = Instant.parse("2022-09-22T12:00:14.000Z");
        IngestJob job = createJobWithTableAndFiles("test-job", tableName, "test-file-1.parquet", "test-file-2.parquet");

        store.jobStarted(ingestJobStarted(taskId, job, startTime));
        assertThat(store.getAllJobs(tableName)).containsExactly(
                jobStatus(job, startedIngestRun(job, taskId, startTime)));
    }

    @Test
    public void shouldReturnOneFinishedJobWithFiles() {
        String tableName = "test-table";
        String taskId = "test-task";
        Instant startTime = Instant.parse("2022-09-22T12:00:14.000Z");
        Instant finishTime = Instant.parse("2022-09-22T12:00:44.000Z");
        IngestJob job = createJobWithTableAndFiles("test-job", tableName, "test-file-1.parquet", "test-file-2.parquet");
        RecordsProcessedSummary summary = new RecordsProcessedSummary(
                new RecordsProcessed(200L, 200L), startTime, finishTime);

        store.jobStarted(ingestJobStarted(taskId, job, startTime));
        store.jobFinished(taskId, job, summary);
        assertThat(store.getAllJobs(tableName)).containsExactly(
                jobStatus(job, finishedIngestRun(job, taskId, summary)));
    }

    @Test
    public void shouldRefuseJobFinishedWhenNotStarted() {
        String tableName = "test-table";
        String taskId = "test-task";
        Instant startTime = Instant.parse("2022-09-22T12:00:14.000Z");
        Instant finishTime = Instant.parse("2022-09-22T12:00:44.000Z");
        IngestJob job = createJobWithTableAndFiles("test-job", tableName, "test-file-1.parquet", "test-file-2.parquet");
        RecordsProcessedSummary summary = new RecordsProcessedSummary(
                new RecordsProcessed(200L, 200L), startTime, finishTime);

        assertThatThrownBy(() -> store.jobFinished(taskId, job, summary))
                .isInstanceOf(IllegalStateException.class);
    }

    @Test
    public void shouldReturnTwoRunsOnSameJob() {
        String tableName = "test-table";
        String taskId = "test-task";
        IngestJob job = createJobWithTableAndFiles("test-job", tableName, "test-file-1.parquet", "test-file-2.parquet");
        Instant startTime1 = Instant.parse("2022-09-22T12:00:15.000Z");
        Instant startTime2 = Instant.parse("2022-09-22T12:00:31.000Z");
        RecordsProcessedSummary summary1 = new RecordsProcessedSummary(
                new RecordsProcessed(100L, 100L), startTime1, Duration.ofSeconds(15));
        RecordsProcessedSummary summary2 = new RecordsProcessedSummary(
                new RecordsProcessed(200L, 200L), startTime2, Duration.ofSeconds(30));

        store.jobStarted(ingestJobStarted(taskId, job, startTime1));
        store.jobFinished(taskId, job, summary1);
        store.jobStarted(ingestJobStarted(taskId, job, startTime2));
        store.jobFinished(taskId, job, summary2);

        assertThat(store.getAllJobs(tableName)).containsExactly(
                jobStatus(job,
                        finishedIngestRun(job, taskId, summary2),
                        finishedIngestRun(job, taskId, summary1)));
    }

    @Test
    public void shouldReturnTwoJobs() {
        String tableName = "test-table";
        String taskId = "test-task";
        IngestJob job1 = createJobWithTableAndFiles("test-job-1", tableName, "test-file-1.parquet", "test-file-2.parquet");
        IngestJob job2 = createJobWithTableAndFiles("test-job-2", tableName, "test-file-3.parquet");
        Instant startTime1 = Instant.parse("2022-09-22T12:00:15.000Z");
        Instant startTime2 = Instant.parse("2022-09-22T12:00:31.000Z");
        RecordsProcessedSummary summary1 = new RecordsProcessedSummary(
                new RecordsProcessed(100L, 100L), startTime1, Duration.ofSeconds(15));
        RecordsProcessedSummary summary2 = new RecordsProcessedSummary(
                new RecordsProcessed(200L, 200L), startTime2, Duration.ofSeconds(30));

        store.jobStarted(ingestJobStarted(taskId, job1, startTime1));
        store.jobFinished(taskId, job1, summary1);
        store.jobStarted(ingestJobStarted(taskId, job2, startTime2));
        store.jobFinished(taskId, job2, summary2);

        assertThat(store.getAllJobs(tableName)).containsExactly(
                jobStatus(job2, finishedIngestRun(job2, taskId, summary2)),
                jobStatus(job1, finishedIngestRun(job1, taskId, summary1)));
    }

    @Test
    public void shouldReturnJobsWithCorrectTableName() {
        // Given
        String tableName1 = "test-table-1";
        String tableName2 = "test-table-2";
        String taskId = "test-task";
        IngestJob job1 = createJobWithTableAndFiles("test-job-1", tableName1, "test-file-1.parquet", "test-file-2.parquet");
        IngestJob job2 = createJobWithTableAndFiles("test-job-2", tableName2, "test-file-3.parquet");
        Instant startTime1 = Instant.parse("2022-09-22T12:00:15.000Z");
        Instant startTime2 = Instant.parse("2022-09-22T12:00:31.000Z");
        RecordsProcessedSummary summary1 = new RecordsProcessedSummary(
                new RecordsProcessed(100L, 100L), startTime1, Duration.ofSeconds(15));
        RecordsProcessedSummary summary2 = new RecordsProcessedSummary(
                new RecordsProcessed(200L, 200L), startTime2, Duration.ofSeconds(30));

        // When
        store.jobStarted(ingestJobStarted(taskId, job1, startTime1));
        store.jobFinished(taskId, job1, summary1);
        store.jobStarted(ingestJobStarted(taskId, job2, startTime2));
        store.jobFinished(taskId, job2, summary2);

        // Then
        assertThat(store.getAllJobs(tableName2)).containsExactly(
                jobStatus(job2, finishedIngestRun(job2, taskId, summary2)));
        assertThat(store.getAllJobs(tableName1)).containsExactly(
                jobStatus(job1, finishedIngestRun(job1, taskId, summary1)));
    }

    @Test
    public void shouldReturnNoJobsIfTableHasNoJobs() {
        assertThat(store.getAllJobs("table-with-no-jobs")).isEmpty();
    }

    @Test
    public void shouldReturnJobsWithSameIdOnDifferentTables() {
        String tableName1 = "test-table-1";
        String tableName2 = "test-table-2";
        String taskId = "test-task";
        IngestJob job1 = createJobWithTableAndFiles("test-job", tableName1, "test-file-1.parquet", "test-file-2.parquet");
        IngestJob job2 = createJobWithTableAndFiles("test-job", tableName2, "test-file-3.parquet");
        Instant startTime1 = Instant.parse("2022-09-22T12:00:15.000Z");
        Instant startTime2 = Instant.parse("2022-09-22T12:00:31.000Z");

        // When
        store.jobStarted(ingestJobStarted(taskId, job1, startTime1));
        store.jobStarted(ingestJobStarted(taskId, job2, startTime2));

        // Then
        assertThat(store.getAllJobs(tableName2)).containsExactly(
                jobStatus(job2, startedIngestRun(job2, taskId, startTime2)));
        assertThat(store.getAllJobs(tableName1)).containsExactly(
                jobStatus(job1, startedIngestRun(job1, taskId, startTime1)));
    }

    @Nested
    @DisplayName("Report validation of job")
    class ReportValidationStatus {
        @Test
        void shouldReportUnstartedJobWithNoValidationFailures() {
            // Given
            String tableName = "test-table";
            String taskId = "test-task";
            IngestJob job = createJobWithTableAndFiles("test-job-1", tableName, "test-file-1.parquet");
            Instant validationTime = Instant.parse("2022-09-22T12:00:10.000Z");

            // When
            store.jobAccepted(taskId, job, validationTime);

            // Then
            assertThat(store.getAllJobs(tableName))
                    .containsExactly(jobStatus(job, acceptedRun(taskId, validationTime)));
        }

        @Test
        void shouldReportStartedJobWithNoValidationFailures() {
            // Given
            String tableName = "test-table";
            String taskId = "test-task";
            IngestJob job = createJobWithTableAndFiles("test-job-1", tableName, "test-file-1.parquet");
            Instant validationTime = Instant.parse("2022-09-22T12:00:10.000Z");
            Instant startTime = Instant.parse("2022-09-22T12:00:15.000Z");

            // When
            store.jobAccepted(taskId, job, validationTime);
            store.jobStarted(IngestJobStartedData.builder()
                    .taskId(taskId)
                    .job(job)
                    .startTime(startTime)
                    .startOfRun(false)
                    .build());

            // Then
            assertThat(store.getAllJobs(tableName))
                    .containsExactly(jobStatus(job, acceptedRunWhichStarted(job, taskId,
                            validationTime, startTime)));
        }

        @Test
        void shouldReportJobWithValidationFailures() {
            // Given
            String tableName = "test-table";
            String taskId = "test-task";
            IngestJob job = createJobWithTableAndFiles("test-job-1", tableName, "test-file-1.parquet");
            Instant validationTime = Instant.parse("2022-09-22T12:00:10.000Z");

            // When
            store.jobRejected(taskId, job, validationTime, "Test validation reason");

            // Then
            assertThat(store.getAllJobs(tableName))
                    .containsExactly(jobStatus(job, rejectedRun(taskId, validationTime, "Test validation reason")));
        }
    }
}
