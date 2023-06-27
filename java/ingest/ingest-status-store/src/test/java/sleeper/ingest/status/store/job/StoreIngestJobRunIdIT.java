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
package sleeper.ingest.status.store.job;

import org.junit.jupiter.api.Test;

import sleeper.core.record.process.RecordsProcessedSummary;
import sleeper.ingest.job.IngestJob;
import sleeper.ingest.status.store.testutils.DynamoDBIngestJobStatusStoreTestBase;

import java.time.Duration;
import java.time.Instant;

import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.core.record.process.RecordsProcessedSummaryTestData.summary;
import static sleeper.ingest.job.IngestJobTestData.createJobWithTableAndFiles;
import static sleeper.ingest.job.status.IngestJobFinishedEvent.ingestJobFinished;
import static sleeper.ingest.job.status.IngestJobStartedEvent.validatedIngestJobStarted;
import static sleeper.ingest.job.status.IngestJobStatusTestData.acceptedRun;
import static sleeper.ingest.job.status.IngestJobStatusTestData.acceptedRunWhichFinished;
import static sleeper.ingest.job.status.IngestJobStatusTestData.acceptedRunWhichStarted;
import static sleeper.ingest.job.status.IngestJobStatusTestData.jobStatus;
import static sleeper.ingest.job.status.IngestJobValidatedEvent.ingestJobAccepted;

public class StoreIngestJobRunIdIT extends DynamoDBIngestJobStatusStoreTestBase {
    @Test
    void shouldReportAcceptedJob() {
        // Given
        String tableName = "test-table";
        IngestJob job = createJobWithTableAndFiles("test-job-1", tableName, "test-file-1.parquet");
        Instant validationTime = Instant.parse("2022-09-22T12:00:10.000Z");

        // When
        store.jobValidated(ingestJobAccepted(job, validationTime).jobRunId("test-run").build());

        // Then
        assertThat(getAllJobStatuses())
                .usingRecursiveFieldByFieldElementComparator(IGNORE_UPDATE_TIMES)
                .containsExactly(jobStatus(job, acceptedRun(validationTime)));
    }

    @Test
    void shouldReportStartedJob() {
        // Given
        String tableName = "test-table";
        String jobRunId = "test-run";
        String taskId = "test-task";
        IngestJob job = createJobWithTableAndFiles("test-job-1", tableName, "test-file-1.parquet");
        Instant validationTime = Instant.parse("2022-09-22T12:00:10.000Z");
        Instant startTime = Instant.parse("2022-09-22T12:00:15.000Z");

        // When
        store.jobValidated(ingestJobAccepted(job, validationTime).jobRunId(jobRunId).build());
        store.jobStarted(validatedIngestJobStarted(job, startTime).jobRunId(jobRunId).taskId(taskId).build());

        // Then
        assertThat(getAllJobStatuses())
                .usingRecursiveFieldByFieldElementComparator(IGNORE_UPDATE_TIMES)
                .containsExactly(jobStatus(job, acceptedRunWhichStarted(job, taskId,
                        validationTime, startTime)));
    }

    @Test
    void shouldReportFinishedJob() {
        // Given
        String tableName = "test-table";
        String jobRunId = "test-run";
        String taskId = "test-task";
        IngestJob job = createJobWithTableAndFiles("test-job-1", tableName, "test-file-1.parquet");
        Instant validationTime = Instant.parse("2022-09-22T12:00:10.000Z");
        Instant startTime = Instant.parse("2022-09-22T12:00:15.000Z");
        RecordsProcessedSummary summary = summary(startTime, Duration.ofMinutes(10), 100L, 100L);

        // When
        store.jobValidated(ingestJobAccepted(job, validationTime).jobRunId(jobRunId).build());
        store.jobStarted(validatedIngestJobStarted(job, startTime).jobRunId(jobRunId).taskId(taskId).build());
        store.jobFinished(ingestJobFinished(job, summary).taskId(taskId).jobRunId(jobRunId).build());

        // Then
        assertThat(getAllJobStatuses())
                .usingRecursiveFieldByFieldElementComparator(IGNORE_UPDATE_TIMES)
                .containsExactly(jobStatus(job, acceptedRunWhichFinished(job, taskId,
                        validationTime, summary)));
    }
}
