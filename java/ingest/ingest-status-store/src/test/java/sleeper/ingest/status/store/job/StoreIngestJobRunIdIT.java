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
package sleeper.ingest.status.store.job;

import org.junit.jupiter.api.Test;

import sleeper.core.partition.PartitionsBuilder;
import sleeper.core.record.process.ProcessRunTime;
import sleeper.core.record.process.RecordsProcessedSummary;
import sleeper.core.record.process.status.ProcessRun;
import sleeper.core.statestore.AllReferencesToAFile;
import sleeper.core.statestore.FileReferenceFactory;
import sleeper.core.tracker.ingest.job.query.IngestJobAddedFilesStatus;
import sleeper.core.tracker.ingest.job.query.IngestJobStartedStatus;
import sleeper.ingest.core.job.IngestJob;
import sleeper.ingest.status.store.testutils.DynamoDBIngestJobStatusStoreTestBase;

import java.time.Duration;
import java.time.Instant;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.core.record.process.RecordsProcessedSummaryTestHelper.summary;
import static sleeper.core.record.process.status.ProcessStatusUpdateTestHelper.defaultUpdateTime;
import static sleeper.core.schema.SchemaTestHelper.schemaWithKey;
import static sleeper.core.statestore.AllReferencesToAFileTestHelper.filesWithReferences;
import static sleeper.ingest.core.job.IngestJobTestData.createJobWithTableAndFiles;
import static sleeper.ingest.core.job.status.IngestJobStatusTestHelper.acceptedRun;
import static sleeper.ingest.core.job.status.IngestJobStatusTestHelper.acceptedRunWhichFailed;
import static sleeper.ingest.core.job.status.IngestJobStatusTestHelper.acceptedRunWhichFinished;
import static sleeper.ingest.core.job.status.IngestJobStatusTestHelper.acceptedRunWhichStarted;
import static sleeper.ingest.core.job.status.IngestJobStatusTestHelper.jobStatus;

public class StoreIngestJobRunIdIT extends DynamoDBIngestJobStatusStoreTestBase {
    @Test
    void shouldReportAcceptedJob() {
        // Given
        IngestJob job = createJobWithTableAndFiles("test-job-1", table, "test-file-1.parquet");
        Instant validationTime = Instant.parse("2022-09-22T12:00:10.000Z");

        // When
        store.jobValidated(job.acceptedEventBuilder(validationTime).jobRunId("test-run").build());

        // Then
        assertThat(getAllJobStatuses())
                .usingRecursiveFieldByFieldElementComparator(IGNORE_UPDATE_TIMES)
                .containsExactly(jobStatus(job, acceptedRun(job, validationTime)));
    }

    @Test
    void shouldReportStartedJob() {
        // Given
        String jobRunId = "test-run";
        String taskId = "test-task";
        IngestJob job = createJobWithTableAndFiles("test-job-1", table, "test-file-1.parquet");
        Instant validationTime = Instant.parse("2022-09-22T12:00:10.000Z");
        Instant startTime = Instant.parse("2022-09-22T12:00:15.000Z");

        // When
        store.jobValidated(job.acceptedEventBuilder(validationTime).jobRunId(jobRunId).build());
        store.jobStarted(job.startedAfterValidationEventBuilder(startTime).jobRunId(jobRunId).taskId(taskId).build());

        // Then
        assertThat(getAllJobStatuses())
                .usingRecursiveFieldByFieldElementComparator(IGNORE_UPDATE_TIMES)
                .containsExactly(jobStatus(job, acceptedRunWhichStarted(job, taskId,
                        validationTime, startTime)));
    }

    @Test
    void shouldReportJobAddedFilesWhenFilesAddedAsynchronouslyWithOutOfSyncClock() {
        // Given
        String jobRunId = "test-run";
        String taskId = "test-task";
        IngestJob job = createJobWithTableAndFiles("test-job-1", table, "test-file-1.parquet");
        Instant startTime = Instant.parse("2022-09-22T12:00:15Z");
        Instant writtenTime = Instant.parse("2022-09-22T12:00:14Z");
        FileReferenceFactory fileFactory = FileReferenceFactory.from(new PartitionsBuilder(schemaWithKey("key")).singlePartition("root").buildTree());
        List<AllReferencesToAFile> outputFiles = filesWithReferences(List.of(
                fileFactory.rootFile("file1.parquet", 123),
                fileFactory.rootFile("file2.parquet", 456)));

        // When
        store.jobAddedFiles(job.addedFilesEventBuilder(writtenTime).files(outputFiles).jobRunId(jobRunId).taskId(taskId).build());
        store.jobStarted(job.startedEventBuilder(startTime).jobRunId(jobRunId).taskId(taskId).build());

        // Then
        assertThat(getAllJobStatuses())
                .usingRecursiveFieldByFieldElementComparator(IGNORE_UPDATE_TIMES)
                .containsExactly(jobStatus(job, ProcessRun.builder()
                        .taskId(taskId)
                        .statusUpdate(IngestJobAddedFilesStatus.builder()
                                .fileCount(2)
                                .writtenTime(writtenTime).updateTime(defaultUpdateTime(writtenTime)).build())
                        .startedStatus(IngestJobStartedStatus.withStartOfRun(true)
                                .inputFileCount(1)
                                .startTime(startTime).updateTime(defaultUpdateTime(startTime)).build())
                        .build()));
    }

    @Test
    void shouldReportFinishedJob() {
        // Given
        String jobRunId = "test-run";
        String taskId = "test-task";
        IngestJob job = createJobWithTableAndFiles("test-job-1", table, "test-file-1.parquet");
        Instant validationTime = Instant.parse("2022-09-22T12:00:10.000Z");
        Instant startTime = Instant.parse("2022-09-22T12:00:15.000Z");
        RecordsProcessedSummary summary = summary(startTime, Duration.ofMinutes(10), 100L, 100L);

        // When
        store.jobValidated(job.acceptedEventBuilder(validationTime).jobRunId(jobRunId).build());
        store.jobStarted(job.startedAfterValidationEventBuilder(startTime).jobRunId(jobRunId).taskId(taskId).build());
        store.jobFinished(job.finishedEventBuilder(summary).jobRunId(jobRunId).taskId(taskId).numFilesWrittenByJob(2).build());

        // Then
        assertThat(getAllJobStatuses())
                .usingRecursiveFieldByFieldElementComparator(IGNORE_UPDATE_TIMES)
                .containsExactly(jobStatus(job, acceptedRunWhichFinished(job, taskId,
                        validationTime, summary, 2)));
    }

    @Test
    void shouldReportFailedJob() {
        // Given
        String jobRunId = "test-run";
        String taskId = "test-task";
        IngestJob job = createJobWithTableAndFiles("test-job-1", table, "test-file-1.parquet");
        Instant validationTime = Instant.parse("2022-09-22T12:00:10.000Z");
        Instant startTime = Instant.parse("2022-09-22T12:00:15.000Z");
        ProcessRunTime runTime = new ProcessRunTime(startTime, Duration.ofMinutes(10));
        List<String> failureReasons = List.of("Something failed");

        // When
        store.jobValidated(job.acceptedEventBuilder(validationTime).jobRunId(jobRunId).build());
        store.jobStarted(job.startedAfterValidationEventBuilder(startTime).jobRunId(jobRunId).taskId(taskId).build());
        store.jobFailed(job.failedEventBuilder(runTime).jobRunId(jobRunId).taskId(taskId).failureReasons(failureReasons).build());

        // Then
        assertThat(getAllJobStatuses())
                .usingRecursiveFieldByFieldElementComparator(IGNORE_UPDATE_TIMES)
                .containsExactly(jobStatus(job, acceptedRunWhichFailed(job, taskId,
                        validationTime, runTime, failureReasons)));
    }
}
