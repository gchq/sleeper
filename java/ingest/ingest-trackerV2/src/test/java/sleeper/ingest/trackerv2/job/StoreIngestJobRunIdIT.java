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
package sleeper.ingest.trackerv2.job;

import org.junit.jupiter.api.Test;

import sleeper.core.partition.PartitionsBuilder;
import sleeper.core.statestore.AllReferencesToAFile;
import sleeper.core.statestore.FileReferenceFactory;
import sleeper.core.tracker.ingest.job.query.IngestJobAddedFilesStatus;
import sleeper.core.tracker.ingest.job.query.IngestJobStartedStatus;
import sleeper.core.tracker.job.run.JobRunSummary;
import sleeper.core.tracker.job.run.JobRunTime;
import sleeper.ingest.core.job.IngestJob;
import sleeper.ingest.tracker.testutils.DynamoDBIngestJobTrackerTestBase;

import java.time.Duration;
import java.time.Instant;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.core.schema.SchemaTestHelper.createSchemaWithKey;
import static sleeper.core.statestore.AllReferencesToAFileTestHelper.filesWithReferences;
import static sleeper.core.tracker.job.run.JobRunSummaryTestHelper.summary;
import static sleeper.core.tracker.job.run.JobRunTestData.jobRunOnTask;
import static sleeper.core.tracker.job.status.JobStatusUpdateTestHelper.defaultUpdateTime;
import static sleeper.ingest.core.job.IngestJobStatusFromJobTestData.acceptedRun;
import static sleeper.ingest.core.job.IngestJobStatusFromJobTestData.acceptedRunWhichFailed;
import static sleeper.ingest.core.job.IngestJobStatusFromJobTestData.acceptedRunWhichFinished;
import static sleeper.ingest.core.job.IngestJobStatusFromJobTestData.acceptedRunWhichStarted;
import static sleeper.ingest.core.job.IngestJobStatusFromJobTestData.ingestJobStatus;
import static sleeper.ingest.core.job.IngestJobTestData.createJobWithTableAndFiles;

public class StoreIngestJobRunIdIT extends DynamoDBIngestJobTrackerTestBase {
    @Test
    void shouldReportAcceptedJob() {
        // Given
        IngestJob job = createJobWithTableAndFiles("test-job-1", table, "test-file-1.parquet");
        Instant validationTime = Instant.parse("2022-09-22T12:00:10.000Z");

        // When
        tracker.jobValidated(job.acceptedEventBuilder(validationTime).jobRunId("test-run").build());

        // Then
        assertThat(getAllJobStatuses())
                .usingRecursiveFieldByFieldElementComparator(IGNORE_UPDATE_TIMES)
                .containsExactly(ingestJobStatus(job, acceptedRun(job, validationTime)));
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
        tracker.jobValidated(job.acceptedEventBuilder(validationTime).jobRunId(jobRunId).build());
        tracker.jobStarted(job.startedAfterValidationEventBuilder(startTime).jobRunId(jobRunId).taskId(taskId).build());

        // Then
        assertThat(getAllJobStatuses())
                .usingRecursiveFieldByFieldElementComparator(IGNORE_UPDATE_TIMES)
                .containsExactly(ingestJobStatus(job, acceptedRunWhichStarted(job, taskId,
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
        FileReferenceFactory fileFactory = FileReferenceFactory.from(new PartitionsBuilder(createSchemaWithKey("key")).singlePartition("root").buildTree());
        List<AllReferencesToAFile> outputFiles = filesWithReferences(List.of(
                fileFactory.rootFile("file1.parquet", 123),
                fileFactory.rootFile("file2.parquet", 456)));

        // When
        tracker.jobAddedFiles(job.addedFilesEventBuilder(writtenTime).files(outputFiles).jobRunId(jobRunId).taskId(taskId).build());
        tracker.jobStarted(job.startedEventBuilder(startTime).jobRunId(jobRunId).taskId(taskId).build());

        // Then
        assertThat(getAllJobStatuses())
                .usingRecursiveFieldByFieldElementComparator(IGNORE_UPDATE_TIMES)
                .containsExactly(ingestJobStatus(job, jobRunOnTask(taskId,
                        IngestJobAddedFilesStatus.builder()
                                .fileCount(2)
                                .writtenTime(writtenTime).updateTime(defaultUpdateTime(writtenTime)).build(),
                        IngestJobStartedStatus.builder()
                                .inputFileCount(1)
                                .startTime(startTime).updateTime(defaultUpdateTime(startTime)).build())));
    }

    @Test
    void shouldReportFinishedJob() {
        // Given
        String jobRunId = "test-run";
        String taskId = "test-task";
        IngestJob job = createJobWithTableAndFiles("test-job-1", table, "test-file-1.parquet");
        Instant validationTime = Instant.parse("2022-09-22T12:00:10.000Z");
        Instant startTime = Instant.parse("2022-09-22T12:00:15.000Z");
        JobRunSummary summary = summary(startTime, Duration.ofMinutes(10), 100L, 100L);

        // When
        tracker.jobValidated(job.acceptedEventBuilder(validationTime).jobRunId(jobRunId).build());
        tracker.jobStarted(job.startedAfterValidationEventBuilder(startTime).jobRunId(jobRunId).taskId(taskId).build());
        tracker.jobFinished(job.finishedEventBuilder(summary).jobRunId(jobRunId).taskId(taskId).numFilesWrittenByJob(2).build());

        // Then
        assertThat(getAllJobStatuses())
                .usingRecursiveFieldByFieldElementComparator(IGNORE_UPDATE_TIMES)
                .containsExactly(ingestJobStatus(job, acceptedRunWhichFinished(job, taskId,
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
        Instant finishTime = Instant.parse("2022-09-22T12:00:25.000Z");
        List<String> failureReasons = List.of("Something failed");

        // When
        tracker.jobValidated(job.acceptedEventBuilder(validationTime).jobRunId(jobRunId).build());
        tracker.jobStarted(job.startedAfterValidationEventBuilder(startTime).jobRunId(jobRunId).taskId(taskId).build());
        tracker.jobFailed(job.failedEventBuilder(finishTime).jobRunId(jobRunId).taskId(taskId).failureReasons(failureReasons).build());

        // Then
        assertThat(getAllJobStatuses())
                .usingRecursiveFieldByFieldElementComparator(IGNORE_UPDATE_TIMES)
                .containsExactly(ingestJobStatus(job, acceptedRunWhichFailed(job, taskId,
                        validationTime, new JobRunTime(startTime, finishTime), failureReasons)));
    }
}
