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
package sleeper.ingest.tracker.job;

import org.junit.jupiter.api.Test;

import sleeper.core.partition.PartitionsBuilder;
import sleeper.core.statestore.FileReference;
import sleeper.core.statestore.FileReferenceFactory;
import sleeper.ingest.core.job.IngestJob;
import sleeper.ingest.tracker.testutils.DynamoDBIngestJobTrackerTestBase;

import java.time.Instant;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.core.schema.SchemaTestHelper.createSchemaWithKey;
import static sleeper.ingest.core.job.IngestJobStatusFromJobTestData.finishedIngestRun;
import static sleeper.ingest.core.job.IngestJobStatusFromJobTestData.ingestJobStatus;

public class StoreIngestJobUpdatesIT extends DynamoDBIngestJobTrackerTestBase {

    @Test
    public void shouldReportIngestJobFinishedSeparatelyFromStarted() {
        // Given
        IngestJob job = jobWithFiles("file");
        Instant startedTime = Instant.parse("2022-12-14T13:51:12.001Z");
        Instant finishedTime = Instant.parse("2022-12-14T13:51:42.001Z");

        // When
        tracker.jobStarted(defaultJobStartedEvent(job, startedTime));
        tracker.jobFinished(defaultJobFinishedEvent(job, startedTime, finishedTime));

        // Then
        assertThat(getAllJobStatuses())
                .usingRecursiveFieldByFieldElementComparator(IGNORE_UPDATE_TIMES)
                .containsExactly(defaultJobFinishedStatus(job, startedTime, finishedTime));
    }

    @Test
    public void shouldReportIngestJobFailed() {
        // Given
        IngestJob job = jobWithFiles("file");
        Instant startedTime = Instant.parse("2022-12-14T13:51:12.001Z");
        Instant failureTime = Instant.parse("2022-12-14T13:51:42.001Z");
        List<String> failureReasons = List.of("Something went wrong");

        // When
        tracker.jobStarted(defaultJobStartedEvent(job, startedTime));
        tracker.jobFailed(defaultJobFailedEvent(job, failureTime, failureReasons));

        // Then
        assertThat(getAllJobStatuses())
                .usingRecursiveFieldByFieldElementComparator(IGNORE_UPDATE_TIMES)
                .containsExactly(defaultJobFailedStatus(job, startedTime, failureTime, failureReasons));
    }

    @Test
    public void shouldReportLatestUpdatesWhenJobIsRunMultipleTimes() {
        // Given
        IngestJob job = jobWithFiles("file");
        Instant startTime1 = Instant.parse("2022-10-03T15:19:01.001Z");
        Instant finishTime1 = Instant.parse("2022-10-03T15:19:31.001Z");
        Instant startTime2 = Instant.parse("2022-10-03T15:19:02.001Z");
        Instant finishTime2 = Instant.parse("2022-10-03T15:19:32.001Z");
        String taskId1 = "first-task";
        String taskId2 = "second-task";

        // When
        tracker.jobStarted(job.startedEventBuilder(startTime1).taskId(taskId1).jobRunId("run-1").build());
        tracker.jobStarted(job.startedEventBuilder(startTime2).taskId(taskId2).jobRunId("run-2").build());
        tracker.jobFinished(job.finishedEventBuilder(defaultSummary(startTime1, finishTime1)).taskId(taskId1).jobRunId("run-1").numFilesWrittenByJob(1).build());
        tracker.jobFinished(job.finishedEventBuilder(defaultSummary(startTime2, finishTime2)).taskId(taskId2).jobRunId("run-2").numFilesWrittenByJob(2).build());

        // Then
        assertThat(getAllJobStatuses())
                .usingRecursiveFieldByFieldElementComparator(IGNORE_UPDATE_TIMES)
                .containsExactly(ingestJobStatus(job,
                        finishedIngestRun(job, taskId2, defaultSummary(startTime2, finishTime2), 2),
                        finishedIngestRun(job, taskId1, defaultSummary(startTime1, finishTime1), 1)));
    }

    @Test
    public void shouldStoreFilesAdded() {
        // Given
        IngestJob job = jobWithFiles("file");
        Instant startedTime = Instant.parse("2022-12-14T13:51:12.001Z");
        Instant writtenTime = Instant.parse("2022-12-14T13:51:42.001Z");
        FileReferenceFactory fileFactory = FileReferenceFactory.from(new PartitionsBuilder(createSchemaWithKey("key")).singlePartition("root").buildTree());
        List<FileReference> files = List.of(
                fileFactory.rootFile("file1.parquet", 123),
                fileFactory.rootFile("file2.parquet", 456));

        // When
        tracker.jobStarted(defaultJobStartedEvent(job, startedTime));
        tracker.jobAddedFiles(defaultJobAddedFilesEvent(job, files, writtenTime));

        // Then
        assertThat(getAllJobStatuses())
                .usingRecursiveFieldByFieldElementComparator(IGNORE_UPDATE_TIMES)
                .containsExactly(defaultJobAddedFilesStatus(job, startedTime, writtenTime, 2));
    }

    @Test
    public void shouldStoreJobFinishedButUncommitted() {
        // Given
        IngestJob job = jobWithFiles("file");
        Instant startedTime = Instant.parse("2022-12-14T13:51:12.001Z");
        Instant writtenTime = Instant.parse("2022-12-14T13:51:42.001Z");

        // When
        tracker.jobStarted(defaultJobStartedEvent(job, startedTime));
        tracker.jobFinished(defaultJobFinishedButUncommittedEvent(job, startedTime, writtenTime, 2));

        // Then
        assertThat(getAllJobStatuses())
                .usingRecursiveFieldByFieldElementComparator(IGNORE_UPDATE_TIMES)
                .containsExactly(defaultJobFinishedButUncommittedStatus(job, startedTime, writtenTime, 2));
    }
}
