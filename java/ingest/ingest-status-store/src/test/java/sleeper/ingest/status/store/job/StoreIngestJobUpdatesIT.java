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
import sleeper.core.record.process.RecordsProcessed;
import sleeper.core.record.process.RecordsProcessedSummary;
import sleeper.core.statestore.FileReference;
import sleeper.core.statestore.FileReferenceFactory;
import sleeper.ingest.core.job.IngestJob;
import sleeper.ingest.status.store.testutils.DynamoDBIngestJobStatusStoreTestBase;

import java.time.Duration;
import java.time.Instant;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.core.schema.SchemaTestHelper.schemaWithKey;
import static sleeper.ingest.core.job.status.IngestJobStartedEvent.ingestJobStarted;
import static sleeper.ingest.core.job.status.IngestJobStatusTestHelper.finishedIngestRun;
import static sleeper.ingest.core.job.status.IngestJobStatusTestHelper.jobStatus;

public class StoreIngestJobUpdatesIT extends DynamoDBIngestJobStatusStoreTestBase {

    @Test
    public void shouldReportIngestJobFinishedSeparatelyFromStarted() {
        // Given
        IngestJob job = jobWithFiles("file");
        Instant startedTime = Instant.parse("2022-12-14T13:51:12.001Z");
        Instant finishedTime = Instant.parse("2022-12-14T13:51:42.001Z");

        // When
        store.jobStarted(defaultJobStartedEvent(job, startedTime));
        store.jobFinished(defaultJobFinishedEvent(job, startedTime, finishedTime));

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
        Instant finishedTime = Instant.parse("2022-12-14T13:51:42.001Z");
        List<String> failureReasons = List.of("Something went wrong");

        // When
        store.jobStarted(defaultJobStartedEvent(job, startedTime));
        store.jobFailed(defaultJobFailedEvent(job, startedTime, finishedTime, failureReasons));

        // Then
        assertThat(getAllJobStatuses())
                .usingRecursiveFieldByFieldElementComparator(IGNORE_UPDATE_TIMES)
                .containsExactly(defaultJobFailedStatus(job, startedTime, finishedTime, failureReasons));
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
        store.jobStarted(ingestJobStarted(job, startTime1).taskId(taskId1).build());
        store.jobStarted(ingestJobStarted(job, startTime2).taskId(taskId2).build());
        store.jobFinished(job.finishedEventBuilder(defaultSummary(startTime1, finishTime1)).taskId(taskId1).numFilesWrittenByJob(1).build());
        store.jobFinished(job.finishedEventBuilder(defaultSummary(startTime2, finishTime2)).taskId(taskId2).numFilesWrittenByJob(2).build());

        // Then
        assertThat(getAllJobStatuses())
                .usingRecursiveFieldByFieldElementComparator(IGNORE_UPDATE_TIMES)
                .containsExactly(jobStatus(job,
                        finishedIngestRun(job, taskId2, defaultSummary(startTime2, finishTime2), 2),
                        finishedIngestRun(job, taskId1, defaultSummary(startTime1, finishTime1), 1)));
    }

    @Test
    public void shouldStoreTimeInProcessWhenFinished() {
        // Given
        IngestJob job = jobWithFiles("file");
        Instant startedTime = Instant.parse("2022-12-14T13:51:12.001Z");
        Instant finishedTime = Instant.parse("2022-12-14T13:51:42.001Z");
        Duration timeInProcess = Duration.ofSeconds(20);
        RecordsProcessedSummary summary = new RecordsProcessedSummary(
                new RecordsProcessed(123L, 45L),
                startedTime, finishedTime, timeInProcess);

        // When
        store.jobStarted(defaultJobStartedEvent(job, startedTime));
        store.jobFinished(defaultJobFinishedEvent(job, summary));

        // Then
        assertThat(getAllJobStatuses())
                .usingRecursiveFieldByFieldElementComparator(IGNORE_UPDATE_TIMES)
                .containsExactly(defaultJobFinishedStatus(job, summary));
    }

    @Test
    public void shouldStoreTimeInProcessWhenFailed() {
        // Given
        IngestJob job = jobWithFiles("file");
        Instant startedTime = Instant.parse("2022-12-14T13:51:12.001Z");
        Instant finishedTime = Instant.parse("2022-12-14T13:51:42.001Z");
        Duration timeInProcess = Duration.ofSeconds(20);
        ProcessRunTime runTime = new ProcessRunTime(
                startedTime, finishedTime, timeInProcess);
        List<String> failureReasons = List.of("Some reason");

        // When
        store.jobStarted(defaultJobStartedEvent(job, startedTime));
        store.jobFailed(defaultJobFailedEvent(job, runTime, failureReasons));

        // Then
        assertThat(getAllJobStatuses())
                .usingRecursiveFieldByFieldElementComparator(IGNORE_UPDATE_TIMES)
                .containsExactly(defaultJobFailedStatus(job, runTime, failureReasons));
    }

    @Test
    public void shouldStoreFilesAdded() {
        // Given
        IngestJob job = jobWithFiles("file");
        Instant startedTime = Instant.parse("2022-12-14T13:51:12.001Z");
        Instant writtenTime = Instant.parse("2022-12-14T13:51:42.001Z");
        FileReferenceFactory fileFactory = FileReferenceFactory.from(new PartitionsBuilder(schemaWithKey("key")).singlePartition("root").buildTree());
        List<FileReference> files = List.of(
                fileFactory.rootFile("file1.parquet", 123),
                fileFactory.rootFile("file2.parquet", 456));

        // When
        store.jobStarted(defaultJobStartedEvent(job, startedTime));
        store.jobAddedFiles(defaultJobAddedFilesEvent(job, files, writtenTime));

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
        store.jobStarted(defaultJobStartedEvent(job, startedTime));
        store.jobFinished(defaultJobFinishedButUncommittedEvent(job, startedTime, writtenTime, 2));

        // Then
        assertThat(getAllJobStatuses())
                .usingRecursiveFieldByFieldElementComparator(IGNORE_UPDATE_TIMES)
                .containsExactly(defaultJobFinishedButUncommittedStatus(job, startedTime, writtenTime, 2));
    }
}
