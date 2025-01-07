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
import sleeper.core.statestore.FileReference;
import sleeper.core.statestore.FileReferenceFactory;
import sleeper.ingest.core.job.IngestJob;
import sleeper.ingest.status.store.testutils.DynamoDBIngestJobTrackerTestBase;

import java.time.Instant;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.ingest.core.job.IngestJobStatusFromJobTestData.ingestJobStatus;

public class QueryIngestJobStatusUnfinishedIT extends DynamoDBIngestJobTrackerTestBase {

    @Test
    public void shouldReturnUnfinishedIngestJobs() {
        // Given
        IngestJob job1 = jobWithFiles("file1");
        IngestJob job2 = jobWithFiles("file2");
        Instant startedTime1 = Instant.parse("2022-12-14T13:51:12.001Z");
        Instant startedTime2 = Instant.parse("2022-12-14T13:52:12.001Z");

        // When
        tracker.jobStarted(defaultJobStartedEvent(job1, startedTime1));
        tracker.jobStarted(defaultJobStartedEvent(job2, startedTime2));

        // Then
        assertThat(tracker.getUnfinishedJobs(tableId))
                .usingRecursiveFieldByFieldElementComparator(IGNORE_UPDATE_TIMES)
                .containsExactly(
                        defaultJobStartedStatus(job2, startedTime2),
                        defaultJobStartedStatus(job1, startedTime1));
    }

    @Test
    public void shouldExcludeFinishedIngestJob() {
        // Given
        IngestJob job1 = jobWithFiles("file1");
        IngestJob job2 = jobWithFiles("file2");
        Instant startedTime1 = Instant.parse("2022-12-14T13:51:12.001Z");
        Instant finishedTime1 = Instant.parse("2022-12-14T13:51:42.001Z");
        Instant startedTime2 = Instant.parse("2022-12-14T13:52:12.001Z");

        // When
        tracker.jobStarted(defaultJobStartedEvent(job1, startedTime1));
        tracker.jobFinished(defaultJobFinishedEvent(job1, startedTime1, finishedTime1));
        tracker.jobStarted(defaultJobStartedEvent(job2, startedTime2));

        // Then
        assertThat(tracker.getUnfinishedJobs(tableId))
                .usingRecursiveFieldByFieldElementComparator(IGNORE_UPDATE_TIMES)
                .containsExactly(defaultJobStartedStatus(job2, startedTime2));
    }

    @Test
    public void shouldExcludeIngestJobInOtherTable() {
        // Given
        IngestJob job1 = jobWithFiles("file1");
        IngestJob job2 = jobWithTableAndFiles("other-table", "file2");
        Instant startedTime1 = Instant.parse("2022-12-14T13:51:12.001Z");
        Instant startedTime2 = Instant.parse("2022-12-14T13:52:12.001Z");

        // When
        tracker.jobStarted(defaultJobStartedEvent(job1, startedTime1));
        tracker.jobStarted(defaultJobStartedEvent(job2, startedTime2));

        // Then
        assertThat(tracker.getUnfinishedJobs(tableId))
                .usingRecursiveFieldByFieldElementComparator(IGNORE_UPDATE_TIMES)
                .containsExactly(defaultJobStartedStatus(job1, startedTime1));
    }

    @Test
    public void shouldIncludeUnfinishedIngestJobWithOneFinishedRun() {
        // Given
        IngestJob job = jobWithFiles("test-file");
        Instant startedTime1 = Instant.parse("2022-12-14T13:51:12.001Z");
        Instant finishedTime1 = Instant.parse("2022-12-14T13:51:42.001Z");
        Instant startedTime2 = Instant.parse("2022-12-14T13:52:12.001Z");

        // When
        tracker.jobStarted(defaultJobStartedEvent(job, startedTime1));
        tracker.jobFinished(defaultJobFinishedEvent(job, startedTime1, finishedTime1));
        tracker.jobStarted(defaultJobStartedEvent(job, startedTime2));

        // Then
        assertThat(tracker.getUnfinishedJobs(tableId))
                .usingRecursiveFieldByFieldElementComparator(IGNORE_UPDATE_TIMES)
                .containsExactly(ingestJobStatus(job,
                        defaultJobStartedRun(job, startedTime2),
                        defaultJobFinishedRun(job, startedTime1, finishedTime1)));
    }

    @Test
    public void shouldReturnUncommittedIngestJob() {
        // Given
        IngestJob job = jobWithFiles("test-file");
        Instant startedTime = Instant.parse("2022-12-14T13:51:12.001Z");
        Instant finishedTime = Instant.parse("2022-12-14T13:52:12.001Z");

        // When
        tracker.jobStarted(defaultJobStartedEvent(job, startedTime));
        tracker.jobFinished(defaultJobFinishedButUncommittedEvent(job, startedTime, finishedTime, 1));

        // Then
        assertThat(tracker.getUnfinishedJobs(tableId))
                .usingRecursiveFieldByFieldElementComparator(IGNORE_UPDATE_TIMES)
                .containsExactly(
                        defaultJobFinishedButUncommittedStatus(job, startedTime, finishedTime, 1));
    }

    @Test
    public void shouldExcludeSeparatelyCommittedIngestJob() {
        // Given
        IngestJob job = jobWithFiles("test-file");
        Instant startedTime = Instant.parse("2022-12-14T13:51:12.001Z");
        Instant writtenTime = Instant.parse("2022-12-14T13:52:00Z");
        Instant finishedTime = Instant.parse("2022-12-14T13:52:12.001Z");
        FileReferenceFactory fileFactory = FileReferenceFactory.from(new PartitionsBuilder(schema).singlePartition("root").buildTree());
        List<FileReference> outputFiles = List.of(fileFactory.rootFile(123L));

        // When
        tracker.jobStarted(defaultJobStartedEvent(job, startedTime));
        tracker.jobFinished(defaultJobFinishedButUncommittedEvent(job, startedTime, finishedTime, 1));
        tracker.jobAddedFiles(defaultJobAddedFilesEvent(job, outputFiles, writtenTime));

        // Then
        assertThat(tracker.getUnfinishedJobs(tableId)).isEmpty();
    }
}
