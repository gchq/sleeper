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
package sleeper.compaction.status.store.job;

import org.junit.jupiter.api.Test;

import sleeper.compaction.job.CompactionJob;
import sleeper.compaction.job.CompactionJobStatusTestData;
import sleeper.compaction.status.store.testutils.DynamoDBCompactionJobStatusStoreTestBase;
import sleeper.core.partition.Partition;
import sleeper.core.record.process.ProcessRunTime;
import sleeper.core.record.process.RecordsProcessed;
import sleeper.core.record.process.RecordsProcessedSummary;
import sleeper.core.statestore.FileReferenceFactory;

import java.time.Duration;
import java.time.Instant;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.compaction.job.CompactionJobStatusTestData.finishedCompactionRun;
import static sleeper.compaction.job.CompactionJobStatusTestData.jobCreated;
import static sleeper.compaction.job.status.CompactionJobCommittedEvent.compactionJobCommitted;
import static sleeper.compaction.job.status.CompactionJobFailedEvent.compactionJobFailed;
import static sleeper.compaction.job.status.CompactionJobFinishedEvent.compactionJobFinished;
import static sleeper.compaction.job.status.CompactionJobStartedEvent.compactionJobStarted;

public class StoreCompactionJobUpdatesIT extends DynamoDBCompactionJobStatusStoreTestBase {

    @Test
    void shouldReportCompactionInputFilesAssigned() {
        // Given
        Partition partition = singlePartition();
        FileReferenceFactory fileFactory = fileFactory(partition);
        CompactionJob job = jobFactory.createCompactionJob(
                List.of(fileFactory.rootFile(100L)),
                partition.getId());
        Instant createdTime = Instant.parse("2024-09-06T09:48:00Z");
        Instant filesAssignedTime = Instant.parse("2024-09-06T09:48:05Z");

        // When
        storeWithUpdateTimes(createdTime).jobCreated(job);
        storeWithUpdateTimes(filesAssignedTime).jobInputFilesAssigned(job);

        // Then
        assertThat(getAllJobStatuses())
                .usingRecursiveFieldByFieldElementComparator(IGNORE_EXPIRY_TIME)
                .containsExactly(CompactionJobStatusTestData.jobFilesAssigned(job, createdTime, filesAssignedTime));
    }

    @Test
    public void shouldReportCompactionJobStarted() {
        // Given
        Partition partition = singlePartition();
        FileReferenceFactory fileFactory = fileFactory(partition);
        CompactionJob job = jobFactory.createCompactionJob(
                List.of(fileFactory.rootFile(100L)),
                partition.getId());

        // When
        store.jobCreated(job);
        store.jobStarted(compactionJobStarted(job, defaultStartTime()).taskId(DEFAULT_TASK_ID).build());

        // Then
        assertThat(getAllJobStatuses())
                .usingRecursiveFieldByFieldElementComparator(IGNORE_UPDATE_TIMES)
                .containsExactly(startedStatusWithDefaults(job));
    }

    @Test
    public void shouldReportCompactionJobUncommitted() {
        // Given
        Partition partition = singlePartition();
        FileReferenceFactory fileFactory = fileFactory(partition);
        CompactionJob job = jobFactory.createCompactionJob(
                List.of(fileFactory.rootFile(100L)),
                partition.getId());

        // When
        store.jobCreated(job);
        store.jobStarted(compactionJobStarted(job, defaultStartTime()).taskId(DEFAULT_TASK_ID).build());
        store.jobFinished(compactionJobFinished(job, defaultSummary()).taskId(DEFAULT_TASK_ID).build());

        // Then
        assertThat(getAllJobStatuses())
                .usingRecursiveFieldByFieldElementComparator(IGNORE_UPDATE_TIMES)
                .containsExactly(finishedUncommittedStatusWithDefaults(job));
    }

    @Test
    public void shouldReportCompactionJobFinished() {
        // Given
        Partition partition = singlePartition();
        FileReferenceFactory fileFactory = fileFactory(partition);
        CompactionJob job = jobFactory.createCompactionJob(
                List.of(fileFactory.rootFile(100L)),
                partition.getId());

        // When
        store.jobCreated(job);
        store.jobStarted(compactionJobStarted(job, defaultStartTime()).taskId(DEFAULT_TASK_ID).build());
        store.jobFinished(compactionJobFinished(job, defaultSummary()).taskId(DEFAULT_TASK_ID).build());
        store.jobCommitted(compactionJobCommitted(job, defaultCommitTime()).taskId(DEFAULT_TASK_ID).build());

        // Then
        assertThat(getAllJobStatuses())
                .usingRecursiveFieldByFieldElementComparator(IGNORE_UPDATE_TIMES)
                .containsExactly(finishedThenCommittedStatusWithDefaults(job));
    }

    @Test
    public void shouldReportCompactionJobFailed() {
        // Given
        Partition partition = singlePartition();
        FileReferenceFactory fileFactory = fileFactory(partition);
        CompactionJob job = jobFactory.createCompactionJob(
                List.of(fileFactory.rootFile(100L)),
                partition.getId());
        List<String> failureReasons = List.of("Something went wrong");

        // When
        store.jobCreated(job);
        store.jobStarted(compactionJobStarted(job, defaultStartTime()).taskId(DEFAULT_TASK_ID).build());
        store.jobFailed(compactionJobFailed(job, defaultRunTime()).failureReasons(failureReasons).taskId(DEFAULT_TASK_ID).build());

        // Then
        assertThat(getAllJobStatuses())
                .usingRecursiveFieldByFieldElementComparator(IGNORE_UPDATE_TIMES)
                .containsExactly(failedStatusWithDefaults(job, failureReasons));
    }

    @Test
    public void shouldReportLatestUpdatesWhenJobIsRunMultipleTimes() {
        // Given
        Partition partition = singlePartition();
        FileReferenceFactory fileFactory = fileFactory(partition);
        CompactionJob job = jobFactory.createCompactionJob(
                List.of(fileFactory.rootFile(100L)),
                partition.getId());
        Instant startTime1 = Instant.parse("2022-10-03T15:19:01.001Z");
        Instant finishTime1 = Instant.parse("2022-10-03T15:19:31.001Z");
        Instant commitTime1 = Instant.parse("2022-10-03T15:19:41.001Z");
        Instant startTime2 = Instant.parse("2022-10-03T15:19:02.001Z");
        Instant finishTime2 = Instant.parse("2022-10-03T15:19:32.001Z");
        Instant commitTime2 = Instant.parse("2022-10-03T15:19:42.001Z");
        RecordsProcessed processed = new RecordsProcessed(100L, 100L);
        RecordsProcessedSummary summary1 = new RecordsProcessedSummary(processed, startTime1, finishTime1);
        RecordsProcessedSummary summary2 = new RecordsProcessedSummary(processed, startTime2, finishTime2);

        // When
        store.jobCreated(job);
        store.jobStarted(compactionJobStarted(job, startTime1).taskId(DEFAULT_TASK_ID).build());
        store.jobStarted(compactionJobStarted(job, startTime2).taskId(DEFAULT_TASK_ID_2).build());
        store.jobFinished(compactionJobFinished(job, summary1).taskId(DEFAULT_TASK_ID).build());
        store.jobFinished(compactionJobFinished(job, summary2).taskId(DEFAULT_TASK_ID_2).build());
        store.jobCommitted(compactionJobCommitted(job, commitTime1).taskId(DEFAULT_TASK_ID).build());
        store.jobCommitted(compactionJobCommitted(job, commitTime2).taskId(DEFAULT_TASK_ID_2).build());

        // Then
        assertThat(getAllJobStatuses())
                .usingRecursiveFieldByFieldElementComparator(IGNORE_UPDATE_TIMES)
                .containsExactly(jobCreated(job, ignoredUpdateTime(),
                        finishedCompactionRun(DEFAULT_TASK_ID_2, new RecordsProcessedSummary(processed, startTime2, finishTime2),
                                commitTime2),
                        finishedCompactionRun(DEFAULT_TASK_ID, new RecordsProcessedSummary(processed, startTime1, finishTime1),
                                commitTime1)));
    }

    @Test
    public void shouldStoreTimeInProcessWhenFinished() {
        // Given
        Partition partition = singlePartition();
        FileReferenceFactory fileFactory = fileFactory(partition);
        CompactionJob job = jobFactory.createCompactionJob(
                List.of(fileFactory.rootFile(100L)),
                partition.getId());
        Instant startedTime = Instant.parse("2022-12-14T13:51:12.001Z");
        Instant finishedTime = Instant.parse("2022-12-14T13:51:42.001Z");
        Duration timeInProcess = Duration.ofSeconds(20);
        RecordsProcessedSummary summary = new RecordsProcessedSummary(
                new RecordsProcessed(123L, 45L),
                startedTime, finishedTime, timeInProcess);

        // When
        store.jobCreated(job);
        store.jobStarted(compactionJobStarted(job, startedTime).taskId(DEFAULT_TASK_ID).build());
        store.jobFinished(compactionJobFinished(job, summary).taskId(DEFAULT_TASK_ID).build());
        store.jobCommitted(compactionJobCommitted(job, defaultCommitTime()).taskId(DEFAULT_TASK_ID).build());

        // Then
        assertThat(getAllJobStatuses())
                .usingRecursiveFieldByFieldElementComparator(IGNORE_UPDATE_TIMES)
                .containsExactly(finishedThenCommittedStatusWithDefaults(job, summary));
    }

    @Test
    public void shouldStoreTimeInProcessWhenFailed() {
        // Given
        Partition partition = singlePartition();
        FileReferenceFactory fileFactory = fileFactory(partition);
        CompactionJob job = jobFactory.createCompactionJob(
                List.of(fileFactory.rootFile(100L)),
                partition.getId());
        Instant startedTime = Instant.parse("2022-12-14T13:51:12.001Z");
        Instant finishedTime = Instant.parse("2022-12-14T13:51:42.001Z");
        Duration timeInProcess = Duration.ofSeconds(20);
        ProcessRunTime runTime = new ProcessRunTime(
                startedTime, finishedTime, timeInProcess);
        List<String> failureReasons = List.of("Something went wrong", "More details");

        // When
        store.jobCreated(job);
        store.jobStarted(compactionJobStarted(job, startedTime).taskId(DEFAULT_TASK_ID).build());
        store.jobFailed(compactionJobFailed(job, runTime).failureReasons(failureReasons).taskId(DEFAULT_TASK_ID).build());

        // Then
        assertThat(getAllJobStatuses())
                .usingRecursiveFieldByFieldElementComparator(IGNORE_UPDATE_TIMES)
                .containsExactly(failedStatusWithDefaults(job, runTime, failureReasons));
    }
}
