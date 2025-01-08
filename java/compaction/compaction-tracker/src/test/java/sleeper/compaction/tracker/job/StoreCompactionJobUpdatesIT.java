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
package sleeper.compaction.tracker.job;

import org.junit.jupiter.api.Test;

import sleeper.compaction.core.job.CompactionJob;
import sleeper.compaction.core.job.CompactionJobStatusFromJobTestData;
import sleeper.compaction.tracker.testutils.DynamoDBCompactionJobTrackerTestBase;
import sleeper.core.partition.Partition;
import sleeper.core.statestore.FileReferenceFactory;
import sleeper.core.tracker.job.JobRunSummary;
import sleeper.core.tracker.job.JobRunTime;
import sleeper.core.tracker.job.RecordsProcessed;

import java.time.Duration;
import java.time.Instant;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.core.tracker.compaction.job.CompactionJobStatusTestData.finishedCompactionRun;

public class StoreCompactionJobUpdatesIT extends DynamoDBCompactionJobTrackerTestBase {

    @Test
    public void shouldReportCompactionJobStarted() {
        // Given
        Partition partition = singlePartition();
        FileReferenceFactory fileFactory = fileFactory(partition);
        CompactionJob job = jobFactory.createCompactionJob(
                List.of(fileFactory.rootFile(100L)),
                partition.getId());

        // When
        storeJobCreated(job);
        tracker.jobStarted(job.startedEventBuilder(defaultStartTime()).taskId(DEFAULT_TASK_ID).build());

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
        storeJobCreated(job);
        tracker.jobStarted(job.startedEventBuilder(defaultStartTime()).taskId(DEFAULT_TASK_ID).build());
        tracker.jobFinished(job.finishedEventBuilder(defaultSummary()).taskId(DEFAULT_TASK_ID).build());

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
        storeJobCreated(job);
        tracker.jobStarted(job.startedEventBuilder(defaultStartTime()).taskId(DEFAULT_TASK_ID).build());
        tracker.jobFinished(job.finishedEventBuilder(defaultSummary()).taskId(DEFAULT_TASK_ID).build());
        tracker.jobCommitted(job.committedEventBuilder(defaultCommitTime()).taskId(DEFAULT_TASK_ID).build());

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
        storeJobCreated(job);
        tracker.jobStarted(job.startedEventBuilder(defaultStartTime()).taskId(DEFAULT_TASK_ID).build());
        tracker.jobFailed(job.failedEventBuilder(defaultRunTime()).failureReasons(failureReasons).taskId(DEFAULT_TASK_ID).build());

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
        JobRunSummary summary1 = new JobRunSummary(processed, startTime1, finishTime1);
        JobRunSummary summary2 = new JobRunSummary(processed, startTime2, finishTime2);

        // When
        storeJobCreated(job);
        tracker.jobStarted(job.startedEventBuilder(startTime1).taskId(DEFAULT_TASK_ID).build());
        tracker.jobStarted(job.startedEventBuilder(startTime2).taskId(DEFAULT_TASK_ID_2).build());
        tracker.jobFinished(job.finishedEventBuilder(summary1).taskId(DEFAULT_TASK_ID).build());
        tracker.jobFinished(job.finishedEventBuilder(summary2).taskId(DEFAULT_TASK_ID_2).build());
        tracker.jobCommitted(job.committedEventBuilder(commitTime1).taskId(DEFAULT_TASK_ID).build());
        tracker.jobCommitted(job.committedEventBuilder(commitTime2).taskId(DEFAULT_TASK_ID_2).build());

        // Then
        assertThat(getAllJobStatuses())
                .usingRecursiveFieldByFieldElementComparator(IGNORE_UPDATE_TIMES)
                .containsExactly(CompactionJobStatusFromJobTestData.compactionJobCreated(job, ignoredUpdateTime(),
                        finishedCompactionRun(DEFAULT_TASK_ID_2, new JobRunSummary(processed, startTime2, finishTime2),
                                commitTime2),
                        finishedCompactionRun(DEFAULT_TASK_ID, new JobRunSummary(processed, startTime1, finishTime1),
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
        JobRunSummary summary = new JobRunSummary(
                new RecordsProcessed(123L, 45L),
                startedTime, finishedTime, timeInProcess);

        // When
        storeJobCreated(job);
        tracker.jobStarted(job.startedEventBuilder(startedTime).taskId(DEFAULT_TASK_ID).build());
        tracker.jobFinished(job.finishedEventBuilder(summary).taskId(DEFAULT_TASK_ID).build());
        tracker.jobCommitted(job.committedEventBuilder(defaultCommitTime()).taskId(DEFAULT_TASK_ID).build());

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
        JobRunTime runTime = new JobRunTime(
                startedTime, finishedTime, timeInProcess);
        List<String> failureReasons = List.of("Something went wrong", "More details");

        // When
        storeJobCreated(job);
        tracker.jobStarted(job.startedEventBuilder(startedTime).taskId(DEFAULT_TASK_ID).build());
        tracker.jobFailed(job.failedEventBuilder(runTime).failureReasons(failureReasons).taskId(DEFAULT_TASK_ID).build());

        // Then
        assertThat(getAllJobStatuses())
                .usingRecursiveFieldByFieldElementComparator(IGNORE_UPDATE_TIMES)
                .containsExactly(failedStatusWithDefaults(job, runTime, failureReasons));
    }
}
