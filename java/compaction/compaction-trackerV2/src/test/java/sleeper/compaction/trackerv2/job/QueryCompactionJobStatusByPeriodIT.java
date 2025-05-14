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
package sleeper.compaction.trackerv2.job;

import org.junit.jupiter.api.Test;

import sleeper.compaction.core.job.CompactionJob;
import sleeper.compaction.trackerv2.testutils.DynamoDBCompactionJobTrackerTestBase;
import sleeper.core.partition.Partition;
import sleeper.core.statestore.FileReferenceFactory;

import java.time.Instant;
import java.time.Period;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.compaction.core.job.CompactionJobStatusFromJobTestData.compactionJobCreated;

public class QueryCompactionJobStatusByPeriodIT extends DynamoDBCompactionJobTrackerTestBase {

    @Test
    public void shouldReturnCompactionJobsInPeriod() {
        // Given
        Partition partition = singlePartition();
        FileReferenceFactory fileFactory = fileFactory(partition);
        CompactionJob job1 = jobFactory.createCompactionJob(
                List.of(fileFactory.rootFile("file1", 123L)),
                partition.getId());
        CompactionJob job2 = jobFactory.createCompactionJob(
                List.of(fileFactory.rootFile("file2", 456L)),
                partition.getId());

        // When
        storeJobsCreated(job1, job2);

        // Then
        Instant epochStart = Instant.ofEpochMilli(0);
        Instant farFuture = epochStart.plus(Period.ofDays(999999999));
        assertThat(tracker.getJobsInTimePeriod(tableId, epochStart, farFuture))
                .usingRecursiveFieldByFieldElementComparator(IGNORE_UPDATE_TIMES)
                .containsExactly(
                        compactionJobCreated(job2, ignoredUpdateTime()),
                        compactionJobCreated(job1, ignoredUpdateTime()));
    }

    @Test
    public void shouldExcludeCompactionJobOutsidePeriod() {
        // Given
        Partition partition = singlePartition();
        FileReferenceFactory fileFactory = fileFactory(partition);
        CompactionJob job = jobFactory.createCompactionJob(
                List.of(fileFactory.rootFile(123L)),
                partition.getId());

        // When
        storeJobCreated(job);

        // Then
        Instant periodStart = Instant.now().minus(Period.ofDays(2));
        Instant periodEnd = periodStart.plus(Period.ofDays(1));
        assertThat(tracker.getJobsInTimePeriod(tableId, periodStart, periodEnd)).isEmpty();
    }

    @Test
    public void shouldExcludeCompactionJobInOtherTable() {
        // Given
        Partition partition = singlePartition();
        FileReferenceFactory fileFactory = fileFactory(partition);
        CompactionJob job1 = jobFactory.createCompactionJob(
                List.of(fileFactory.rootFile("file1", 123L)),
                partition.getId());
        CompactionJob job2 = jobFactoryForOtherTable().createCompactionJob(
                List.of(fileFactory.rootFile("file2", 456L)),
                partition.getId());

        // When
        storeJobsCreated(job1, job2);

        // Then
        Instant epochStart = Instant.ofEpochMilli(0);
        Instant farFuture = epochStart.plus(Period.ofDays(999999999));
        assertThat(tracker.getJobsInTimePeriod(tableId, epochStart, farFuture))
                .usingRecursiveFieldByFieldElementComparator(IGNORE_UPDATE_TIMES)
                .containsExactly(compactionJobCreated(job1, ignoredUpdateTime()));
    }

    @Test
    public void shouldIncludeFinishedStatusUpdateOutsidePeriod() throws Exception {
        // Given
        Partition partition = singlePartition();
        FileReferenceFactory fileFactory = fileFactory(partition);
        CompactionJob job = jobFactory.createCompactionJob(
                List.of(fileFactory.rootFile(123L)),
                partition.getId());

        // When
        Instant periodStart = Instant.now().minus(Period.ofDays(1));
        storeJobCreated(job);
        tracker.jobStarted(job.startedEventBuilder(defaultStartTime()).taskId(DEFAULT_TASK_ID).jobRunId("test-run").build());
        Thread.sleep(1);
        Instant periodEnd = Instant.now();
        Thread.sleep(1);
        tracker.jobFinished(job.finishedEventBuilder(defaultSummary()).taskId(DEFAULT_TASK_ID).jobRunId("test-run").build());
        tracker.jobCommitted(job.committedEventBuilder(defaultCommitTime()).taskId(DEFAULT_TASK_ID).jobRunId("test-run").build());

        // Then
        assertThat(tracker.getJobsInTimePeriod(tableId, periodStart, periodEnd))
                .usingRecursiveFieldByFieldElementComparator(IGNORE_UPDATE_TIMES)
                .containsExactly(finishedThenCommittedStatusWithDefaults(job));
    }

    @Test
    public void shouldIncludeJobCreatedOutsidePeriod() throws Exception {
        // Given
        Partition partition = singlePartition();
        FileReferenceFactory fileFactory = fileFactory(partition);
        CompactionJob job = jobFactory.createCompactionJob(
                List.of(fileFactory.rootFile(123L)),
                partition.getId());

        // When
        storeJobCreated(job);
        Thread.sleep(1);
        Instant periodStart = Instant.now();
        Thread.sleep(1);
        tracker.jobStarted(job.startedEventBuilder(defaultStartTime()).taskId(DEFAULT_TASK_ID).jobRunId("test-run").build());
        tracker.jobFinished(job.finishedEventBuilder(defaultSummary()).taskId(DEFAULT_TASK_ID).jobRunId("test-run").build());
        tracker.jobCommitted(job.committedEventBuilder(defaultCommitTime()).taskId(DEFAULT_TASK_ID).jobRunId("test-run").build());
        Instant periodEnd = periodStart.plus(Period.ofDays(1));

        // Then
        assertThat(tracker.getJobsInTimePeriod(tableId, periodStart, periodEnd))
                .usingRecursiveFieldByFieldElementComparator(IGNORE_UPDATE_TIMES)
                .containsExactly(finishedThenCommittedStatusWithDefaults(job));
    }
}
