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

import sleeper.compaction.core.job.CompactionJob;
import sleeper.compaction.status.store.testutils.DynamoDBCompactionJobStatusStoreTestBase;
import sleeper.core.partition.Partition;
import sleeper.core.statestore.FileReferenceFactory;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.compaction.core.job.CompactionJobStatusTestData.compactionJobCreated;
import static sleeper.compaction.core.job.CompactionJobStatusTestData.finishedCompactionRun;
import static sleeper.compaction.core.job.CompactionJobStatusTestData.startedCompactionRun;
import static sleeper.compaction.core.job.CompactionJobStatusTestData.uncommittedCompactionRun;

public class QueryCompactionJobStatusUnfinishedIT extends DynamoDBCompactionJobStatusStoreTestBase {

    @Test
    public void shouldReturnUnfinishedCompactionJobs() {
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
        assertThat(store.getUnfinishedJobs(tableId))
                .usingRecursiveFieldByFieldElementComparator(IGNORE_UPDATE_TIMES)
                .containsExactly(
                        compactionJobCreated(job2, ignoredUpdateTime()),
                        compactionJobCreated(job1, ignoredUpdateTime()));
    }

    @Test
    public void shouldIncludeUncommittedCompactionJob() {
        // Given
        Partition partition = singlePartition();
        FileReferenceFactory fileFactory = fileFactory(partition);
        CompactionJob job = jobFactory.createCompactionJob(
                List.of(fileFactory.rootFile("file1", 456L)),
                partition.getId());

        // When
        storeJobCreated(job);
        store.jobStarted(job.startedEventBuilder(defaultStartTime()).taskId(DEFAULT_TASK_ID).build());
        store.jobFinished(job.finishedEventBuilder(defaultSummary()).taskId(DEFAULT_TASK_ID).build());

        // Then
        assertThat(store.getUnfinishedJobs(tableId))
                .usingRecursiveFieldByFieldElementComparator(IGNORE_UPDATE_TIMES)
                .containsExactly(
                        compactionJobCreated(job, ignoredUpdateTime(),
                                uncommittedCompactionRun(DEFAULT_TASK_ID, defaultSummary())));
    }

    @Test
    public void shouldExcludeFinishedCompactionJob() {
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
        store.jobStarted(job2.startedEventBuilder(defaultStartTime()).taskId(DEFAULT_TASK_ID).build());
        store.jobFinished(job2.finishedEventBuilder(defaultSummary()).taskId(DEFAULT_TASK_ID).build());
        store.jobCommitted(job2.committedEventBuilder(defaultCommitTime()).taskId(DEFAULT_TASK_ID).build());

        // Then
        assertThat(store.getUnfinishedJobs(tableId))
                .usingRecursiveFieldByFieldElementComparator(IGNORE_UPDATE_TIMES)
                .containsExactly(compactionJobCreated(job1, ignoredUpdateTime()));
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
        assertThat(store.getUnfinishedJobs(tableId))
                .usingRecursiveFieldByFieldElementComparator(IGNORE_UPDATE_TIMES)
                .containsExactly(compactionJobCreated(job1, ignoredUpdateTime()));
    }

    @Test
    public void shouldIncludeUnfinishedCompactionJobWithOneFinishedRun() {
        // Given
        Partition partition = singlePartition();
        FileReferenceFactory fileFactory = fileFactory(partition);
        CompactionJob job = jobFactory.createCompactionJob(
                List.of(fileFactory.rootFile("file1", 123L)),
                partition.getId());

        // When
        storeJobCreated(job);
        store.jobStarted(job.startedEventBuilder(defaultStartTime()).taskId(DEFAULT_TASK_ID).build());
        store.jobFinished(job.finishedEventBuilder(defaultSummary()).taskId(DEFAULT_TASK_ID).build());
        store.jobCommitted(job.committedEventBuilder(defaultCommitTime()).taskId(DEFAULT_TASK_ID).build());
        store.jobStarted(job.startedEventBuilder(defaultStartTime()).taskId(DEFAULT_TASK_ID).build());

        // Then
        assertThat(store.getUnfinishedJobs(tableId))
                .usingRecursiveFieldByFieldElementComparator(IGNORE_UPDATE_TIMES)
                .containsExactly(compactionJobCreated(job, ignoredUpdateTime(),
                        startedCompactionRun(DEFAULT_TASK_ID, defaultStartTime()),
                        finishedCompactionRun(DEFAULT_TASK_ID, defaultSummary(), defaultCommitTime())));
    }
}
