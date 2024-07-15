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
import sleeper.compaction.status.store.testutils.DynamoDBCompactionJobStatusStoreTestBase;
import sleeper.core.partition.Partition;
import sleeper.core.statestore.FileReferenceFactory;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.compaction.job.CompactionJobStatusTestData.finishedCompactionRun;
import static sleeper.compaction.job.CompactionJobStatusTestData.jobCreated;
import static sleeper.compaction.job.CompactionJobStatusTestData.startedCompactionRun;
import static sleeper.compaction.job.status.CompactionJobCommittedEvent.compactionJobCommitted;
import static sleeper.compaction.job.status.CompactionJobFinishedEvent.compactionJobFinished;
import static sleeper.compaction.job.status.CompactionJobStartedEvent.compactionJobStarted;

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
        store.jobCreated(job1);
        store.jobCreated(job2);

        // Then
        assertThat(store.getUnfinishedJobs(tableId))
                .usingRecursiveFieldByFieldElementComparator(IGNORE_UPDATE_TIMES)
                .containsExactly(
                        jobCreated(job2, ignoredUpdateTime()),
                        jobCreated(job1, ignoredUpdateTime()));
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
        store.jobCreated(job1);
        store.jobCreated(job2);
        store.jobStarted(compactionJobStarted(job2, defaultStartTime()).taskId(DEFAULT_TASK_ID).build());
        store.jobFinished(compactionJobFinished(job2, defaultSummary()).taskId(DEFAULT_TASK_ID).build());
        store.jobCommitted(compactionJobCommitted(job2, defaultCommitTime()).taskId(DEFAULT_TASK_ID).build());

        // Then
        assertThat(store.getUnfinishedJobs(tableId))
                .usingRecursiveFieldByFieldElementComparator(IGNORE_UPDATE_TIMES)
                .containsExactly(jobCreated(job1, ignoredUpdateTime()));
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
        store.jobCreated(job1);
        store.jobCreated(job2);

        // Then
        assertThat(store.getUnfinishedJobs(tableId))
                .usingRecursiveFieldByFieldElementComparator(IGNORE_UPDATE_TIMES)
                .containsExactly(jobCreated(job1, ignoredUpdateTime()));
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
        store.jobCreated(job);
        store.jobCreated(job);
        store.jobStarted(compactionJobStarted(job, defaultStartTime()).taskId(DEFAULT_TASK_ID).build());
        store.jobFinished(compactionJobFinished(job, defaultSummary()).taskId(DEFAULT_TASK_ID).build());
        store.jobCommitted(compactionJobCommitted(job, defaultCommitTime()).taskId(DEFAULT_TASK_ID).build());
        store.jobStarted(compactionJobStarted(job, defaultStartTime()).taskId(DEFAULT_TASK_ID).build());

        // Then
        assertThat(store.getUnfinishedJobs(tableId))
                .usingRecursiveFieldByFieldElementComparator(IGNORE_UPDATE_TIMES)
                .containsExactly(jobCreated(job, ignoredUpdateTime(),
                        startedCompactionRun(DEFAULT_TASK_ID, defaultStartTime()),
                        finishedCompactionRun(DEFAULT_TASK_ID, defaultSummary(), defaultCommitTime())));
    }
}
