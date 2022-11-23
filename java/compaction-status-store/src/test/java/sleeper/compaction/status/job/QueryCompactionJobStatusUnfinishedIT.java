/*
 * Copyright 2022 Crown Copyright
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
package sleeper.compaction.status.job;

import org.junit.Test;
import sleeper.compaction.job.CompactionJob;
import sleeper.compaction.job.status.CompactionJobCreatedStatus;
import sleeper.compaction.job.status.CompactionJobStatus;
import sleeper.compaction.job.status.ProcessRun;
import sleeper.compaction.status.testutils.DynamoDBCompactionJobStatusStoreTestBase;
import sleeper.core.partition.Partition;
import sleeper.core.status.ProcessFinishedStatus;
import sleeper.core.status.ProcessStartedStatus;
import sleeper.statestore.FileInfoFactory;

import java.util.Arrays;
import java.util.Collections;

import static org.assertj.core.api.Assertions.assertThat;

public class QueryCompactionJobStatusUnfinishedIT extends DynamoDBCompactionJobStatusStoreTestBase {

    @Test
    public void shouldReturnUnfinishedCompactionJobs() {
        // Given
        Partition partition = singlePartition();
        FileInfoFactory fileFactory = fileFactory(partition);
        CompactionJob job1 = jobFactory.createCompactionJob(
                Collections.singletonList(fileFactory.leafFile("file1", 123L, "a", "c")),
                partition.getId());
        CompactionJob job2 = jobFactory.createCompactionJob(
                Collections.singletonList(fileFactory.leafFile("file2", 456L, "d", "f")),
                partition.getId());

        // When
        store.jobCreated(job1);
        store.jobCreated(job2);

        // Then
        assertThat(store.getUnfinishedJobs(tableName))
                .usingRecursiveFieldByFieldElementComparator(IGNORE_UPDATE_TIMES)
                .containsExactly(
                        CompactionJobStatus.created(job2, ignoredUpdateTime()),
                        CompactionJobStatus.created(job1, ignoredUpdateTime()));
    }

    @Test
    public void shouldExcludeFinishedCompactionJob() {
        // Given
        Partition partition = singlePartition();
        FileInfoFactory fileFactory = fileFactory(partition);
        CompactionJob job1 = jobFactory.createCompactionJob(
                Collections.singletonList(fileFactory.leafFile("file1", 123L, "a", "c")),
                partition.getId());
        CompactionJob job2 = jobFactory.createCompactionJob(
                Collections.singletonList(fileFactory.leafFile("file2", 456L, "d", "f")),
                partition.getId());

        // When
        store.jobCreated(job1);
        store.jobCreated(job2);
        store.jobStarted(job2, defaultStartTime(), DEFAULT_TASK_ID);
        store.jobFinished(job2, defaultSummary(), DEFAULT_TASK_ID);

        // Then
        assertThat(store.getUnfinishedJobs(tableName))
                .usingRecursiveFieldByFieldElementComparator(IGNORE_UPDATE_TIMES)
                .containsExactly(CompactionJobStatus.created(job1, ignoredUpdateTime()));
    }

    @Test
    public void shouldExcludeCompactionJobInOtherTable() {
        // Given
        Partition partition = singlePartition();
        FileInfoFactory fileFactory = fileFactory(partition);
        CompactionJob job1 = jobFactory.createCompactionJob(
                Collections.singletonList(fileFactory.leafFile("file1", 123L, "a", "c")),
                partition.getId());
        CompactionJob job2 = jobFactoryForTable("other-table").createCompactionJob(
                Collections.singletonList(fileFactory.leafFile("file2", 456L, "d", "f")),
                partition.getId());

        // When
        store.jobCreated(job1);
        store.jobCreated(job2);

        // Then
        assertThat(store.getUnfinishedJobs(tableName))
                .usingRecursiveFieldByFieldElementComparator(IGNORE_UPDATE_TIMES)
                .containsExactly(CompactionJobStatus.created(job1, ignoredUpdateTime()));
    }

    @Test
    public void shouldIncludeUnfinishedCompactionJobWithOneFinishedRun() {
        // Given
        Partition partition = singlePartition();
        FileInfoFactory fileFactory = fileFactory(partition);
        CompactionJob job = jobFactory.createCompactionJob(
                Collections.singletonList(fileFactory.leafFile("file1", 123L, "a", "c")),
                partition.getId());

        // When
        store.jobCreated(job);
        store.jobCreated(job);
        store.jobStarted(job, defaultStartTime(), DEFAULT_TASK_ID);
        store.jobFinished(job, defaultSummary(), DEFAULT_TASK_ID);
        store.jobStarted(job, defaultStartTime(), DEFAULT_TASK_ID);

        // Then
        assertThat(store.getUnfinishedJobs(tableName))
                .usingRecursiveFieldByFieldElementComparator(IGNORE_UPDATE_TIMES)
                .containsExactly(CompactionJobStatus.builder().jobId(job.getId())
                        .createdStatus(CompactionJobCreatedStatus.from(job, ignoredUpdateTime()))
                        .jobRunsLatestFirst(Arrays.asList(
                                ProcessRun.started(DEFAULT_TASK_ID,
                                        ProcessStartedStatus.updateAndStartTime(ignoredUpdateTime(), defaultStartTime())),
                                ProcessRun.finished(DEFAULT_TASK_ID,
                                        ProcessStartedStatus.updateAndStartTime(ignoredUpdateTime(), defaultStartTime()),
                                        ProcessFinishedStatus.updateTimeAndSummary(ignoredUpdateTime(), defaultSummary()))
                        )).build());
    }
}
