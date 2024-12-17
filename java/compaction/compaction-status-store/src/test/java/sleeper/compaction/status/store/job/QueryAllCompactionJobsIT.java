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
import static sleeper.compaction.core.job.CompactionJobStatusFromJobTestData.compactionJobCreated;

public class QueryAllCompactionJobsIT extends DynamoDBCompactionJobStatusStoreTestBase {

    @Test
    public void shouldReturnMultipleCompactionJobsSortedMostRecentFirst() {
        // Given
        Partition partition = singlePartition();
        FileReferenceFactory fileFactory = fileFactory(partition);
        CompactionJob job1 = jobFactory.createCompactionJob(
                List.of(fileFactory.rootFile("file1", 123L)),
                partition.getId());
        CompactionJob job2 = jobFactory.createCompactionJob(
                List.of(fileFactory.rootFile("file2", 456L)),
                partition.getId());
        CompactionJob job3 = jobFactory.createCompactionJob(
                List.of(fileFactory.rootFile("file3", 789L)),
                partition.getId());

        // When
        storeJobsCreated(job1, job2, job3);

        // Then
        assertThat(store.getAllJobs(tableId))
                .usingRecursiveFieldByFieldElementComparator(IGNORE_UPDATE_TIMES)
                .containsExactly(
                        compactionJobCreated(job3, ignoredUpdateTime()),
                        compactionJobCreated(job2, ignoredUpdateTime()),
                        compactionJobCreated(job1, ignoredUpdateTime()));
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
        assertThat(store.getAllJobs(tableId))
                .usingRecursiveFieldByFieldElementComparator(IGNORE_UPDATE_TIMES)
                .containsExactly(compactionJobCreated(job1, ignoredUpdateTime()));
    }

    @Test
    public void shouldReturnNoCompactionJobs() {
        // When / Then
        assertThat(store.getAllJobs(tableId)).isEmpty();
    }
}
