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
import sleeper.compaction.core.job.CompactionJobStatusFromJobTestData;
import sleeper.compaction.status.store.testutils.DynamoDBCompactionJobStatusStoreTestBase;
import sleeper.core.partition.Partition;
import sleeper.core.statestore.FileReferenceFactory;

import java.time.Instant;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

public class StoreCompactionJobCreatedIT extends DynamoDBCompactionJobStatusStoreTestBase {

    @Test
    public void shouldReportCompactionJobCreated() {
        // Given
        Partition partition = singlePartition();
        FileReferenceFactory fileFactory = fileFactory(partition);
        CompactionJob job = jobFactory.createCompactionJob(
                List.of(fileFactory.rootFile(100L)),
                partition.getId());
        Instant createdTime = Instant.parse("2024-09-06T09:53:00Z");

        // When
        storeJobCreatedAtTime(createdTime, job);

        // Then
        assertThat(getAllJobStatuses())
                .usingRecursiveFieldByFieldElementComparator(IGNORE_EXPIRY_TIME)
                .containsExactly(CompactionJobStatusFromJobTestData.compactionJobCreated(job, createdTime));
    }

    @Test
    public void shouldReportCompactionJobCreatedWithSeveralFiles() {
        // Given
        Partition partition = singlePartition();
        FileReferenceFactory fileFactory = fileFactory(partition);
        CompactionJob job = jobFactory.createCompactionJob(
                List.of(
                        fileFactory.rootFile("file1", 100L),
                        fileFactory.rootFile("file2", 100L)),
                partition.getId());
        Instant createdTime = Instant.parse("2024-09-06T09:53:00Z");

        // When
        storeJobCreatedAtTime(createdTime, job);

        // Then
        assertThat(getAllJobStatuses())
                .usingRecursiveFieldByFieldElementComparator(IGNORE_EXPIRY_TIME)
                .containsExactly(CompactionJobStatusFromJobTestData.compactionJobCreated(job, createdTime));
    }

    @Test
    public void shouldReportSeveralCompactionJobsCreated() {
        // Given
        FileReferenceFactory fileFactory = fileFactoryWithPartitions(builder -> builder
                .rootFirst("A")
                .splitToNewChildren("A", "B", "C", "ggg"));
        CompactionJob job1 = jobFactory.createCompactionJob(
                List.of(fileFactory.partitionFile("B", 100L)), "B");
        CompactionJob job2 = jobFactory.createCompactionJob(
                List.of(fileFactory.partitionFile("C", 100L)), "C");
        Instant createdTime1 = Instant.parse("2024-09-06T09:54:00Z");
        Instant createdTime2 = Instant.parse("2024-09-06T09:55:00Z");

        // When
        storeJobCreatedAtTime(createdTime1, job1);
        storeJobCreatedAtTime(createdTime2, job2);

        // Then
        assertThat(getAllJobStatuses())
                .usingRecursiveFieldByFieldElementComparator(IGNORE_EXPIRY_TIME)
                .containsExactlyInAnyOrder(
                        CompactionJobStatusFromJobTestData.compactionJobCreated(job1, createdTime1),
                        CompactionJobStatusFromJobTestData.compactionJobCreated(job2, createdTime2));
    }
}
