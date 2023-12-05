/*
 * Copyright 2022-2023 Crown Copyright
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
import sleeper.core.statestore.FileInfoFactory;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

public class StoreCompactionJobCreatedIT extends DynamoDBCompactionJobStatusStoreTestBase {

    @Test
    public void shouldReportCompactionJobCreated() {
        // Given
        Partition partition = singlePartition();
        FileInfoFactory fileFactory = fileFactory(partition);
        CompactionJob job = jobFactory.createCompactionJob(
                List.of(fileFactory.rootFile(100L)),
                partition.getId());

        // When
        store.jobCreated(job);

        // Then
        assertThat(getAllJobStatuses())
                .usingRecursiveFieldByFieldElementComparator(IGNORE_UPDATE_TIMES)
                .containsExactly(CompactionJobStatusTestData.jobCreated(job, ignoredUpdateTime()));
    }

    @Test
    public void shouldReportSplittingCompactionJobCreated() {
        // Given
        FileInfoFactory fileFactory = fileFactoryWithPartitions(builder -> builder
                .rootFirst("A")
                .splitToNewChildren("A", "B", "C", "ggg"));
        CompactionJob job = jobFactory.createSplittingCompactionJob(
                List.of(
                        fileFactory.rootFile("file1", 100L),
                        fileFactory.rootFile("file2", 100L)),
                "A", "B", "C");

        // When
        store.jobCreated(job);

        // Then
        assertThat(getAllJobStatuses())
                .usingRecursiveFieldByFieldElementComparator(IGNORE_UPDATE_TIMES)
                .containsExactly(CompactionJobStatusTestData.jobCreated(job, ignoredUpdateTime()));
    }

    @Test
    public void shouldReportCompactionJobCreatedWithSeveralFiles() {
        // Given
        Partition partition = singlePartition();
        FileInfoFactory fileFactory = fileFactory(partition);
        CompactionJob job = jobFactory.createCompactionJob(
                List.of(
                        fileFactory.rootFile("file1", 100L),
                        fileFactory.rootFile("file2", 100L)),
                partition.getId());

        // When
        store.jobCreated(job);

        // Then
        assertThat(getAllJobStatuses())
                .usingRecursiveFieldByFieldElementComparator(IGNORE_UPDATE_TIMES)
                .containsExactly(CompactionJobStatusTestData.jobCreated(job, ignoredUpdateTime()));
    }

    @Test
    public void shouldReportSeveralCompactionJobsCreated() {
        // Given
        FileInfoFactory fileFactory = fileFactoryWithPartitions(builder -> builder
                .rootFirst("A")
                .splitToNewChildren("A", "B", "C", "ggg"));
        CompactionJob job1 = jobFactory.createCompactionJob(
                List.of(fileFactory.partitionFile("B", 100L)), "B");
        CompactionJob job2 = jobFactory.createCompactionJob(
                List.of(fileFactory.partitionFile("C", 100L)), "C");

        // When
        store.jobCreated(job1);
        store.jobCreated(job2);

        // Then
        assertThat(getAllJobStatuses())
                .usingRecursiveFieldByFieldElementComparator(IGNORE_UPDATE_TIMES)
                .containsExactlyInAnyOrder(
                        CompactionJobStatusTestData.jobCreated(job1, ignoredUpdateTime()),
                        CompactionJobStatusTestData.jobCreated(job2, ignoredUpdateTime()));
    }

    @Test
    public void shouldReportCompactionAndSplittingJobCreated() {
        // Given
        FileInfoFactory fileFactory = fileFactoryWithPartitions(builder -> builder
                .rootFirst("A")
                .splitToNewChildren("A", "B", "C", "ggg"));
        CompactionJob job1 = jobFactory.createCompactionJob(
                List.of(fileFactory.partitionFile("B", 100L)), "B");
        CompactionJob job2 = jobFactory.createSplittingCompactionJob(
                List.of(fileFactory.rootFile(100L)),
                "A", "B", "C");

        // When
        store.jobCreated(job1);
        store.jobCreated(job2);

        // Then
        assertThat(getAllJobStatuses())
                .usingRecursiveFieldByFieldElementComparator(IGNORE_UPDATE_TIMES)
                .containsExactlyInAnyOrder(
                        CompactionJobStatusTestData.jobCreated(job1, ignoredUpdateTime()),
                        CompactionJobStatusTestData.jobCreated(job2, ignoredUpdateTime()));
    }
}
