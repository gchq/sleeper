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
import sleeper.compaction.job.status.CompactionJobStatus;
import sleeper.compaction.status.testutils.DynamoDBCompactionJobStatusStoreTestBase;
import sleeper.core.partition.Partition;
import sleeper.statestore.FileInfoFactory;

import java.util.Arrays;
import java.util.Collections;

import static org.assertj.core.api.Assertions.assertThat;

public class StoreCompactionJobCreatedIT extends DynamoDBCompactionJobStatusStoreTestBase {

    @Test
    public void shouldReportCompactionJobCreated() {
        // Given
        Partition partition = singlePartition();
        FileInfoFactory fileFactory = fileFactory(partition);
        CompactionJob job = jobFactory.createCompactionJob(
                Collections.singletonList(fileFactory.leafFile(100L, "a", "z")),
                partition.getId());

        // When
        store.jobCreated(job);

        // Then
        assertThat(getAllJobStatuses())
                .usingRecursiveFieldByFieldElementComparator(IGNORE_UPDATE_TIMES)
                .containsExactly(CompactionJobStatus.created(job, ignoredUpdateTime()));
    }

    @Test
    public void shouldReportSplittingCompactionJobCreated() {
        // Given
        FileInfoFactory fileFactory = fileFactoryWithPartitions(builder -> builder
                .leavesWithSplits(
                        Arrays.asList("A", "B"),
                        Collections.singletonList("ggg"))
                .parentJoining("C", "A", "B"));
        CompactionJob job = jobFactory.createSplittingCompactionJob(
                Arrays.asList(
                        fileFactory.rootFile("file1", 100L, "a", "c"),
                        fileFactory.rootFile("file2", 100L, "w", "z")),
                "C", "A", "B", "ggg", 0);

        // When
        store.jobCreated(job);

        // Then
        assertThat(getAllJobStatuses())
                .usingRecursiveFieldByFieldElementComparator(IGNORE_UPDATE_TIMES)
                .containsExactly(CompactionJobStatus.created(job, ignoredUpdateTime()));
    }

    @Test
    public void shouldReportCompactionJobCreatedWithSeveralFiles() {
        // Given
        Partition partition = singlePartition();
        FileInfoFactory fileFactory = fileFactory(partition);
        CompactionJob job = jobFactory.createCompactionJob(
                Arrays.asList(
                        fileFactory.leafFile("file1", 100L, "a", "c"),
                        fileFactory.leafFile("file2", 100L, "w", "z")),
                partition.getId());

        // When
        store.jobCreated(job);

        // Then
        assertThat(getAllJobStatuses())
                .usingRecursiveFieldByFieldElementComparator(IGNORE_UPDATE_TIMES)
                .containsExactly(CompactionJobStatus.created(job, ignoredUpdateTime()));
    }

    @Test
    public void shouldReportSeveralCompactionJobsCreated() {
        // Given
        FileInfoFactory fileFactory = fileFactoryWithPartitions(builder -> builder
                .leavesWithSplits(
                        Arrays.asList("A", "B"),
                        Collections.singletonList("ggg"))
                .parentJoining("C", "A", "B"));
        CompactionJob job1 = jobFactory.createCompactionJob(
                Collections.singletonList(fileFactory.leafFile(100L, "a", "c")), "A");
        CompactionJob job2 = jobFactory.createCompactionJob(
                Collections.singletonList(fileFactory.leafFile(100L, "w", "z")), "B");

        // When
        store.jobCreated(job1);
        store.jobCreated(job2);

        // Then
        assertThat(getAllJobStatuses())
                .usingRecursiveFieldByFieldElementComparator(IGNORE_UPDATE_TIMES)
                .containsExactlyInAnyOrder(
                        CompactionJobStatus.created(job1, ignoredUpdateTime()),
                        CompactionJobStatus.created(job2, ignoredUpdateTime()));
    }

    @Test
    public void shouldReportCompactionAndSplittingJobCreated() {
        // Given
        FileInfoFactory fileFactory = fileFactoryWithPartitions(builder -> builder
                .leavesWithSplits(
                        Arrays.asList("A", "B"),
                        Collections.singletonList("ggg"))
                .parentJoining("C", "A", "B"));
        CompactionJob job1 = jobFactory.createCompactionJob(
                Collections.singletonList(fileFactory.leafFile(100L, "a", "c")), "A");
        CompactionJob job2 = jobFactory.createSplittingCompactionJob(
                Collections.singletonList(fileFactory.rootFile(100L, "b", "w")),
                "C", "A", "B", "ggg", 0);

        // When
        store.jobCreated(job1);
        store.jobCreated(job2);

        // Then
        assertThat(getAllJobStatuses())
                .usingRecursiveFieldByFieldElementComparator(IGNORE_UPDATE_TIMES)
                .containsExactlyInAnyOrder(
                        CompactionJobStatus.created(job1, ignoredUpdateTime()),
                        CompactionJobStatus.created(job2, ignoredUpdateTime()));
    }
}
