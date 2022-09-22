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

import org.assertj.core.api.AbstractListAssert;
import org.assertj.core.api.ObjectAssert;
import org.assertj.core.groups.Tuple;
import org.junit.Test;
import sleeper.compaction.job.CompactionJob;
import sleeper.compaction.status.job.testutils.DynamoDBCompactionJobStatusStoreTestBase;
import sleeper.core.partition.Partition;
import sleeper.statestore.FileInfoFactory;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.tuple;
import static sleeper.compaction.status.DynamoDBAttributes.getNumberAttribute;
import static sleeper.compaction.status.DynamoDBAttributes.getStringAttribute;
import static sleeper.compaction.status.job.DynamoDBCompactionJobStatusFormat.INPUT_FILES_COUNT;
import static sleeper.compaction.status.job.DynamoDBCompactionJobStatusFormat.JOB_ID;
import static sleeper.compaction.status.job.DynamoDBCompactionJobStatusFormat.PARTITION_ID;
import static sleeper.compaction.status.job.DynamoDBCompactionJobStatusFormat.SPLIT_TO_PARTITION_IDS;
import static sleeper.compaction.status.job.DynamoDBCompactionJobStatusFormat.UPDATE_TIME;
import static sleeper.compaction.status.job.DynamoDBCompactionJobStatusFormat.UPDATE_TYPE;
import static sleeper.compaction.status.job.DynamoDBCompactionJobStatusFormat.UPDATE_TYPE_CREATED;

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
        assertThatItemsInTable().containsExactly(
                createCompactionItem(job.getId(), 1, partition.getId()));
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
        assertThatItemsInTable().containsExactly(
                createSplittingCompactionItem(job.getId(), 2, "C", "A, B"));
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
        assertThatItemsInTable().containsExactly(
                createCompactionItem(job.getId(), 2, partition.getId()));
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
        assertThatItemsInTable().containsExactlyInAnyOrder(
                createCompactionItem(job1.getId(), 1, "A"),
                createCompactionItem(job2.getId(), 1, "B"));
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
        assertThatItemsInTable().containsExactlyInAnyOrder(
                createCompactionItem(job1.getId(), 1, "A"),
                createSplittingCompactionItem(job2.getId(), 1, "C", "A, B"));
    }

    private AbstractListAssert<?, List<? extends Tuple>, Tuple, ObjectAssert<Tuple>> assertThatItemsInTable() {
        return assertThatRawItemsInTable()
                .extracting(
                        Map::keySet,
                        map -> getStringAttribute(map, JOB_ID),
                        map -> getStringAttribute(map, UPDATE_TYPE),
                        map -> getStringAttribute(map, PARTITION_ID),
                        map -> getNumberAttribute(map, INPUT_FILES_COUNT),
                        map -> getStringAttribute(map, SPLIT_TO_PARTITION_IDS));
    }

    private Tuple createCompactionItem(String jobId, int inputFilesCount, String partitionId) {
        return tuple(
                Stream.of(JOB_ID, UPDATE_TIME, UPDATE_TYPE, PARTITION_ID, INPUT_FILES_COUNT).collect(Collectors.toSet()),
                jobId, UPDATE_TYPE_CREATED, partitionId, "" + inputFilesCount, null);
    }

    private Tuple createSplittingCompactionItem(String jobId, int inputFilesCount, String partitionId, String splitToPartitionIds) {
        return tuple(
                Stream.of(JOB_ID, UPDATE_TIME, UPDATE_TYPE, PARTITION_ID, INPUT_FILES_COUNT, SPLIT_TO_PARTITION_IDS).collect(Collectors.toSet()),
                jobId, UPDATE_TYPE_CREATED, partitionId, "" + inputFilesCount, splitToPartitionIds);
    }
}
