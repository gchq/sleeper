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
package sleeper.compaction.jobexecution;

import org.junit.jupiter.api.Test;

import sleeper.compaction.job.CompactionJob;
import sleeper.compaction.jobexecution.testutils.CompactSortedFilesTestBase;
import sleeper.compaction.jobexecution.testutils.CompactSortedFilesTestDataHelper;
import sleeper.core.partition.PartitionsBuilder;
import sleeper.core.record.Record;
import sleeper.core.record.process.RecordsProcessedSummary;
import sleeper.core.schema.Schema;
import sleeper.core.schema.type.LongType;
import sleeper.statestore.FileInfo;
import sleeper.statestore.FileInfo.FileStatus;
import sleeper.statestore.StateStore;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.compaction.jobexecution.testutils.CompactSortedFilesTestData.combineSortedBySingleKey;
import static sleeper.compaction.jobexecution.testutils.CompactSortedFilesTestData.keyAndTwoValuesSortedEvenLongs;
import static sleeper.compaction.jobexecution.testutils.CompactSortedFilesTestData.keyAndTwoValuesSortedOddLongs;
import static sleeper.compaction.jobexecution.testutils.CompactSortedFilesTestData.readDataFile;
import static sleeper.compaction.jobexecution.testutils.CompactSortedFilesTestUtils.createCompactSortedFiles;
import static sleeper.compaction.jobexecution.testutils.CompactSortedFilesTestUtils.createSchemaWithTypesForKeyAndTwoValues;
import static sleeper.statestore.inmemory.StateStoreTestHelper.inMemoryStateStoreWithFixedPartitions;
import static sleeper.statestore.inmemory.StateStoreTestHelper.inMemoryStateStoreWithFixedSinglePartition;

class CompactSortedFilesEmptyOutputIT extends CompactSortedFilesTestBase {

    @Test
    void filesShouldMergeCorrectlyWhenSomeAreEmpty() throws Exception {
        // Given
        Schema schema = createSchemaWithTypesForKeyAndTwoValues(new LongType(), new LongType(), new LongType());
        StateStore stateStore = inMemoryStateStoreWithFixedSinglePartition(schema);
        CompactSortedFilesTestDataHelper dataHelper = new CompactSortedFilesTestDataHelper(schema, stateStore);

        List<Record> data = keyAndTwoValuesSortedEvenLongs();
        dataHelper.writeLeafFile(folderName + "/file1.parquet", data, 0L, 198L);
        dataHelper.writeLeafFile(folderName + "/file2.parquet", Collections.emptyList(), null, null);

        CompactionJob compactionJob = compactionFactory().createCompactionJob(
                dataHelper.allFileInfos(), dataHelper.singlePartition().getId());
        dataHelper.addFilesToStateStoreForJob(compactionJob);

        // When
        CompactSortedFiles compactSortedFiles = createCompactSortedFiles(schema, compactionJob, stateStore, DEFAULT_TASK_ID);
        RecordsProcessedSummary summary = compactSortedFiles.compact();

        // Then
        //  - Read output file and check that it contains the right results
        assertThat(summary.getRecordsRead()).isEqualTo(data.size());
        assertThat(summary.getRecordsWritten()).isEqualTo(data.size());
        assertThat(readDataFile(schema, compactionJob.getOutputFile())).isEqualTo(data);

        // - Check DynamoDBStateStore has the correct file-in-partition entries
        assertThat(stateStore.getFileInPartitionList())
                .usingRecursiveFieldByFieldElementComparatorIgnoringFields("lastStateStoreUpdateTime")
                .containsExactly(dataHelper.expectedLeafFile(compactionJob.getOutputFile(), 100L, 0L, 198L));

        // - Check DynamoDBStateStore has the correct file-lifecycle entries
        List<FileInfo> expectedFileInfos = new ArrayList<>();
        expectedFileInfos.add(dataHelper.expectedLeafFile(compactionJob.getOutputFile(), 100L, 0L, 198L).cloneWithStatus(FileStatus.ACTIVE));
        dataHelper.allFileInfos().stream()
                .map(fi -> fi.cloneWithStatus(FileStatus.ACTIVE))
                .forEach(expectedFileInfos::add);
        assertThat(stateStore.getFileLifecycleList())
                .usingRecursiveFieldByFieldElementComparatorIgnoringFields("lastStateStoreUpdateTime")
                .containsExactlyInAnyOrder(expectedFileInfos.toArray(new FileInfo[0]));
    }

    @Test
    void filesShouldMergeCorrectlyWhenAllAreEmpty() throws Exception {
        // Given
        Schema schema = createSchemaWithTypesForKeyAndTwoValues(new LongType(), new LongType(), new LongType());
        StateStore stateStore = inMemoryStateStoreWithFixedSinglePartition(schema);
        CompactSortedFilesTestDataHelper dataHelper = new CompactSortedFilesTestDataHelper(schema, stateStore);

        dataHelper.writeLeafFile(folderName + "/file1.parquet", Collections.emptyList(), null, null);
        dataHelper.writeLeafFile(folderName + "/file2.parquet", Collections.emptyList(), null, null);

        CompactionJob compactionJob = compactionFactory().createCompactionJob(
                dataHelper.allFileInfos(), dataHelper.singlePartition().getId());
        dataHelper.addFilesToStateStoreForJob(compactionJob);

        // When
        CompactSortedFiles compactSortedFiles = createCompactSortedFiles(schema, compactionJob, stateStore, DEFAULT_TASK_ID);
        RecordsProcessedSummary summary = compactSortedFiles.compact();

        // Then
        //  - Read output file and check that it contains the right results
        assertThat(summary.getRecordsRead()).isZero();
        assertThat(summary.getRecordsWritten()).isZero();
        assertThat(readDataFile(schema, compactionJob.getOutputFile())).isEmpty();

        // - Check DynamoDBStateStore has the correct file-in-partition entries
        assertThat(stateStore.getFileInPartitionList())
                .usingRecursiveFieldByFieldElementComparatorIgnoringFields("lastStateStoreUpdateTime")
                .containsExactly(dataHelper.expectedLeafFile(compactionJob.getOutputFile(), 0L, null, null));

        // - Check DynamoDBStateStore has the correct file-lifecycle entries
        List<FileInfo> expectedFileInfos = new ArrayList<>();
        expectedFileInfos.add(dataHelper.expectedLeafFile(compactionJob.getOutputFile(), 0L, null, null).cloneWithStatus(FileStatus.ACTIVE));
        dataHelper.allFileInfos().stream()
                .map(fi -> fi.cloneWithStatus(FileStatus.ACTIVE))
                .forEach(expectedFileInfos::add);
        assertThat(stateStore.getFileLifecycleList())
                .usingRecursiveFieldByFieldElementComparatorIgnoringFields("lastStateStoreUpdateTime")
                .containsExactlyInAnyOrder(expectedFileInfos.toArray(new FileInfo[0]));
    }

    @Test
    void filesShouldMergeAndSplitCorrectlyWhenOneChildFileIsEmpty() throws Exception {
        // Given
        Schema schema = createSchemaWithTypesForKeyAndTwoValues(new LongType(), new LongType(), new LongType());
        StateStore stateStore = inMemoryStateStoreWithFixedPartitions(new PartitionsBuilder(schema)
                .leavesWithSplits(Arrays.asList("A", "B"), Collections.singletonList(200L))
                .parentJoining("C", "A", "B")
                .buildList());
        CompactSortedFilesTestDataHelper dataHelper = new CompactSortedFilesTestDataHelper(schema, stateStore);

        List<Record> data1 = keyAndTwoValuesSortedEvenLongs();
        List<Record> data2 = keyAndTwoValuesSortedOddLongs();
        dataHelper.writeRootFile(folderName + "/file1.parquet", data1, 0L, 198L);
        dataHelper.writeRootFile(folderName + "/file2.parquet", data2, 1L, 199L);

        CompactionJob compactionJob = compactionFactory().createSplittingCompactionJob(
                dataHelper.allFileInfos(), "C", "A", "B", 200L, 0);
        dataHelper.addFilesToStateStoreForJob(compactionJob);

        // When
        CompactSortedFiles compactSortedFiles = createCompactSortedFiles(schema, compactionJob, stateStore, DEFAULT_TASK_ID);
        RecordsProcessedSummary summary = compactSortedFiles.compact();

        // Then
        //  - Read output files and check that they contain the right results
        List<Record> expectedResults = combineSortedBySingleKey(data1, data2);
        assertThat(summary.getRecordsRead()).isEqualTo(200L);
        assertThat(summary.getRecordsWritten()).isEqualTo(200L);
        assertThat(readDataFile(schema, compactionJob.getOutputFiles().getLeft())).isEqualTo(expectedResults);
        assertThat(readDataFile(schema, compactionJob.getOutputFiles().getRight())).isEmpty();

        // - Check DynamoDBStateStore has the correct file-in-partition entries
        assertThat(stateStore.getFileInPartitionList())
                .usingRecursiveFieldByFieldElementComparatorIgnoringFields("lastStateStoreUpdateTime")
                .containsExactlyInAnyOrder(
                        dataHelper.expectedPartitionFile("A", compactionJob.getOutputFiles().getLeft(), 200L, 0L, 199L),
                        dataHelper.expectedPartitionFile("B", compactionJob.getOutputFiles().getRight(), 0L, null, null));

        // - Check DynamoDBStateStore has the correct file-lifecycle entries
        List<FileInfo> expectedFileInfos = new ArrayList<>();
        expectedFileInfos.add(dataHelper.expectedPartitionFile("A", compactionJob.getOutputFiles().getLeft(), 200L, 0L, 199L).cloneWithStatus(FileStatus.ACTIVE));
        expectedFileInfos.add(dataHelper.expectedPartitionFile("B", compactionJob.getOutputFiles().getRight(), 0L, null, null).cloneWithStatus(FileStatus.ACTIVE));
        dataHelper.allFileInfos().stream()
                .map(fi -> fi.cloneWithStatus(FileStatus.ACTIVE))
                .forEach(expectedFileInfos::add);
        assertThat(stateStore.getFileLifecycleList())
                .usingRecursiveFieldByFieldElementComparatorIgnoringFields("lastStateStoreUpdateTime")
                .containsExactlyInAnyOrder(expectedFileInfos.toArray(new FileInfo[0]));
    }
}
