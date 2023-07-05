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
import sleeper.core.schema.Field;
import sleeper.core.schema.Schema;
import sleeper.core.schema.type.LongType;
import sleeper.core.schema.type.StringType;
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
import static sleeper.compaction.jobexecution.testutils.CompactSortedFilesTestData.specifiedAndTwoValuesFromEvens;
import static sleeper.compaction.jobexecution.testutils.CompactSortedFilesTestData.specifiedAndTwoValuesFromOdds;
import static sleeper.compaction.jobexecution.testutils.CompactSortedFilesTestUtils.createCompactSortedFiles;
import static sleeper.compaction.jobexecution.testutils.CompactSortedFilesTestUtils.createSchemaWithTwoTypedValuesAndKeyFields;
import static sleeper.compaction.jobexecution.testutils.CompactSortedFilesTestUtils.createSchemaWithTypesForKeyAndTwoValues;
import static sleeper.statestore.inmemory.StateStoreTestHelper.inMemoryStateStoreWithFixedPartitions;

class CompactSortedFilesSplittingIT extends CompactSortedFilesTestBase {

    @Test
    void filesShouldMergeAndSplitCorrectlyAndStateStoreUpdated() throws Exception {
        // Given
        Schema schema = createSchemaWithTypesForKeyAndTwoValues(new LongType(), new LongType(), new LongType());
        StateStore stateStore = inMemoryStateStoreWithFixedPartitions(new PartitionsBuilder(schema)
                .leavesWithSplits(Arrays.asList("A", "B"), Collections.singletonList(100L))
                .parentJoining("C", "A", "B")
                .buildList());
        CompactSortedFilesTestDataHelper dataHelper = new CompactSortedFilesTestDataHelper(schema, stateStore);

        List<Record> data1 = keyAndTwoValuesSortedEvenLongs();
        List<Record> data2 = keyAndTwoValuesSortedOddLongs();
        dataHelper.writeRootFile(folderName + "/file1.parquet", data1, 0L, 198L);
        dataHelper.writeRootFile(folderName + "/file2.parquet", data2, 1L, 199L);

        CompactionJob compactionJob = compactionFactory().createSplittingCompactionJob(
                dataHelper.allFileInfos(), "C", "A", "B", 100L, 0);
        dataHelper.addFilesToStateStoreForJob(compactionJob);

        // When
        CompactSortedFiles compactSortedFiles = createCompactSortedFiles(schema, compactionJob, stateStore, DEFAULT_TASK_ID);
        RecordsProcessedSummary summary = compactSortedFiles.compact();

        // Then
        //  - Read output files and check that they contain the right results
        List<Record> expectedResults = combineSortedBySingleKey(data1, data2);
        assertThat(summary.getRecordsRead()).isEqualTo(200L);
        assertThat(summary.getRecordsWritten()).isEqualTo(200L);
        assertThat(readDataFile(schema, compactionJob.getOutputFiles().getLeft())).isEqualTo(expectedResults.subList(0, 100));
        assertThat(readDataFile(schema, compactionJob.getOutputFiles().getRight())).isEqualTo(expectedResults.subList(100, 200));

        // - Check DynamoDBStateStore has the correct file-in-partition list
        assertThat(stateStore.getFileInPartitionList())
                .usingRecursiveFieldByFieldElementComparatorIgnoringFields("lastStateStoreUpdateTime")
                .containsExactlyInAnyOrder(
                        dataHelper.expectedPartitionFile("A", compactionJob.getOutputFiles().getLeft(), 100L, 0L, 99L),
                        dataHelper.expectedPartitionFile("B", compactionJob.getOutputFiles().getRight(), 100L, 100L, 199L));

        // - Check DynamoDBStateStore has the correct file-lifecycle entries
        List<FileInfo> expectedFileInfos = new ArrayList<>();
        expectedFileInfos.add(dataHelper.expectedPartitionFile("A", compactionJob.getOutputFiles().getLeft(), 100L, 0L, 99L).cloneWithStatus(FileStatus.ACTIVE));
        expectedFileInfos.add(dataHelper.expectedPartitionFile("B", compactionJob.getOutputFiles().getRight(), 100L, 100L, 199L).cloneWithStatus(FileStatus.ACTIVE));
        dataHelper.allFileInfos().stream()
                .map(fi -> fi.cloneWithStatus(FileStatus.ACTIVE))
                .forEach(expectedFileInfos::add);
        assertThat(stateStore.getFileLifecycleList())
                .usingRecursiveFieldByFieldElementComparatorIgnoringFields("lastStateStoreUpdateTime")
                .containsExactlyInAnyOrder(expectedFileInfos.toArray(new FileInfo[0]));
    }

    @Test
    void filesShouldMergeAndSplitCorrectlyWith2DimKeySplitOnFirstKey() throws Exception {
        // Given
        Field field1 = new Field("key1", new LongType());
        Field field2 = new Field("key2", new StringType());
        Schema schema = createSchemaWithTwoTypedValuesAndKeyFields(new LongType(), new LongType(), field1, field2);
        StateStore stateStore = inMemoryStateStoreWithFixedPartitions(new PartitionsBuilder(schema)
                .leavesWithSplits(Arrays.asList("A", "B"), Collections.singletonList(100L))
                .parentJoining("C", "A", "B")
                .buildList());
        CompactSortedFilesTestDataHelper dataHelper = new CompactSortedFilesTestDataHelper(schema, stateStore);

        List<Record> data1 = specifiedAndTwoValuesFromEvens((even, record) -> {
            record.put(field1.getName(), (long) even);
            record.put(field2.getName(), "A");
        });
        List<Record> data2 = specifiedAndTwoValuesFromOdds((odd, record) -> {
            record.put(field1.getName(), (long) odd);
            record.put(field2.getName(), "A");
        });
        dataHelper.writeRootFile(folderName + "/file1.parquet", data1, 0L, 198L);
        dataHelper.writeRootFile(folderName + "/file2.parquet", data2, 1L, 199L);

        CompactionJob compactionJob = compactionFactory().createSplittingCompactionJob(
                dataHelper.allFileInfos(), "C", "A", "B", 100L, 0);
        dataHelper.addFilesToStateStoreForJob(compactionJob);

        // When
        CompactSortedFiles compactSortedFiles = createCompactSortedFiles(schema, compactionJob, stateStore, DEFAULT_TASK_ID);
        RecordsProcessedSummary summary = compactSortedFiles.compact();

        // Then
        //  - Read output files and check that they contain the right results
        List<Record> expectedResults = combineSortedBySingleKey(data1, data2, record -> record.get(field1.getName()));
        assertThat(summary.getRecordsRead()).isEqualTo(200L);
        assertThat(summary.getRecordsWritten()).isEqualTo(200L);
        assertThat(readDataFile(schema, compactionJob.getOutputFiles().getLeft())).isEqualTo(expectedResults.subList(0, 100));
        assertThat(readDataFile(schema, compactionJob.getOutputFiles().getRight())).isEqualTo(expectedResults.subList(100, 200));

        // - Check DynamoDBStateStore has the correct file-in-partition list
        assertThat(stateStore.getFileInPartitionList())
                .usingRecursiveFieldByFieldElementComparatorIgnoringFields("lastStateStoreUpdateTime")
                .containsExactlyInAnyOrder(
                        dataHelper.expectedPartitionFile("A", compactionJob.getOutputFiles().getLeft(), 100L, 0L, 99L),
                        dataHelper.expectedPartitionFile("B", compactionJob.getOutputFiles().getRight(), 100L, 100L, 199L));

        // - Check DynamoDBStateStore has the correct file-lifecycle entries
        List<FileInfo> expectedFileInfos = new ArrayList<>();
        expectedFileInfos.add(dataHelper.expectedPartitionFile("A", compactionJob.getOutputFiles().getLeft(), 100L, 0L, 99L).cloneWithStatus(FileStatus.ACTIVE));
        expectedFileInfos.add(dataHelper.expectedPartitionFile("B", compactionJob.getOutputFiles().getRight(), 100L, 100L, 199L).cloneWithStatus(FileStatus.ACTIVE));
        dataHelper.allFileInfos().stream()
                .map(fi -> fi.cloneWithStatus(FileStatus.ACTIVE))
                .forEach(expectedFileInfos::add);
        assertThat(stateStore.getFileLifecycleList())
                .usingRecursiveFieldByFieldElementComparatorIgnoringFields("lastStateStoreUpdateTime")
                .containsExactlyInAnyOrder(expectedFileInfos.toArray(new FileInfo[0]));
    }

    @Test
    void filesShouldMergeAndSplitCorrectlyWith2DimKeySplitOnSecondKey() throws Exception {
        // Given
        Field field1 = new Field("key1", new LongType());
        Field field2 = new Field("key2", new StringType());
        Schema schema = createSchemaWithTwoTypedValuesAndKeyFields(new LongType(), new LongType(), field1, field2);
        StateStore stateStore = inMemoryStateStoreWithFixedPartitions(new PartitionsBuilder(schema)
                .leavesWithSplitsOnDimension(1, Arrays.asList("A", "B"), Collections.singletonList("A2"))
                .parentJoining("C", "A", "B")
                .buildList());
        CompactSortedFilesTestDataHelper dataHelper = new CompactSortedFilesTestDataHelper(schema, stateStore);

        List<Record> data1 = specifiedAndTwoValuesFromEvens((even, record) -> {
            record.put(field1.getName(), (long) even);
            record.put(field2.getName(), "A");
        });
        List<Record> data2 = specifiedAndTwoValuesFromOdds((odd, record) -> {
            record.put(field1.getName(), (long) odd);
            record.put(field2.getName(), "B");
        });
        dataHelper.writeRootFile(folderName + "/file1.parquet", data1, 0L, 198L);
        dataHelper.writeRootFile(folderName + "/file2.parquet", data2, 1L, 199L);

        CompactionJob compactionJob = compactionFactory().createSplittingCompactionJob(
                dataHelper.allFileInfos(), "C", "A", "B", "A2", 1);
        dataHelper.addFilesToStateStoreForJob(compactionJob);

        // When
        CompactSortedFiles compactSortedFiles = createCompactSortedFiles(schema, compactionJob, stateStore, DEFAULT_TASK_ID);
        RecordsProcessedSummary summary = compactSortedFiles.compact();

        // Then
        //  - Read output files and check that they contain the right results
        assertThat(summary.getRecordsRead()).isEqualTo(200L);
        assertThat(summary.getRecordsWritten()).isEqualTo(200L);
        assertThat(readDataFile(schema, compactionJob.getOutputFiles().getLeft())).isEqualTo(data1);
        assertThat(readDataFile(schema, compactionJob.getOutputFiles().getRight())).isEqualTo(data2);

        // - Check DynamoDBStateStore has the correct file-in-partition list
        assertThat(stateStore.getFileInPartitionList())
                .usingRecursiveFieldByFieldElementComparatorIgnoringFields("lastStateStoreUpdateTime")
                .containsExactlyInAnyOrder(
                        dataHelper.expectedPartitionFile("A", compactionJob.getOutputFiles().getLeft(), 100L, 0L, 198L),
                        dataHelper.expectedPartitionFile("B", compactionJob.getOutputFiles().getRight(), 100L, 1L, 199L));

        // - Check DynamoDBStateStore has the correct file-lifecycle entries
        List<FileInfo> expectedFileInfos = new ArrayList<>();
        expectedFileInfos.add(dataHelper.expectedPartitionFile("A", compactionJob.getOutputFiles().getLeft(), 100L, 0L, 198L).cloneWithStatus(FileStatus.ACTIVE));
        expectedFileInfos.add(dataHelper.expectedPartitionFile("B", compactionJob.getOutputFiles().getRight(), 100L, 1L, 199L).cloneWithStatus(FileStatus.ACTIVE));
        dataHelper.allFileInfos().stream()
                .map(fi -> fi.cloneWithStatus(FileStatus.ACTIVE))
                .forEach(expectedFileInfos::add);
        assertThat(stateStore.getFileLifecycleList())
                .usingRecursiveFieldByFieldElementComparatorIgnoringFields("lastStateStoreUpdateTime")
                .containsExactlyInAnyOrder(expectedFileInfos.toArray(new FileInfo[0]));
    }
}
