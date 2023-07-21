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
import sleeper.core.key.Key;
import sleeper.core.partition.Partition;
import sleeper.core.range.Range;
import sleeper.core.range.Range.RangeFactory;
import sleeper.core.range.Region;
import sleeper.core.record.Record;
import sleeper.core.record.process.RecordsProcessedSummary;
import sleeper.core.schema.Field;
import sleeper.core.schema.Schema;
import sleeper.core.schema.type.ByteArrayType;
import sleeper.core.schema.type.LongType;
import sleeper.core.schema.type.StringType;
import sleeper.statestore.FileInfo;
import sleeper.statestore.FileLifecycleInfo;
import sleeper.statestore.StateStore;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.compaction.jobexecution.testutils.CompactSortedFilesTestData.combineSortedBySingleByteArrayKey;
import static sleeper.compaction.jobexecution.testutils.CompactSortedFilesTestData.combineSortedBySingleKey;
import static sleeper.compaction.jobexecution.testutils.CompactSortedFilesTestData.keyAndTwoValuesSortedEvenByteArrays;
import static sleeper.compaction.jobexecution.testutils.CompactSortedFilesTestData.keyAndTwoValuesSortedEvenLongs;
import static sleeper.compaction.jobexecution.testutils.CompactSortedFilesTestData.keyAndTwoValuesSortedEvenStrings;
import static sleeper.compaction.jobexecution.testutils.CompactSortedFilesTestData.keyAndTwoValuesSortedOddByteArrays;
import static sleeper.compaction.jobexecution.testutils.CompactSortedFilesTestData.keyAndTwoValuesSortedOddLongs;
import static sleeper.compaction.jobexecution.testutils.CompactSortedFilesTestData.keyAndTwoValuesSortedOddStrings;
import static sleeper.compaction.jobexecution.testutils.CompactSortedFilesTestData.readDataFile;
import static sleeper.compaction.jobexecution.testutils.CompactSortedFilesTestUtils.createCompactSortedFiles;
import static sleeper.compaction.jobexecution.testutils.CompactSortedFilesTestUtils.createSchemaWithTypesForKeyAndTwoValues;
import static sleeper.statestore.FileLifecycleInfo.FileStatus.ACTIVE;
import static sleeper.statestore.inmemory.StateStoreTestHelper.inMemoryStateStoreWithFixedSinglePartition;

class CompactSortedFilesIT extends CompactSortedFilesTestBase {

    @Test
    void filesShouldMergeCorrectlyAndStateStoreUpdatedLongKey() throws Exception {
        // Given
        Schema schema = createSchemaWithTypesForKeyAndTwoValues(new LongType(), new LongType(), new LongType());
        StateStore stateStore = inMemoryStateStoreWithFixedSinglePartition(schema);
        CompactSortedFilesTestDataHelper dataHelper = new CompactSortedFilesTestDataHelper(schema, stateStore);

        List<Record> data1 = keyAndTwoValuesSortedEvenLongs();
        List<Record> data2 = keyAndTwoValuesSortedOddLongs();
        dataHelper.writeLeafFile(folderName + "/file1.parquet", data1, 0L, 198L);
        dataHelper.writeLeafFile(folderName + "/file2.parquet", data2, 1L, 199L);

        CompactionJob compactionJob = compactionFactory().createCompactionJob(
                dataHelper.allFileInfos(), dataHelper.singlePartition().getId());
        dataHelper.addFilesToStateStoreForJob(compactionJob);

        // When
        CompactSortedFiles compactSortedFiles = createCompactSortedFiles(schema, compactionJob, stateStore, DEFAULT_TASK_ID);
        RecordsProcessedSummary summary = compactSortedFiles.compact();

        // Then
        //  - Read output file and check that it contains the right results
        List<Record> expectedResults = combineSortedBySingleKey(data1, data2);
        assertThat(summary.getRecordsRead()).isEqualTo(expectedResults.size());
        assertThat(summary.getRecordsWritten()).isEqualTo(expectedResults.size());
        assertThat(readDataFile(schema, compactionJob.getOutputFile())).isEqualTo(expectedResults);

        // - Check DynamoDBStateStore has the correct file-in-partition entries
        assertThat(stateStore.getFileInPartitionList())
                .usingRecursiveFieldByFieldElementComparatorIgnoringFields("lastStateStoreUpdateTime")
                .containsExactly(dataHelper.expectedLeafFile(compactionJob.getOutputFile(), 200L, 0L, 199L));

        // - Check DynamoDBStateStore has the correct file-lifecycle entries
        List<FileLifecycleInfo> expectedFileInfos = new ArrayList<>();
        expectedFileInfos.add(dataHelper.expectedLeafFile(compactionJob.getOutputFile(), 200L, 0L, 199L).toFileLifecycleInfo(ACTIVE));
        dataHelper.allFileInfos().stream()
                .map(fi -> fi.toFileLifecycleInfo(ACTIVE))
                .forEach(expectedFileInfos::add);
        assertThat(stateStore.getFileLifecycleList())
                .usingRecursiveFieldByFieldElementComparatorIgnoringFields("lastStateStoreUpdateTime")
                .containsExactlyInAnyOrder(expectedFileInfos.toArray(new FileLifecycleInfo[0]));
    }

    @Test
    void filesShouldMergeCorrectlyAndStateStoreUpdatedLongKeyWhenFileContainsDataFromOutsidePartition() throws Exception {
        // Given
        Schema schema = createSchemaWithTypesForKeyAndTwoValues(new LongType(), new LongType(), new LongType());
        StateStore stateStore = inMemoryStateStoreWithFixedSinglePartition(schema);
        CompactSortedFilesTestDataHelper dataHelper = new CompactSortedFilesTestDataHelper(schema, stateStore);

        List<Record> data1 = keyAndTwoValuesSortedEvenLongs();
        List<Record> data2 = keyAndTwoValuesSortedOddLongs();
        stateStore.addFile(dataHelper.writeLeafFile(folderName + "/file1.parquet", data1, 0L, 198L));
        stateStore.addFile(dataHelper.writeLeafFile(folderName + "/file2.parquet", data2, 0L, 199L));

        // - Now we have 2 files in the root partition. Split the root partition in two.
        RangeFactory rangeFactory = new RangeFactory(schema);
        Partition rootPartition = stateStore.getAllPartitions().get(0);
        rootPartition.setChildPartitionIds(Arrays.asList("left", "right"));
        Field keyField = schema.getRowKeyFields().get(0);
        Range leftRange = rangeFactory.createRange(keyField, Long.MIN_VALUE, 100L);
        Region leftRegion = new Region(leftRange);
        Partition leftLeafPartition = Partition.builder()
                .dimension(0)
                .rowKeyTypes(schema.getRowKeyTypes())
                .id("left")
                .leafPartition(true)
                .region(leftRegion)
                .build();
        Range rightRange = rangeFactory.createRange(keyField, 100L, null);
        Region rightRegion = new Region(rightRange);
        Partition rightLeafPartition = Partition.builder()
                .dimension(0)
                .rowKeyTypes(schema.getRowKeyTypes())
                .id("right")
                .leafPartition(true)
                .region(rightRegion)
                .build();
        stateStore.atomicallyUpdatePartitionAndCreateNewOnes(rootPartition, leftLeafPartition, rightLeafPartition);
        // - Split the file-in-partition records
        List<FileInfo> fileInPartitionInfos = stateStore.getFileInPartitionList();
        for (FileInfo fileInfo : fileInPartitionInfos) {
            stateStore.atomicallySplitFileInPartitionRecord(fileInfo, "left", "right");
        }
        List<FileInfo> fileInPartitionEntriesLeftLeafPartition = stateStore.getFileInPartitionList().stream()
                .filter(f -> f.getPartitionId().equals("left"))
                .collect(Collectors.toList());
        CompactionJob compactionJob = compactionFactory().createCompactionJob(fileInPartitionEntriesLeftLeafPartition, "left");

        // When
        CompactSortedFiles compactSortedFiles = createCompactSortedFiles(schema, compactionJob, stateStore, DEFAULT_TASK_ID);
        RecordsProcessedSummary summary = compactSortedFiles.compact();

        // Then
        //  - Read output file and check that it contains the right results
        List<Record> data1LeftChild = data1.stream().filter(r -> ((long) r.get(keyField.getName())) < 100).collect(Collectors.toList());
        List<Record> data2LeftChild = data2.stream().filter(r -> ((long) r.get(keyField.getName())) < 100).collect(Collectors.toList());
        List<Record> expectedResults = combineSortedBySingleKey(data1LeftChild, data2LeftChild);
        assertThat(summary.getRecordsRead()).isEqualTo(expectedResults.size());
        assertThat(summary.getRecordsWritten()).isEqualTo(expectedResults.size());
        assertThat(readDataFile(schema, compactionJob.getOutputFile())).isEqualTo(expectedResults);

        // - Check StateStore has the correct file-in-partition entries for left partition
        FileInfo expectedFileInfoLeft = FileInfo.builder()
                .rowKeyTypes(schema.getRowKeyTypes())
                .filename(compactionJob.getOutputFile())
                .partitionId("left")
                .numberOfRecords(100L)
                .minRowKey(Key.create(0L))
                .maxRowKey(Key.create(99L))
                .onlyContainsDataForThisPartition(true)
                .build();
        assertThat(stateStore.getFileInPartitionList().stream().filter(f -> f.getPartitionId().equals("left")))
                .usingRecursiveFieldByFieldElementComparatorIgnoringFields("lastStateStoreUpdateTime")
                .containsExactly(expectedFileInfoLeft);

        // - Check StateStore has the correct file-in-partition entries for right partition
        FileInfo expectedFile1InfoRight = FileInfo.builder()
                .rowKeyTypes(schema.getRowKeyTypes())
                .filename(folderName + "/file1.parquet")
                .partitionId("right")
                .numberOfRecords(100L)
                .minRowKey(Key.create(0L))
                .maxRowKey(Key.create(198L))
                .onlyContainsDataForThisPartition(false)
                .build();
        FileInfo expectedFile2InfoRight = FileInfo.builder()
                .rowKeyTypes(schema.getRowKeyTypes())
                .filename(folderName + "/file2.parquet")
                .partitionId("right")
                .numberOfRecords(100L)
                .minRowKey(Key.create(0L))
                .maxRowKey(Key.create(199L))
                .onlyContainsDataForThisPartition(false)
                .build();
        assertThat(stateStore.getFileInPartitionList().stream().filter(f -> f.getPartitionId().equals("right")))
                .usingRecursiveFieldByFieldElementComparatorIgnoringFields("lastStateStoreUpdateTime")
                .containsExactlyInAnyOrder(expectedFile1InfoRight, expectedFile2InfoRight);

        // - Check StateStore has the correct file-lifecycle entries
        // FileInfo expectedFileLifecycleRecord1 = FileInfo.builder()
        //         .rowKeyTypes(schema.getRowKeyTypes())
        //         .filename(folderName + "/file1.parquet")
        //         .partitionId("root")
        //         .numberOfRecords(100L)
        //         .minRowKey(Key.create(0L))
        //         .maxRowKey(Key.create(198L))
        //         .onlyContainsDataForThisPartition(true)
        //         .build();
        // FileInfo expectedFileLifecycleRecord2 = FileInfo.builder()
        //         .rowKeyTypes(schema.getRowKeyTypes())
        //         .filename(folderName + "/file2.parquet")
        //         .partitionId("root")
        //         .numberOfRecords(100L)
        //         .minRowKey(Key.create(0L))
        //         .maxRowKey(Key.create(199L))
        //         .onlyContainsDataForThisPartition(true)
        //         .build();
        // FileInfo expectedFileLifecycleOutput = FileInfo.builder()
        //         .rowKeyTypes(schema.getRowKeyTypes())
        //         .filename(compactionJob.getOutputFile())
        //         .partitionId("left")
        //         .numberOfRecords(100L)
        //         .minRowKey(Key.create(0L))
        //         .maxRowKey(Key.create(99L))
        //         .onlyContainsDataForThisPartition(true)
        //         .build();
        List<FileLifecycleInfo> expectedFileInfos = new ArrayList<>();
        expectedFileInfos.add(dataHelper.expectedLeafFile(compactionJob.getOutputFile(), 200L, 0L, 199L).toFileLifecycleInfo(ACTIVE));
        dataHelper.allFileInfos().stream()
                .map(fi -> fi.toFileLifecycleInfo(ACTIVE))
                .forEach(expectedFileInfos::add);
        assertThat(stateStore.getFileLifecycleList())
                .usingRecursiveFieldByFieldElementComparatorIgnoringFields("lastStateStoreUpdateTime")
                .containsExactlyInAnyOrder(expectedFileInfos.toArray(new FileLifecycleInfo[0]));
    }

    @Test
    void shouldGenerateTestData200EvenAndOddStrings() {
        // When
        List<Record> evens = keyAndTwoValuesSortedEvenStrings();
        List<Record> odds = keyAndTwoValuesSortedOddStrings();
        List<Record> combined = combineSortedBySingleKey(evens, odds);

        // Then
        assertThat(evens).hasSize(100).elements(0, 99).extracting(e -> e.get("key"))
                .containsExactly("aa", "hq");
        assertThat(odds).hasSize(100).elements(0, 99).extracting(e -> e.get("key"))
                .containsExactly("ab", "hr");
        assertThat(combined).hasSize(200)
                .elements(0, 1, 26, 27, 198, 199).extracting(e -> e.get("key"))
                .containsExactly("aa", "ab", "ba", "bb", "hq", "hr");
    }

    @Test
    void filesShouldMergeCorrectlyAndStateStoreUpdatedStringKey() throws Exception {
        // Given
        Schema schema = createSchemaWithTypesForKeyAndTwoValues(new StringType(), new StringType(), new LongType());
        StateStore stateStore = inMemoryStateStoreWithFixedSinglePartition(schema);
        CompactSortedFilesTestDataHelper dataHelper = new CompactSortedFilesTestDataHelper(schema, stateStore);

        List<Record> data1 = keyAndTwoValuesSortedEvenStrings();
        List<Record> data2 = keyAndTwoValuesSortedOddStrings();
        dataHelper.writeLeafFile(folderName + "/file1.parquet", data1, "aa", "hq");
        dataHelper.writeLeafFile(folderName + "/file2.parquet", data2, "ab", "hr");

        CompactionJob compactionJob = compactionFactory().createCompactionJob(
                dataHelper.allFileInfos(), dataHelper.singlePartition().getId());
        dataHelper.addFilesToStateStoreForJob(compactionJob);

        // When
        CompactSortedFiles compactSortedFiles = createCompactSortedFiles(schema, compactionJob, stateStore, DEFAULT_TASK_ID);
        RecordsProcessedSummary summary = compactSortedFiles.compact();

        // Then
        //  - Read output file and check that it contains the right results
        List<Record> expectedResults = combineSortedBySingleKey(data1, data2);
        assertThat(summary.getRecordsRead()).isEqualTo(expectedResults.size());
        assertThat(summary.getRecordsWritten()).isEqualTo(expectedResults.size());
        assertThat(readDataFile(schema, compactionJob.getOutputFile())).isEqualTo(expectedResults);

        // - Check DynamoDBStateStore has correct ready for GC files
        // assertReadyForGC(stateStore, dataHelper.allFileInfos());

        // - Check DynamoDBStateStore has the correct file-in-partition entries
        assertThat(stateStore.getFileInPartitionList())
                .usingRecursiveFieldByFieldElementComparatorIgnoringFields("lastStateStoreUpdateTime")
                .containsExactly(dataHelper.expectedLeafFile(compactionJob.getOutputFile(), 200L, "aa", "hr"));

        // - Check DynamoDBStateStore has the correct file-lifecycle entries
        List<FileLifecycleInfo> expectedFileInfos = new ArrayList<>();
        expectedFileInfos.add(dataHelper.expectedLeafFile(compactionJob.getOutputFile(), 200L, "aa", "hr").toFileLifecycleInfo(ACTIVE));
        dataHelper.allFileInfos().stream()
                .map(fi -> fi.toFileLifecycleInfo(ACTIVE))
                .forEach(expectedFileInfos::add);
        assertThat(stateStore.getFileLifecycleList())
                .usingRecursiveFieldByFieldElementComparatorIgnoringFields("lastStateStoreUpdateTime")
                .containsExactlyInAnyOrder(expectedFileInfos.toArray(new FileLifecycleInfo[0]));
    }

    @Test
    void shouldGenerateTestData200EvenAndOddByteArrays() {
        // When
        List<Record> evens = keyAndTwoValuesSortedEvenByteArrays();
        List<Record> odds = keyAndTwoValuesSortedOddByteArrays();
        List<Record> combined = combineSortedBySingleByteArrayKey(evens, odds);

        // Then
        assertThat(evens).hasSize(100)
                .elements(0, 99).extracting(e -> e.get("key"))
                .containsExactly(new byte[]{0, 0}, new byte[]{1, 70});
        assertThat(odds).hasSize(100)
                .elements(0, 99).extracting(e -> e.get("key"))
                .containsExactly(new byte[]{0, 1}, new byte[]{1, 71});
        assertThat(combined).hasSize(200)
                .elements(0, 1, 128, 129, 198, 199).extracting(e -> e.get("key"))
                .containsExactly(
                        new byte[]{0, 0}, new byte[]{0, 1},
                        new byte[]{1, 0}, new byte[]{1, 1},
                        new byte[]{1, 70}, new byte[]{1, 71});
    }

    @Test
    void filesShouldMergeCorrectlyAndStateStoreUpdatedByteArrayKey() throws Exception {
        // Given
        Schema schema = createSchemaWithTypesForKeyAndTwoValues(new ByteArrayType(), new ByteArrayType(), new LongType());
        StateStore stateStore = inMemoryStateStoreWithFixedSinglePartition(schema);
        CompactSortedFilesTestDataHelper dataHelper = new CompactSortedFilesTestDataHelper(schema, stateStore);

        List<Record> data1 = keyAndTwoValuesSortedEvenByteArrays();
        List<Record> data2 = keyAndTwoValuesSortedOddByteArrays();
        dataHelper.writeLeafFile(folderName + "/file1.parquet", data1, new byte[]{0, 0}, new byte[]{1, 70});
        dataHelper.writeLeafFile(folderName + "/file2.parquet", data2, new byte[]{0, 1}, new byte[]{1, 71});

        CompactionJob compactionJob = compactionFactory().createCompactionJob(
                dataHelper.allFileInfos(), dataHelper.singlePartition().getId());
        dataHelper.addFilesToStateStoreForJob(compactionJob);

        // When
        CompactSortedFiles compactSortedFiles = createCompactSortedFiles(schema, compactionJob, stateStore, DEFAULT_TASK_ID);
        RecordsProcessedSummary summary = compactSortedFiles.compact();

        // Then
        //  - Read output file and check that it contains the right results
        List<Record> expectedResults = combineSortedBySingleByteArrayKey(data1, data2);
        assertThat(summary.getRecordsRead()).isEqualTo(expectedResults.size());
        assertThat(summary.getRecordsWritten()).isEqualTo(expectedResults.size());
        assertThat(readDataFile(schema, compactionJob.getOutputFile())).isEqualTo(expectedResults);

        // - Check DynamoDBStateStore has the correct file-in-partition entries
        assertThat(stateStore.getFileInPartitionList())
                .usingRecursiveFieldByFieldElementComparatorIgnoringFields("lastStateStoreUpdateTime")
                .containsExactly(dataHelper.expectedLeafFile(compactionJob.getOutputFile(), 200L, new byte[]{0, 0}, new byte[]{1, 71}));

        // - Check DynamoDBStateStore has the correct file-lifecycle entries
        List<FileLifecycleInfo> expectedFileInfos = new ArrayList<>();
        expectedFileInfos.add(dataHelper.expectedLeafFile(compactionJob.getOutputFile(), 200L, new byte[]{0, 0}, new byte[]{1, 71}).toFileLifecycleInfo(ACTIVE));
        dataHelper.allFileInfos().stream()
                .map(fi -> fi.toFileLifecycleInfo(ACTIVE))
                .forEach(expectedFileInfos::add);
        assertThat(stateStore.getFileLifecycleList())
                .usingRecursiveFieldByFieldElementComparatorIgnoringFields("lastStateStoreUpdateTime")
                .containsExactlyInAnyOrder(expectedFileInfos.toArray(new FileLifecycleInfo[0]));
    }
}
