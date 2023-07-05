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
import sleeper.compaction.jobexecution.testutils.CompactSortedFilesTestUtils;
import sleeper.core.iterator.impl.AgeOffIterator;
import sleeper.core.partition.PartitionsBuilder;
import sleeper.core.record.Record;
import sleeper.core.record.process.RecordsProcessedSummary;
import sleeper.core.schema.Schema;
import sleeper.statestore.FileInfo;
import sleeper.statestore.FileInfo.FileStatus;
import sleeper.statestore.StateStore;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.compaction.jobexecution.testutils.CompactSortedFilesTestData.readDataFile;
import static sleeper.compaction.jobexecution.testutils.CompactSortedFilesTestData.specifiedFromEvens;
import static sleeper.compaction.jobexecution.testutils.CompactSortedFilesTestData.specifiedFromOdds;
import static sleeper.compaction.jobexecution.testutils.CompactSortedFilesTestUtils.createCompactSortedFiles;
import static sleeper.statestore.inmemory.StateStoreTestHelper.inMemoryStateStoreWithFixedPartitions;
import static sleeper.statestore.inmemory.StateStoreTestHelper.inMemoryStateStoreWithFixedSinglePartition;

class CompactSortedFilesIteratorIT extends CompactSortedFilesTestBase {

    @Test
    void filesShouldMergeAndApplyIteratorCorrectlyLongKey() throws Exception {
        // Given
        Schema schema = CompactSortedFilesTestUtils.createSchemaWithKeyTimestampValue();
        StateStore stateStore = inMemoryStateStoreWithFixedSinglePartition(schema);
        CompactSortedFilesTestDataHelper dataHelper = new CompactSortedFilesTestDataHelper(schema, stateStore);

        List<Record> data1 = specifiedFromEvens((even, record) -> {
            record.put("key", (long) even);
            record.put("timestamp", System.currentTimeMillis());
            record.put("value", 987654321L);
        });
        List<Record> data2 = specifiedFromOdds((odd, record) -> {
            record.put("key", (long) odd);
            record.put("timestamp", 0L);
            record.put("value", 123456789L);
        });
        dataHelper.writeLeafFile(folderName + "/file1.parquet", data1, 0L, 198L);
        dataHelper.writeLeafFile(folderName + "/file2.parquet", data2, 1L, 199L);

        CompactionJob compactionJob = compactionFactoryBuilder()
                .iteratorClassName(AgeOffIterator.class.getName())
                .iteratorConfig("timestamp,1000000")
                .build().createCompactionJob(dataHelper.allFileInfos(), dataHelper.singlePartition().getId());
        dataHelper.addFilesToStateStoreForJob(compactionJob);

        // When
        CompactSortedFiles compactSortedFiles = createCompactSortedFiles(schema, compactionJob, stateStore, DEFAULT_TASK_ID);
        RecordsProcessedSummary summary = compactSortedFiles.compact();

        // Then
        //  - Read output files and check that they contain the right results
        assertThat(summary.getRecordsRead()).isEqualTo(200L);
        assertThat(summary.getRecordsWritten()).isEqualTo(100L);
        assertThat(readDataFile(schema, compactionJob.getOutputFile())).isEqualTo(data1);

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
    void filesShouldMergeAndSplitAndApplyIteratorCorrectlyLongKey() throws Exception {
        // Given
        Schema schema = CompactSortedFilesTestUtils.createSchemaWithKeyTimestampValue();
        StateStore stateStore = inMemoryStateStoreWithFixedPartitions(new PartitionsBuilder(schema)
                .leavesWithSplits(Arrays.asList("A", "B"), Collections.singletonList(100L))
                .parentJoining("C", "A", "B")
                .buildList());
        CompactSortedFilesTestDataHelper dataHelper = new CompactSortedFilesTestDataHelper(schema, stateStore);

        List<Record> data1 = specifiedFromEvens((even, record) -> {
            record.put("key", (long) even);
            record.put("timestamp", System.currentTimeMillis());
            record.put("value", 987654321L);
        });
        List<Record> data2 = specifiedFromOdds((odd, record) -> {
            record.put("key", (long) odd);
            record.put("timestamp", 0L);
            record.put("value", 123456789L);
        });
        dataHelper.writeRootFile(folderName + "/file1.parquet", data1, 0L, 198L);
        dataHelper.writeRootFile(folderName + "/file2.parquet", data2, 1L, 199L);

        CompactionJob compactionJob = compactionFactoryBuilder()
                .iteratorClassName(AgeOffIterator.class.getName())
                .iteratorConfig("timestamp,1000000")
                .build().createSplittingCompactionJob(dataHelper.allFileInfos(), "C", "A", "B", 100L, 0);
        dataHelper.addFilesToStateStoreForJob(compactionJob);

        // When
        CompactSortedFiles compactSortedFiles = createCompactSortedFiles(schema, compactionJob, stateStore, DEFAULT_TASK_ID);
        RecordsProcessedSummary summary = compactSortedFiles.compact();

        // Then
        //  - Read output files and check that they contain the right results
        assertThat(summary.getRecordsRead()).isEqualTo(200L);
        assertThat(summary.getRecordsWritten()).isEqualTo(100L);
        assertThat(readDataFile(schema, compactionJob.getOutputFiles().getLeft())).isEqualTo(data1.subList(0, 50));
        assertThat(readDataFile(schema, compactionJob.getOutputFiles().getRight())).isEqualTo(data1.subList(50, 100));

        // - Check DynamoDBStateStore has correct file in partition list
        assertThat(stateStore.getFileInPartitionList())
                .usingRecursiveFieldByFieldElementComparatorIgnoringFields("lastStateStoreUpdateTime")
                .containsExactlyInAnyOrder(
                        dataHelper.expectedPartitionFile("A", compactionJob.getOutputFiles().getLeft(), 50L, 0L, 98L),
                        dataHelper.expectedPartitionFile("B", compactionJob.getOutputFiles().getRight(), 50L, 100L, 198L));

        // - Check DynamoDBStateStore has the correct file-lifecycle entries
        List<FileInfo> expectedFileInfos = new ArrayList<>();
        expectedFileInfos.add(dataHelper.expectedPartitionFile("A", compactionJob.getOutputFiles().getLeft(), 50L, 0L, 98L).cloneWithStatus(FileStatus.ACTIVE));
        expectedFileInfos.add(dataHelper.expectedPartitionFile("B", compactionJob.getOutputFiles().getRight(), 50L, 100L, 198L).cloneWithStatus(FileStatus.ACTIVE));
        dataHelper.allFileInfos().stream()
                .map(fi -> fi.cloneWithStatus(FileStatus.ACTIVE))
                .forEach(expectedFileInfos::add);
        assertThat(stateStore.getFileLifecycleList())
                .usingRecursiveFieldByFieldElementComparatorIgnoringFields("lastStateStoreUpdateTime")
                .containsExactlyInAnyOrder(expectedFileInfos.toArray(new FileInfo[0]));
    }
}
