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
import sleeper.core.record.Record;
import sleeper.core.record.process.RecordsProcessedSummary;
import sleeper.core.schema.Schema;
import sleeper.core.schema.type.ByteArrayType;
import sleeper.core.schema.type.LongType;
import sleeper.core.schema.type.StringType;
import sleeper.statestore.StateStore;

import java.util.List;

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
import static sleeper.statestore.inmemory.StateStoreTestHelper.inMemoryStateStoreWithFixedSinglePartition;

public class CompactSortedFilesTest extends CompactSortedFilesTestBase {

    @Test
    public void filesShouldMergeCorrectlyAndStateStoreUpdatedLongKey() throws Exception {
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
        assertThat(summary.getLinesRead()).isEqualTo(expectedResults.size());
        assertThat(summary.getLinesWritten()).isEqualTo(expectedResults.size());
        assertThat(readDataFile(schema, compactionJob.getOutputFile())).isEqualTo(expectedResults);

        // - Check DynamoDBStateStore has correct file in partition list
        assertThat(stateStore.getFileInPartitionList())
                .usingRecursiveFieldByFieldElementComparatorIgnoringFields("lastStateStoreUpdateTime")
                .containsExactly(dataHelper.expectedLeafFileInPartition(compactionJob.getOutputFile(), 200L, 0L, 199L));
    }

    @Test
    public void shouldGenerateTestData200EvenAndOddStrings() {
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
    public void filesShouldMergeCorrectlyAndStateStoreUpdatedStringKey() throws Exception {
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
        assertThat(summary.getLinesRead()).isEqualTo(expectedResults.size());
        assertThat(summary.getLinesWritten()).isEqualTo(expectedResults.size());
        assertThat(readDataFile(schema, compactionJob.getOutputFile())).isEqualTo(expectedResults);

        // - Check DynamoDBStateStore has correct file in partition list
        assertThat(stateStore.getFileInPartitionList())
                .usingRecursiveFieldByFieldElementComparatorIgnoringFields("lastStateStoreUpdateTime")
                .containsExactly(dataHelper.expectedLeafFileInPartition(compactionJob.getOutputFile(), 200L, "aa", "hr"));
    }

    @Test
    public void shouldGenerateTestData200EvenAndOddByteArrays() {
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
    public void filesShouldMergeCorrectlyAndStateStoreUpdatedByteArrayKey() throws Exception {
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
        assertThat(summary.getLinesRead()).isEqualTo(expectedResults.size());
        assertThat(summary.getLinesWritten()).isEqualTo(expectedResults.size());
        assertThat(readDataFile(schema, compactionJob.getOutputFile())).isEqualTo(expectedResults);

        // - Check DynamoDBStateStore has correct file in partition list
        assertThat(stateStore.getFileInPartitionList())
                .usingRecursiveFieldByFieldElementComparatorIgnoringFields("lastStateStoreUpdateTime")
                .containsExactly(dataHelper.expectedLeafFileInPartition(compactionJob.getOutputFile(), 200L, new byte[]{0, 0}, new byte[]{1, 71}));
    }
}
