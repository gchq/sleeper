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
package sleeper.compaction.job.execution;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import sleeper.compaction.job.CompactionJob;
import sleeper.compaction.job.execution.testutils.CompactSortedFilesTestBase;
import sleeper.compaction.job.execution.testutils.CompactSortedFilesTestData;
import sleeper.core.partition.PartitionsBuilder;
import sleeper.core.record.Record;
import sleeper.core.record.process.RecordsProcessed;
import sleeper.core.schema.Schema;
import sleeper.core.schema.type.ByteArrayType;
import sleeper.core.schema.type.LongType;
import sleeper.core.schema.type.StringType;
import sleeper.core.statestore.FileReference;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.compaction.job.execution.testutils.CompactSortedFilesTestUtils.assignJobIdToInputFiles;
import static sleeper.compaction.job.execution.testutils.CompactSortedFilesTestUtils.createSchemaWithTypesForKeyAndTwoValues;

class CompactSortedFilesIT extends CompactSortedFilesTestBase {

    @Test
    void shouldMergeFilesCorrectlyAndUpdateStateStoreWithLongKey() throws Exception {
        // Given
        Schema schema = createSchemaWithTypesForKeyAndTwoValues(new LongType(), new LongType(), new LongType());
        tableProperties.setSchema(schema);
        stateStore.initialise(new PartitionsBuilder(schema).singlePartition("root").buildList());

        List<Record> data1 = CompactSortedFilesTestData.keyAndTwoValuesSortedEvenLongs();
        List<Record> data2 = CompactSortedFilesTestData.keyAndTwoValuesSortedOddLongs();
        FileReference file1 = ingestRecordsGetFile(data1);
        FileReference file2 = ingestRecordsGetFile(data2);

        CompactionJob compactionJob = compactionFactory().createCompactionJob(List.of(file1, file2), "root");
        assignJobIdToInputFiles(stateStore, compactionJob);

        // When
        RecordsProcessed summary = compact(schema, compactionJob);

        // Then
        //  - Read output file and check that it contains the right results
        List<Record> expectedResults = CompactSortedFilesTestData.combineSortedBySingleKey(data1, data2);
        assertThat(summary.getRecordsRead()).isEqualTo(expectedResults.size());
        assertThat(summary.getRecordsWritten()).isEqualTo(expectedResults.size());
        assertThat(CompactSortedFilesTestData.readDataFile(schema, compactionJob.getOutputFile())).isEqualTo(expectedResults);
    }

    @Nested
    @DisplayName("Process string key")
    class ProcessStringKey {
        @Test
        void shouldGenerateTestData200EvenAndOddStrings() {
            // When
            List<Record> evens = CompactSortedFilesTestData.keyAndTwoValuesSortedEvenStrings();
            List<Record> odds = CompactSortedFilesTestData.keyAndTwoValuesSortedOddStrings();
            List<Record> combined = CompactSortedFilesTestData.combineSortedBySingleKey(evens, odds);

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
        void shouldMergeFilesCorrectlyAndUpdateStateStoreWithStringKey() throws Exception {
            // Given
            Schema schema = createSchemaWithTypesForKeyAndTwoValues(new StringType(), new StringType(), new LongType());
            tableProperties.setSchema(schema);
            stateStore.initialise(new PartitionsBuilder(schema).singlePartition("root").buildList());

            List<Record> data1 = CompactSortedFilesTestData.keyAndTwoValuesSortedEvenStrings();
            List<Record> data2 = CompactSortedFilesTestData.keyAndTwoValuesSortedOddStrings();
            FileReference file1 = ingestRecordsGetFile(data1);
            FileReference file2 = ingestRecordsGetFile(data2);

            CompactionJob compactionJob = compactionFactory().createCompactionJob(List.of(file1, file2), "root");
            assignJobIdToInputFiles(stateStore, compactionJob);

            // When
            RecordsProcessed summary = compact(schema, compactionJob);

            // Then
            //  - Read output file and check that it contains the right results
            List<Record> expectedResults = CompactSortedFilesTestData.combineSortedBySingleKey(data1, data2);
            assertThat(summary.getRecordsRead()).isEqualTo(expectedResults.size());
            assertThat(summary.getRecordsWritten()).isEqualTo(expectedResults.size());
            assertThat(CompactSortedFilesTestData.readDataFile(schema, compactionJob.getOutputFile())).isEqualTo(expectedResults);
        }
    }

    @Nested
    @DisplayName("Process byte array key")
    class ProcessByteArrayKey {

        @Test
        void shouldGenerateTestData200EvenAndOddByteArrays() {
            // When
            List<Record> evens = CompactSortedFilesTestData.keyAndTwoValuesSortedEvenByteArrays();
            List<Record> odds = CompactSortedFilesTestData.keyAndTwoValuesSortedOddByteArrays();
            List<Record> combined = CompactSortedFilesTestData.combineSortedBySingleByteArrayKey(evens, odds);

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
        void shouldMergeFilesCorrectlyAndUpdateStateStoreWithByteArrayKey() throws Exception {
            // Given
            Schema schema = createSchemaWithTypesForKeyAndTwoValues(new ByteArrayType(), new ByteArrayType(), new LongType());
            tableProperties.setSchema(schema);
            stateStore.initialise(new PartitionsBuilder(schema).singlePartition("root").buildList());

            List<Record> data1 = CompactSortedFilesTestData.keyAndTwoValuesSortedEvenByteArrays();
            List<Record> data2 = CompactSortedFilesTestData.keyAndTwoValuesSortedOddByteArrays();
            FileReference file1 = ingestRecordsGetFile(data1);
            FileReference file2 = ingestRecordsGetFile(data2);

            CompactionJob compactionJob = compactionFactory().createCompactionJob(List.of(file1, file2), "root");
            assignJobIdToInputFiles(stateStore, compactionJob);

            // When
            RecordsProcessed summary = compact(schema, compactionJob);

            // Then
            //  - Read output file and check that it contains the right results
            List<Record> expectedResults = CompactSortedFilesTestData.combineSortedBySingleByteArrayKey(data1, data2);
            assertThat(summary.getRecordsRead()).isEqualTo(expectedResults.size());
            assertThat(summary.getRecordsWritten()).isEqualTo(expectedResults.size());
            assertThat(CompactSortedFilesTestData.readDataFile(schema, compactionJob.getOutputFile())).isEqualTo(expectedResults);
        }
    }
}
