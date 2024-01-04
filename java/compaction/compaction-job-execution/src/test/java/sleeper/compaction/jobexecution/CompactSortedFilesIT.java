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

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import sleeper.compaction.job.CompactionJob;
import sleeper.compaction.jobexecution.testutils.CompactSortedFilesTestBase;
import sleeper.core.partition.PartitionsBuilder;
import sleeper.core.record.Record;
import sleeper.core.record.process.RecordsProcessedSummary;
import sleeper.core.schema.Schema;
import sleeper.core.schema.type.ByteArrayType;
import sleeper.core.schema.type.LongType;
import sleeper.core.schema.type.StringType;
import sleeper.core.statestore.FileInfo;
import sleeper.core.statestore.FileInfoFactory;

import java.time.Instant;
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
import static sleeper.compaction.jobexecution.testutils.CompactSortedFilesTestUtils.createSchemaWithTypesForKeyAndTwoValues;

class CompactSortedFilesIT extends CompactSortedFilesTestBase {

    @Test
    void shouldMergeFilesCorrectlyAndUpdateStateStoreWithLongKey() throws Exception {
        // Given
        Schema schema = createSchemaWithTypesForKeyAndTwoValues(new LongType(), new LongType(), new LongType());
        tableProperties.setSchema(schema);
        stateStore.initialise(new PartitionsBuilder(schema).singlePartition("root").buildList());

        List<Record> data1 = keyAndTwoValuesSortedEvenLongs();
        List<Record> data2 = keyAndTwoValuesSortedOddLongs();
        FileInfo file1 = ingestRecordsGetFile(data1);
        FileInfo file2 = ingestRecordsGetFile(data2);

        CompactionJob compactionJob = compactionFactory().createCompactionJob(List.of(file1, file2), "root");

        // When
        CompactSortedFiles compactSortedFiles = createCompactSortedFiles(schema, compactionJob);
        RecordsProcessedSummary summary = compactSortedFiles.compact();

        // Then
        //  - Read output file and check that it contains the right results
        List<Record> expectedResults = combineSortedBySingleKey(data1, data2);
        assertThat(summary.getRecordsRead()).isEqualTo(expectedResults.size());
        assertThat(summary.getRecordsWritten()).isEqualTo(expectedResults.size());
        assertThat(readDataFile(schema, compactionJob.getOutputFile())).isEqualTo(expectedResults);

        // - Check DynamoDBStateStore has correct ready for GC files
        assertThat(stateStore.getReadyForGCFilenamesBefore(Instant.ofEpochMilli(Long.MAX_VALUE)))
                .containsExactly(file1.getFilename(), file2.getFilename());

        // - Check DynamoDBStateStore has correct active files
        assertThat(stateStore.getActiveFiles())
                .usingRecursiveFieldByFieldElementComparatorIgnoringFields("lastStateStoreUpdateTime")
                .containsExactly(FileInfoFactory.from(schema, stateStore)
                        .rootFile(compactionJob.getOutputFile(), 200L));
    }

    @Nested
    @DisplayName("Process string key")
    class ProcessStringKey {
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
        void shouldMergeFilesCorrectlyAndUpdateStateStoreWithStringKey() throws Exception {
            // Given
            Schema schema = createSchemaWithTypesForKeyAndTwoValues(new StringType(), new StringType(), new LongType());
            tableProperties.setSchema(schema);
            stateStore.initialise(new PartitionsBuilder(schema).singlePartition("root").buildList());

            List<Record> data1 = keyAndTwoValuesSortedEvenStrings();
            List<Record> data2 = keyAndTwoValuesSortedOddStrings();
            FileInfo file1 = ingestRecordsGetFile(data1);
            FileInfo file2 = ingestRecordsGetFile(data2);

            CompactionJob compactionJob = compactionFactory().createCompactionJob(List.of(file1, file2), "root");

            // When
            CompactSortedFiles compactSortedFiles = createCompactSortedFiles(schema, compactionJob);
            RecordsProcessedSummary summary = compactSortedFiles.compact();

            // Then
            //  - Read output file and check that it contains the right results
            List<Record> expectedResults = combineSortedBySingleKey(data1, data2);
            assertThat(summary.getRecordsRead()).isEqualTo(expectedResults.size());
            assertThat(summary.getRecordsWritten()).isEqualTo(expectedResults.size());
            assertThat(readDataFile(schema, compactionJob.getOutputFile())).isEqualTo(expectedResults);

            // - Check DynamoDBStateStore has correct ready for GC files
            assertThat(stateStore.getReadyForGCFilenamesBefore(Instant.ofEpochMilli(Long.MAX_VALUE)))
                    .containsExactly(file1.getFilename(), file2.getFilename());

            // - Check DynamoDBStateStore has correct active files
            assertThat(stateStore.getActiveFiles())
                    .usingRecursiveFieldByFieldElementComparatorIgnoringFields("lastStateStoreUpdateTime")
                    .containsExactly(FileInfoFactory.from(schema, stateStore)
                            .rootFile(compactionJob.getOutputFile(), 200L));
        }
    }

    @Nested
    @DisplayName("Process byte array key")
    class ProcessByteArrayKey {

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
        void shouldMergeFilesCorrectlyAndUpdateStateStoreWithByteArrayKey() throws Exception {
            // Given
            Schema schema = createSchemaWithTypesForKeyAndTwoValues(new ByteArrayType(), new ByteArrayType(), new LongType());
            tableProperties.setSchema(schema);
            stateStore.initialise(new PartitionsBuilder(schema).singlePartition("root").buildList());

            List<Record> data1 = keyAndTwoValuesSortedEvenByteArrays();
            List<Record> data2 = keyAndTwoValuesSortedOddByteArrays();
            FileInfo file1 = ingestRecordsGetFile(data1);
            FileInfo file2 = ingestRecordsGetFile(data2);

            CompactionJob compactionJob = compactionFactory().createCompactionJob(List.of(file1, file2), "root");

            // When
            CompactSortedFiles compactSortedFiles = createCompactSortedFiles(schema, compactionJob);
            RecordsProcessedSummary summary = compactSortedFiles.compact();

            // Then
            //  - Read output file and check that it contains the right results
            List<Record> expectedResults = combineSortedBySingleByteArrayKey(data1, data2);
            assertThat(summary.getRecordsRead()).isEqualTo(expectedResults.size());
            assertThat(summary.getRecordsWritten()).isEqualTo(expectedResults.size());
            assertThat(readDataFile(schema, compactionJob.getOutputFile())).isEqualTo(expectedResults);

            // - Check DynamoDBStateStore has correct ready for GC files
            assertThat(stateStore.getReadyForGCFilenamesBefore(Instant.ofEpochMilli(Long.MAX_VALUE)))
                    .containsExactly(file1.getFilename(), file2.getFilename());

            // - Check DynamoDBStateStore has correct active files
            assertThat(stateStore.getActiveFiles())
                    .usingRecursiveFieldByFieldElementComparatorIgnoringFields("lastStateStoreUpdateTime")
                    .containsExactly(FileInfoFactory.from(schema, stateStore)
                            .rootFile(compactionJob.getOutputFile(), 200L));
        }
    }
}
