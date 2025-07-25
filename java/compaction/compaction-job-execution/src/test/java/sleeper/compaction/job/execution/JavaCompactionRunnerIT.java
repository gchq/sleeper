/*
 * Copyright 2022-2025 Crown Copyright
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

import sleeper.compaction.core.job.CompactionJob;
import sleeper.compaction.job.execution.testutils.CompactionRunnerTestBase;
import sleeper.compaction.job.execution.testutils.CompactionRunnerTestData;
import sleeper.core.partition.PartitionsBuilder;
import sleeper.core.row.Row;
import sleeper.core.schema.Schema;
import sleeper.core.schema.type.ByteArrayType;
import sleeper.core.schema.type.LongType;
import sleeper.core.schema.type.StringType;
import sleeper.core.statestore.FileReference;
import sleeper.core.tracker.job.run.RowsProcessed;
import sleeper.sketches.testutils.SketchesDeciles;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.compaction.job.execution.testutils.CompactionRunnerTestUtils.assignJobIdToInputFiles;
import static sleeper.compaction.job.execution.testutils.CompactionRunnerTestUtils.createSchemaWithTypesForKeyAndTwoValues;
import static sleeper.core.statestore.testutils.StateStoreUpdatesWrapper.update;

class JavaCompactionRunnerIT extends CompactionRunnerTestBase {

    @Test
    void shouldMergeFilesWithLongKey() throws Exception {
        // Given
        Schema schema = createSchemaWithTypesForKeyAndTwoValues(new LongType(), new LongType(), new LongType());
        tableProperties.setSchema(schema);
        update(stateStore).initialise(new PartitionsBuilder(schema).singlePartition("root").buildList());

        List<Row> data1 = CompactionRunnerTestData.keyAndTwoValuesSortedEvenLongs();
        List<Row> data2 = CompactionRunnerTestData.keyAndTwoValuesSortedOddLongs();
        FileReference file1 = ingestRowsGetFile(data1);
        FileReference file2 = ingestRowsGetFile(data2);

        CompactionJob compactionJob = compactionFactory().createCompactionJob(List.of(file1, file2), "root");
        assignJobIdToInputFiles(stateStore, compactionJob);

        // When
        RowsProcessed summary = compact(schema, compactionJob);

        // Then
        //  - Read output file and check that it contains the right results
        List<Row> expectedResults = CompactionRunnerTestData.combineSortedBySingleKey(data1, data2);
        assertThat(summary.getRowsRead()).isEqualTo(expectedResults.size());
        assertThat(summary.getRowsWritten()).isEqualTo(expectedResults.size());
        assertThat(CompactionRunnerTestData.readDataFile(schema, compactionJob.getOutputFile())).isEqualTo(expectedResults);
    }

    @Test
    void shouldWriteSketchWhenMergingFiles() throws Exception {
        // Given
        Schema schema = createSchemaWithTypesForKeyAndTwoValues(new LongType(), new LongType(), new LongType());
        tableProperties.setSchema(schema);
        update(stateStore).initialise(new PartitionsBuilder(schema).singlePartition("root").buildList());

        List<Row> data1 = CompactionRunnerTestData.keyAndTwoValuesSortedEvenLongs();
        List<Row> data2 = CompactionRunnerTestData.keyAndTwoValuesSortedOddLongs();
        FileReference file1 = ingestRowsGetFile(data1);
        FileReference file2 = ingestRowsGetFile(data2);

        CompactionJob compactionJob = compactionFactory().createCompactionJob(List.of(file1, file2), "root");
        assignJobIdToInputFiles(stateStore, compactionJob);

        // When
        compact(schema, compactionJob);

        // Then
        assertThat(SketchesDeciles.from(readSketches(schema, compactionJob.getOutputFile())))
                .isEqualTo(SketchesDeciles.builder()
                        .field("key", deciles -> deciles
                                .min(0L).max(199L)
                                .rank(0.1, 20L).rank(0.2, 40L).rank(0.3, 60L)
                                .rank(0.4, 80L).rank(0.5, 100L).rank(0.6, 120L)
                                .rank(0.7, 140L).rank(0.8, 160L).rank(0.9, 180L))
                        .build());
    }

    @Nested
    @DisplayName("Process string key")
    class ProcessStringKey {
        @Test
        void shouldGenerateTestData200EvenAndOddStrings() {
            // When
            List<Row> evens = CompactionRunnerTestData.keyAndTwoValuesSortedEvenStrings();
            List<Row> odds = CompactionRunnerTestData.keyAndTwoValuesSortedOddStrings();
            List<Row> combined = CompactionRunnerTestData.combineSortedBySingleKey(evens, odds);

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
        void shouldMergeFilesWithStringKey() throws Exception {
            // Given
            Schema schema = createSchemaWithTypesForKeyAndTwoValues(new StringType(), new StringType(), new LongType());
            tableProperties.setSchema(schema);
            update(stateStore).initialise(new PartitionsBuilder(schema).singlePartition("root").buildList());

            List<Row> data1 = CompactionRunnerTestData.keyAndTwoValuesSortedEvenStrings();
            List<Row> data2 = CompactionRunnerTestData.keyAndTwoValuesSortedOddStrings();
            FileReference file1 = ingestRowsGetFile(data1);
            FileReference file2 = ingestRowsGetFile(data2);

            CompactionJob compactionJob = compactionFactory().createCompactionJob(List.of(file1, file2), "root");
            assignJobIdToInputFiles(stateStore, compactionJob);

            // When
            RowsProcessed summary = compact(schema, compactionJob);

            // Then
            //  - Read output file and check that it contains the right results
            List<Row> expectedResults = CompactionRunnerTestData.combineSortedBySingleKey(data1, data2);
            assertThat(summary.getRowsRead()).isEqualTo(expectedResults.size());
            assertThat(summary.getRowsWritten()).isEqualTo(expectedResults.size());
            assertThat(CompactionRunnerTestData.readDataFile(schema, compactionJob.getOutputFile())).isEqualTo(expectedResults);
        }
    }

    @Nested
    @DisplayName("Process byte array key")
    class ProcessByteArrayKey {

        @Test
        void shouldGenerateTestData200EvenAndOddByteArrays() {
            // When
            List<Row> evens = CompactionRunnerTestData.keyAndTwoValuesSortedEvenByteArrays();
            List<Row> odds = CompactionRunnerTestData.keyAndTwoValuesSortedOddByteArrays();
            List<Row> combined = CompactionRunnerTestData.combineSortedBySingleByteArrayKey(evens, odds);

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
        void shouldMergeFilesWithByteArrayKey() throws Exception {
            // Given
            Schema schema = createSchemaWithTypesForKeyAndTwoValues(new ByteArrayType(), new ByteArrayType(), new LongType());
            tableProperties.setSchema(schema);
            update(stateStore).initialise(new PartitionsBuilder(schema).singlePartition("root").buildList());

            List<Row> data1 = CompactionRunnerTestData.keyAndTwoValuesSortedEvenByteArrays();
            List<Row> data2 = CompactionRunnerTestData.keyAndTwoValuesSortedOddByteArrays();
            FileReference file1 = ingestRowsGetFile(data1);
            FileReference file2 = ingestRowsGetFile(data2);

            CompactionJob compactionJob = compactionFactory().createCompactionJob(List.of(file1, file2), "root");
            assignJobIdToInputFiles(stateStore, compactionJob);

            // When
            RowsProcessed summary = compact(schema, compactionJob);

            // Then
            //  - Read output file and check that it contains the right results
            List<Row> expectedResults = CompactionRunnerTestData.combineSortedBySingleByteArrayKey(data1, data2);
            assertThat(summary.getRowsRead()).isEqualTo(expectedResults.size());
            assertThat(summary.getRowsWritten()).isEqualTo(expectedResults.size());
            assertThat(CompactionRunnerTestData.readDataFile(schema, compactionJob.getOutputFile())).isEqualTo(expectedResults);
        }
    }
}
