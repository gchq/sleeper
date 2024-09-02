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
import sleeper.compaction.job.execution.testutils.CompactionRunnerTestBase;
import sleeper.compaction.job.execution.testutils.CompactionRunnerTestData;
import sleeper.core.partition.PartitionsBuilder;
import sleeper.core.record.Record;
import sleeper.core.record.process.RecordsProcessed;
import sleeper.core.schema.Schema;
import sleeper.core.schema.type.ByteArrayType;
import sleeper.core.schema.type.LongType;
import sleeper.core.schema.type.StringType;
import sleeper.core.statestore.FileReference;
import sleeper.sketches.Sketches;
import sleeper.sketches.testutils.AssertQuantiles;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.compaction.job.execution.testutils.CompactionRunnerTestUtils.assignJobIdToInputFiles;
import static sleeper.compaction.job.execution.testutils.CompactionRunnerTestUtils.createSchemaWithTypesForKeyAndTwoValues;

class JavaCompactionRunnerIT extends CompactionRunnerTestBase {

    @Test
    void shouldMergeFilesWithLongKey() throws Exception {
        // Given
        Schema schema = createSchemaWithTypesForKeyAndTwoValues(new LongType(), new LongType(), new LongType());
        tableProperties.setSchema(schema);
        stateStore.initialise(new PartitionsBuilder(schema).singlePartition("root").buildList());

        List<Record> data1 = CompactionRunnerTestData.keyAndTwoValuesSortedEvenLongs();
        List<Record> data2 = CompactionRunnerTestData.keyAndTwoValuesSortedOddLongs();
        FileReference file1 = ingestRecordsGetFile(data1);
        FileReference file2 = ingestRecordsGetFile(data2);

        CompactionJob compactionJob = compactionFactory().createCompactionJob(List.of(file1, file2), "root");
        assignJobIdToInputFiles(stateStore, compactionJob);

        // When
        RecordsProcessed summary = compact(schema, compactionJob);

        // Then
        //  - Read output file and check that it contains the right results
        List<Record> expectedResults = CompactionRunnerTestData.combineSortedBySingleKey(data1, data2);
        assertThat(summary.getRecordsRead()).isEqualTo(expectedResults.size());
        assertThat(summary.getRecordsWritten()).isEqualTo(expectedResults.size());
        assertThat(CompactionRunnerTestData.readDataFile(schema, compactionJob.getOutputFile())).isEqualTo(expectedResults);
    }

    @Test
    void shouldWriteSketchWhenMergingFiles() throws Exception {
        // Given
        Schema schema = createSchemaWithTypesForKeyAndTwoValues(new LongType(), new LongType(), new LongType());
        tableProperties.setSchema(schema);
        stateStore.initialise(new PartitionsBuilder(schema).singlePartition("root").buildList());

        List<Record> data1 = CompactionRunnerTestData.keyAndTwoValuesSortedEvenLongs();
        List<Record> data2 = CompactionRunnerTestData.keyAndTwoValuesSortedOddLongs();
        FileReference file1 = ingestRecordsGetFile(data1);
        FileReference file2 = ingestRecordsGetFile(data2);

        CompactionJob compactionJob = compactionFactory().createCompactionJob(List.of(file1, file2), "root");
        assignJobIdToInputFiles(stateStore, compactionJob);

        // When
        compact(schema, compactionJob);

        // Then
        Sketches sketches = getSketches(schema, compactionJob.getOutputFile());
        assertThat(sketches.getQuantilesSketches().keySet()).containsExactly("key");
        AssertQuantiles.forSketch(sketches.getQuantilesSketch("key"))
                .min(0L).max(199L)
                .quantile(0.0, 0L).quantile(0.1, 20L)
                .quantile(0.2, 40L).quantile(0.3, 60L)
                .quantile(0.4, 80L).quantile(0.5, 100L)
                .quantile(0.6, 120L).quantile(0.7, 140L)
                .quantile(0.8, 160L).quantile(0.9, 180L)
                .verify();
    }

    @Nested
    @DisplayName("Process string key")
    class ProcessStringKey {
        @Test
        void shouldGenerateTestData200EvenAndOddStrings() {
            // When
            List<Record> evens = CompactionRunnerTestData.keyAndTwoValuesSortedEvenStrings();
            List<Record> odds = CompactionRunnerTestData.keyAndTwoValuesSortedOddStrings();
            List<Record> combined = CompactionRunnerTestData.combineSortedBySingleKey(evens, odds);

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
            stateStore.initialise(new PartitionsBuilder(schema).singlePartition("root").buildList());

            List<Record> data1 = CompactionRunnerTestData.keyAndTwoValuesSortedEvenStrings();
            List<Record> data2 = CompactionRunnerTestData.keyAndTwoValuesSortedOddStrings();
            FileReference file1 = ingestRecordsGetFile(data1);
            FileReference file2 = ingestRecordsGetFile(data2);

            CompactionJob compactionJob = compactionFactory().createCompactionJob(List.of(file1, file2), "root");
            assignJobIdToInputFiles(stateStore, compactionJob);

            // When
            RecordsProcessed summary = compact(schema, compactionJob);

            // Then
            //  - Read output file and check that it contains the right results
            List<Record> expectedResults = CompactionRunnerTestData.combineSortedBySingleKey(data1, data2);
            assertThat(summary.getRecordsRead()).isEqualTo(expectedResults.size());
            assertThat(summary.getRecordsWritten()).isEqualTo(expectedResults.size());
            assertThat(CompactionRunnerTestData.readDataFile(schema, compactionJob.getOutputFile())).isEqualTo(expectedResults);
        }
    }

    @Nested
    @DisplayName("Process byte array key")
    class ProcessByteArrayKey {

        @Test
        void shouldGenerateTestData200EvenAndOddByteArrays() {
            // When
            List<Record> evens = CompactionRunnerTestData.keyAndTwoValuesSortedEvenByteArrays();
            List<Record> odds = CompactionRunnerTestData.keyAndTwoValuesSortedOddByteArrays();
            List<Record> combined = CompactionRunnerTestData.combineSortedBySingleByteArrayKey(evens, odds);

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
            stateStore.initialise(new PartitionsBuilder(schema).singlePartition("root").buildList());

            List<Record> data1 = CompactionRunnerTestData.keyAndTwoValuesSortedEvenByteArrays();
            List<Record> data2 = CompactionRunnerTestData.keyAndTwoValuesSortedOddByteArrays();
            FileReference file1 = ingestRecordsGetFile(data1);
            FileReference file2 = ingestRecordsGetFile(data2);

            CompactionJob compactionJob = compactionFactory().createCompactionJob(List.of(file1, file2), "root");
            assignJobIdToInputFiles(stateStore, compactionJob);

            // When
            RecordsProcessed summary = compact(schema, compactionJob);

            // Then
            //  - Read output file and check that it contains the right results
            List<Record> expectedResults = CompactionRunnerTestData.combineSortedBySingleByteArrayKey(data1, data2);
            assertThat(summary.getRecordsRead()).isEqualTo(expectedResults.size());
            assertThat(summary.getRecordsWritten()).isEqualTo(expectedResults.size());
            assertThat(CompactionRunnerTestData.readDataFile(schema, compactionJob.getOutputFile())).isEqualTo(expectedResults);
        }
    }
}
