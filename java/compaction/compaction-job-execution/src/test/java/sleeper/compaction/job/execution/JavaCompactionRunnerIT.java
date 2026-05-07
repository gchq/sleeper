/*
 * Copyright 2022-2026 Crown Copyright
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
import sleeper.core.range.Region;
import sleeper.core.row.Row;
import sleeper.core.schema.Schema;
import sleeper.core.schema.type.ByteArrayType;
import sleeper.core.schema.type.LongType;
import sleeper.core.schema.type.StringType;
import sleeper.core.statestore.FileReference;
import sleeper.core.tracker.job.run.RowsProcessed;
import sleeper.core.util.ObjectFactory;
import sleeper.parquet.utils.HadoopConfigurationProvider;
import sleeper.sketches.testutils.SketchesDeciles;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.Phaser;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatNullPointerException;
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
        runTask(compactionJob);

        // Then
        //  - Read output file and check that it contains the right results
        assertThat(getRowsProcessed(compactionJob)).isEqualTo(new RowsProcessed(200, 200));
        assertThat(CompactionRunnerTestData.readDataFile(schema, compactionJob.getOutputFile()))
                .isEqualTo(CompactionRunnerTestData.combineSortedBySingleKey(data1, data2));
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
        runTask(compactionJob);

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
            runTask(compactionJob);

            // Then
            //  - Read output file and check that it contains the right results
            assertThat(getRowsProcessed(compactionJob)).isEqualTo(new RowsProcessed(200, 200));
            assertThat(CompactionRunnerTestData.readDataFile(schema, compactionJob.getOutputFile()))
                    .isEqualTo(CompactionRunnerTestData.combineSortedBySingleKey(data1, data2));
        }
    }

    @Nested
    @DisplayName("Track compaction progress")
    class TrackProgress {

        @Test
        void shouldReturnEmptyOptionalForUnknownJobIdBeforeAnyCompaction() {
            // Given
            JavaCompactionRunner runner = new JavaCompactionRunner(
                    ObjectFactory.noUserJars(),
                    HadoopConfigurationProvider.getConfigurationForECS(instanceProperties),
                    createSketchesStore());

            // When / Then
            assertThat(runner.getCompactionRowsRead("not-a-job")).isEmpty();
        }

        @Test
        void shouldThrowNullPointerExceptionWhenJobIdIsNull() {
            // Given
            JavaCompactionRunner runner = new JavaCompactionRunner(
                    ObjectFactory.noUserJars(),
                    HadoopConfigurationProvider.getConfigurationForECS(instanceProperties),
                    createSketchesStore());

            // When / Then
            assertThatNullPointerException().isThrownBy(() -> runner.getCompactionRowsRead((String) null));
        }

        @Test
        void shouldReportRowsReadFromAnotherThreadWhileCompactionIsRunning() throws Exception {
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
            Region region = stateStore.getPartition(compactionJob.getPartitionId()).getRegion();

            // The compaction thread will arrive at the phaser inside the final updateRowCount call;
            // the test thread arrives once it has observed the reported value, allowing both threads to advance.
            Phaser phaser = new Phaser(2);
            JavaCompactionRunner runner = new JavaCompactionRunner(
                    ObjectFactory.noUserJars(),
                    HadoopConfigurationProvider.getConfigurationForECS(instanceProperties),
                    createSketchesStore()) {
                @Override
                protected void updateRowCount(AtomicLong counter, long newRowsRead) {
                    super.updateRowCount(counter, newRowsRead);
                    phaser.arriveAndAwaitAdvance();
                }
            };

            // When
            ExecutorService executor = Executors.newSingleThreadExecutor();
            try {
                Future<RowsProcessed> compactionResult = executor.submit(
                        () -> runner.compact(compactionJob, tableProperties, region));

                // Wait for the compaction thread to reach the final updateRowCount call,
                // then read the reported value while it is paused there.
                phaser.arriveAndAwaitAdvance();
                Optional<Long> reportedRowsRead = runner.getCompactionRowsRead(compactionJob);

                // Then the running compaction reports the rows it has read so far.
                assertThat(reportedRowsRead).contains(200L);

                // Let the compaction finish and verify it completed successfully.
                RowsProcessed processed = compactionResult.get(30, TimeUnit.SECONDS);
                assertThat(processed).isEqualTo(new RowsProcessed(200, 200));
            } finally {
                executor.shutdownNow();
            }
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
            runTask(compactionJob);

            // Then
            //  - Read output file and check that it contains the right results
            assertThat(getRowsProcessed(compactionJob)).isEqualTo(new RowsProcessed(200, 200));
            assertThat(CompactionRunnerTestData.readDataFile(schema, compactionJob.getOutputFile()))
                    .isEqualTo(CompactionRunnerTestData.combineSortedBySingleByteArrayKey(data1, data2));
        }
    }
}
