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

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import sleeper.compaction.core.job.CompactionJob;
import sleeper.compaction.job.execution.testutils.CompactionRunnerTestBase;
import sleeper.compaction.job.execution.testutils.CompactionRunnerTestData;
import sleeper.compaction.job.execution.testutils.CompactionRunnerTestUtils;
import sleeper.core.iterator.AgeOffIterator;
import sleeper.core.partition.PartitionsBuilder;
import sleeper.core.row.Row;
import sleeper.core.schema.Schema;
import sleeper.core.statestore.FileReference;
import sleeper.core.tracker.job.run.RowsProcessed;
import sleeper.sketches.testutils.SketchesDeciles;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.compaction.job.execution.testutils.CompactionRunnerTestUtils.assignJobIdToInputFiles;
import static sleeper.core.properties.table.TableProperty.AGGREGATION_CONFIG;
import static sleeper.core.properties.table.TableProperty.FILTERING_CONFIG;
import static sleeper.core.properties.table.TableProperty.ITERATOR_CLASS_NAME;
import static sleeper.core.properties.table.TableProperty.ITERATOR_CONFIG;
import static sleeper.core.statestore.testutils.StateStoreUpdatesWrapper.update;

class JavaCompactionRunnerIteratorIT extends CompactionRunnerTestBase {

    @ParameterizedTest
    @CsvSource({"true", "false"})
    void shouldApplyFilterIteratorDuringCompaction(Boolean shouldUseFiltersConfig) throws Exception {
        // Given
        Schema schema = CompactionRunnerTestUtils.createSchemaWithKeyTimestampValue();
        tableProperties.setSchema(schema);
        update(stateStore).initialise(new PartitionsBuilder(schema).singlePartition("root").buildList());

        List<Row> data1 = CompactionRunnerTestData.specifiedFromEvens((even, row) -> {
            row.put("key", (long) even);
            row.put("timestamp", System.currentTimeMillis());
            row.put("value", 987654321L);
        });
        List<Row> data2 = CompactionRunnerTestData.specifiedFromOdds((odd, row) -> {
            row.put("key", (long) odd);
            row.put("timestamp", 0L);
            row.put("value", 123456789L);
        });
        FileReference file1 = ingestRowsGetFile(data1);
        FileReference file2 = ingestRowsGetFile(data2);

        if (shouldUseFiltersConfig) {
            tableProperties.set(FILTERING_CONFIG, "ageOff(timestamp,1000000)");
        } else {
            tableProperties.set(ITERATOR_CLASS_NAME, AgeOffIterator.class.getName());
            tableProperties.set(ITERATOR_CONFIG, "timestamp,1000000");
        }

        CompactionJob compactionJob = compactionFactory().createCompactionJob(List.of(file1, file2), "root");
        assignJobIdToInputFiles(stateStore, compactionJob);

        // When
        runTask(compactionJob);

        // Then
        //  - Read output files and check that they contain the right results
        assertThat(getRowsProcessed(compactionJob)).isEqualTo(new RowsProcessed(200, 100));
        assertThat(CompactionRunnerTestData.readDataFile(schema, compactionJob.getOutputFile())).isEqualTo(data1);
        assertThat(SketchesDeciles.from(readSketches(schema, compactionJob.getOutputFile())))
                .isEqualTo(SketchesDeciles.builder()
                        .field("key", deciles -> deciles
                                .min(0L).max(198L)
                                .rank(0.1, 20L).rank(0.2, 40L).rank(0.3, 60L)
                                .rank(0.4, 80L).rank(0.5, 100L).rank(0.6, 120L)
                                .rank(0.7, 140L).rank(0.8, 160L).rank(0.9, 180L))
                        .build());
        assertThat(stateStore.getFileReferences()).containsExactly(outputFileReference(compactionJob, 100));
    }

    @Test
    void shouldApplyAggregationIteratorDuringCompaction() throws Exception {
        // Given
        Schema schema = CompactionRunnerTestUtils.createSchemaWithKeyTimestampValue();
        tableProperties.setSchema(schema);
        update(stateStore).initialise(new PartitionsBuilder(schema).singlePartition("root").buildList());

        List<Row> data1 = CompactionRunnerTestData.specifiedFromEvens((even, row) -> {
            row.put("key", (long) even);
            row.put("timestamp", System.currentTimeMillis());
            row.put("value", 5L);
        });
        List<Row> data2 = CompactionRunnerTestData.specifiedFromEvens((even, row) -> {
            row.put("key", (long) even);
            row.put("timestamp", 0L);
            row.put("value", 7L);
        });
        FileReference file1 = ingestRowsGetFile(data1);
        FileReference file2 = ingestRowsGetFile(data2);

        tableProperties.set(AGGREGATION_CONFIG, "min(timestamp),sum(value)");

        CompactionJob compactionJob = compactionFactory().createCompactionJob(List.of(file1, file2), "root");
        assignJobIdToInputFiles(stateStore, compactionJob);

        // When
        runTask(compactionJob);

        // Then
        //  - Read output files and check that they contain the right results
        assertThat(getRowsProcessed(compactionJob)).isEqualTo(new RowsProcessed(200, 100));
        assertThat(CompactionRunnerTestData.readDataFile(schema, compactionJob.getOutputFile()))
                .isEqualTo(
                        CompactionRunnerTestData.specifiedFromEvens((even, row) -> {
                            row.put("key", (long) even);
                            row.put("timestamp", 0L);
                            row.put("value", 12L);
                        }));
        assertThat(SketchesDeciles.from(readSketches(schema, compactionJob.getOutputFile())))
                .isEqualTo(SketchesDeciles.builder()
                        .field("key", deciles -> deciles
                                .min(0L).max(198L)
                                .rank(0.1, 20L).rank(0.2, 40L).rank(0.3, 60L)
                                .rank(0.4, 80L).rank(0.5, 100L).rank(0.6, 120L)
                                .rank(0.7, 140L).rank(0.8, 160L).rank(0.9, 180L))
                        .build());
        assertThat(stateStore.getFileReferences()).containsExactly(outputFileReference(compactionJob, 100));
    }
}
