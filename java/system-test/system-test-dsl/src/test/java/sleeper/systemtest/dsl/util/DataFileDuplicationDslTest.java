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
package sleeper.systemtest.dsl.util;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import sleeper.core.partition.PartitionsBuilder;
import sleeper.core.properties.model.IngestFileWritingStrategy;
import sleeper.core.properties.table.TableProperty;
import sleeper.systemtest.dsl.SleeperDsl;
import sleeper.systemtest.dsl.testutil.InMemoryDslTest;

import java.util.Map;
import java.util.stream.LongStream;

import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.systemtest.dsl.sourcedata.GenerateNumberedValue.addPrefix;
import static sleeper.systemtest.dsl.sourcedata.GenerateNumberedValue.numberStringAndZeroPadTo;
import static sleeper.systemtest.dsl.sourcedata.GenerateNumberedValueOverrides.overrideField;
import static sleeper.systemtest.dsl.testutil.InMemoryTestInstance.IN_MEMORY_MAIN;
import static sleeper.systemtest.dsl.util.SystemTestSchema.DEFAULT_SCHEMA;
import static sleeper.systemtest.dsl.util.SystemTestSchema.ROW_KEY_FIELD_NAME;

@InMemoryDslTest
public class DataFileDuplicationDslTest {

    @BeforeEach
    void setUp(SleeperDsl sleeper) {
        sleeper.connectToInstanceAddOnlineTable(IN_MEMORY_MAIN);
    }

    @Test
    void shouldDuplicateFiles(SleeperDsl sleeper) {
        // Given
        sleeper.ingest().direct(null)
                .numberedRows(LongStream.of(1, 2, 3));

        // When
        sleeper.ingest().toStateStore()
                .duplicateFilesOnSamePartitions(2);

        // Then
        assertThat(sleeper.tableFiles().references()).hasSize(3);
        assertThat(sleeper.directQuery().allRowsInTable())
                .containsExactlyInAnyOrderElementsOf(sleeper.generateNumberedRows()
                        .iterableOver(1, 1, 1, 2, 2, 2, 3, 3, 3));
    }

    @Test
    void shouldCreateCompactionsFromDuplicates(SleeperDsl sleeper) {
        // Given
        sleeper.ingest().direct(null)
                .numberedRows(LongStream.of(1, 2))
                .numberedRows(LongStream.of(3, 4));
        DataFileDuplications duplications = sleeper.ingest().toStateStore()
                .duplicateFilesOnSamePartitions(1);

        // When
        sleeper.compaction()
                .createSeparateCompactionsForOriginalAndDuplicates(duplications)
                .waitForTasks(1).waitForJobs();

        // Then
        assertThat(sleeper.tableFiles().references()).hasSize(2);
        assertThat(sleeper.directQuery().allRowsInTable())
                .containsExactlyInAnyOrderElementsOf(sleeper.generateNumberedRows()
                        .iterableOver(1, 1, 2, 2, 3, 3, 4, 4));
        assertThat(sleeper.reporting().compactionJobs().finishedStatistics())
                .matches(stats -> stats.isAllFinishedOneRunEach(2),
                        "has two finished jobs")
                .matches(stats -> stats.isRowsReadAndWritten(8),
                        "read and wrote all rows");
    }

    @Test
    void shouldCreateCompactionsFromDuplicatesOnMultiplePartitions(SleeperDsl sleeper) {
        // Given
        sleeper.setGeneratorOverrides(overrideField(
                ROW_KEY_FIELD_NAME, numberStringAndZeroPadTo(2).then(addPrefix("row-"))));
        sleeper.partitioning().setPartitions(new PartitionsBuilder(DEFAULT_SCHEMA)
                .rootFirst("root")
                .splitToNewChildren("root", "L", "R", "row-5")
                .buildTree());
        sleeper.updateTableProperties(Map.of(
                TableProperty.INGEST_FILE_WRITING_STRATEGY, IngestFileWritingStrategy.ONE_FILE_PER_LEAF.name()));
        sleeper.sourceFiles().inDataBucket()
                .createWithNumberedRows("file1", LongStream.of(1, 6))
                .createWithNumberedRows("file2", LongStream.of(2, 7));
        DataFileDuplications duplications = sleeper.ingest().toStateStore()
                .addFileOnEveryPartition("file1", 2)
                .addFileOnEveryPartition("file2", 2)
                .duplicateFilesOnSamePartitions(1);

        // When
        sleeper.compaction()
                .createSeparateCompactionsForOriginalAndDuplicates(duplications)
                .waitForTasks(1).waitForJobs();

        // Then
        assertThat(sleeper.tableFiles().references()).hasSize(4);
        assertThat(sleeper.directQuery().allRowsInTable())
                .containsExactlyInAnyOrderElementsOf(sleeper.generateNumberedRows()
                        .iterableOver(1, 1, 2, 2, 6, 6, 7, 7));
        assertThat(sleeper.reporting().compactionJobs().finishedStatistics())
                .matches(stats -> stats.isAllFinishedOneRunEach(4),
                        "has 4 finished jobs")
                .matches(stats -> stats.isRowsReadAndWritten(8),
                        "read and wrote all rows");
    }

}
