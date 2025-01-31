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

package sleeper.systemtest.suite;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import sleeper.core.metrics.TableMetrics;
import sleeper.core.partition.PartitionsBuilder;
import sleeper.systemtest.dsl.SleeperSystemTest;
import sleeper.systemtest.dsl.util.SystemTestSchema;
import sleeper.systemtest.suite.testutil.SystemTest;

import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.stream.LongStream;

import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.core.properties.table.TableProperty.TABLE_ONLINE;
import static sleeper.systemtest.dsl.sourcedata.GenerateNumberedValue.addPrefix;
import static sleeper.systemtest.dsl.sourcedata.GenerateNumberedValue.numberStringAndZeroPadTo;
import static sleeper.systemtest.dsl.sourcedata.GenerateNumberedValueOverrides.overrideField;
import static sleeper.systemtest.dsl.testutil.SystemTestTableMetricsHelper.tableMetrics;
import static sleeper.systemtest.dsl.util.SystemTestSchema.DEFAULT_SCHEMA;
import static sleeper.systemtest.suite.fixtures.SystemTestInstance.MAIN;

@SystemTest
public class TableMetricsST {

    @TempDir
    private Path tempDir;

    @BeforeEach
    void setUp(SleeperSystemTest sleeper) {
        sleeper.connectToInstanceNoTables(MAIN);
        sleeper.setGeneratorOverrides(
                overrideField(SystemTestSchema.ROW_KEY_FIELD_NAME,
                        numberStringAndZeroPadTo(2).then(addPrefix("row-"))));
    }

    @Test
    void shouldReportTableMetrics(SleeperSystemTest sleeper) {
        // Given
        sleeper.tables().createWithProperties("test", DEFAULT_SCHEMA,
                Map.of(TABLE_ONLINE, "false"));
        sleeper.partitioning().setPartitions(new PartitionsBuilder(DEFAULT_SCHEMA)
                .rootFirst("root")
                .splitToNewChildren("root", "L", "R", "row-50")
                .buildTree());
        sleeper.ingest().direct(tempDir)
                .numberedRecords(LongStream.range(0, 100))
                .numberedRecords(LongStream.range(0, 23));

        // When
        TableMetrics metrics = sleeper.tableMetrics().generate().get();

        // Then
        assertThat(metrics).isEqualTo(tableMetrics(sleeper)
                .partitionCount(3).leafPartitionCount(2)
                .fileCount(2).recordCount(123)
                .averageFileReferencesPerPartition(1.5)
                .build());
    }

    @Test
    void shouldReportTableMetricsForMoreTablesThanBatchSize(SleeperSystemTest sleeper) {
        // Given
        sleeper.tables().createWithProperties(
                List.of("A", "B", "C"), DEFAULT_SCHEMA, Map.of(TABLE_ONLINE, "false"))
                .forEach(() -> {
                    sleeper.partitioning().setPartitions(new PartitionsBuilder(DEFAULT_SCHEMA)
                            .rootFirst("root")
                            .splitToNewChildren("root", "L", "R", "row-50")
                            .buildTree());
                    sleeper.ingest().direct(tempDir)
                            .numberedRecords(LongStream.range(0, 100));
                });
        sleeper.table("A").ingest().direct(tempDir)
                .numberedRecords(LongStream.range(0, 23));

        // When
        sleeper.tableMetrics().generate();

        // Then
        assertThat(sleeper.tables().list()).hasSize(3);
        assertThat(sleeper.table("A").tableMetrics().get()).isEqualTo(tableMetrics(sleeper)
                .partitionCount(3).leafPartitionCount(2)
                .fileCount(2).recordCount(123)
                .averageFileReferencesPerPartition(1.5)
                .build());
        assertThat(sleeper.table("B").tableMetrics().get()).isEqualTo(tableMetrics(sleeper)
                .partitionCount(3).leafPartitionCount(2)
                .fileCount(1).recordCount(100)
                .averageFileReferencesPerPartition(1)
                .build());
        assertThat(sleeper.table("C").tableMetrics().get()).isEqualTo(tableMetrics(sleeper)
                .partitionCount(3).leafPartitionCount(2)
                .fileCount(1).recordCount(100)
                .averageFileReferencesPerPartition(1)
                .build());
    }
}
