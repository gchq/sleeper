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

import sleeper.systemtest.dsl.SleeperSystemTest;
import sleeper.systemtest.suite.fixtures.SystemTestSchema;
import sleeper.systemtest.suite.testutil.SystemTest;

import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.stream.LongStream;

import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.systemtest.dsl.sourcedata.GenerateNumberedValue.addPrefix;
import static sleeper.systemtest.dsl.sourcedata.GenerateNumberedValue.numberStringAndZeroPadTo;
import static sleeper.systemtest.dsl.sourcedata.GenerateNumberedValueOverrides.overrideField;
import static sleeper.systemtest.suite.fixtures.SystemTestInstance.MAIN;
import static sleeper.systemtest.suite.testutil.PartitionsTestHelper.partitionsBuilder;

@SystemTest
public class TableMetricsIT {

    @TempDir
    private Path tempDir;

    @BeforeEach
    void setUp(SleeperSystemTest sleeper) {
        sleeper.connectToInstance(MAIN);
    }

    @Test
    void shouldReportTableMetrics(SleeperSystemTest sleeper) {
        // Given
        sleeper.setGeneratorOverrides(
                overrideField(SystemTestSchema.ROW_KEY_FIELD_NAME,
                        numberStringAndZeroPadTo(2).then(addPrefix("row-"))));
        sleeper.partitioning().setPartitions(partitionsBuilder(sleeper)
                .rootFirst("root")
                .splitToNewChildren("root", "L", "R", "row-50")
                .buildTree());
        sleeper.ingest().direct(tempDir)
                .numberedRecords(LongStream.range(0, 100))
                .numberedRecords(LongStream.range(0, 23));

        // When
        Map<String, List<Double>> metrics = sleeper.metrics()
                .generateTableMetrics()
                .getTableMetrics();

        // Then
        assertThat(metrics).isEqualTo(Map.of(
                "ActiveFileCount", List.of(3.0),
                "AverageActiveFilesPerPartition", List.of(1.5),
                "LeafPartitionCount", List.of(2.0),
                "PartitionCount", List.of(3.0),
                "RecordCount", List.of(123.0)));
    }
}
