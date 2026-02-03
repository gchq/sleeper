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
package sleeper.systemtest.dsl.partitioning;

import org.approvaltests.Approvals;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import sleeper.core.partition.PartitionTree;
import sleeper.core.schema.Schema;
import sleeper.core.statestore.AllReferencesToAllFiles;
import sleeper.systemtest.dsl.SleeperDsl;
import sleeper.systemtest.dsl.testutil.InMemoryDslTest;

import java.util.Map;
import java.util.stream.LongStream;

import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.core.properties.table.TableProperty.PARTITION_SPLIT_THRESHOLD;
import static sleeper.core.testutils.printers.FileReferencePrinter.printFiles;
import static sleeper.core.testutils.printers.PartitionsPrinter.printPartitions;
import static sleeper.systemtest.dsl.sourcedata.GenerateNumberedValue.addPrefix;
import static sleeper.systemtest.dsl.sourcedata.GenerateNumberedValue.numberStringAndZeroPadTo;
import static sleeper.systemtest.dsl.sourcedata.GenerateNumberedValueOverrides.overrideField;
import static sleeper.systemtest.dsl.testutil.InMemoryTestInstance.IN_MEMORY_MAIN;
import static sleeper.systemtest.dsl.util.SystemTestSchema.ROW_KEY_FIELD_NAME;

@InMemoryDslTest
public class PartitionSplittingTest {

    @BeforeEach
    void setUp(SleeperDsl sleeper) {
        sleeper.connectToInstanceAddOnlineTable(IN_MEMORY_MAIN);
    }

    @Test
    void shouldSplitPartitionsWith100RowsAndThresholdOf20(SleeperDsl sleeper) {
        // Given
        sleeper.setGeneratorOverrides(
                overrideField(ROW_KEY_FIELD_NAME,
                        numberStringAndZeroPadTo(2).then(addPrefix("row-"))));
        sleeper.updateTableProperties(Map.of(PARTITION_SPLIT_THRESHOLD, "20"));
        sleeper.ingest().direct(null).numberedRows(LongStream.range(0, 100));

        // When
        sleeper.partitioning().split();
        sleeper.compaction().splitFilesAndRunJobs(2);
        sleeper.partitioning().split();
        sleeper.compaction().splitFilesAndRunJobs(4);
        sleeper.partitioning().split();
        sleeper.compaction().splitFilesAndRunJobs(8);

        // Then
        assertThat(sleeper.directQuery().allRowsInTable())
                .containsExactlyInAnyOrderElementsOf(sleeper.generateNumberedRows()
                        .iterableOverRange(0, 100));
        Schema schema = sleeper.tableProperties().getSchema();
        PartitionTree partitions = sleeper.partitioning().tree();
        AllReferencesToAllFiles files = sleeper.tableFiles().all();
        Approvals.verify(printPartitions(schema, partitions) + "\n" +
                printFiles(partitions, files));
    }
}
