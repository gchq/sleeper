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
package sleeper.splitter.core.extend;

import org.junit.jupiter.api.Test;

import sleeper.core.partition.PartitionTree;
import sleeper.core.partition.PartitionsBuilder;
import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.table.TableProperties;
import sleeper.core.row.Row;
import sleeper.core.schema.type.IntType;
import sleeper.core.statestore.transactionlog.transaction.impl.ExtendPartitionTreeTransaction;
import sleeper.sketches.Sketches;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.core.properties.table.TableProperty.BULK_IMPORT_MIN_LEAF_PARTITION_COUNT;
import static sleeper.core.properties.testutils.InstancePropertiesTestHelper.createTestInstanceProperties;
import static sleeper.core.properties.testutils.TablePropertiesTestHelper.createTestTableProperties;
import static sleeper.core.schema.SchemaTestHelper.createSchemaWithKey;
import static sleeper.core.testutils.SupplierTestHelper.supplyNumberedIdsWithPrefix;

public class ExtendPartitionTreeBasedOnSketchesTest {
    // Test list:
    // - Partitions without enough data should not be split
    // - Some leaf partitions are never split, but should still count towards minimum leaf partition count (see TODO in SplitsTracker)
    // - Split from a larger existing partition tree
    // - More splits at once
    // - Partition tree cannot be extended enough due to insufficient data

    InstanceProperties instanceProperties = createTestInstanceProperties();
    TableProperties tableProperties = createTestTableProperties(instanceProperties, createSchemaWithKey("key", new IntType()));
    Map<String, Sketches> partitionIdToSketches = new HashMap<>();

    @Test
    void shouldSplitSingleExistingPartitionOnce() {
        // Given
        tableProperties.setNumber(BULK_IMPORT_MIN_LEAF_PARTITION_COUNT, 2);
        PartitionTree tree = new PartitionsBuilder(tableProperties).singlePartition("root").buildTree();
        setPartitionSketchData("root", List.of(
                new Row(Map.of("key", 25)),
                new Row(Map.of("key", 50)),
                new Row(Map.of("key", 75))));

        // When
        ExtendPartitionTreeTransaction transaction = createTransaction(tree);

        // Then
        assertThat(transaction).isEqualTo(transactionWithUpdatedAndNewPartitions(
                new PartitionsBuilder(tableProperties).singlePartition("root")
                        .splitToNewChildren("root", "P1", "P2", 50)
                        .buildTree(),
                List.of("root"),
                List.of("P1", "P2")));
    }

    @Test
    void shouldSplitFromSingleExistingPartitionTwice() {
        // Given
        tableProperties.setNumber(BULK_IMPORT_MIN_LEAF_PARTITION_COUNT, 3);
        PartitionTree tree = new PartitionsBuilder(tableProperties).singlePartition("root").buildTree();
        setPartitionSketchData("root", List.of(
                new Row(Map.of("key", 10)),
                new Row(Map.of("key", 20)),
                new Row(Map.of("key", 30)),
                new Row(Map.of("key", 40)),
                new Row(Map.of("key", 50)),
                new Row(Map.of("key", 60)),
                new Row(Map.of("key", 70)),
                new Row(Map.of("key", 80))));

        // When
        ExtendPartitionTreeTransaction transaction = createTransaction(tree);

        // Then
        assertThat(transaction).isEqualTo(transactionWithUpdatedAndNewPartitions(
                new PartitionsBuilder(tableProperties).singlePartition("root")
                        .splitToNewChildren("root", "P1", "P2", 50)
                        .splitToNewChildren("P1", "P3", "P4", 30)
                        .splitToNewChildren("P2", "P5", "P6", 70)
                        .buildTree(),
                List.of("root"),
                List.of("P1", "P2", "P3", "P4", "P5", "P6")));
    }

    private ExtendPartitionTreeTransaction createTransaction(PartitionTree tree) {
        return ExtendPartitionTreeBasedOnSketches.forBulkImport(tableProperties, supplyNumberedIdsWithPrefix("P"))
                .createTransaction(tree, partitionIdToSketches);
    }

    private void setPartitionSketchData(String partitionId, List<Row> rows) {
        Sketches sketches = Sketches.from(tableProperties.getSchema());
        rows.forEach(sketches::update);
        partitionIdToSketches.put(partitionId, sketches);
    }

    private ExtendPartitionTreeTransaction transactionWithUpdatedAndNewPartitions(
            PartitionTree treeAfterTransaction, List<String> updatedPartitions, List<String> newPartitions) {
        return new ExtendPartitionTreeTransaction(
                updatedPartitions.stream().map(treeAfterTransaction::getPartition).toList(),
                newPartitions.stream().map(treeAfterTransaction::getPartition).toList());
    }
}
