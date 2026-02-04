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
package sleeper.bulkimport.runner;

import org.junit.jupiter.api.Test;

import sleeper.bulkimport.core.job.BulkImportJob;
import sleeper.core.partition.PartitionTree;
import sleeper.core.partition.PartitionsBuilder;
import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.table.TableProperties;
import sleeper.core.row.Row;
import sleeper.core.schema.Schema;
import sleeper.core.schema.type.IntType;
import sleeper.core.statestore.StateStore;
import sleeper.core.statestore.testutils.FixedStateStoreProvider;
import sleeper.core.statestore.testutils.InMemoryTransactionLogStateStore;
import sleeper.core.statestore.testutils.InMemoryTransactionLogStore;
import sleeper.core.statestore.testutils.InMemoryTransactionLogs;
import sleeper.core.statestore.transactionlog.transaction.impl.ExtendPartitionTreeTransaction;
import sleeper.core.testutils.printers.PartitionsPrinter;
import sleeper.sketches.Sketches;
import sleeper.splitter.core.extend.InsufficientDataForPartitionSplittingException;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static sleeper.core.properties.table.TableProperty.BULK_IMPORT_MIN_LEAF_PARTITION_COUNT;
import static sleeper.core.properties.table.TableProperty.BULK_IMPORT_PARTITION_SPLITTING_ATTEMPTS;
import static sleeper.core.properties.table.TableProperty.PARTITION_SPLIT_MIN_ROWS;
import static sleeper.core.properties.table.TableProperty.TABLE_ID;
import static sleeper.core.properties.table.TableProperty.TABLE_NAME;
import static sleeper.core.properties.testutils.InstancePropertiesTestHelper.createTestInstanceProperties;
import static sleeper.core.properties.testutils.TablePropertiesTestHelper.createTestTableProperties;
import static sleeper.core.schema.SchemaTestHelper.createSchemaWithKey;
import static sleeper.core.statestore.testutils.StateStoreUpdatesWrapper.update;
import static sleeper.core.testutils.SupplierTestHelper.supplyNumberedIdsWithPrefix;

public class PartitionPreSplitterTest {
    private final InstanceProperties instanceProperties = createTestInstanceProperties();
    private final Schema schema = createSchemaWithKey("key", new IntType());
    private final TableProperties tableProperties = createTestTableProperties(instanceProperties, schema);
    private final InMemoryTransactionLogs transactionLogs = new InMemoryTransactionLogs();
    private final InMemoryTransactionLogStore partitionsLogStore = transactionLogs.getPartitionsLogStore();
    private final StateStore stateStore = InMemoryTransactionLogStateStore.createAndInitialise(tableProperties, transactionLogs);
    private final Map<String, Sketches> partitionIdToSketches = new HashMap<>();

    @Test
    void shouldDoNothingWhenEnoughLeafPartitionsArePresent() {
        // Given
        tableProperties.setNumber(BULK_IMPORT_MIN_LEAF_PARTITION_COUNT, 2);
        PartitionTree partitionsBefore = new PartitionsBuilder(tableProperties)
                .rootFirst("root")
                .splitToNewChildren("root", "P1", "P2", 50)
                .buildTree();
        update(stateStore).initialise(partitionsBefore);

        // When
        preSplitPartitionsIfNecessary();

        // Then
        assertThat(stateStore.getAllPartitions())
                .withRepresentation(PartitionsPrinter.representation(tableProperties))
                .isEqualTo(partitionsBefore.getAllPartitions());
    }

    @Test
    void shouldSplitTree() {
        // Given
        tableProperties.setNumber(BULK_IMPORT_MIN_LEAF_PARTITION_COUNT, 2);
        tableProperties.setNumber(PARTITION_SPLIT_MIN_ROWS, 1);
        update(stateStore).initialise(new PartitionsBuilder(tableProperties)
                .singlePartition("root")
                .buildTree());
        setPartitionSketchData("root", List.of(
                new Row(Map.of("key", 25)),
                new Row(Map.of("key", 50)),
                new Row(Map.of("key", 75))));

        // When
        preSplitPartitionsIfNecessary();

        // Then
        assertThat(new PartitionTree(stateStore.getAllPartitions()))
                .withRepresentation(PartitionsPrinter.representation(tableProperties))
                .isEqualTo(new PartitionsBuilder(tableProperties)
                        .rootFirst("root")
                        .splitToNewChildren("root", "P1", "P2", 50)
                        .buildTree());
    }

    @Test
    void shouldFailWhenNotEnoughDataIsPresent() {
        // Given we need 10 rows to split a partition, and we have 3
        tableProperties.setNumber(BULK_IMPORT_MIN_LEAF_PARTITION_COUNT, 2);
        tableProperties.setNumber(PARTITION_SPLIT_MIN_ROWS, 10);
        update(stateStore).initialise(new PartitionsBuilder(tableProperties)
                .singlePartition("root")
                .buildTree());
        setPartitionSketchData("root", List.of(
                new Row(Map.of("key", 25)),
                new Row(Map.of("key", 50)),
                new Row(Map.of("key", 75))));

        // When / Then
        assertThatThrownBy(this::preSplitPartitionsIfNecessary)
                .isInstanceOf(InsufficientDataForPartitionSplittingException.class);
    }

    @Test
    void shouldAcceptNewTreeWhenPartitionsAreSplitByAnotherProcessBeforeWeCommitOurSplit() {
        // Given we configure to split from one partition to two
        tableProperties.setNumber(BULK_IMPORT_MIN_LEAF_PARTITION_COUNT, 2);
        tableProperties.setNumber(PARTITION_SPLIT_MIN_ROWS, 1);
        update(stateStore).initialise(new PartitionsBuilder(tableProperties)
                .singlePartition("root")
                .buildTree());
        // And we provide data to expect a split point at 50
        setPartitionSketchData("root", List.of(
                new Row(Map.of("key", 25)),
                new Row(Map.of("key", 50)),
                new Row(Map.of("key", 75))));
        // And the partition tree will be extended by another process just before our split is applied
        PartitionTree partitionsFromOtherProcess = new PartitionsBuilder(tableProperties)
                .rootFirst("root")
                .splitToNewChildren("root", "L", "R", 50)
                .buildTree();
        partitionsLogStore.atStartOfNextAddTransaction(() -> extendPartitionTreeWithUpdatedAndNew(
                partitionsFromOtherProcess,
                List.of("root"),
                List.of("L", "R")));

        // When
        preSplitPartitionsIfNecessary();

        // Then
        assertThat(stateStore.getAllPartitions())
                .withRepresentation(PartitionsPrinter.representation(tableProperties))
                .isEqualTo(partitionsFromOtherProcess.getAllPartitions());
    }

    @Test
    void shouldExtendNewTreeWhenPartitionsAreSplitByAnotherProcessBeforeWeCommitOurSplit() {

        // Given we configure to split from one partition to four
        tableProperties.setNumber(BULK_IMPORT_MIN_LEAF_PARTITION_COUNT, 4);
        tableProperties.setNumber(PARTITION_SPLIT_MIN_ROWS, 1);
        update(stateStore).initialise(new PartitionsBuilder(tableProperties)
                .singlePartition("root")
                .buildTree());

        // And we provide enough data for split points to extend the tree to 4 leaf partitions
        setPartitionSketchData("root", List.of(
                new Row(Map.of("key", 15)),
                new Row(Map.of("key", 20)),
                new Row(Map.of("key", 25)),
                new Row(Map.of("key", 50)),
                new Row(Map.of("key", 55)),
                new Row(Map.of("key", 60)),
                new Row(Map.of("key", 75))));

        // And we provide the same sketch data for the intermediate partitions that will be created in conflict with this split
        setPartitionSketchData("L", List.of(
                new Row(Map.of("key", 15)),
                new Row(Map.of("key", 20)),
                new Row(Map.of("key", 25))));
        setPartitionSketchData("R", List.of(
                new Row(Map.of("key", 50)),
                new Row(Map.of("key", 55)),
                new Row(Map.of("key", 60)),
                new Row(Map.of("key", 75))));

        // And the partition tree will be extended by another process just before our split is applied
        partitionsLogStore.atStartOfNextAddTransaction(() -> extendPartitionTreeWithUpdatedAndNew(
                new PartitionsBuilder(tableProperties)
                        .rootFirst("root")
                        .splitToNewChildren("root", "L", "R", 50)
                        .buildTree(),
                List.of("root"),
                List.of("L", "R")));

        // When
        preSplitPartitionsIfNecessary();

        // Then
        assertThat(new PartitionTree(stateStore.getAllPartitions()))
                .withRepresentation(PartitionsPrinter.representation(tableProperties))
                .isEqualTo(new PartitionsBuilder(tableProperties)
                        .rootFirst("root")
                        .splitToNewChildren("root", "L", "R", 50)
                        .splitToNewChildren("L", "P9", "P10", 20)
                        .splitToNewChildren("R", "P7", "P8", 60)
                        .buildTree());
    }

    @Test
    void shouldNotRetryWithOneAttempt() {

        // Given we configure to split from one partition to four, with a maximum of one attempt
        tableProperties.setNumber(BULK_IMPORT_PARTITION_SPLITTING_ATTEMPTS, 1);
        tableProperties.setNumber(BULK_IMPORT_MIN_LEAF_PARTITION_COUNT, 4);
        tableProperties.setNumber(PARTITION_SPLIT_MIN_ROWS, 1);
        update(stateStore).initialise(new PartitionsBuilder(tableProperties)
                .singlePartition("root")
                .buildTree());

        // And we provide enough data for split points to extend the tree to 4 leaf partitions
        setPartitionSketchData("root", List.of(
                new Row(Map.of("key", 15)),
                new Row(Map.of("key", 20)),
                new Row(Map.of("key", 25)),
                new Row(Map.of("key", 50)),
                new Row(Map.of("key", 55)),
                new Row(Map.of("key", 60)),
                new Row(Map.of("key", 75))));

        // And the partition tree will be extended by another process just before our split is applied
        partitionsLogStore.atStartOfNextAddTransaction(() -> extendPartitionTreeWithUpdatedAndNew(
                new PartitionsBuilder(tableProperties)
                        .rootFirst("root")
                        .splitToNewChildren("root", "L", "R", 50)
                        .buildTree(),
                List.of("root"),
                List.of("L", "R")));

        // When / Then
        assertThatThrownBy(this::preSplitPartitionsIfNecessary)
                .isInstanceOf(TooManyAttemptsPartitionSplitterException.class);
    }

    @Test
    void shouldNotRetryWithTwoAttempts() {

        // Given we configure to split from one partition to eight, with a maximum of 2 attempts
        tableProperties.setNumber(BULK_IMPORT_PARTITION_SPLITTING_ATTEMPTS, 2);
        tableProperties.setNumber(BULK_IMPORT_MIN_LEAF_PARTITION_COUNT, 8);
        tableProperties.setNumber(PARTITION_SPLIT_MIN_ROWS, 1);
        update(stateStore).initialise(new PartitionsBuilder(tableProperties)
                .singlePartition("root")
                .buildTree());

        // And we provide enough data for split points to extend the tree to 8 leaf partitions
        setPartitionSketchData("root", rowsFromRangeClosed(1, 24));

        // And we provide the same sketch data for the intermediate partitions that will be created in conflict with this split
        setPartitionSketchData("L", rowsFromRangeClosed(1, 12));
        setPartitionSketchData("R", rowsFromRangeClosed(13, 24));
        setPartitionSketchData("LL", rowsFromRangeClosed(1, 6));
        setPartitionSketchData("LR", rowsFromRangeClosed(7, 12));
        setPartitionSketchData("RL", rowsFromRangeClosed(13, 18));
        setPartitionSketchData("RR", rowsFromRangeClosed(19, 24));

        // And the partition tree will be extended by another process just before our first two split attempts are applied
        partitionsLogStore.atStartOfNextAddTransactions(List.of(
                () -> extendPartitionTreeWithUpdatedAndNew(
                        new PartitionsBuilder(tableProperties)
                                .rootFirst("root")
                                .splitToNewChildren("root", "L", "R", 13)
                                .buildTree(),
                        List.of("root"), List.of("L", "R")),
                () -> extendPartitionTreeWithUpdatedAndNew(
                        new PartitionsBuilder(tableProperties)
                                .rootFirst("root")
                                .splitToNewChildren("root", "L", "R", 13)
                                .splitToNewChildren("L", "LL", "LR", 7)
                                .splitToNewChildren("R", "RL", "RR", 19)
                                .buildTree(),
                        List.of("L", "R"), List.of("LL", "LR", "RL", "RR"))));

        // When / Then
        assertThatThrownBy(this::preSplitPartitionsIfNecessary)
                .isInstanceOf(TooManyAttemptsPartitionSplitterException.class);
    }

    private List<Row> rowsFromRangeClosed(int min, int max) {
        return IntStream.rangeClosed(min, max)
                .mapToObj(i -> new Row(Map.of("key", i)))
                .toList();
    }

    private void preSplitPartitionsIfNecessary() {
        splitter().preSplitPartitionsIfNecessary(tableProperties, stateStore.getAllPartitions(), singleFileImportContext());
    }

    private FakeBulkImportContext singleFileImportContext() {
        return new FakeBulkImportContext(tableProperties, stateStore.getAllPartitions(), singleFileImportJob(), c -> {
        }, c -> {
        });
    }

    private void extendPartitionTreeWithUpdatedAndNew(PartitionTree partitions, List<String> updatedIds, List<String> newIds) {
        new ExtendPartitionTreeTransaction(
                updatedIds.stream().map(partitions::getPartition).toList(),
                newIds.stream().map(partitions::getPartition).toList())
                .synchronousCommit(stateStore);
    }

    private BulkImportJob singleFileImportJob() {
        return BulkImportJob.builder()
                .id("test-job")
                .tableId(tableProperties.get(TABLE_ID))
                .tableName(tableProperties.get(TABLE_NAME))
                .files(List.of("test.parquet")).build();
    }

    private void setPartitionSketchData(String partitionId, List<Row> rows) {
        Sketches sketches = Sketches.from(tableProperties.getSchema());
        rows.forEach(sketches::update);
        partitionIdToSketches.put(partitionId, sketches);
    }

    private PartitionPreSplitter<FakeBulkImportContext> splitter() {
        return new PartitionPreSplitter<>(
                context -> partitionIdToSketches,
                FixedStateStoreProvider.singleTable(tableProperties, stateStore),
                supplyNumberedIdsWithPrefix("P"));
    }

}
