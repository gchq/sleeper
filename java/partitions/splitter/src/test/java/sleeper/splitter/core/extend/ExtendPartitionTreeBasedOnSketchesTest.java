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

import org.assertj.core.api.InstanceOfAssertFactories;
import org.assertj.core.presentation.Representation;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import sleeper.core.partition.Partition;
import sleeper.core.partition.PartitionTree;
import sleeper.core.partition.PartitionsBuilder;
import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.table.TableProperties;
import sleeper.core.row.Row;
import sleeper.core.schema.type.IntType;
import sleeper.core.statestore.transactionlog.state.StateStorePartitions;
import sleeper.core.statestore.transactionlog.transaction.impl.ExtendPartitionTreeTransaction;
import sleeper.core.testutils.printers.PartitionsPrinter;
import sleeper.sketches.Sketches;

import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static java.util.stream.Collectors.joining;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static sleeper.core.properties.table.TableProperty.BULK_IMPORT_MIN_LEAF_PARTITION_COUNT;
import static sleeper.core.properties.table.TableProperty.PARTITION_SPLIT_MIN_DISTRIBUTION_PERCENT;
import static sleeper.core.properties.table.TableProperty.PARTITION_SPLIT_MIN_ROWS;
import static sleeper.core.properties.testutils.InstancePropertiesTestHelper.createTestInstanceProperties;
import static sleeper.core.properties.testutils.TablePropertiesTestHelper.createTestTableProperties;
import static sleeper.core.schema.SchemaTestHelper.createSchemaWithKey;
import static sleeper.core.testutils.SupplierTestHelper.supplyNumberedIdsWithPrefix;

public class ExtendPartitionTreeBasedOnSketchesTest {

    InstanceProperties instanceProperties = createTestInstanceProperties();
    TableProperties tableProperties = createTestTableProperties(instanceProperties, createSchemaWithKey("key", new IntType()));
    PartitionTree partitionsBefore;
    Map<String, Sketches> partitionIdToSketches = new HashMap<>();

    @BeforeEach
    void setUp() {
        tableProperties.setNumber(PARTITION_SPLIT_MIN_ROWS, 0);
        tableProperties.setNumber(PARTITION_SPLIT_MIN_DISTRIBUTION_PERCENT, 0);
    }

    @Nested
    @DisplayName("Split from a single root partition")
    class SplitSingleRoot {

        @Test
        void shouldSplitSingleExistingPartitionOnce() {
            // Given
            tableProperties.setNumber(BULK_IMPORT_MIN_LEAF_PARTITION_COUNT, 2);
            setPartitionsBefore(new PartitionsBuilder(tableProperties).singlePartition("root").buildTree());
            setPartitionSketchData("root", List.of(
                    new Row(Map.of("key", 25)),
                    new Row(Map.of("key", 50)),
                    new Row(Map.of("key", 75))));

            // When
            ExtendPartitionTreeTransaction transaction = createTransaction();

            // Then
            assertThat(transaction)
                    .withRepresentation(transactionRepresentation())
                    .isEqualTo(transactionWithUpdatedAndNewPartitions(
                            new PartitionsBuilder(tableProperties).singlePartition("root")
                                    .splitToNewChildren("root", "P1", "P2", 50)
                                    .buildTree(),
                            List.of("root"),
                            List.of("P1", "P2")));
        }

        @Test
        void shouldSplitFromSingleExistingPartitionTwice() {
            // Given
            tableProperties.setNumber(BULK_IMPORT_MIN_LEAF_PARTITION_COUNT, 4);
            setPartitionsBefore(new PartitionsBuilder(tableProperties).singlePartition("root").buildTree());
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
            ExtendPartitionTreeTransaction transaction = createTransaction();

            // Then
            assertThat(transaction)
                    .withRepresentation(transactionRepresentation())
                    .isEqualTo(transactionWithUpdatedAndNewPartitions(
                            new PartitionsBuilder(tableProperties).singlePartition("root")
                                    .splitToNewChildren("root", "P1", "P2", 50)
                                    .splitToNewChildren("P1", "P3", "P4", 30)
                                    .splitToNewChildren("P2", "P5", "P6", 70)
                                    .buildTree(),
                            List.of("root"),
                            List.of("P1", "P2", "P3", "P4", "P5", "P6")));
        }
    }

    @Nested
    @DisplayName("Split from multiple leaves")
    class SplitMultipleLeaves {

        @Test
        void shouldSplitFromTwoToFourLeaves() {
            // Given
            tableProperties.setNumber(BULK_IMPORT_MIN_LEAF_PARTITION_COUNT, 4);
            setPartitionsBefore(new PartitionsBuilder(tableProperties)
                    .rootFirst("root")
                    .splitToNewChildren("root", "L", "R", 50)
                    .buildTree());
            setPartitionSketchData("L", List.of(
                    new Row(Map.of("key", 10)),
                    new Row(Map.of("key", 25)),
                    new Row(Map.of("key", 40))));
            setPartitionSketchData("R", List.of(
                    new Row(Map.of("key", 60)),
                    new Row(Map.of("key", 75)),
                    new Row(Map.of("key", 90))));

            // When
            ExtendPartitionTreeTransaction transaction = createTransaction();

            // Then
            assertThat(transaction)
                    .withRepresentation(transactionRepresentation())
                    .isEqualTo(transactionWithUpdatedAndNewPartitions(
                            new PartitionsBuilder(tableProperties).singlePartition("root")
                                    .splitToNewChildren("root", "L", "R", 50)
                                    .splitToNewChildren("L", "P1", "P2", 25)
                                    .splitToNewChildren("R", "P3", "P4", 75)
                                    .buildTree(),
                            List.of("L", "R"),
                            List.of("P1", "P2", "P3", "P4")));
        }

        @Test
        void shouldSplitFromTwoToEightLeaves() {
            // Given
            tableProperties.setNumber(BULK_IMPORT_MIN_LEAF_PARTITION_COUNT, 8);
            setPartitionsBefore(new PartitionsBuilder(tableProperties)
                    .rootFirst("root")
                    .splitToNewChildren("root", "L", "R", 50)
                    .buildTree());
            setPartitionSketchData("L", List.of(
                    new Row(Map.of("key", 0)),
                    new Row(Map.of("key", 10)),
                    new Row(Map.of("key", 20)),
                    new Row(Map.of("key", 30)),
                    new Row(Map.of("key", 40))));
            setPartitionSketchData("R", List.of(
                    new Row(Map.of("key", 60)),
                    new Row(Map.of("key", 70)),
                    new Row(Map.of("key", 80)),
                    new Row(Map.of("key", 90)),
                    new Row(Map.of("key", 100))));

            // When
            ExtendPartitionTreeTransaction transaction = createTransaction();

            // Then
            assertThat(transaction)
                    .withRepresentation(transactionRepresentation())
                    .isEqualTo(transactionWithUpdatedAndNewPartitions(
                            new PartitionsBuilder(tableProperties).singlePartition("root")
                                    .splitToNewChildren("root", "L", "R", 50)
                                    .splitToNewChildren("L", "P1", "P2", 20)
                                    .splitToNewChildren("R", "P3", "P4", 80)
                                    .splitToNewChildren("P1", "P5", "P6", 10)
                                    .splitToNewChildren("P2", "P7", "P8", 30)
                                    .splitToNewChildren("P3", "P9", "P10", 70)
                                    .splitToNewChildren("P4", "P11", "P12", 90)
                                    .buildTree(),
                            List.of("L", "R"),
                            List.of("P1", "P2", "P3", "P4", "P5", "P6", "P7", "P8", "P9", "P10", "P11", "P12")));
        }
    }

    @Nested
    @DisplayName("Fail to split due to insufficient unique row key values")
    class InsufficientRowKeyValues {

        @Test
        void shouldFailWhenAllValuesAreSameForRowKey() {
            // Given
            tableProperties.setNumber(BULK_IMPORT_MIN_LEAF_PARTITION_COUNT, 2);
            setPartitionsBefore(new PartitionsBuilder(tableProperties).singlePartition("root").buildTree());
            setPartitionSketchData("root", List.of(
                    new Row(Map.of("key", 50)),
                    new Row(Map.of("key", 50)),
                    new Row(Map.of("key", 50))));

            // When / Then
            assertThatThrownBy(() -> createTransaction())
                    .asInstanceOf(InstanceOfAssertFactories.type(InsufficientDataForPartitionSplittingException.class))
                    .extracting(
                            InsufficientDataForPartitionSplittingException::getMinLeafPartitions,
                            InsufficientDataForPartitionSplittingException::getMaxLeafPartitionsAfterSplits)
                    .containsExactly(2, 1);
        }

        @Test
        void shouldFailWhenOnePartitionHasAllValuesSameForRowKey() {
            // Given
            tableProperties.setNumber(BULK_IMPORT_MIN_LEAF_PARTITION_COUNT, 4);
            setPartitionsBefore(new PartitionsBuilder(tableProperties)
                    .rootFirst("root")
                    .splitToNewChildren("root", "L", "R", 50)
                    .buildTree());
            setPartitionSketchData("L", List.of(
                    new Row(Map.of("key", 25)),
                    new Row(Map.of("key", 25)),
                    new Row(Map.of("key", 25))));
            setPartitionSketchData("R", List.of(
                    new Row(Map.of("key", 60)),
                    new Row(Map.of("key", 75)),
                    new Row(Map.of("key", 90))));

            // When / Then
            assertThatThrownBy(() -> createTransaction())
                    .asInstanceOf(InstanceOfAssertFactories.type(InsufficientDataForPartitionSplittingException.class))
                    .extracting(
                            InsufficientDataForPartitionSplittingException::getMinLeafPartitions,
                            InsufficientDataForPartitionSplittingException::getMaxLeafPartitionsAfterSplits)
                    .containsExactly(4, 3);
        }
    }

    @Nested
    @DisplayName("Do not split a partition when we have less than a minimum number of sketched rows")
    class MinimumRowsToSplit {

        @Test
        void shouldNotSplitPartitionWithLessThanMinimumRowsInSketch() {
            // Given
            tableProperties.setNumber(BULK_IMPORT_MIN_LEAF_PARTITION_COUNT, 3);
            tableProperties.setNumber(PARTITION_SPLIT_MIN_ROWS, 5);
            setPartitionsBefore(new PartitionsBuilder(tableProperties)
                    .rootFirst("root")
                    .splitToNewChildren("root", "L", "R", 50)
                    .buildTree());
            setPartitionSketchData("L", List.of(
                    new Row(Map.of("key", 10)),
                    new Row(Map.of("key", 25)),
                    new Row(Map.of("key", 40))));
            setPartitionSketchData("R", List.of(
                    new Row(Map.of("key", 60)),
                    new Row(Map.of("key", 70)),
                    new Row(Map.of("key", 75)),
                    new Row(Map.of("key", 80)),
                    new Row(Map.of("key", 90))));

            // When
            ExtendPartitionTreeTransaction transaction = createTransaction();

            // Then
            assertThat(transaction)
                    .withRepresentation(transactionRepresentation())
                    .isEqualTo(transactionWithUpdatedAndNewPartitions(
                            new PartitionsBuilder(tableProperties).singlePartition("root")
                                    .splitToNewChildren("root", "L", "R", 50)
                                    .splitToNewChildren("R", "P1", "P2", 75)
                                    .buildTree(),
                            List.of("R"),
                            List.of("P1", "P2")));
        }

        @Test
        void shouldFailWhenLessThanMinimumRowsInOneOfTwoPartitionsMeansWeCannotMeetMinimumCount() {
            // Given
            tableProperties.setNumber(BULK_IMPORT_MIN_LEAF_PARTITION_COUNT, 4);
            tableProperties.setNumber(PARTITION_SPLIT_MIN_ROWS, 5);
            setPartitionsBefore(new PartitionsBuilder(tableProperties)
                    .rootFirst("root")
                    .splitToNewChildren("root", "L", "R", 50)
                    .buildTree());
            setPartitionSketchData("L", List.of(
                    new Row(Map.of("key", 10)),
                    new Row(Map.of("key", 25)),
                    new Row(Map.of("key", 40))));
            setPartitionSketchData("R", List.of(
                    new Row(Map.of("key", 60)),
                    new Row(Map.of("key", 70)),
                    new Row(Map.of("key", 75)),
                    new Row(Map.of("key", 80)),
                    new Row(Map.of("key", 90))));

            // When / Then
            assertThatThrownBy(() -> createTransaction())
                    .asInstanceOf(InstanceOfAssertFactories.type(InsufficientDataForPartitionSplittingException.class))
                    .extracting(
                            InsufficientDataForPartitionSplittingException::getMinLeafPartitions,
                            InsufficientDataForPartitionSplittingException::getMaxLeafPartitionsAfterSplits)
                    .containsExactly(4, 3);
        }

        @Test
        void shouldFailWithLessThanMinimumRowsInHalfOfSketch() {
            // Given
            tableProperties.setNumber(BULK_IMPORT_MIN_LEAF_PARTITION_COUNT, 4);
            tableProperties.setNumber(PARTITION_SPLIT_MIN_ROWS, 5);
            setPartitionsBefore(new PartitionsBuilder(tableProperties)
                    .rootFirst("root")
                    .buildTree());
            setPartitionSketchData("root", List.of(
                    new Row(Map.of("key", 10)),
                    new Row(Map.of("key", 25)),
                    new Row(Map.of("key", 40)),
                    new Row(Map.of("key", 50)),
                    new Row(Map.of("key", 60)),
                    new Row(Map.of("key", 75)),
                    new Row(Map.of("key", 90))));

            // When / Then
            assertThatThrownBy(() -> createTransaction())
                    .asInstanceOf(InstanceOfAssertFactories.type(InsufficientDataForPartitionSplittingException.class))
                    .extracting(
                            InsufficientDataForPartitionSplittingException::getMinLeafPartitions,
                            InsufficientDataForPartitionSplittingException::getMaxLeafPartitionsAfterSplits)
                    .containsExactly(4, 2);
        }
    }

    @Nested
    @DisplayName("Do not split a partition when we have less than a percentage of the expected number of rows based on an even distribution")
    class MinimumDistributionToSplit {

        @Test
        void shouldNotSplitPartitionWithLessThanExpectedRowsAssumingEvenDistribution() {
            // Given
            tableProperties.setNumber(BULK_IMPORT_MIN_LEAF_PARTITION_COUNT, 3);
            tableProperties.setNumber(PARTITION_SPLIT_MIN_DISTRIBUTION_PERCENT, 60);
            setPartitionsBefore(new PartitionsBuilder(tableProperties)
                    .rootFirst("root")
                    .splitToNewChildren("root", "L", "R", 50)
                    .buildTree());
            // 12 rows spread over 2 partitions means we expect 6 per partition.
            // 60% of 6 is 3.6 rows.
            // 3 rows in partition L does not meet that threshold.
            // 9 rows in partition R does.
            setPartitionSketchData("L", List.of(
                    new Row(Map.of("key", 10)),
                    new Row(Map.of("key", 25)),
                    new Row(Map.of("key", 40))));
            setPartitionSketchData("R", List.of(
                    new Row(Map.of("key", 55)),
                    new Row(Map.of("key", 60)),
                    new Row(Map.of("key", 65)),
                    new Row(Map.of("key", 70)),
                    new Row(Map.of("key", 75)),
                    new Row(Map.of("key", 80)),
                    new Row(Map.of("key", 85)),
                    new Row(Map.of("key", 90)),
                    new Row(Map.of("key", 95))));

            // When
            ExtendPartitionTreeTransaction transaction = createTransaction();

            // Then
            assertThat(transaction)
                    .withRepresentation(transactionRepresentation())
                    .isEqualTo(transactionWithUpdatedAndNewPartitions(
                            new PartitionsBuilder(tableProperties).singlePartition("root")
                                    .splitToNewChildren("root", "L", "R", 50)
                                    .splitToNewChildren("R", "P1", "P2", 75)
                                    .buildTree(),
                            List.of("R"),
                            List.of("P1", "P2")));
        }

        @Test
        void shouldFailWhenLessThanMinimumRowsInOneOfTwoPartitionsMeansWeCannotMeetMinimumCount() {
            // Given
            tableProperties.setNumber(BULK_IMPORT_MIN_LEAF_PARTITION_COUNT, 4);
            tableProperties.setNumber(PARTITION_SPLIT_MIN_DISTRIBUTION_PERCENT, 60);
            setPartitionsBefore(new PartitionsBuilder(tableProperties)
                    .rootFirst("root")
                    .splitToNewChildren("root", "L", "R", 50)
                    .buildTree());
            // 12 rows spread over 2 partitions means we expect 6 per partition.
            // 60% of 6 is 3.6 rows.
            // 3 rows in partition L does not meet that threshold.
            // 9 rows in partition R does.
            // Partition R can only be split once because it has only 3 unique values of the row key.
            setPartitionSketchData("L", List.of(
                    new Row(Map.of("key", 10)),
                    new Row(Map.of("key", 25)),
                    new Row(Map.of("key", 40))));
            setPartitionSketchData("R", List.of(
                    new Row(Map.of("key", 60)),
                    new Row(Map.of("key", 60)),
                    new Row(Map.of("key", 60)),
                    new Row(Map.of("key", 60)),
                    new Row(Map.of("key", 75)),
                    new Row(Map.of("key", 80)),
                    new Row(Map.of("key", 80)),
                    new Row(Map.of("key", 80)),
                    new Row(Map.of("key", 80))));

            // When / Then
            assertThatThrownBy(() -> createTransaction())
                    .asInstanceOf(InstanceOfAssertFactories.type(InsufficientDataForPartitionSplittingException.class))
                    .extracting(
                            InsufficientDataForPartitionSplittingException::getMinLeafPartitions,
                            InsufficientDataForPartitionSplittingException::getMaxLeafPartitionsAfterSplits)
                    .containsExactly(4, 3);
        }
    }

    @Nested
    @DisplayName("Split partitions with more data first")
    class SplitMoreDataFirst {

        @Test
        void shouldSplitPartitionWithMoreDataWhenTwoCanBeSplit() {
            // Given
            tableProperties.setNumber(BULK_IMPORT_MIN_LEAF_PARTITION_COUNT, 3);
            setPartitionsBefore(new PartitionsBuilder(tableProperties)
                    .rootFirst("root")
                    .splitToNewChildren("root", "L", "R", 50)
                    .buildTree());
            setPartitionSketchData("L", List.of(
                    new Row(Map.of("key", 10)),
                    new Row(Map.of("key", 25)),
                    new Row(Map.of("key", 40))));
            setPartitionSketchData("R", List.of(
                    new Row(Map.of("key", 50)),
                    new Row(Map.of("key", 60)),
                    new Row(Map.of("key", 75)),
                    new Row(Map.of("key", 80)),
                    new Row(Map.of("key", 90))));

            // When
            ExtendPartitionTreeTransaction transaction = createTransaction();

            // Then
            assertThat(transaction)
                    .withRepresentation(transactionRepresentation())
                    .isEqualTo(transactionWithUpdatedAndNewPartitions(
                            new PartitionsBuilder(tableProperties).singlePartition("root")
                                    .splitToNewChildren("root", "L", "R", 50)
                                    .splitToNewChildren("R", "P1", "P2", 75)
                                    .buildTree(),
                            List.of("R"),
                            List.of("P1", "P2")));
        }

        @Test
        void shouldSplitOnePartitionDownTwoLevelsBeforeSplittingOtherOriginalLeaf() {
            // Given
            tableProperties.setNumber(BULK_IMPORT_MIN_LEAF_PARTITION_COUNT, 6);
            setPartitionsBefore(new PartitionsBuilder(tableProperties)
                    .rootFirst("root")
                    .splitToNewChildren("root", "L", "R", 50)
                    .buildTree());
            // L has 3 rows, R has 11 rows
            // R has 5 rows per child excluding the split point, so each child is bigger than L
            setPartitionSketchData("L", List.of(
                    new Row(Map.of("key", 10)),
                    new Row(Map.of("key", 25)),
                    new Row(Map.of("key", 40))));
            setPartitionSketchData("R", List.of(
                    new Row(Map.of("key", 51)),
                    new Row(Map.of("key", 55)),
                    new Row(Map.of("key", 60)),
                    new Row(Map.of("key", 65)),
                    new Row(Map.of("key", 70)),
                    new Row(Map.of("key", 75)),
                    new Row(Map.of("key", 80)),
                    new Row(Map.of("key", 85)),
                    new Row(Map.of("key", 90)),
                    new Row(Map.of("key", 95)),
                    new Row(Map.of("key", 99))));

            // When
            ExtendPartitionTreeTransaction transaction = createTransaction();

            // Then
            assertThat(transaction)
                    .withRepresentation(transactionRepresentation())
                    .isEqualTo(transactionWithUpdatedAndNewPartitions(
                            new PartitionsBuilder(tableProperties).singlePartition("root")
                                    .splitToNewChildren("root", "L", "R", 50)
                                    .splitToNewChildren("R", "P1", "P2", 75)
                                    .splitToNewChildren("P1", "P3", "P4", 60)
                                    .splitToNewChildren("P2", "P5", "P6", 90)
                                    .splitToNewChildren("L", "P7", "P8", 25)
                                    .buildTree(),
                            List.of("R", "L"),
                            List.of("P1", "P2", "P3", "P4", "P5", "P6", "P7", "P8")));
        }

        @Test
        void shouldSplitOnePartitionDownOneLevelThenSplitOtherOriginalLeafThenSplitAnotherLevelUnderFirstPartition() {
            // Given
            tableProperties.setNumber(BULK_IMPORT_MIN_LEAF_PARTITION_COUNT, 6);
            setPartitionsBefore(new PartitionsBuilder(tableProperties)
                    .rootFirst("root")
                    .splitToNewChildren("root", "L", "R", 50)
                    .buildTree());
            // L has 3 rows, R has 7 rows
            // R has 3 rows per child excluding the split point, so L takes priority as it was defined earlier
            setPartitionSketchData("L", List.of(
                    new Row(Map.of("key", 10)),
                    new Row(Map.of("key", 25)),
                    new Row(Map.of("key", 40))));
            setPartitionSketchData("R", List.of(
                    new Row(Map.of("key", 55)),
                    new Row(Map.of("key", 60)),
                    new Row(Map.of("key", 70)),
                    new Row(Map.of("key", 75)),
                    new Row(Map.of("key", 80)),
                    new Row(Map.of("key", 90)),
                    new Row(Map.of("key", 95))));

            // When
            ExtendPartitionTreeTransaction transaction = createTransaction();

            // Then
            assertThat(transaction)
                    .withRepresentation(transactionRepresentation())
                    .isEqualTo(transactionWithUpdatedAndNewPartitions(
                            new PartitionsBuilder(tableProperties).singlePartition("root")
                                    .splitToNewChildren("root", "L", "R", 50)
                                    .splitToNewChildren("R", "P1", "P2", 75)
                                    .splitToNewChildren("L", "P3", "P4", 25)
                                    .splitToNewChildren("P1", "P5", "P6", 60)
                                    .splitToNewChildren("P2", "P7", "P8", 90)
                                    .buildTree(),
                            List.of("R", "L"),
                            List.of("P1", "P2", "P3", "P4", "P5", "P6", "P7", "P8")));
        }
    }

    @Test
    void shouldExtendToLargePartitionTree() {
        // Given
        tableProperties.setNumber(BULK_IMPORT_MIN_LEAF_PARTITION_COUNT, 10_000);
        setPartitionsBefore(new PartitionsBuilder(tableProperties).singlePartition("root").buildTree());
        // Use a k value that makes the sketch big enough to hold enough unique split point values
        setPartitionSketchDataWithKValue("root", 2048,
                IntStream.rangeClosed(0, 500_000)
                        .map(i -> i * 100)
                        .mapToObj(i -> new Row(Map.of("key", i))));

        // When
        ExtendPartitionTreeTransaction transaction = createTransaction();

        // Then
        assertThat(partitionsAfter(transaction).getLeafPartitions().size())
                .isEqualTo(10_000);
    }

    private ExtendPartitionTreeTransaction createTransaction() {
        return ExtendPartitionTreeBasedOnSketches.forBulkImport(tableProperties, supplyNumberedIdsWithPrefix("P"))
                .createTransaction(partitionsBefore, partitionIdToSketches);
    }

    private void setPartitionsBefore(PartitionTree tree) {
        partitionsBefore = tree;
    }

    private void setPartitionSketchData(String partitionId, List<Row> rows) {
        setPartitionSketchDataWithKValue(partitionId, 1024, rows.stream());
    }

    private void setPartitionSketchDataWithKValue(String partitionId, int k, Stream<Row> rows) {
        Sketches sketches = Sketches.from(tableProperties.getSchema(), k);
        rows.forEach(sketches::update);
        partitionIdToSketches.put(partitionId, sketches);
    }

    private ExtendPartitionTreeTransaction transactionWithUpdatedAndNewPartitions(
            PartitionTree treeAfterTransaction, List<String> updatedPartitions, List<String> newPartitions) {
        return new ExtendPartitionTreeTransaction(
                updatedPartitions.stream().map(treeAfterTransaction::getPartition).toList(),
                newPartitions.stream().map(treeAfterTransaction::getPartition).toList());
    }

    private Representation transactionRepresentation() {
        return object -> {
            ExtendPartitionTreeTransaction transaction = (ExtendPartitionTreeTransaction) object;
            PartitionTree partitionsAfter = partitionsAfter(transaction);
            return PartitionsPrinter.printPartitions(tableProperties.getSchema(), partitionsAfter)
                    + "\nUpdated partition IDs:\n"
                    + printPartitionLocationNamesAndIds(transaction.getUpdatePartitions(), partitionsAfter)
                    + "\n\nNew partition IDs:\n"
                    + printPartitionLocationNamesAndIds(transaction.getNewPartitions(), partitionsAfter);
        };
    }

    private PartitionTree partitionsAfter(ExtendPartitionTreeTransaction transaction) {
        StateStorePartitions state = StateStorePartitions.from(partitionsBefore.getAllPartitions());
        transaction.validate(state, tableProperties);
        transaction.apply(state, Instant.parse("2026-01-12T14:46:00Z"));
        return new PartitionTree(state.all());
    }

    private static String printPartitionLocationNamesAndIds(List<Partition> partitions, PartitionTree tree) {
        return partitions.stream()
                .map(partition -> PartitionsPrinter.buildPartitionLocationName(partition, tree) + ": " + partition.getId())
                .collect(joining("\n"));
    }
}
