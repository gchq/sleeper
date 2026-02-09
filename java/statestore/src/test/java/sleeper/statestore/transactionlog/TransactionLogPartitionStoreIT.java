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
package sleeper.statestore.transactionlog;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import sleeper.core.partition.PartitionTree;
import sleeper.core.partition.PartitionsBuilder;
import sleeper.core.range.Range.RangeFactory;
import sleeper.core.range.Region;
import sleeper.core.schema.Field;
import sleeper.core.schema.Schema;
import sleeper.core.schema.type.ByteArrayType;
import sleeper.core.schema.type.IntType;
import sleeper.core.schema.type.LongType;
import sleeper.core.schema.type.StringType;
import sleeper.core.statestore.StateStoreException;
import sleeper.core.statestore.transactionlog.transaction.impl.ExtendPartitionTreeTransaction;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static sleeper.core.schema.SchemaTestHelper.createSchemaWithKey;
import static sleeper.core.statestore.testutils.StateStoreUpdatesWrapper.update;

public class TransactionLogPartitionStoreIT extends TransactionLogStateStoreOneTableTestBase {

    @Nested
    @DisplayName("Initialise partitions with all key types")
    class InitialiseWithKeyTypes {

        @Test
        public void shouldInitialiseRootPartitionWithIntKey() {
            // Given
            Schema schema = createSchemaWithKey("key", new IntType());

            // When
            initialiseWithSchema(schema);

            // Then
            assertThat(store.getAllPartitions()).isEqualTo(
                    new PartitionsBuilder(schema).singlePartition("root").buildList());
        }

        @Test
        public void shouldInitialiseRootPartitionWithLongKey() {
            // Given
            Schema schema = createSchemaWithKey("key", new LongType());

            // When
            initialiseWithSchema(schema);

            // Then
            assertThat(store.getAllPartitions()).isEqualTo(
                    new PartitionsBuilder(schema).singlePartition("root").buildList());
        }

        @Test
        public void shouldInitialiseRootPartitionWithStringKey() {
            // Given
            Schema schema = createSchemaWithKey("key", new StringType());

            // When
            initialiseWithSchema(schema);

            // Then
            assertThat(store.getAllPartitions()).isEqualTo(
                    new PartitionsBuilder(schema).singlePartition("root").buildList());
        }

        @Test
        public void shouldInitialiseRootPartitionWithByteArrayKey() {
            // Given
            Schema schema = createSchemaWithKey("key", new ByteArrayType());

            // When
            initialiseWithSchema(schema);

            // Then
            assertThat(store.getAllPartitions()).isEqualTo(
                    new PartitionsBuilder(schema).singlePartition("root").buildList());
        }

        @Test
        public void shouldStorePartitionsSplitOnLongKey() {
            // Given
            Schema schema = createSchemaWithKey("key", new LongType());
            PartitionsBuilder partitions = new PartitionsBuilder(schema)
                    .rootFirst("root")
                    .splitToNewChildren("root", "L", "R", 100L);

            // When
            initialiseWithPartitions(partitions);

            // Then
            assertThat(store.getAllPartitions()).containsExactlyInAnyOrderElementsOf(partitions.buildList());
        }

        @Test
        public void shouldStorePartitionsSplitOnStringKey() {
            // Given
            Schema schema = createSchemaWithKey("key", new StringType());
            PartitionsBuilder partitions = new PartitionsBuilder(schema)
                    .rootFirst("root")
                    .splitToNewChildren("root", "L", "R", "A");

            // When
            initialiseWithPartitions(partitions);

            // Then
            assertThat(store.getAllPartitions()).containsExactlyInAnyOrderElementsOf(partitions.buildList());
        }

        @Test
        public void shouldStorePartitionsSplitOnByteArrayKey() {
            // Given
            Schema schema = createSchemaWithKey("key", new ByteArrayType());
            PartitionsBuilder partitions = new PartitionsBuilder(schema)
                    .rootFirst("root")
                    .splitToNewChildren("root", "L", "R", new byte[]{1, 2, 3, 4});

            // When
            initialiseWithPartitions(partitions);

            // Then
            assertThat(store.getAllPartitions()).containsExactlyInAnyOrderElementsOf(partitions.buildList());
        }

        @Test
        public void shouldStorePartitionsSplitOnMultidimensionalByteArrayKey() {
            // Given
            Schema schema = Schema.builder()
                    .rowKeyFields(
                            new Field("key1", new ByteArrayType()),
                            new Field("key2", new ByteArrayType()))
                    .build();
            PartitionsBuilder partitions = new PartitionsBuilder(schema)
                    .rootFirst("root")
                    .splitToNewChildrenOnDimension("root", "L", "R", 0, new byte[]{1, 2, 3, 4})
                    .splitToNewChildrenOnDimension("L", "LL", "LR", 1, new byte[]{99, 5})
                    .splitToNewChildrenOnDimension("R", "RL", "RR", 1, new byte[]{101, 0});

            // When
            initialiseWithPartitions(partitions);

            // Then
            assertThat(store.getAllPartitions()).containsExactlyInAnyOrderElementsOf(partitions.buildList());
        }

        @Test
        public void shouldStoreSeveralLayersOfPartitions() {
            // Given
            Schema schema = createSchemaWithKey("key", new LongType());
            PartitionsBuilder partitions = new PartitionsBuilder(schema)
                    .rootFirst("root")
                    .splitToNewChildren("root", "L", "R", 100L)
                    .splitToNewChildren("L", "LL", "LR", 1L)
                    .splitToNewChildren("R", "RL", "RR", 200L);

            // When
            initialiseWithPartitions(partitions);

            // Then
            assertThat(store.getAllPartitions()).containsExactlyInAnyOrderElementsOf(partitions.buildList());
        }
    }

    @Nested
    @DisplayName("Reinitialise partitions")
    class Reinitialise {

        @Test
        void shouldReinitialisePartitionsWhenNoFilesArePresent() {
            // Given
            Schema schema = createSchemaWithKey("key", new LongType());
            PartitionsBuilder partitionsBefore = new PartitionsBuilder(schema)
                    .rootFirst("root")
                    .splitToNewChildren("root", "before1", "before2", 0L);
            PartitionsBuilder partitionsAfter = new PartitionsBuilder(schema)
                    .rootFirst("root")
                    .splitToNewChildren("root", "after1", "after2", 10L);
            initialiseWithPartitions(partitionsBefore);

            // When
            update(store).initialise(partitionsAfter.buildList());

            // Then
            assertThat(store.getAllPartitions())
                    .containsExactlyInAnyOrderElementsOf(partitionsAfter.buildList());
        }

        @Test
        void shouldNotReinitialisePartitionsWhenAFileIsPresent() {
            // Given
            Schema schema = createSchemaWithKey("key", new LongType());
            PartitionsBuilder partitionsBefore = new PartitionsBuilder(schema)
                    .rootFirst("root")
                    .splitToNewChildren("root", "before1", "before2", 0L);
            PartitionsBuilder partitionsAfter = new PartitionsBuilder(schema)
                    .rootFirst("root")
                    .splitToNewChildren("root", "after1", "after2", 10L);
            initialiseWithPartitions(partitionsBefore);

            update(store).addFile(factory.partitionFile("before2", 100L));

            // When / Then
            assertThatThrownBy(() -> update(store).initialise(partitionsAfter.buildList()))
                    .isInstanceOf(StateStoreException.class);
            assertThat(store.getAllPartitions())
                    .containsExactlyInAnyOrderElementsOf(partitionsBefore.buildList());
        }
    }

    @Nested
    @DisplayName("Split partitions")
    class SplitPartitions {

        @Test
        public void shouldSplitAPartition() {
            // Given
            Schema schema = createSchemaWithKey("key", new LongType());
            initialiseWithSchema(schema);
            PartitionTree tree = new PartitionsBuilder(schema)
                    .rootFirst("root")
                    .splitToNewChildren("root", "leftChild", "rightChild", 0L)
                    .buildTree();

            // When
            update(store).atomicallyUpdatePartitionAndCreateNewOnes(
                    tree.getPartition("root"),
                    tree.getPartition("leftChild"),
                    tree.getPartition("rightChild"));

            // Then
            assertThat(store.getAllPartitions())
                    .containsExactlyInAnyOrderElementsOf(tree.getAllPartitions());
        }

        @Test
        public void shouldSplitAChildToTwoNestedAndGetLeafPartitions() {
            // Given
            Schema schema = createSchemaWithKey("key", new LongType());
            initialiseWithSchema(schema);

            // When
            splitPartition("root", "L", "R", 1L);
            splitPartition("R", "RL", "RR", 9L);

            // Then
            PartitionTree expectedTree = new PartitionsBuilder(schema)
                    .rootFirst("root")
                    .splitToNewChildren("root", "L", "R", 1L)
                    .splitToNewChildren("R", "RL", "RR", 9L)
                    .buildTree();
            assertThat(store.getLeafPartitions())
                    .containsExactlyInAnyOrder(
                            expectedTree.getPartition("L"),
                            expectedTree.getPartition("RL"),
                            expectedTree.getPartition("RR"));
        }

        @Test
        public void shouldFailSplittingAPartitionWhichHasAlreadyBeenSplit() {
            // Given
            Schema schema = createSchemaWithKey("key", new LongType());
            initialiseWithSchema(schema);
            PartitionTree tree = new PartitionsBuilder(schema)
                    .rootFirst("root")
                    .splitToNewChildren("root", "leftChild", "rightChild", 0L)
                    .buildTree();
            update(store).atomicallyUpdatePartitionAndCreateNewOnes(
                    tree.getPartition("root"),
                    tree.getPartition("leftChild"),
                    tree.getPartition("rightChild"));

            // When / Then
            assertThatThrownBy(() -> update(store).atomicallyUpdatePartitionAndCreateNewOnes(
                    tree.getPartition("root"),
                    tree.getPartition("leftChild"),
                    tree.getPartition("rightChild")))
                    .isInstanceOf(StateStoreException.class);
        }

        @Test
        public void shouldFailSplittingAPartitionWithWrongChildren() {
            // Given
            Schema schema = createSchemaWithKey("key", new LongType());
            initialiseWithSchema(schema);
            PartitionTree tree = new PartitionsBuilder(schema)
                    .rootFirst("root")
                    .splitToNewChildren("root", "L", "R", 0L)
                    .splitToNewChildren("L", "LL", "LR", -100L)
                    .buildTree();

            // When / Then
            assertThatThrownBy(() -> update(store).atomicallyUpdatePartitionAndCreateNewOnes(
                    tree.getPartition("root"),
                    tree.getPartition("LL"),
                    tree.getPartition("LR")))
                    .isInstanceOf(StateStoreException.class);
        }

        @Test
        public void shouldFailSplittingAPartitionWithChildrenOfWrongParent() {
            // Given
            Schema schema = createSchemaWithKey("key", new LongType());
            initialiseWithSchema(schema);
            PartitionTree parentTree = new PartitionsBuilder(schema)
                    .rootFirst("root")
                    .splitToNewChildren("root", "child1", "child2", 0L)
                    .buildTree();
            PartitionTree childrenTree = new PartitionsBuilder(schema)
                    .rootFirst("root")
                    .splitToNewChildren("root", "L", "R", 100L)
                    .splitToNewChildren("L", "child1", "child2", 0L)
                    .buildTree();

            // When / Then
            assertThatThrownBy(() -> update(store).atomicallyUpdatePartitionAndCreateNewOnes(
                    parentTree.getPartition("root"),
                    childrenTree.getPartition("child1"),
                    childrenTree.getPartition("child2")))
                    .isInstanceOf(StateStoreException.class);
        }

        @Test
        public void shouldFailSplittingAPartitionWhenNewPartitionIsNotALeaf() {
            // Given
            Schema schema = createSchemaWithKey("key", new LongType());
            initialiseWithSchema(schema);
            PartitionTree tree = new PartitionsBuilder(schema)
                    .rootFirst("root")
                    .splitToNewChildren("root", "L", "R", 0L)
                    .splitToNewChildren("L", "LL", "LR", -100L)
                    .buildTree();

            // When / Then
            assertThatThrownBy(() -> update(store).atomicallyUpdatePartitionAndCreateNewOnes(
                    tree.getPartition("root"),
                    tree.getPartition("L"), // Not a leaf
                    tree.getPartition("R")))
                    .isInstanceOf(StateStoreException.class);
        }
    }

    @Nested
    @DisplayName("Extend partition tree")
    class ExtendTree {

        @BeforeEach
        void setUp() {
            initialiseWithSchema(createSchemaWithKey("key", new IntType()));
        }

        @Test
        void shouldAddOneSplitToRoot() {
            // Given
            PartitionTree tree = new PartitionsBuilder(tableProperties)
                    .rootFirst("root")
                    .splitToNewChildren("root", "L", "R", 50)
                    .buildTree();
            ExtendPartitionTreeTransaction transaction = new ExtendPartitionTreeTransaction(
                    List.of(tree.getRootPartition()),
                    List.of(tree.getPartition("L"), tree.getPartition("R")));

            // When
            transaction.synchronousCommit(store);

            // Then
            assertThat(new PartitionTree(store.getAllPartitions())).isEqualTo(tree);
        }

        @Test
        void shouldAddMultipleLevelsToNonRoots() {
            // Given
            initialiseWithPartitions(new PartitionsBuilder(tableProperties)
                    .rootFirst("root")
                    .splitToNewChildren("root", "L", "R", 50));
            PartitionTree tree = new PartitionsBuilder(tableProperties)
                    .rootFirst("root")
                    .splitToNewChildren("root", "L", "R", 50)
                    .splitToNewChildren("L", "LL", "LR", 25)
                    .splitToNewChildren("LR", "LRL", "LRR", 30)
                    .splitToNewChildren("R", "RL", "RR", 75)
                    .buildTree();
            ExtendPartitionTreeTransaction transaction = new ExtendPartitionTreeTransaction(
                    List.of(tree.getPartition("L"), tree.getPartition("R")),
                    List.of(tree.getPartition("LL"), tree.getPartition("LR"),
                            tree.getPartition("LRL"), tree.getPartition("LRR"),
                            tree.getPartition("RL"), tree.getPartition("RR")));

            // When
            transaction.synchronousCommit(store);

            // Then
            assertThat(new PartitionTree(store.getAllPartitions())).isEqualTo(tree);
        }

        @Test
        void shouldFailToUpdatePartitionWhichDoesNotExist() {
            // Given
            initialiseWithPartitions(new PartitionsBuilder(tableProperties).singlePartition("root"));
            PartitionTree tree = new PartitionsBuilder(tableProperties)
                    .rootFirst("new")
                    .splitToNewChildren("new", "L", "R", 75)
                    .buildTree();
            ExtendPartitionTreeTransaction transaction = new ExtendPartitionTreeTransaction(
                    List.of(tree.getRootPartition()),
                    List.of(tree.getPartition("L"), tree.getPartition("R")));

            // When / Then
            assertThatThrownBy(() -> transaction.synchronousCommit(store))
                    .isInstanceOf(StateStoreException.class)
                    .hasMessage("Attempted to update a partition which does not exist: new");
        }

        @Test
        void shouldFailToUpdatePartitionWhichHasAlreadyBeenSplit() {
            // Given
            initialiseWithPartitions(new PartitionsBuilder(tableProperties)
                    .rootFirst("root")
                    .splitToNewChildren("root", "L1", "R1", 50));
            PartitionTree tree = new PartitionsBuilder(tableProperties)
                    .rootFirst("root")
                    .splitToNewChildren("root", "L2", "R2", 75)
                    .buildTree();
            ExtendPartitionTreeTransaction transaction = new ExtendPartitionTreeTransaction(
                    List.of(tree.getRootPartition()),
                    List.of(tree.getPartition("L2"), tree.getPartition("R2")));

            // When / Then
            assertThatThrownBy(() -> transaction.synchronousCommit(store))
                    .isInstanceOf(StateStoreException.class)
                    .hasMessage("Attempted to update a partition which has already been split: root");
        }

        @Test
        void shouldFailToAddPartitionWhichAlreadyExists() {
            // Given
            initialiseWithPartitions(new PartitionsBuilder(tableProperties)
                    .rootFirst("root")
                    .splitToNewChildren("root", "A", "B", 50));
            PartitionTree tree = new PartitionsBuilder(tableProperties)
                    .rootFirst("root")
                    .splitToNewChildren("root", "A", "D", 50)
                    .splitToNewChildren("A", "B", "C", 25)
                    .buildTree();
            ExtendPartitionTreeTransaction transaction = new ExtendPartitionTreeTransaction(
                    List.of(tree.getPartition("A")),
                    List.of(tree.getPartition("B"), tree.getPartition("C")));

            // When / Then
            assertThatThrownBy(() -> transaction.synchronousCommit(store))
                    .isInstanceOf(StateStoreException.class)
                    .hasMessage("Attempted to add a partition which already exists: B");
        }

        @Test
        void shouldFailToUpdatePartitionWithoutSplitting() {
            // Given
            initialiseWithPartitions(new PartitionsBuilder(tableProperties).singlePartition("root"));
            PartitionTree tree = new PartitionsBuilder(tableProperties).singlePartition("root").buildTree();
            ExtendPartitionTreeTransaction transaction = new ExtendPartitionTreeTransaction(
                    List.of(tree.getPartition("root")), List.of());

            // When / Then
            assertThatThrownBy(() -> transaction.synchronousCommit(store))
                    .isInstanceOf(StateStoreException.class)
                    .hasMessage("Attempted to update a partition without splitting it: root");
        }

        @Test
        void shouldFailToAddSecondRoot() {
            // Given
            PartitionTree tree = new PartitionsBuilder(tableProperties)
                    .singlePartition("new")
                    .buildTree();
            ExtendPartitionTreeTransaction transaction = new ExtendPartitionTreeTransaction(
                    List.of(),
                    List.of(tree.getRootPartition()));

            // When / Then
            assertThatThrownBy(() -> transaction.synchronousCommit(store))
                    .isInstanceOf(StateStoreException.class)
                    .hasMessage("Update results in invalid partition tree")
                    .cause().isInstanceOf(IllegalArgumentException.class)
                    .hasMessage("There should be exactly one root partition, found 2");
        }

        @Test
        void shouldFailSplittingAPartitionWhenChildPartitionsDoNotMeetAtSplitPoint() {
            // Given
            PartitionTree tree = new PartitionsBuilder(tableProperties)
                    .rootFirst("root")
                    .splitToNewChildren("root", "L", "R", 0)
                    .buildTree();
            ExtendPartitionTreeTransaction transaction = new ExtendPartitionTreeTransaction(
                    List.of(tree.getPartition("root")),
                    List.of(tree.getPartition("L"),
                            tree.getPartition("R")
                                    .toBuilder().region(new Region(rangeFactory().createRange("key", 1, null))).build()));

            // When / Then
            assertThatThrownBy(() -> transaction.synchronousCommit(store))
                    .isInstanceOf(StateStoreException.class)
                    .hasMessage("Update results in invalid partition tree")
                    .cause().isInstanceOf(IllegalArgumentException.class)
                    .hasMessage("Child partitions do not meet at a split point on dimension 0 set in parent partition root");
        }

        private RangeFactory rangeFactory() {
            return new RangeFactory(tableProperties.getSchema());
        }
    }

    @Nested
    @DisplayName("Clear partitions")
    class ClearPartitions {
        @Test
        void shouldDeleteSinglePartitionOnClear() {
            // Given
            Schema schema = createSchemaWithKey("key", new IntType());
            initialiseWithSchema(schema);

            // When
            update(store).clearSleeperTable();

            // Then
            assertThat(store.getAllPartitions()).isEmpty();
            assertThat(store.getLeafPartitions()).isEmpty();
        }

        @Test
        void shouldDeletePartitionTreeOnClear() {
            // Given
            Schema schema = createSchemaWithKey("key", new IntType());
            initialiseWithPartitions(new PartitionsBuilder(schema)
                    .rootFirst("root")
                    .splitToNewChildren("root", "L", "R", 123));

            // When
            update(store).clearSleeperTable();

            // Then
            assertThat(store.getAllPartitions()).isEmpty();
            assertThat(store.getLeafPartitions()).isEmpty();
        }
    }
}
