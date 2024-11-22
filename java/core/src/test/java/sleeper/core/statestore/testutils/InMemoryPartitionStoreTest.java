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

package sleeper.core.statestore.testutils;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import sleeper.core.partition.PartitionTree;
import sleeper.core.partition.PartitionsBuilder;
import sleeper.core.schema.Field;
import sleeper.core.schema.Schema;
import sleeper.core.schema.type.ByteArrayType;
import sleeper.core.schema.type.IntType;
import sleeper.core.schema.type.LongType;
import sleeper.core.schema.type.StringType;
import sleeper.core.statestore.StateStoreException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static sleeper.core.schema.SchemaTestHelper.schemaWithKey;

public class InMemoryPartitionStoreTest extends InMemoryStateStoreTestBase {

    @Nested
    @DisplayName("Initialise partitions with all key types")
    class InitialiseWithKeyTypes {

        @Test
        public void shouldInitialiseRootPartitionWithIntKey() {
            // Given
            Schema schema = schemaWithKey("key", new IntType());

            // When
            initialiseWithSchema(schema);

            // Then
            assertThat(store.getAllPartitions()).isEqualTo(
                    new PartitionsBuilder(schema).singlePartition("root").buildList());
        }

        @Test
        public void shouldInitialiseRootPartitionWithLongKey() {
            // Given
            Schema schema = schemaWithKey("key", new LongType());

            // When
            initialiseWithSchema(schema);

            // Then
            assertThat(store.getAllPartitions()).isEqualTo(
                    new PartitionsBuilder(schema).singlePartition("root").buildList());
        }

        @Test
        public void shouldInitialiseRootPartitionWithStringKey() {
            // Given
            Schema schema = schemaWithKey("key", new StringType());

            // When
            initialiseWithSchema(schema);

            // Then
            assertThat(store.getAllPartitions()).isEqualTo(
                    new PartitionsBuilder(schema).singlePartition("root").buildList());
        }

        @Test
        public void shouldInitialiseRootPartitionWithByteArrayKey() {
            // Given
            Schema schema = schemaWithKey("key", new ByteArrayType());

            // When
            initialiseWithSchema(schema);

            // Then
            assertThat(store.getAllPartitions()).isEqualTo(
                    new PartitionsBuilder(schema).singlePartition("root").buildList());
        }

        @Test
        public void shouldStorePartitionsSplitOnLongKey() {
            // Given
            Schema schema = schemaWithKey("key", new LongType());
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
            Schema schema = schemaWithKey("key", new StringType());
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
            Schema schema = schemaWithKey("key", new ByteArrayType());
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
            Schema schema = schemaWithKey("key", new LongType());
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
            Schema schema = schemaWithKey("key", new LongType());
            PartitionsBuilder partitionsBefore = new PartitionsBuilder(schema)
                    .rootFirst("root")
                    .splitToNewChildren("root", "before1", "before2", 0L);
            PartitionsBuilder partitionsAfter = new PartitionsBuilder(schema)
                    .rootFirst("root")
                    .splitToNewChildren("root", "after1", "after2", 10L);
            initialiseWithPartitions(partitionsBefore);

            // When
            store.initialise(partitionsAfter.buildList());

            // Then
            assertThat(store.getAllPartitions())
                    .containsExactlyInAnyOrderElementsOf(partitionsAfter.buildList());
        }

        @Test
        void shouldNotReinitialisePartitionsWhenAFileIsPresent() {
            // Given
            Schema schema = schemaWithKey("key", new LongType());
            PartitionsBuilder partitionsBefore = new PartitionsBuilder(schema)
                    .rootFirst("root")
                    .splitToNewChildren("root", "before1", "before2", 0L);
            PartitionsBuilder partitionsAfter = new PartitionsBuilder(schema)
                    .rootFirst("root")
                    .splitToNewChildren("root", "after1", "after2", 10L);
            initialiseWithPartitions(partitionsBefore);

            store.addFile(factory.partitionFile("before2", 100L));

            // When / Then
            assertThatThrownBy(() -> store.initialise(partitionsAfter.buildList()))
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
            Schema schema = schemaWithKey("key", new LongType());
            initialiseWithSchema(schema);
            PartitionTree tree = new PartitionsBuilder(schema)
                    .rootFirst("root")
                    .splitToNewChildren("root", "leftChild", "rightChild", 0L)
                    .buildTree();

            // When
            store.atomicallyUpdatePartitionAndCreateNewOnes(
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
            Schema schema = schemaWithKey("key", new LongType());
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
            Schema schema = schemaWithKey("key", new LongType());
            initialiseWithSchema(schema);
            PartitionTree tree = new PartitionsBuilder(schema)
                    .rootFirst("root")
                    .splitToNewChildren("root", "leftChild", "rightChild", 0L)
                    .buildTree();
            store.atomicallyUpdatePartitionAndCreateNewOnes(
                    tree.getPartition("root"),
                    tree.getPartition("leftChild"),
                    tree.getPartition("rightChild"));

            // When / Then
            assertThatThrownBy(() -> store.atomicallyUpdatePartitionAndCreateNewOnes(
                    tree.getPartition("root"),
                    tree.getPartition("leftChild"),
                    tree.getPartition("rightChild")))
                    .isInstanceOf(StateStoreException.class);
        }

        @Test
        public void shouldFailSplittingAPartitionWithWrongChildren() {
            // Given
            Schema schema = schemaWithKey("key", new LongType());
            initialiseWithSchema(schema);
            PartitionTree tree = new PartitionsBuilder(schema)
                    .rootFirst("root")
                    .splitToNewChildren("root", "L", "R", 0L)
                    .splitToNewChildren("L", "LL", "LR", -100L)
                    .buildTree();

            // When / Then
            assertThatThrownBy(() -> store.atomicallyUpdatePartitionAndCreateNewOnes(
                    tree.getPartition("root"),
                    tree.getPartition("LL"),
                    tree.getPartition("LR")))
                    .isInstanceOf(StateStoreException.class);
        }

        @Test
        public void shouldFailSplittingAPartitionWithChildrenOfWrongParent() {
            // Given
            Schema schema = schemaWithKey("key", new LongType());
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
            assertThatThrownBy(() -> store.atomicallyUpdatePartitionAndCreateNewOnes(
                    parentTree.getPartition("root"),
                    childrenTree.getPartition("child1"),
                    childrenTree.getPartition("child2")))
                    .isInstanceOf(StateStoreException.class);
        }

        @Test
        public void shouldFailSplittingAPartitionWhenNewPartitionIsNotALeaf() {
            // Given
            Schema schema = schemaWithKey("key", new LongType());
            initialiseWithSchema(schema);
            PartitionTree tree = new PartitionsBuilder(schema)
                    .rootFirst("root")
                    .splitToNewChildren("root", "L", "R", 0L)
                    .splitToNewChildren("L", "LL", "LR", -100L)
                    .buildTree();

            // When / Then
            assertThatThrownBy(() -> store.atomicallyUpdatePartitionAndCreateNewOnes(
                    tree.getPartition("root"),
                    tree.getPartition("L"), // Not a leaf
                    tree.getPartition("R")))
                    .isInstanceOf(StateStoreException.class);
        }
    }
}
