/*
 * Copyright 2022-2023 Crown Copyright
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

package sleeper.statestore.inmemory;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import sleeper.core.partition.Partition;
import sleeper.core.partition.PartitionTree;
import sleeper.core.partition.PartitionsBuilder;
import sleeper.core.schema.Field;
import sleeper.core.schema.Schema;
import sleeper.core.schema.type.StringType;
import sleeper.statestore.PartitionStore;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class InMemoryPartitionStoreTest {

    @DisplayName("Initialise store with set partitions")
    @Nested
    class Initialise {

        @Test
        public void shouldInitialiseStoreWithSinglePartition() throws Exception {
            // Given
            Schema schema = Schema.builder().rowKeyFields(new Field("key", new StringType())).build();

            // When
            PartitionStore store = InMemoryPartitionStore.withSinglePartition(schema);
            PartitionTree tree = new PartitionTree(schema, store.getAllPartitions());
            Partition root = tree.getRootPartition();

            // Then
            assertThat(store.getAllPartitions()).containsExactly(root);
            assertThat(store.getLeafPartitions()).containsExactly(root);
            assertThat(root.getChildPartitionIds()).isEmpty();
        }

        @Test
        public void shouldInitialiseStoreWithPartitionTree() throws Exception {
            // Given
            Schema schema = Schema.builder().rowKeyFields(new Field("key", new StringType())).build();
            List<Partition> partitions = new PartitionsBuilder(schema)
                    .leavesWithSplits(Arrays.asList("A", "B"), Collections.singletonList("aaa"))
                    .parentJoining("C", "A", "B")
                    .buildList();
            PartitionTree tree = new PartitionTree(schema, partitions);

            // When
            PartitionStore store = new InMemoryPartitionStore(partitions);

            // Then
            assertThat(store.getAllPartitions()).containsExactlyInAnyOrder(
                    tree.getPartition("A"), tree.getPartition("B"), tree.getPartition("C"));
            assertThat(store.getLeafPartitions()).containsExactlyInAnyOrder(
                    tree.getPartition("A"), tree.getPartition("B"));
        }

        @Test
        void shouldReinitialiseStoreWithPartitionTree() throws Exception {
            // Given
            Schema schema = Schema.builder().rowKeyFields(new Field("key", new StringType())).build();
            List<Partition> partitions = new PartitionsBuilder(schema)
                    .leavesWithSplits(Arrays.asList("A", "B"), Collections.singletonList("aaa"))
                    .parentJoining("C", "A", "B")
                    .buildList();
            PartitionTree tree = new PartitionTree(schema, partitions);

            // When
            PartitionStore store = InMemoryPartitionStore.withSinglePartition(schema);
            store.initialise(partitions);

            // Then
            assertThat(store.getAllPartitions()).containsExactlyInAnyOrder(
                    tree.getPartition("A"), tree.getPartition("B"), tree.getPartition("C"));
            assertThat(store.getLeafPartitions()).containsExactlyInAnyOrder(
                    tree.getPartition("A"), tree.getPartition("B"));
        }

        @Test
        void shouldRefuseInitialiseWithNoParameters() {
            PartitionStore store = new InMemoryPartitionStore(List.of());
            assertThatThrownBy(store::initialise)
                    .isInstanceOf(UnsupportedOperationException.class);
        }
    }

    @DisplayName("Update store with a partition split")
    @Nested
    class Update {

        @Test
        void shouldSplitRootToTwoChildren() throws Exception {
            // Given
            Schema schema = Schema.builder().rowKeyFields(new Field("key", new StringType())).build();
            PartitionTree treeBefore = new PartitionsBuilder(schema).singlePartition("A").buildTree();
            PartitionTree treeAfter = new PartitionsBuilder(schema)
                    .rootFirst("A")
                    .splitToNewChildren("A", "B", "C", "aaa")
                    .buildTree();
            PartitionStore store = new InMemoryPartitionStore(treeBefore.getAllPartitions());

            // When
            store.atomicallyUpdatePartitionAndCreateNewOnes(
                    treeAfter.getPartition("A"),
                    treeAfter.getPartition("B"),
                    treeAfter.getPartition("C"));

            // Then
            assertThat(store.getAllPartitions()).containsExactlyInAnyOrder(
                    treeAfter.getPartition("A"), treeAfter.getPartition("B"), treeAfter.getPartition("C"));
            assertThat(store.getLeafPartitions()).containsExactlyInAnyOrder(
                    treeAfter.getPartition("B"), treeAfter.getPartition("C"));
        }

        @Test
        void shouldSplitChildToTwoNested() throws Exception {
            // Given
            Schema schema = Schema.builder().rowKeyFields(new Field("key", new StringType())).build();
            PartitionTree treeBefore = new PartitionsBuilder(schema)
                    .rootFirst("A")
                    .splitToNewChildren("A", "B", "C", "aaa")
                    .buildTree();
            PartitionTree treeAfter = new PartitionsBuilder(schema)
                    .rootFirst("A")
                    .splitToNewChildren("A", "B", "C", "aaa")
                    .splitToNewChildren("C", "D", "E", "bbb")
                    .buildTree();
            PartitionStore store = new InMemoryPartitionStore(treeBefore.getAllPartitions());

            // When
            store.atomicallyUpdatePartitionAndCreateNewOnes(
                    treeAfter.getPartition("C"),
                    treeAfter.getPartition("D"),
                    treeAfter.getPartition("E"));

            // Then
            assertThat(store.getAllPartitions()).containsExactlyInAnyOrder(
                    treeAfter.getPartition("A"),
                    treeAfter.getPartition("B"),
                    treeAfter.getPartition("C"),
                    treeAfter.getPartition("D"),
                    treeAfter.getPartition("E"));
            assertThat(store.getLeafPartitions()).containsExactlyInAnyOrder(
                    treeAfter.getPartition("B"),
                    treeAfter.getPartition("D"),
                    treeAfter.getPartition("E"));
        }
    }
}
