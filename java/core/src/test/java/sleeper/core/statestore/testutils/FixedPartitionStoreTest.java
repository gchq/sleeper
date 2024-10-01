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

import org.junit.jupiter.api.Test;

import sleeper.core.partition.Partition;
import sleeper.core.partition.PartitionTree;
import sleeper.core.partition.PartitionsBuilder;
import sleeper.core.schema.Field;
import sleeper.core.schema.Schema;
import sleeper.core.schema.type.StringType;
import sleeper.core.statestore.PartitionStore;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class FixedPartitionStoreTest {

    @Test
    public void shouldInitialiseStoreWithSinglePartition() throws Exception {
        // Given
        Schema schema = Schema.builder().rowKeyFields(new Field("key", new StringType())).build();

        // When
        PartitionStore store = new FixedPartitionStore(schema);
        PartitionTree tree = new PartitionTree(store.getAllPartitions());
        Partition root = tree.getRootPartition();

        // Then
        assertThat(store.getAllPartitions()).containsExactly(root);
        assertThat(store.getLeafPartitions()).containsExactly(root);
        assertThat(root.getChildPartitionIds()).isEmpty();
        assertThatCode(store::initialise).doesNotThrowAnyException();
    }

    @Test
    public void shouldInitialiseStoreWithPartitionTree() throws Exception {
        // Given
        Schema schema = Schema.builder().rowKeyFields(new Field("key", new StringType())).build();
        List<Partition> partitions = new PartitionsBuilder(schema)
                .rootFirst("A")
                .splitToNewChildren("A", "B", "C", "aaa")
                .buildList();
        PartitionTree tree = new PartitionTree(partitions);

        // When
        PartitionStore store = new FixedPartitionStore(partitions);

        // Then
        assertThat(store.getAllPartitions()).containsExactlyInAnyOrder(
                tree.getPartition("A"), tree.getPartition("B"), tree.getPartition("C"));
        assertThat(store.getLeafPartitions()).containsExactlyInAnyOrder(
                tree.getPartition("B"), tree.getPartition("C"));
        assertThatCode(() -> store.initialise(partitions)).doesNotThrowAnyException();
    }

    @Test
    public void shouldRefusePartitionSplit() {
        // Given
        Schema schema = Schema.builder().rowKeyFields(new Field("key", new StringType())).build();
        List<Partition> partitionsInit = new PartitionsBuilder(schema)
                .singlePartition("root")
                .buildList();
        PartitionStore store = new FixedPartitionStore(partitionsInit);

        // When / Then
        PartitionTree updateTree = new PartitionsBuilder(schema)
                .rootFirst("root")
                .splitToNewChildren("root", "left", "right", "aaa")
                .buildTree();
        Partition root = updateTree.getPartition("root");
        Partition left = updateTree.getPartition("left");
        Partition right = updateTree.getPartition("right");
        assertThatThrownBy(() -> store.atomicallyUpdatePartitionAndCreateNewOnes(root, left, right))
                .isInstanceOf(UnsupportedOperationException.class);
    }

    @Test
    public void shouldRefuseInitialiseThatChangesPartitions() {
        // Given
        Schema schema = Schema.builder().rowKeyFields(new Field("key", new StringType())).build();
        List<Partition> partitionsInit = new PartitionsBuilder(schema)
                .singlePartition("root")
                .buildList();
        PartitionStore store = new FixedPartitionStore(partitionsInit);

        // When / Then
        List<Partition> partitionsUpdate = new PartitionsBuilder(schema)
                .rootFirst("root")
                .splitToNewChildren("root", "left", "right", "aaa")
                .buildList();
        assertThatThrownBy(() -> store.initialise(partitionsUpdate))
                .isInstanceOf(UnsupportedOperationException.class);
    }

    @Test
    public void shouldRefuseEmptyInitialiseThatChangesPartitions() {
        // Given
        Schema schema = Schema.builder().rowKeyFields(new Field("key", new StringType())).build();
        List<Partition> partitionsInit = new PartitionsBuilder(schema)
                .rootFirst("root")
                .splitToNewChildren("root", "left", "right", "aaa")
                .buildList();
        PartitionStore store = new FixedPartitionStore(partitionsInit);

        // When / Then
        assertThatThrownBy(store::initialise)
                .isInstanceOf(UnsupportedOperationException.class);
    }
}
