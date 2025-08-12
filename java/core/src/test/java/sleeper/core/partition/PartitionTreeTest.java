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
package sleeper.core.partition;

import org.junit.jupiter.api.Test;

import sleeper.core.key.Key;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

public class PartitionTreeTest extends PartitionTreeTestBase {

    @Test
    public void shouldReturnCorrectChildren() {
        // Given
        PartitionTree partitionTree = new PartitionsBuilder(schema)
                .rootFirst(ROOT)
                .splitToNewChildren(ROOT, L1_LEFT, L1_RIGHT, 0L)
                .splitToNewChildren(L1_LEFT, L2_LEFT_OF_L1L, L2_RIGHT_OF_L1L, -1000000L)
                .splitToNewChildren(L1_RIGHT, L2_LEFT_OF_L1R, L2_RIGHT_OF_L1R, 123456789L)
                .buildTree();

        // When
        List<String> children1 = partitionTree.getChildIds(ROOT);
        List<String> children2 = partitionTree.getChildIds(L1_RIGHT);

        // Then
        assertThat(children1).containsExactly(L1_LEFT, L1_RIGHT);
        assertThat(children2).containsExactly(L2_LEFT_OF_L1R, L2_RIGHT_OF_L1R);
    }

    @Test
    public void shouldReturnCorrectAncestors() {
        // Given
        PartitionTree partitionTree = new PartitionsBuilder(schema)
                .rootFirst(ROOT)
                .splitToNewChildren(ROOT, L1_LEFT, L1_RIGHT, 0L)
                .splitToNewChildren(L1_LEFT, L2_LEFT_OF_L1L, L2_RIGHT_OF_L1L, -1000000L)
                .splitToNewChildren(L1_RIGHT, L2_LEFT_OF_L1R, L2_RIGHT_OF_L1R, 123456789L)
                .buildTree();

        // When
        List<Partition> ancestorsOfRoot = partitionTree.getAllAncestors(ROOT);
        List<String> ancestorsOfRootIds = partitionTree.getAllAncestorIds(ROOT);
        List<Partition> ancestorsOfL2LOfL1L = partitionTree.getAllAncestors(L2_LEFT_OF_L1L);
        List<String> ancestorsOfL2LOfL1LIds = partitionTree.getAllAncestorIds(L2_LEFT_OF_L1L);

        // Then
        assertThat(ancestorsOfRootIds).isEmpty();
        assertThat(ancestorsOfRoot).isEmpty();
        assertThat(ancestorsOfL2LOfL1LIds).containsExactly(l1Left.getId(), rootWithChildren.getId());
        assertThat(ancestorsOfL2LOfL1L).containsExactly(l1Left, rootWithChildren);
    }

    @Test
    public void shouldReturnRootIfOnlyPartitionAndAskedForLeafPartitionContainingKey() {
        // Given
        PartitionTree partitionTree = new PartitionsBuilder(schema)
                .rootFirst(ROOT)
                .buildTree();

        // When
        Partition partition = partitionTree.getLeafPartition(schema, Key.create(10L));

        // Then 1
        assertThat(partition.isRowKeyInPartition(schema, Key.create(10L))).isTrue();
        assertThat(partition).isEqualTo(rootOnly);
    }

    @Test
    public void shouldReturnCorrectLeafWhenAskedForLeafPartitionContainingKeyInTwoLevelTree() {
        // Given
        PartitionTree partitionTree = new PartitionsBuilder(schema)
                .rootFirst(ROOT)
                .splitToNewChildren(ROOT, L1_LEFT, L1_RIGHT, 0L)
                .splitToNewChildren(L1_LEFT, L2_LEFT_OF_L1L, L2_RIGHT_OF_L1L, -1000000L)
                .splitToNewChildren(L1_RIGHT, L2_LEFT_OF_L1R, L2_RIGHT_OF_L1R, 123456789L)
                .buildTree();

        // When 1
        Partition partition = partitionTree.getLeafPartition(schema, Key.create(10L));

        // Then 1
        assertThat(partition.isRowKeyInPartition(schema, Key.create(10L))).isTrue();
        assertThat(partition).isEqualTo(l2LeftOfL1R);

        // When 2
        partition = partitionTree.getLeafPartition(schema, Key.create(Long.MIN_VALUE));

        // Then 2
        assertThat(partition.isRowKeyInPartition(schema, Key.create(Long.MIN_VALUE))).isTrue();
        assertThat(partition).isEqualTo(l2LeftOfL1L);
    }

    @Test
    public void shouldReturnCorrectLeafWhenAskedForLeafPartitionContainingKeyInThreeLevelTree() {
        // Given
        PartitionTree partitionTree = new PartitionsBuilder(schema)
                .rootFirst(ROOT)
                .splitToNewChildren(ROOT, L1_LEFT, L1_RIGHT, 0L)
                .splitToNewChildren(L1_LEFT, L2_LEFT_OF_L1L, L2_RIGHT_OF_L1L, -1000000L)
                .splitToNewChildren(L1_RIGHT, L2_LEFT_OF_L1R, L2_RIGHT_OF_L1R, 123456789L)
                .splitToNewChildren(L2_LEFT_OF_L1L, L3_LEFT_OF_L2LL, L3_RIGHT_OF_L2LL, -2000000L)
                .buildTree();

        // When 1
        Partition partition = partitionTree.getLeafPartition(schema, Key.create(123456789L));

        // Then 1
        assertThat(partition.isRowKeyInPartition(schema, Key.create(123456789L))).isTrue();
        assertThat(partition).isEqualTo(l2RightOfL1R);

        // When 2
        partition = partitionTree.getLeafPartition(schema, Key.create(Long.MIN_VALUE));

        // Then 2
        assertThat(partition.isRowKeyInPartition(schema, Key.create(Long.MIN_VALUE))).isTrue();
        assertThat(partition).isEqualTo(l3LeftOfL2LL);
    }

}
