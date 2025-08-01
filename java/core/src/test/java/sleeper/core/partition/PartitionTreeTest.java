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
import sleeper.core.range.Region;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

public class PartitionTreeTest extends PartitionTreeTestBase {

    private static final String L3_LEFT_OF_L2_LEFT_OF_L1_LEFT = "l3_left_of_l2_left_of_l1_left";
    private static final String L3_RIGHT_OF_L2_LEFT_OF_L1_LEFT = "l3_right_of_l2_left_of_l1_left";

    @Test
    public void shouldReturnCorrectChildren() {
        // Given
        PartitionTree partitionTree = generateTreeTo2Levels();

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
        PartitionTree partitionTree = generateTreeTo2Levels();

        // When
        List<Partition> ancestorsOfRoot = partitionTree.getAllAncestors(ROOT);
        List<String> ancestorsOfRootIds = partitionTree.getAllAncestorIds(ROOT);
        List<Partition> ancestorsOfL2LOfL1L = partitionTree.getAllAncestors(L2_LEFT_OF_L1L);
        List<String> ancestorsOfL2LOfL1LIds = partitionTree.getAllAncestorIds(L2_LEFT_OF_L1L);

        // Then
        assertThat(ancestorsOfRootIds).isEmpty();
        assertThat(ancestorsOfRoot).isEmpty();
        assertThat(ancestorsOfL2LOfL1LIds).containsExactly(l1Left.getId(), root.getId());
        assertThat(ancestorsOfL2LOfL1L).containsExactly(l1Left, root);
    }

    @Test
    public void shouldReturnRootIfOnlyPartitionAndAskedForLeafPartitionContainingKey() {
        // Given
        PartitionTree partitionTree = generateTreeToRootLevel();

        // When
        Partition partition = partitionTree.getLeafPartition(schema, Key.create(10L));

        // Then 1
        assertThat(partition.isRowKeyInPartition(schema, Key.create(10L))).isTrue();
        assertThat(partition).isEqualTo(adjustLeafStatus(root, true));
    }

    @Test
    public void shouldReturnCorrectLeafWhenAskedForLeafPartitionContainingKeyInTwoLevelTree() {
        // Given
        PartitionTree partitionTree = generateTreeTo2Levels();

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
        Region l3LeftOfL2LoL1LRegion = new Region(rangeFactory.createRange("id", Long.MIN_VALUE, true, -9000000L, false));
        Partition l3LeftOfL2LoL1L = Partition.builder()
                .region(l3LeftOfL2LoL1LRegion)
                .id(L3_LEFT_OF_L2_LEFT_OF_L1_LEFT)
                .leafPartition(true)
                .parentPartitionId(L2_LEFT_OF_L1L)
                .childPartitionIds(Collections.emptyList())
                .dimension(-1)
                .build();
        Region l3RightOfL2LoL1LRegion = new Region(rangeFactory.createRange("id", -9000000L, true, -1000000L, false));
        Partition l3RightOfL2LoL1L = Partition.builder()
                .region(l3RightOfL2LoL1LRegion)
                .id(L3_RIGHT_OF_L2_LEFT_OF_L1_LEFT)
                .leafPartition(true)
                .parentPartitionId(L2_LEFT_OF_L1L)
                .childPartitionIds(Collections.emptyList())
                .dimension(-1)
                .build();

        //Adjust the child partitions to include new elements
        l2LeftOfL1L = l2LeftOfL1L.toBuilder()
                .leafPartition(false)
                .childPartitionIds(Arrays.asList(L3_LEFT_OF_L2_LEFT_OF_L1_LEFT, L3_RIGHT_OF_L2_LEFT_OF_L1_LEFT))
                .build();

        PartitionTree partitionTree = new PartitionTree(
                List.of(root, l1Left, l1Right, l2LeftOfL1L, adjustLeafStatus(l2RightOfL1L, false), l2RightOfL1L, l2LeftOfL1R, l2RightOfL1R, l3LeftOfL2LoL1L, l3RightOfL2LoL1L));

        // When 1
        Partition partition = partitionTree.getLeafPartition(schema, Key.create(123456789L));

        // Then 1
        assertThat(partition.isRowKeyInPartition(schema, Key.create(123456789L))).isTrue();
        assertThat(partition).isEqualTo(l2RightOfL1R);

        // When 2
        partition = partitionTree.getLeafPartition(schema, Key.create(Long.MIN_VALUE));

        // Then 2
        assertThat(partition.isRowKeyInPartition(schema, Key.create(Long.MIN_VALUE))).isTrue();
        assertThat(partition).isEqualTo(l3LeftOfL2LoL1L);
    }
}
