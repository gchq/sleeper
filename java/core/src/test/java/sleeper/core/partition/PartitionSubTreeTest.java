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

import sleeper.core.range.Range;
import sleeper.core.range.Region;
import sleeper.core.schema.Field;
import sleeper.core.schema.Schema;
import sleeper.core.schema.type.LongType;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

public class PartitionSubTreeTest {

    private static final String ROOT = "root";
    private static final String L1_LEFT = "l1_left";
    private static final String L1_RIGHT = "l1_right";
    private static final String L2_LEFT_OF_L1L = "l2_left_of_l1l";
    private static final String L2_RIGHT_OF_L1L = "l2_right_of_l1l";
    private static final String L2_LEFT_OF_L1R = "l2_left_of_l1r";
    private static final String L2_RIGHT_OF_L1R = "l2_right_of_l1r";

    private Schema schema = Schema.builder().rowKeyFields(new Field("id", new LongType())).build();
    private Range.RangeFactory rangeFactory = new Range.RangeFactory(schema);
    private Region rootRegion = new Region(rangeFactory.createRange("id", Long.MIN_VALUE, true, null, false));
    private Partition root = Partition.builder()
            .region(rootRegion)
            .id(ROOT)
            .leafPartition(false)
            .parentPartitionId(null)
            .childPartitionIds(Arrays.asList(L1_LEFT, L1_RIGHT))
            .dimension(-1)
            .build();
    private Region l1LeftRegion = new Region(rangeFactory.createRange("id", Long.MIN_VALUE, true, 0L, false));
    private Partition l1Left = Partition.builder()
            .region(l1LeftRegion)
            .id(L1_LEFT)
            .leafPartition(false)
            .parentPartitionId(ROOT)
            .childPartitionIds(Arrays.asList(L2_LEFT_OF_L1L, L2_RIGHT_OF_L1L))
            .dimension(-1)
            .build();
    private Region l1RightRegion = new Region(rangeFactory.createRange("id", 0L, true, null, false));
    private Partition l1Right = Partition.builder()
            .region(l1RightRegion)
            .id(L1_RIGHT)
            .leafPartition(false)
            .parentPartitionId(ROOT)
            .childPartitionIds(Arrays.asList(L2_LEFT_OF_L1R, L2_RIGHT_OF_L1R))
            .dimension(-1)
            .build();
    private Region l2LeftOfL1LRegion = new Region(rangeFactory.createRange("id", Long.MIN_VALUE, true, -1000000L, false));
    private Partition l2LeftOfL1L = Partition.builder()
            .region(l2LeftOfL1LRegion)
            .id(L2_LEFT_OF_L1L)
            .leafPartition(true)
            .parentPartitionId(L1_LEFT)
            .childPartitionIds(Collections.emptyList())
            .dimension(-1)
            .build();
    private Region l2RightOfL1LRegion = new Region(rangeFactory.createRange("id", -1000000L, true, 0L, false));
    private Partition l2RightOfL1L = Partition.builder()
            .region(l2RightOfL1LRegion)
            .id(L2_RIGHT_OF_L1L)
            .leafPartition(true)
            .parentPartitionId(L1_LEFT)
            .childPartitionIds(Collections.emptyList())
            .dimension(-1)
            .build();
    private Region l2LeftOfL1RRegion = new Region(rangeFactory.createRange("id", 0L, true, 123456789L, false));
    private Partition l2LeftOfL1R = Partition.builder()
            .region(l2LeftOfL1RRegion)
            .id(L2_LEFT_OF_L1R)
            .leafPartition(true)
            .parentPartitionId(L1_RIGHT)
            .childPartitionIds(Collections.emptyList())
            .dimension(-1)
            .build();
    private Region l2RightOfL1RRegion = new Region(rangeFactory.createRange("id", 123456789L, true, null, false));
    private Partition l2RightOfL1R = Partition.builder()
            .region(l2RightOfL1RRegion)
            .id(L2_RIGHT_OF_L1R)
            .leafPartition(true)
            .parentPartitionId(L1_RIGHT)
            .childPartitionIds(Collections.emptyList())
            .dimension(-1)
            .build();

    private PartitionTree generateFullTree() {
        return new PartitionTree(List.of(root, l1Left, l1Right, l2LeftOfL1L, l2RightOfL1L, l2LeftOfL1R, l2RightOfL1R));
    }

    @Test
    void shouldCreateSimpleSubTreeWithExactElements() {
        Partition l1LeftAsLeaf = l1Left.toBuilder().leafPartition(true).build();
        Partition l1RightAsLeaf = l1Left.toBuilder().leafPartition(true).build();

        PartitionSubTree subTree = new PartitionSubTree(generateFullTree(), 2);
        assertThat(subTree.getLeafPartitions()).contains(l1LeftAsLeaf, l1RightAsLeaf);
        assertThat(subTree.getAllPartitions().size()).isEqualTo(3);
    }

    @Test
    void shouldCreateSimpleSubTreeWithElementsButExceedPartitionCount() {
        PartitionSubTree subTree = new PartitionSubTree(generateFullTree(), 3);

        assertThat(subTree.getAllPartitions()).contains(l1Left, l1Right);
        assertThat(subTree.getLeafPartitions()).doesNotContain(l1Left, l1Right);
        assertThat(subTree.getLeafPartitions()).contains(l2LeftOfL1L, l2RightOfL1L, l2LeftOfL1R, l2RightOfL1R);
        assertThat(subTree.getAllPartitions().size()).isEqualTo(7);
    }

    @Test
    void shouldCreateRootOnlyTreeWhenGivenZeroCount() {
        PartitionSubTree subTree = new PartitionSubTree(generateFullTree(), 0);
        assertThat(subTree.getRootPartition()).isEqualTo(root);
        assertThat(subTree.getAllPartitions().size()).isEqualTo(1);
    }
}
