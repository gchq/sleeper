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
package sleeper.core.partition;

import org.junit.jupiter.api.Test;

import sleeper.core.key.Key;
import sleeper.core.range.Range;
import sleeper.core.range.Region;
import sleeper.core.schema.Field;
import sleeper.core.schema.Schema;
import sleeper.core.schema.type.LongType;
import sleeper.core.schema.type.PrimitiveType;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

public class PartitionTreeTest {
    private static final String ROOT = "root";
    private static final String L1_LEFT = "l1_left";
    private static final String L1_RIGHT = "l1_right";
    private static final String L2_LEFT_OF_L1L = "l2_left_of_l1l";
    private static final String L2_RIGHT_OF_L1L = "l2_right_of_l1l";
    private static final String L2_LEFT_OF_L1R = "l2_left_of_l1r";
    private static final String L2_RIGHT_OF_L1R = "l2_right_of_l1r";
    private static final String L3_LEFT_OF_L2_LEFT_OF_L1_LEFT = "l3_left_of_l2_left_of_l1_left";
    private static final String L3_RIGHT_OF_L2_LEFT_OF_L1_LEFT = "l3_right_of_l2_left_of_l1_left";
    private static final List<PrimitiveType> LONG_TYPE = Collections.singletonList(new LongType());

    @Test
    public void shouldReturnCorrectChildren() {
        // Given
        Schema schema = Schema.builder().rowKeyFields(new Field("id", new LongType())).build();
        Range.RangeFactory rangeFactory = new Range.RangeFactory(schema);
        Region rootRegion = new Region(rangeFactory.createRange("id", Long.MIN_VALUE, true, null, false));
        Partition root = Partition.builder()
                .rowKeyTypes(LONG_TYPE)
                .region(rootRegion)
                .id(ROOT)
                .leafPartition(false)
                .parentPartitionId(null)
                .childPartitionIds(Arrays.asList(L1_LEFT, L1_RIGHT))
                .dimension(-1)
                .build();
        Region l1LeftRegion = new Region(rangeFactory.createRange("id", Long.MIN_VALUE, true, 0L, false));
        Partition l1Left = Partition.builder()
                .rowKeyTypes(LONG_TYPE)
                .region(l1LeftRegion)
                .id(L1_LEFT)
                .leafPartition(false)
                .parentPartitionId(ROOT)
                .childPartitionIds(Arrays.asList(L2_LEFT_OF_L1L, L2_RIGHT_OF_L1L))
                .dimension(-1)
                .build();
        Region l1RightRegion = new Region(rangeFactory.createRange("id", 0L, true, null, false));
        Partition l1Right = Partition.builder()
                .rowKeyTypes(LONG_TYPE)
                .region(l1RightRegion)
                .id(L1_RIGHT)
                .leafPartition(false)
                .parentPartitionId(ROOT)
                .childPartitionIds(Arrays.asList(L2_LEFT_OF_L1R, L2_RIGHT_OF_L1R))
                .dimension(-1)
                .build();
        Region l2LeftOfL1LRegion = new Region(rangeFactory.createRange("id", Long.MIN_VALUE, true, -1000000L, false));
        Partition l2LeftOfL1L = Partition.builder()
                .rowKeyTypes(LONG_TYPE)
                .region(l2LeftOfL1LRegion)
                .id(L2_LEFT_OF_L1L)
                .leafPartition(true)
                .parentPartitionId(L1_LEFT)
                .childPartitionIds(Collections.emptyList())
                .dimension(-1)
                .build();
        Region l2RightOfL1LRegion = new Region(rangeFactory.createRange("id", -1000000L, true, 0L, false));
        Partition l2RightOfL1L = Partition.builder()
                .rowKeyTypes(LONG_TYPE)
                .region(l2RightOfL1LRegion)
                .id(L2_RIGHT_OF_L1L)
                .leafPartition(true)
                .parentPartitionId(L1_LEFT)
                .childPartitionIds(Collections.emptyList())
                .dimension(-1)
                .build();
        Region l2LeftOfL1RRegion = new Region(rangeFactory.createRange("id", 0L, true, 123456789L, false));
        Partition l2LeftOfL1R = Partition.builder()
                .rowKeyTypes(LONG_TYPE)
                .region(l2LeftOfL1RRegion)
                .id(L2_LEFT_OF_L1R)
                .leafPartition(true)
                .parentPartitionId(L1_RIGHT)
                .childPartitionIds(Collections.emptyList())
                .dimension(-1)
                .build();
        Region l2RightOfL1RRegion = new Region(rangeFactory.createRange("id", 123456789L, true, null, false));
        Partition l2RightOfL1R = Partition.builder()
                .rowKeyTypes(LONG_TYPE)
                .region(l2RightOfL1RRegion)
                .id(L2_RIGHT_OF_L1R)
                .leafPartition(true)
                .parentPartitionId(L1_RIGHT)
                .childPartitionIds(Collections.emptyList())
                .dimension(-1)
                .build();

        List<Partition> partitions = new ArrayList<>();
        partitions.add(root);
        partitions.add(l1Left);
        partitions.add(l1Right);
        partitions.add(l2LeftOfL1L);
        partitions.add(l2RightOfL1L);
        partitions.add(l2LeftOfL1R);
        partitions.add(l2RightOfL1R);
        PartitionTree partitionTree = new PartitionTree(schema, partitions);

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
        Schema schema = Schema.builder().rowKeyFields(new Field("id", new LongType())).build();
        Range.RangeFactory rangeFactory = new Range.RangeFactory(schema);
        Region rootRegion = new Region(rangeFactory.createRange("id", Long.MIN_VALUE, true, null, false));
        Partition root = Partition.builder()
                .rowKeyTypes(LONG_TYPE)
                .region(rootRegion)
                .id(ROOT)
                .leafPartition(false)
                .parentPartitionId(null)
                .childPartitionIds(Arrays.asList(L1_LEFT, L1_RIGHT))
                .dimension(-1)
                .build();
        Region l1LeftRegion = new Region(rangeFactory.createRange("id", Long.MIN_VALUE, true, 0L, false));
        Partition l1Left = Partition.builder()
                .rowKeyTypes(LONG_TYPE)
                .region(l1LeftRegion)
                .id(L1_LEFT)
                .leafPartition(false)
                .parentPartitionId(ROOT)
                .childPartitionIds(Arrays.asList(L2_LEFT_OF_L1L, L2_RIGHT_OF_L1L))
                .dimension(-1)
                .build();
        Region l1RightRegion = new Region(rangeFactory.createRange("id", 0L, true, null, false));
        Partition l1Right = Partition.builder()
                .rowKeyTypes(LONG_TYPE)
                .region(l1RightRegion)
                .id(L1_RIGHT)
                .leafPartition(false)
                .parentPartitionId(ROOT)
                .childPartitionIds(Arrays.asList(L2_LEFT_OF_L1R, L2_RIGHT_OF_L1R))
                .dimension(-1)
                .build();
        Region l2LeftOfL1LRegion = new Region(rangeFactory.createRange("id", Long.MIN_VALUE, true, -1000000L, false));
        Partition l2LeftOfL1L = Partition.builder()
                .rowKeyTypes(LONG_TYPE)
                .region(l2LeftOfL1LRegion)
                .id(L2_LEFT_OF_L1L)
                .leafPartition(true)
                .parentPartitionId(L1_LEFT)
                .childPartitionIds(Collections.emptyList())
                .dimension(-1)
                .build();
        Region l2RightOfL1LRegion = new Region(rangeFactory.createRange("id", -1000000L, true, 0L, false));
        Partition l2RightOfL1L = Partition.builder()
                .rowKeyTypes(LONG_TYPE)
                .region(l2RightOfL1LRegion)
                .id(L2_RIGHT_OF_L1L)
                .leafPartition(true)
                .parentPartitionId(L1_LEFT)
                .childPartitionIds(Collections.emptyList())
                .dimension(-1)
                .build();
        Region l2LeftOfL1RRegion = new Region(rangeFactory.createRange("id", 0L, true, 123456789L, false));
        Partition l2LeftOfL1R = Partition.builder()
                .rowKeyTypes(LONG_TYPE)
                .region(l2LeftOfL1RRegion)
                .id(L2_LEFT_OF_L1R)
                .leafPartition(true)
                .parentPartitionId(L1_RIGHT)
                .childPartitionIds(Collections.emptyList())
                .dimension(-1)
                .build();
        Region l2RightOfL1RRegion = new Region(rangeFactory.createRange("id", 123456789L, true, null, false));
        Partition l2RightOfL1R = Partition.builder()
                .rowKeyTypes(LONG_TYPE)
                .region(l2RightOfL1RRegion)
                .id(L2_RIGHT_OF_L1R)
                .leafPartition(true)
                .parentPartitionId(L1_RIGHT)
                .childPartitionIds(Collections.emptyList())
                .dimension(-1)
                .build();

        List<Partition> partitions = new ArrayList<>();
        partitions.add(root);
        partitions.add(l1Left);
        partitions.add(l1Right);
        partitions.add(l2LeftOfL1L);
        partitions.add(l2RightOfL1L);
        partitions.add(l2LeftOfL1R);
        partitions.add(l2RightOfL1R);
        PartitionTree partitionTree = new PartitionTree(schema, partitions);

        // When
        List<Partition> ancestorsOfRoot = partitionTree.getAllAncestors(ROOT);
        List<String> ancestorsOfRootIds = partitionTree.getAllAncestorIds(ROOT);
        List<Partition> ancestorsOfL2LOfL1L = partitionTree.getAllAncestors(L2_LEFT_OF_L1L);
        List<String> ancestorsOfL2LOfL1LIds = partitionTree.getAllAncestorIds(L2_LEFT_OF_L1L);

        // Then
        assertThat(ancestorsOfRoot).isEmpty();
        assertThat(ancestorsOfRootIds).isEmpty();
        assertThat(ancestorsOfL2LOfL1L).containsExactly(l1Left, root);
        assertThat(ancestorsOfL2LOfL1LIds).containsExactly(l1Left.getId(), root.getId());
    }

    @Test
    public void shouldReturnRootIfOnlyPartitionAndAskedForLeafPartitionContainingKey() {
        // Given
        Schema schema = Schema.builder().rowKeyFields(new Field("id", new LongType())).build();
        Range.RangeFactory rangeFactory = new Range.RangeFactory(schema);

        Region region = new Region(rangeFactory.createRange("id", Long.MIN_VALUE, true, null, false));
        Partition root = Partition.builder()
                .rowKeyTypes(LONG_TYPE)
                .region(region)
                .id(ROOT)
                .leafPartition(true)
                .parentPartitionId(null)
                .childPartitionIds(new ArrayList<>())
                .dimension(-1)
                .build();
        PartitionTree partitionTree = new PartitionTree(schema, Collections.singletonList(root));

        // When
        Partition partition = partitionTree.getLeafPartition(Key.create(10L));

        // Then 1
        assertThat(partition.isRowKeyInPartition(schema, Key.create(10L))).isTrue();
        assertThat(partition).isEqualTo(root);
    }

    @Test
    public void shouldReturnCorrectLeafWhenAskedForLeafPartitionContainingKeyInTwoLevelTree() {
        // Given
        Schema schema = Schema.builder().rowKeyFields(new Field("id", new LongType())).build();
        Range.RangeFactory rangeFactory = new Range.RangeFactory(schema);
        Region rootRegion = new Region(rangeFactory.createRange("id", Long.MIN_VALUE, true, null, false));
        Partition root = Partition.builder()
                .rowKeyTypes(LONG_TYPE)
                .region(rootRegion)
                .id(ROOT)
                .leafPartition(false)
                .parentPartitionId(null)
                .childPartitionIds(Arrays.asList(L1_LEFT, L1_RIGHT))
                .dimension(-1)
                .build();
        Region l1LeftRegion = new Region(rangeFactory.createRange("id", Long.MIN_VALUE, true, 0L, false));
        Partition l1Left = Partition.builder()
                .rowKeyTypes(LONG_TYPE)
                .region(l1LeftRegion)
                .id(L1_LEFT)
                .leafPartition(false)
                .parentPartitionId(ROOT)
                .childPartitionIds(Arrays.asList(L2_LEFT_OF_L1L, L2_RIGHT_OF_L1L))
                .dimension(-1)
                .build();
        Region l1RightRegion = new Region(rangeFactory.createRange("id", 0L, true, null, false));
        Partition l1Right = Partition.builder()
                .rowKeyTypes(LONG_TYPE)
                .region(l1RightRegion)
                .id(L1_RIGHT)
                .leafPartition(false)
                .parentPartitionId(ROOT)
                .childPartitionIds(Arrays.asList(L2_LEFT_OF_L1R, L2_RIGHT_OF_L1R))
                .dimension(-1)
                .build();
        Region l2LeftOfL1LRegion = new Region(rangeFactory.createRange("id", Long.MIN_VALUE, true, -1000000L, false));
        Partition l2LeftOfL1L = Partition.builder()
                .rowKeyTypes(LONG_TYPE)
                .region(l2LeftOfL1LRegion)
                .id(L2_LEFT_OF_L1L)
                .leafPartition(true)
                .parentPartitionId(L1_LEFT)
                .childPartitionIds(Collections.emptyList())
                .dimension(-1)
                .build();
        Region l2RightOfL1LRegion = new Region(rangeFactory.createRange("id", -1000000L, true, 0L, false));
        Partition l2RightOfL1L = Partition.builder()
                .rowKeyTypes(LONG_TYPE)
                .region(l2RightOfL1LRegion)
                .id(L2_RIGHT_OF_L1L)
                .leafPartition(true)
                .parentPartitionId(L1_LEFT)
                .childPartitionIds(Collections.emptyList())
                .dimension(-1)
                .build();
        Region l2LeftOfL1RRegion = new Region(rangeFactory.createRange("id", 0L, true, 123456789L, false));
        Partition l2LeftOfL1R = Partition.builder()
                .rowKeyTypes(LONG_TYPE)
                .region(l2LeftOfL1RRegion)
                .id(L2_LEFT_OF_L1R)
                .leafPartition(true)
                .parentPartitionId(L1_RIGHT)
                .childPartitionIds(Collections.emptyList())
                .dimension(-1)
                .build();
        Region l2RightOfL1RRegion = new Region(rangeFactory.createRange("id", 123456789L, true, null, false));
        Partition l2RightOfL1R = Partition.builder()
                .rowKeyTypes(LONG_TYPE)
                .region(l2RightOfL1RRegion)
                .id(L2_RIGHT_OF_L1R)
                .leafPartition(true)
                .parentPartitionId(L1_RIGHT)
                .childPartitionIds(Collections.emptyList())
                .dimension(-1)
                .build();

        List<Partition> partitions = new ArrayList<>();
        partitions.add(root);
        partitions.add(l1Left);
        partitions.add(l1Right);
        partitions.add(l2LeftOfL1L);
        partitions.add(l2RightOfL1L);
        partitions.add(l2LeftOfL1R);
        partitions.add(l2RightOfL1R);
        PartitionTree partitionTree = new PartitionTree(schema, partitions);

        // When 1
        Partition partition = partitionTree.getLeafPartition(Key.create(10L));

        // Then 1
        assertThat(partition.isRowKeyInPartition(schema, Key.create(10L))).isTrue();
        assertThat(partition).isEqualTo(l2LeftOfL1R);

        // When 2
        partition = partitionTree.getLeafPartition(Key.create(Long.MIN_VALUE));

        // Then 2
        assertThat(partition.isRowKeyInPartition(schema, Key.create(Long.MIN_VALUE))).isTrue();
        assertThat(partition).isEqualTo(l2LeftOfL1L);
    }

    @Test
    public void shouldReturnCorrectLeafWhenAskedForLeafPartitionContainingKeyInThreeLevelTree() {
        // Given
        Schema schema = Schema.builder().rowKeyFields(new Field("id", new LongType())).build();
        Range.RangeFactory rangeFactory = new Range.RangeFactory(schema);
        Region rootRegion = new Region(rangeFactory.createRange("id", Long.MIN_VALUE, true, null, false));
        Partition root = Partition.builder()
                .rowKeyTypes(LONG_TYPE)
                .region(rootRegion)
                .id(ROOT)
                .leafPartition(false)
                .parentPartitionId(null)
                .childPartitionIds(Arrays.asList(L1_LEFT, L1_RIGHT))
                .dimension(-1)
                .build();
        Region l1LeftRegion = new Region(rangeFactory.createRange("id", Long.MIN_VALUE, true, 0L, false));
        Partition l1Left = Partition.builder()
                .rowKeyTypes(LONG_TYPE)
                .region(l1LeftRegion)
                .id(L1_LEFT)
                .leafPartition(false)
                .parentPartitionId(ROOT)
                .childPartitionIds(Arrays.asList(L2_LEFT_OF_L1L, L2_RIGHT_OF_L1L))
                .dimension(-1)
                .build();
        Region l1RightRegion = new Region(rangeFactory.createRange("id", 0L, true, null, false));
        Partition l1Right = Partition.builder()
                .rowKeyTypes(LONG_TYPE)
                .region(l1RightRegion)
                .id(L1_RIGHT)
                .leafPartition(false)
                .parentPartitionId(ROOT)
                .childPartitionIds(Arrays.asList(L2_LEFT_OF_L1R, L2_RIGHT_OF_L1R))
                .dimension(-1)
                .build();
        Region l2LeftOfL1LRegion = new Region(rangeFactory.createRange("id", Long.MIN_VALUE, true, -1000000L, false));
        Partition l2LeftOfL1L = Partition.builder()
                .rowKeyTypes(LONG_TYPE)
                .region(l2LeftOfL1LRegion)
                .id(L2_LEFT_OF_L1L)
                .leafPartition(false)
                .parentPartitionId(L1_LEFT)
                .childPartitionIds(Arrays.asList(L3_LEFT_OF_L2_LEFT_OF_L1_LEFT, L3_RIGHT_OF_L2_LEFT_OF_L1_LEFT))
                .dimension(-1)
                .build();
        Region l2RightOfL1LRegion = new Region(rangeFactory.createRange("id", -1000000L, true, 0L, false));
        Partition l2RightOfL1L = Partition.builder()
                .rowKeyTypes(LONG_TYPE)
                .region(l2RightOfL1LRegion)
                .id(L2_RIGHT_OF_L1L)
                .leafPartition(false)
                .parentPartitionId(L1_LEFT)
                .childPartitionIds(Collections.emptyList())
                .dimension(-1)
                .build();
        Region l2LeftOfL1RRegion = new Region(rangeFactory.createRange("id", 0L, true, 123456789L, false));
        Partition l2LeftOfL1R = Partition.builder()
                .rowKeyTypes(LONG_TYPE)
                .region(l2LeftOfL1RRegion)
                .id(L2_LEFT_OF_L1R)
                .leafPartition(true)
                .parentPartitionId(L1_RIGHT)
                .childPartitionIds(Collections.emptyList())
                .dimension(-1)
                .build();
        Region l2RightOfL1RRegion = new Region(rangeFactory.createRange("id", 123456789L, true, null, false));
        Partition l2RightOfL1R = Partition.builder()
                .rowKeyTypes(LONG_TYPE)
                .region(l2RightOfL1RRegion)
                .id(L2_RIGHT_OF_L1R)
                .leafPartition(true)
                .parentPartitionId(L1_RIGHT)
                .childPartitionIds(Collections.emptyList())
                .dimension(-1)
                .build();
        Region l3LeftOfL2LoL1LRegion = new Region(rangeFactory.createRange("id", Long.MIN_VALUE, true, -9000000L, false));
        Partition l3LeftOfL2LoL1L = Partition.builder()
                .rowKeyTypes(LONG_TYPE)
                .region(l3LeftOfL2LoL1LRegion)
                .id(L3_LEFT_OF_L2_LEFT_OF_L1_LEFT)
                .leafPartition(true)
                .parentPartitionId(L2_LEFT_OF_L1L)
                .childPartitionIds(Collections.emptyList())
                .dimension(-1)
                .build();
        Region l3RightOfL2LoL1LRegion = new Region(rangeFactory.createRange("id", -9000000L, true, -1000000L, false));
        Partition l3RightOfL2LoL1L = Partition.builder()
                .rowKeyTypes(LONG_TYPE)
                .region(l3RightOfL2LoL1LRegion)
                .id(L3_RIGHT_OF_L2_LEFT_OF_L1_LEFT)
                .leafPartition(true)
                .parentPartitionId(L2_LEFT_OF_L1L)
                .childPartitionIds(Collections.emptyList())
                .dimension(-1)
                .build();

        List<Partition> partitions = new ArrayList<>();
        partitions.add(root);
        partitions.add(l1Left);
        partitions.add(l1Right);
        partitions.add(l2LeftOfL1L);
        partitions.add(l2RightOfL1L);
        partitions.add(l2LeftOfL1R);
        partitions.add(l2RightOfL1R);
        partitions.add(l3LeftOfL2LoL1L);
        partitions.add(l3RightOfL2LoL1L);
        PartitionTree partitionTree = new PartitionTree(schema, partitions);

        // When 1
        Partition partition = partitionTree.getLeafPartition(Key.create(123456789L));

        // Then 1
        assertThat(partition.isRowKeyInPartition(schema, Key.create(123456789L))).isTrue();
        assertThat(partition).isEqualTo(l2RightOfL1R);

        // When 2
        partition = partitionTree.getLeafPartition(Key.create(Long.MIN_VALUE));

        // Then 2
        assertThat(partition.isRowKeyInPartition(schema, Key.create(Long.MIN_VALUE))).isTrue();
        assertThat(partition).isEqualTo(l3LeftOfL2LoL1L);
    }
}
