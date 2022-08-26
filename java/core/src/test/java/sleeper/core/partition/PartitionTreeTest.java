/*
 * Copyright 2022 Crown Copyright
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

import org.junit.Test;
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
    private final static String ROOT = "root";
    private final static String L1_LEFT = "l1_left";
    private final static String L1_RIGHT = "l1_right";
    private final static String L2_LEFT_OF_L1L = "l2_left_of_l1l";
    private final static String L2_RIGHT_OF_L1L = "l2_right_of_l1l";
    private final static String L2_LEFT_OF_L1R = "l2_left_of_l1r";
    private final static String L2_RIGHT_OF_L1R = "l2_right_of_l1r";
    private final static String L3_LEFT_OF_L2_LEFT_OF_L1_LEFT = "l3_left_of_l2_left_of_l1_left";
    private final static String L3_RIGHT_OF_L2_LEFT_OF_L1_LEFT = "l3_right_of_l2_left_of_l1_left";
    private final static List<PrimitiveType> LONG_TYPE = Collections.singletonList(new LongType());

    @Test
    public void shouldReturnCorrectChildren() {
        // Given
        Schema schema = new Schema();
        schema.setRowKeyFields(new Field("id", new LongType()));
        Range.RangeFactory rangeFactory = new Range.RangeFactory(schema);
        Region rootRegion = new Region(rangeFactory.createRange("id", Long.MIN_VALUE, true, null, false));
        Partition root = new Partition(LONG_TYPE, rootRegion, ROOT, false, null, Arrays.asList(L1_LEFT, L1_RIGHT), -1);
        Region l1LeftRegion = new Region(rangeFactory.createRange("id", Long.MIN_VALUE, true, 0L, false));
        Partition l1Left = new Partition(LONG_TYPE, l1LeftRegion, L1_LEFT, false, ROOT, Arrays.asList(L2_LEFT_OF_L1L, L2_RIGHT_OF_L1L), -1);
        Region l1RightRegion = new Region(rangeFactory.createRange("id", 0L, true, null, false));
        Partition l1Right = new Partition(LONG_TYPE, l1RightRegion, L1_RIGHT, false, ROOT, Arrays.asList(L2_LEFT_OF_L1R, L2_RIGHT_OF_L1R), -1);
        Region l2LeftOfL1LRegion = new Region(rangeFactory.createRange("id", Long.MIN_VALUE, true, -1000000L, false));
        Partition l2LeftOfL1L = new Partition(LONG_TYPE, l2LeftOfL1LRegion, L2_LEFT_OF_L1L, true, L1_LEFT, null, -1);
        Region l2RightOfL1LRegion = new Region(rangeFactory.createRange("id", -1000000L, true, 0L, false));
        Partition l2RightOfL1L = new Partition(LONG_TYPE, l2RightOfL1LRegion, L2_RIGHT_OF_L1L, true, L1_LEFT, null, -1);
        Region l2LeftOfL1RRegion = new Region(rangeFactory.createRange("id", 0L, true, 123456789L, false));
        Partition l2LeftOfL1R = new Partition(LONG_TYPE, l2LeftOfL1RRegion, L2_LEFT_OF_L1R, true, L1_RIGHT, null, -1);
        Region l2RightOfL1RRegion = new Region(rangeFactory.createRange("id", 123456789L, true, null, false));
        Partition l2RightOfL1R = new Partition(LONG_TYPE, l2RightOfL1RRegion, L2_RIGHT_OF_L1R, true, L1_RIGHT, null, -1);

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
        Schema schema = new Schema();
        schema.setRowKeyFields(new Field("id", new LongType()));
        Range.RangeFactory rangeFactory = new Range.RangeFactory(schema);
        Region rootRegion = new Region(rangeFactory.createRange("id", Long.MIN_VALUE, true, null, false));
        Partition root = new Partition(LONG_TYPE, rootRegion, ROOT, false, null, Arrays.asList(L1_LEFT, L1_RIGHT), -1);
        Region l1LeftRegion = new Region(rangeFactory.createRange("id", Long.MIN_VALUE, true, 0L, false));
        Partition l1Left = new Partition(LONG_TYPE, l1LeftRegion, L1_LEFT, false, ROOT, Arrays.asList(L2_LEFT_OF_L1L, L2_RIGHT_OF_L1L), -1);
        Region l1RightRegion = new Region(rangeFactory.createRange("id", 0L, true, null, false));
        Partition l1Right = new Partition(LONG_TYPE, l1RightRegion, L1_RIGHT, false, ROOT, Arrays.asList(L2_LEFT_OF_L1R, L2_RIGHT_OF_L1R), -1);
        Region l2LeftOfL1LRegion = new Region(rangeFactory.createRange("id", Long.MIN_VALUE, true, -1000000L, false));
        Partition l2LeftOfL1L = new Partition(LONG_TYPE, l2LeftOfL1LRegion, L2_LEFT_OF_L1L, true, L1_LEFT, null, -1);
        Region l2RightOfL1LRegion = new Region(rangeFactory.createRange("id", -1000000L, true, 0L, false));
        Partition l2RightOfL1L = new Partition(LONG_TYPE, l2RightOfL1LRegion, L2_RIGHT_OF_L1L, true, L1_LEFT, null, -1);
        Region l2LeftOfL1RRegion = new Region(rangeFactory.createRange("id", 0L, true, 123456789L, false));
        Partition l2LeftOfL1R = new Partition(LONG_TYPE, l2LeftOfL1RRegion, L2_LEFT_OF_L1R, true, L1_RIGHT, null, -1);
        Region l2RightOfL1RRegion = new Region(rangeFactory.createRange("id", 123456789L, true, null, false));
        Partition l2RightOfL1R = new Partition(LONG_TYPE, l2RightOfL1RRegion, L2_RIGHT_OF_L1R, true, L1_RIGHT, null, -1);

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
        Schema schema = new Schema();
        schema.setRowKeyFields(new Field("id", new LongType()));
        Range.RangeFactory rangeFactory = new Range.RangeFactory(schema);

        Region region = new Region(rangeFactory.createRange("id", Long.MIN_VALUE, true, null, false));
        Partition root = new Partition(LONG_TYPE, region, ROOT, true, null, Arrays.asList(L1_LEFT, L1_RIGHT), -1);
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
        Schema schema = new Schema();
        schema.setRowKeyFields(new Field("id", new LongType()));
        Range.RangeFactory rangeFactory = new Range.RangeFactory(schema);
        Region rootRegion = new Region(rangeFactory.createRange("id", Long.MIN_VALUE, true, null, false));
        Partition root = new Partition(LONG_TYPE, rootRegion, ROOT, false, null, Arrays.asList(L1_LEFT, L1_RIGHT), -1);
        Region l1LeftRegion = new Region(rangeFactory.createRange("id", Long.MIN_VALUE, true, 0L, false));
        Partition l1Left = new Partition(LONG_TYPE, l1LeftRegion, L1_LEFT, false, ROOT, Arrays.asList(L2_LEFT_OF_L1L, L2_RIGHT_OF_L1L), -1);
        Region l1RightRegion = new Region(rangeFactory.createRange("id", 0L, true, null, false));
        Partition l1Right = new Partition(LONG_TYPE, l1RightRegion, L1_RIGHT, false, ROOT, Arrays.asList(L2_LEFT_OF_L1R, L2_RIGHT_OF_L1R), -1);
        Region l2LeftOfL1LRegion = new Region(rangeFactory.createRange("id", Long.MIN_VALUE, true, -1000000L, false));
        Partition l2LeftOfL1L = new Partition(LONG_TYPE, l2LeftOfL1LRegion, L2_LEFT_OF_L1L, true, L1_LEFT, null, -1);
        Region l2RightOfL1LRegion = new Region(rangeFactory.createRange("id", -1000000L, true, 0L, false));
        Partition l2RightOfL1L = new Partition(LONG_TYPE, l2RightOfL1LRegion, L2_RIGHT_OF_L1L, true, L1_LEFT, null, -1);
        Region l2LeftOfL1RRegion = new Region(rangeFactory.createRange("id", 0L, true, 123456789L, false));
        Partition l2LeftOfL1R = new Partition(LONG_TYPE, l2LeftOfL1RRegion, L2_LEFT_OF_L1R, true, L1_RIGHT, null, -1);
        Region l2RightOfL1RRegion = new Region(rangeFactory.createRange("id", 123456789L, true, null, false));
        Partition l2RightOfL1R = new Partition(LONG_TYPE, l2RightOfL1RRegion, L2_RIGHT_OF_L1R, true, L1_RIGHT, null, -1);

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
        Schema schema = new Schema();
        schema.setRowKeyFields(new Field("id", new LongType()));
        Range.RangeFactory rangeFactory = new Range.RangeFactory(schema);
        Region rootRegion = new Region(rangeFactory.createRange("id", Long.MIN_VALUE, true, null, false));
        Partition root = new Partition(LONG_TYPE, rootRegion, ROOT, false, null, Arrays.asList(L1_LEFT, L1_RIGHT), -1);
        Region l1LeftRegion = new Region(rangeFactory.createRange("id", Long.MIN_VALUE, true, 0L, false));
        Partition l1Left = new Partition(LONG_TYPE, l1LeftRegion, L1_LEFT, false, ROOT, Arrays.asList(L2_LEFT_OF_L1L, L2_RIGHT_OF_L1L), -1);
        Region l1RightRegion = new Region(rangeFactory.createRange("id", 0L, true, null, false));
        Partition l1Right = new Partition(LONG_TYPE, l1RightRegion, L1_RIGHT, false, ROOT, Arrays.asList(L2_LEFT_OF_L1R, L2_RIGHT_OF_L1R), -1);
        Region l2LeftOfL1LRegion = new Region(rangeFactory.createRange("id", Long.MIN_VALUE, true, -1000000L, false));
        Partition l2LeftOfL1L = new Partition(LONG_TYPE, l2LeftOfL1LRegion, L2_LEFT_OF_L1L, false, L1_LEFT, Arrays.asList(L3_LEFT_OF_L2_LEFT_OF_L1_LEFT, L3_RIGHT_OF_L2_LEFT_OF_L1_LEFT), -1);
        Region l2RightOfL1LRegion = new Region(rangeFactory.createRange("id", -1000000L, true, 0L, false));
        Partition l2RightOfL1L = new Partition(LONG_TYPE, l2RightOfL1LRegion, L2_RIGHT_OF_L1L, false, L1_LEFT, null, -1);
        Region l2LeftOfL1RRegion = new Region(rangeFactory.createRange("id", 0L, true, 123456789L, false));
        Partition l2LeftOfL1R = new Partition(LONG_TYPE, l2LeftOfL1RRegion, L2_LEFT_OF_L1R, true, L1_RIGHT, null, -1);
        Region l2RightOfL1RRegion = new Region(rangeFactory.createRange("id", 123456789L, true, null, false));
        Partition l2RightOfL1R = new Partition(LONG_TYPE, l2RightOfL1RRegion, L2_RIGHT_OF_L1R, true, L1_RIGHT, null, -1);
        Region l3LeftOfL2LoL1LRegion = new Region(rangeFactory.createRange("id", Long.MIN_VALUE, true, -9000000L, false));
        Partition l3LeftOfL2LoL1L = new Partition(LONG_TYPE, l3LeftOfL2LoL1LRegion, L3_LEFT_OF_L2_LEFT_OF_L1_LEFT, true, L2_LEFT_OF_L1L, null, -1);
        Region l3RightOfL2LoL1LRegion = new Region(rangeFactory.createRange("id", -9000000L, true, -1000000L, false));
        Partition l3RightOfL2LoL1L = new Partition(LONG_TYPE, l3RightOfL2LoL1LRegion, L3_RIGHT_OF_L2_LEFT_OF_L1_LEFT, true, L2_LEFT_OF_L1L, null, -1);

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
