/*
 * Copyright 2023 Crown Copyright
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

import sleeper.core.range.Range;
import sleeper.core.range.Range.RangeFactory;
import sleeper.core.range.Region;
import sleeper.core.schema.Field;
import sleeper.core.schema.Schema;
import sleeper.core.schema.type.LongType;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

public class PartitionsFromSplitPointsTest {

    @Test
    public void shouldCreateTreeWithOneRootNodeIfNoSplitPoints() {
        // Given
        Schema schema = Schema.builder().rowKeyFields(new Field("id", new LongType())).build();
        RangeFactory rangeFactory = new RangeFactory(schema);
        PartitionsFromSplitPoints partitionsFromSplitPoints = new PartitionsFromSplitPoints(schema, Collections.emptyList());

        // When
        List<Partition> partitions = partitionsFromSplitPoints.construct();

        // Then
        assertThat(partitions).hasSize(1);
        Range expectedRange = rangeFactory.createRange(schema.getRowKeyFields().get(0),
                Long.MIN_VALUE, true, null, false);
        Region expectedRegion = new Region(expectedRange);
        Partition expectedPartition = Partition.builder()
                .id(partitions.get(0).getId())
                .parentPartitionId(null)
                .childPartitionIds(Collections.emptyList())
                .rowKeyTypes(schema.getRowKeyTypes())
                .dimension(-1)
                .leafPartition(true)
                .region(expectedRegion)
                .build();
        assertThat(partitions).containsExactly(expectedPartition);
    }

    @Test
    public void shouldCreateTreeWithOneRootAndTwoChildrenIfOneSplitPoint() {
        // Given
        Schema schema = Schema.builder().rowKeyFields(new Field("id", new LongType())).build();
        RangeFactory rangeFactory = new RangeFactory(schema);
        List<Object> splitPoints = Collections.singletonList(0L);
        PartitionsFromSplitPoints partitionsFromSplitPoints = new PartitionsFromSplitPoints(schema, splitPoints);

        // When
        List<Partition> partitions = partitionsFromSplitPoints.construct();

        // Then
        assertThat(partitions).hasSize(3);

        List<Partition> rootPartitions = partitions.stream()
                .filter(p -> null == p.getParentPartitionId())
                .collect(Collectors.toList());
        assertThat(rootPartitions).hasSize(1);
        Partition rootPartition = rootPartitions.get(0);
        List<Partition> childPartitions = partitions.stream()
                .filter(p -> null != p.getParentPartitionId())
                .collect(Collectors.toList());
        assertThat(childPartitions).hasSize(2);

        Range expectedRootRange = rangeFactory.createRange(schema.getRowKeyFields().get(0),
                Long.MIN_VALUE, true, null, false);
        Region expectedRootRegion = new Region(expectedRootRange);
        Partition expectedRootPartition = Partition.builder()
                .id(rootPartition.getId())
                .parentPartitionId(null)
                .childPartitionIds(childPartitions.get(0).getId(), childPartitions.get(1).getId())
                .rowKeyTypes(schema.getRowKeyTypes())
                .dimension(0)
                .leafPartition(false)
                .region(expectedRootRegion)
                .build();
        assertThat(rootPartition).isEqualTo(expectedRootPartition);

        Range expectedLeftRange = rangeFactory.createRange(schema.getRowKeyFields().get(0),
                Long.MIN_VALUE, true, 0L, false);
        Region expectedLeftRegion = new Region(expectedLeftRange);
        Partition expectedLeftPartition = Partition.builder()
                .id(childPartitions.get(0).getId())
                .parentPartitionId(rootPartition.getId())
                .childPartitionIds(Collections.emptyList())
                .rowKeyTypes(schema.getRowKeyTypes())
                .dimension(-1)
                .leafPartition(true)
                .region(expectedLeftRegion)
                .build();

        Range expectedRightRange = rangeFactory.createRange(schema.getRowKeyFields().get(0),
                0L, true, null, false);
        Region expectedRightRegion = new Region(expectedRightRange);
        Partition expectedRightPartition = Partition.builder()
                .id(childPartitions.get(1).getId())
                .parentPartitionId(rootPartition.getId())
                .childPartitionIds(Collections.emptyList())
                .rowKeyTypes(schema.getRowKeyTypes())
                .dimension(-1)
                .leafPartition(true)
                .region(expectedRightRegion)
                .build();

        assertThat(childPartitions).containsExactly(expectedLeftPartition, expectedRightPartition);
    }

    @Test
    public void shouldCreateTreeWithThreeLayersIfTwoSplitPoints() {
        // Given
        Schema schema = Schema.builder().rowKeyFields(new Field("id", new LongType())).build();
        RangeFactory rangeFactory = new RangeFactory(schema);
        List<Object> splitPoints = Arrays.asList(0L, 100L);
        PartitionsFromSplitPoints partitionsFromSplitPoints = new PartitionsFromSplitPoints(schema, splitPoints);

        // When
        List<Partition> partitions = partitionsFromSplitPoints.construct();

        // Then
        assertThat(partitions).hasSize(5);

        List<Partition> rootPartitions = partitions.stream()
                .filter(p -> null == p.getParentPartitionId())
                .collect(Collectors.toList());
        assertThat(rootPartitions).hasSize(1);
        Partition rootPartition = rootPartitions.get(0);

        List<Partition> leafPartitions = partitions.stream()
                .filter(Partition::isLeafPartition)
                .collect(Collectors.toList());
        assertThat(leafPartitions).hasSize(3);

        List<Partition> internalPartitions = partitions.stream()
                .filter(p -> !p.isLeafPartition())
                .filter(p -> null != p.getParentPartitionId())
                .collect(Collectors.toList());
        assertThat(internalPartitions).hasSize(1);
        Partition internalPartition = internalPartitions.get(0);

        Range expectedRootRange = rangeFactory.createRange(schema.getRowKeyFields().get(0),
                Long.MIN_VALUE, true, null, false);
        Region expectedRootRegion = new Region(expectedRootRange);
        Partition expectedRootPartition = Partition.builder()
                .id(rootPartition.getId())
                .parentPartitionId(null)
                .childPartitionIds(internalPartition.getId(), leafPartitions.get(2).getId())
                .rowKeyTypes(schema.getRowKeyTypes())
                .dimension(0)
                .leafPartition(false)
                .region(expectedRootRegion)
                .build();
        assertThat(rootPartition).isEqualTo(expectedRootPartition);

        Range expectedInternalRange = rangeFactory.createRange(schema.getRowKeyFields().get(0),
                Long.MIN_VALUE, true, 100L, false);
        Region expectedInternalRegion = new Region(expectedInternalRange);
        Partition expectedInternalPartition = Partition.builder()
                .id(internalPartition.getId())
                .parentPartitionId(rootPartition.getId())
                .childPartitionIds(leafPartitions.get(0).getId(), leafPartitions.get(1).getId())
                .rowKeyTypes(schema.getRowKeyTypes())
                .dimension(0)
                .leafPartition(false)
                .region(expectedInternalRegion)
                .build();
        assertThat(internalPartition).isEqualTo(expectedInternalPartition);

        Range expectedLeafPartition0Range = rangeFactory.createRange(schema.getRowKeyFields().get(0),
                Long.MIN_VALUE, true, 0L, false);
        Region expectedLeafPartition0Region = new Region(expectedLeafPartition0Range);
        Partition expectedLeafPartition0 = Partition.builder()
                .id(leafPartitions.get(0).getId())
                .parentPartitionId(internalPartitions.get(0).getId())
                .childPartitionIds(Collections.emptyList())
                .rowKeyTypes(schema.getRowKeyTypes())
                .dimension(-1)
                .leafPartition(true)
                .region(expectedLeafPartition0Region)
                .build();

        Range expectedLeafPartition1Range = rangeFactory.createRange(schema.getRowKeyFields().get(0),
                0L, true, 100L, false);
        Region expectedLeafPartition1Region = new Region(expectedLeafPartition1Range);
        Partition expectedLeafPartition1 = Partition.builder()
                .id(leafPartitions.get(1).getId())
                .parentPartitionId(internalPartitions.get(0).getId())
                .childPartitionIds(Collections.emptyList())
                .rowKeyTypes(schema.getRowKeyTypes())
                .dimension(-1)
                .leafPartition(true)
                .region(expectedLeafPartition1Region)
                .build();

        Range expectedLeafPartition2Range = rangeFactory.createRange(schema.getRowKeyFields().get(0),
                100L, true, null, false);
        Region expectedLeafPartition2Region = new Region(expectedLeafPartition2Range);
        Partition expectedLeafPartition2 = Partition.builder()
                .id(leafPartitions.get(2).getId())
                .parentPartitionId(rootPartition.getId())
                .childPartitionIds(Collections.emptyList())
                .rowKeyTypes(schema.getRowKeyTypes())
                .dimension(-1)
                .leafPartition(true)
                .region(expectedLeafPartition2Region)
                .build();

        assertThat(leafPartitions).containsExactly(
                expectedLeafPartition0, expectedLeafPartition1, expectedLeafPartition2);
    }

    @Test
    public void shouldCreateTreeWithXLayersIf63SplitPoints() {
        // Given
        Schema schema = Schema.builder().rowKeyFields(new Field("id", new LongType())).build();
        RangeFactory rangeFactory = new RangeFactory(schema);
        List<Object> splitPoints = new ArrayList<>();
        for (int i = 0; i < 63; i++) {
            splitPoints.add((long) i);
        }
        PartitionsFromSplitPoints partitionsFromSplitPoints = new PartitionsFromSplitPoints(schema, splitPoints);

        // When
        List<Partition> partitions = partitionsFromSplitPoints.construct();

        // Then
        //  - Number of partitions is 127
        //      (64 at bottom level, then 32, 16, 8, 4, 2, 1)
        assertThat(partitions).hasSize(127);

        List<Partition> rootPartitions = partitions.stream()
                .filter(p -> null == p.getParentPartitionId())
                .collect(Collectors.toList());
        assertThat(rootPartitions).hasSize(1);
        Partition rootPartition = rootPartitions.get(0);

        List<Partition> leafPartitions = partitions.stream()
                .filter(Partition::isLeafPartition)
                .collect(Collectors.toList());
        assertThat(leafPartitions).hasSize(64);

        List<Partition> internalPartitions = partitions.stream()
                .filter(p -> !p.isLeafPartition())
                .filter(p -> null != p.getParentPartitionId())
                .collect(Collectors.toList());
        assertThat(internalPartitions).hasSize(62);

        Set<String> leafPartitionIds = leafPartitions.stream().map(Partition::getId).collect(Collectors.toSet());
        Map<Integer, List<Partition>> levelToPartitions = new HashMap<>();
        Map<Integer, Set<String>> levelToPartitionIds = new HashMap<>();
        levelToPartitionIds.put(0, leafPartitionIds);

        for (int level = 1; level <= 5; level++) {
            int levelBelow = level - 1;
            List<Partition> partitionsAtLevel = internalPartitions.stream()
                    .filter(p -> levelToPartitionIds.get(levelBelow).contains(p.getChildPartitionIds().get(0)))
                    .collect(Collectors.toList());
            Set<String> partitionIdsAtLevel = partitionsAtLevel.stream().map(Partition::getId).collect(Collectors.toSet());
            levelToPartitions.put(level, partitionsAtLevel);
            levelToPartitionIds.put(level, partitionIdsAtLevel);
        }

        assertThat(levelToPartitions.get(1)).hasSize(32);
        assertThat(levelToPartitions.get(2)).hasSize(16);
        assertThat(levelToPartitions.get(3)).hasSize(8);
        assertThat(levelToPartitions.get(4)).hasSize(4);
        assertThat(levelToPartitions.get(5)).hasSize(2);

        Range expectedRootRange = rangeFactory.createRange(schema.getRowKeyFields().get(0),
                Long.MIN_VALUE, true, null, false);
        Region expectedRootRegion = new Region(expectedRootRange);
        Partition expectedRootPartition = Partition.builder()
                .id(rootPartition.getId())
                .parentPartitionId(null)
                .childPartitionIds(levelToPartitions.get(5).get(0).getId(), levelToPartitions.get(5).get(1).getId())
                .rowKeyTypes(schema.getRowKeyTypes())
                .dimension(0)
                .leafPartition(false)
                .region(expectedRootRegion)
                .build();
        assertThat(rootPartition).isEqualTo(expectedRootPartition);

        for (int level = 1; level <= 5; level++) {
            List<Partition> partitionsAtLevel = levelToPartitions.get(level);
            Set<String> partitionIdsOfLevelAbove = level < 5 ? levelToPartitionIds.get(level + 1) : Collections.singleton(rootPartition.getId());
            Set<String> partitionIdsOfLevelBelow = levelToPartitionIds.get(level - 1);
            for (Partition partition : partitionsAtLevel) {
                assertThat(partition.isLeafPartition()).isFalse();
                assertThat(partitionIdsOfLevelAbove).contains(partition.getParentPartitionId());
                assertThat(partitionIdsOfLevelBelow).contains(
                        partition.getChildPartitionIds().get(0), partition.getChildPartitionIds().get(1));
            }
        }
    }
}
