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
import sleeper.core.range.Range;
import sleeper.core.range.Region;
import sleeper.core.schema.Field;
import sleeper.core.schema.Schema;
import sleeper.core.schema.type.LongType;
import sleeper.core.schema.type.StringType;

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
        Schema schema = new Schema();
        schema.setRowKeyFields(new Field("id", new LongType()));
        PartitionsFromSplitPoints partitionsFromSplitPoints = new PartitionsFromSplitPoints(schema, Collections.emptyList());

        // When
        List<Partition> partitions = partitionsFromSplitPoints.construct();

        // Then
        assertThat(partitions).hasSize(1);
        Partition expectedPartition = new Partition();
        expectedPartition.setId(partitions.get(0).getId());
        expectedPartition.setParentPartitionId(null);
        expectedPartition.setChildPartitionIds(Collections.emptyList());
        expectedPartition.setRowKeyTypes(schema.getRowKeyTypes());
        expectedPartition.setDimension(-1);
        expectedPartition.setLeafPartition(true);
        Range expectedRange = new Range(schema.getRowKeyFields().get(0),
                Long.MIN_VALUE, true, null, false);
        Region expectedRegion = new Region(expectedRange);
        expectedPartition.setRegion(expectedRegion);
        assertThat(partitions.get(0)).isEqualTo(expectedPartition);
    }

    @Test
    public void shouldCreateTreeWithOneRootAndTwoChildrenIfOneSplitPoint() {
        // Given
        Schema schema = new Schema();
        schema.setRowKeyFields(new Field("id", new LongType()));
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

        Partition expectedRootPartition = new Partition();
        expectedRootPartition.setId(rootPartition.getId());
        expectedRootPartition.setParentPartitionId(null);
        List<String> childPartitionIds = new ArrayList<>();
        childPartitionIds.add(childPartitions.get(0).getId());
        childPartitionIds.add(childPartitions.get(1).getId());
        expectedRootPartition.setChildPartitionIds(childPartitionIds);
        expectedRootPartition.setRowKeyTypes(schema.getRowKeyTypes());
        expectedRootPartition.setDimension(0);
        expectedRootPartition.setLeafPartition(false);
        Range expectedRootRange = new Range(schema.getRowKeyFields().get(0),
                Long.MIN_VALUE, true, null, false);
        Region expectedRootRegion = new Region(expectedRootRange);
        expectedRootPartition.setRegion(expectedRootRegion);
        assertThat(rootPartition).isEqualTo(expectedRootPartition);

        Partition expectedLeftPartition = new Partition();
        expectedLeftPartition.setId(childPartitions.get(0).getId());
        expectedLeftPartition.setParentPartitionId(rootPartition.getId());
        expectedLeftPartition.setChildPartitionIds(Collections.emptyList());
        expectedLeftPartition.setRowKeyTypes(schema.getRowKeyTypes());
        expectedLeftPartition.setDimension(-1);
        expectedLeftPartition.setLeafPartition(true);
        Range expectedLeftRange = new Range(schema.getRowKeyFields().get(0),
                Long.MIN_VALUE, true, 0L, false);
        Region expectedLeftRegion = new Region(expectedLeftRange);
        expectedLeftPartition.setRegion(expectedLeftRegion);
        assertThat(childPartitions.get(0)).isEqualTo(expectedLeftPartition);

        Partition expectedRightPartition = new Partition();
        expectedRightPartition.setId(childPartitions.get(1).getId());
        expectedRightPartition.setParentPartitionId(rootPartition.getId());
        expectedRightPartition.setChildPartitionIds(Collections.emptyList());
        expectedRightPartition.setRowKeyTypes(schema.getRowKeyTypes());
        expectedRightPartition.setDimension(-1);
        expectedRightPartition.setLeafPartition(true);
        Range expectedRightRange = new Range(schema.getRowKeyFields().get(0),
                0L, true, null, false);
        Region expectedRightRegion = new Region(expectedRightRange);
        expectedRightPartition.setRegion(expectedRightRegion);
        assertThat(childPartitions.get(1)).isEqualTo(expectedRightPartition);
    }

    @Test
    public void shouldCreateTreeWithThreeLayersIfTwoSplitPoints() {
        // Given
        Schema schema = new Schema();
        schema.setRowKeyFields(new Field("id", new LongType()));
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

        Partition expectedRootPartition = new Partition();
        expectedRootPartition.setId(rootPartition.getId());
        expectedRootPartition.setParentPartitionId(null);
        expectedRootPartition.setChildPartitionIds(Arrays.asList(internalPartition.getId(), leafPartitions.get(2).getId()));
        expectedRootPartition.setRowKeyTypes(schema.getRowKeyTypes());
        expectedRootPartition.setDimension(0);
        expectedRootPartition.setLeafPartition(false);
        Range expectedRootRange = new Range(schema.getRowKeyFields().get(0),
                Long.MIN_VALUE, true, null, false);
        Region expectedRootRegion = new Region(expectedRootRange);
        expectedRootPartition.setRegion(expectedRootRegion);
        assertThat(rootPartition).isEqualTo(expectedRootPartition);

        Partition expectedInternalPartition = new Partition();
        expectedInternalPartition.setId(internalPartition.getId());
        expectedInternalPartition.setParentPartitionId(rootPartition.getId());
        expectedInternalPartition.setChildPartitionIds(Arrays.asList(leafPartitions.get(0).getId(), leafPartitions.get(1).getId()));
        expectedInternalPartition.setRowKeyTypes(schema.getRowKeyTypes());
        expectedInternalPartition.setDimension(0);
        expectedInternalPartition.setLeafPartition(false);
        Range expectedInternalRange = new Range(schema.getRowKeyFields().get(0),
                Long.MIN_VALUE, true, 100L, false);
        Region expectedInternalRegion = new Region(expectedInternalRange);
        expectedInternalPartition.setRegion(expectedInternalRegion);
        assertThat(internalPartition).isEqualTo(expectedInternalPartition);

        Partition expectedLeafPartition0 = new Partition();
        expectedLeafPartition0.setId(leafPartitions.get(0).getId());
        expectedLeafPartition0.setParentPartitionId(internalPartitions.get(0).getId());
        expectedLeafPartition0.setChildPartitionIds(Collections.emptyList());
        expectedLeafPartition0.setRowKeyTypes(schema.getRowKeyTypes());
        expectedLeafPartition0.setDimension(-1);
        expectedLeafPartition0.setLeafPartition(true);
        Range expectedLeafPartition0Range = new Range(schema.getRowKeyFields().get(0),
                Long.MIN_VALUE, true, 0L, false);
        Region expectedLeafPartition0Region = new Region(expectedLeafPartition0Range);
        expectedLeafPartition0.setRegion(expectedLeafPartition0Region);
        assertThat(leafPartitions.get(0)).isEqualTo(expectedLeafPartition0);

        Partition expectedLeafPartition1 = new Partition();
        expectedLeafPartition1.setId(leafPartitions.get(1).getId());
        expectedLeafPartition1.setParentPartitionId(internalPartitions.get(0).getId());
        expectedLeafPartition1.setChildPartitionIds(Collections.emptyList());
        expectedLeafPartition1.setRowKeyTypes(schema.getRowKeyTypes());
        expectedLeafPartition1.setDimension(-1);
        expectedLeafPartition1.setLeafPartition(true);
        Range expectedLeafPartition1Range = new Range(schema.getRowKeyFields().get(0),
                0L, true, 100L, false);
        Region expectedLeafPartition1Region = new Region(expectedLeafPartition1Range);
        expectedLeafPartition1.setRegion(expectedLeafPartition1Region);
        assertThat(leafPartitions.get(1)).isEqualTo(expectedLeafPartition1);

        Partition expectedLeafPartition2 = new Partition();
        expectedLeafPartition2.setId(leafPartitions.get(2).getId());
        expectedLeafPartition2.setParentPartitionId(rootPartition.getId());
        expectedLeafPartition2.setChildPartitionIds(Collections.emptyList());
        expectedLeafPartition2.setRowKeyTypes(schema.getRowKeyTypes());
        expectedLeafPartition2.setDimension(-1);
        expectedLeafPartition2.setLeafPartition(true);
        Range expectedLeafPartition2Range = new Range(schema.getRowKeyFields().get(0),
                100L, true, null, false);
        Region expectedLeafPartition2Region = new Region(expectedLeafPartition2Range);
        expectedLeafPartition2.setRegion(expectedLeafPartition2Region);
        assertThat(leafPartitions.get(2)).isEqualTo(expectedLeafPartition2);
    }

    @Test
    public void shouldCreateTreeWithXLayersIf63SplitPoints() {
        // Given
        Schema schema = new Schema();
        schema.setRowKeyFields(new Field("id", new LongType()));
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

        Partition expectedRootPartition = new Partition();
        expectedRootPartition.setId(rootPartition.getId());
        expectedRootPartition.setParentPartitionId(null);
        expectedRootPartition.setChildPartitionIds(Arrays.asList(levelToPartitions.get(5).get(0).getId(), levelToPartitions.get(5).get(1).getId()));
        expectedRootPartition.setRowKeyTypes(schema.getRowKeyTypes());
        expectedRootPartition.setDimension(0);
        expectedRootPartition.setLeafPartition(false);
        Range expectedRootRange = new Range(schema.getRowKeyFields().get(0),
                Long.MIN_VALUE, true, null, false);
        Region expectedRootRegion = new Region(expectedRootRange);
        expectedRootPartition.setRegion(expectedRootRegion);
        assertThat(rootPartition).isEqualTo(expectedRootPartition);

        for (int level = 1; level <= 5; level++) {
            List<Partition> partitionsAtLevel = levelToPartitions.get(level);
            Set<String> partitionIdsOfLevelAbove = level < 5 ? levelToPartitionIds.get(level + 1) : Collections.singleton(rootPartition.getId());
            Set<String> partitionIdsOfLevelBelow = levelToPartitionIds.get(level - 1);
            for (Partition partition : partitionsAtLevel) {
                assertThat(partition.isLeafPartition()).isFalse();
                assertThat(partitionIdsOfLevelAbove).contains(partition.getParentPartitionId());
                assertThat(partitionIdsOfLevelBelow).contains(partition.getChildPartitionIds().get(0), partition.getChildPartitionIds().get(1));
            }
        }
    }

    @Test
    public void canGeneratePartitionIdsSequentiallyForTests() {
        // Given
        Field field = new Field("key1", new StringType());
        Schema schema = Schema.builder().rowKeyFields(field).build();
        List<Object> splitPoints = Arrays.asList("abc", "def");

        // When
        List<Partition> partitions = PartitionsFromSplitPoints.sequentialIds(schema, splitPoints);

        // Then
        PartitionsBuilder builder = new PartitionsBuilder(schema);
        Partition a = builder.partition("A", "", "abc");
        Partition b = builder.partition("B", "abc", "def");
        Partition c = builder.partition("C", "def", null);
        Partition d = builder.parent(Arrays.asList(a, b), "D", "", "def");
        Partition e = builder.parent(Arrays.asList(d, c), "E", "", null);
        assertThat(partitions).containsExactly(a, b, c, d, e);
    }
}
