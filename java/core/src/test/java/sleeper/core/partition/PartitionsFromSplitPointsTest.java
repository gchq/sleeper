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

import org.assertj.core.api.recursive.comparison.RecursiveComparisonConfiguration;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import sleeper.core.range.Range;
import sleeper.core.range.Range.RangeFactory;
import sleeper.core.range.Region;
import sleeper.core.schema.Field;
import sleeper.core.schema.Schema;
import sleeper.core.schema.type.ByteArrayType;
import sleeper.core.schema.type.IntType;
import sleeper.core.schema.type.LongType;
import sleeper.core.schema.type.StringType;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

public class PartitionsFromSplitPointsTest {
    private static final RecursiveComparisonConfiguration IGNORE_IDS = RecursiveComparisonConfiguration.builder()
            .withIgnoredFields("id", "parentPartitionId", "childPartitionIds").build();

    @Nested
    @DisplayName("Schema with long row key")
    class SchemaWithLongRowKey {
        @Test
        void shouldCreateTreeWithOneRootNodeIfNoSplitPoints() {
            // Given
            Schema schema = Schema.builder().rowKeyFields(new Field("id", new LongType())).build();
            RangeFactory rangeFactory = new RangeFactory(schema);
            PartitionsFromSplitPoints partitionsFromSplitPoints = new PartitionsFromSplitPoints(schema, Collections.emptyList());

            // When / Then
            assertThat(partitionsFromSplitPoints.construct())
                    .containsExactlyElementsOf(new PartitionsBuilder(schema)
                            .rootFirst("root")
                            .buildList());
        }

        @Test
        void shouldCreateTreeWithOneRootAndTwoChildrenIfOneSplitPoint() {
            // Given
            Schema schema = Schema.builder().rowKeyFields(new Field("id", new LongType())).build();
            RangeFactory rangeFactory = new RangeFactory(schema);
            List<Object> splitPoints = List.of(0L);
            PartitionsFromSplitPoints partitionsFromSplitPoints = new PartitionsFromSplitPoints(schema, splitPoints);

            // When / Then
            assertThat(partitionsFromSplitPoints.construct())
                    .usingRecursiveFieldByFieldElementComparatorIgnoringFields("id", "parentPartitionId", "childPartitionIds")
                    .containsExactlyInAnyOrderElementsOf(new PartitionsBuilder(schema)
                            .rootFirst("root")
                            .splitToNewChildren("root", "left", "right", 0L)
                            .buildList());
        }

        @Test
        void shouldCreateTreeWithThreeLayersIfTwoSplitPoints() {
            // Given
            Schema schema = Schema.builder().rowKeyFields(new Field("id", new LongType())).build();
            RangeFactory rangeFactory = new RangeFactory(schema);
            List<Object> splitPoints = List.of(0L, 100L);
            PartitionsFromSplitPoints partitionsFromSplitPoints = new PartitionsFromSplitPoints(schema, splitPoints);

            // When / Then
            assertThat(partitionsFromSplitPoints.construct())
                    .usingRecursiveFieldByFieldElementComparator(IGNORE_IDS)
                    .containsExactlyInAnyOrderElementsOf(new PartitionsBuilder(schema)
                            .rootFirst("root")
                            .splitToNewChildren("root", "L", "R", 100L)
                            .splitToNewChildren("L", "LL", "LR", 0L)
                            .buildList());
        }

        @Test
        void shouldCreateTreeWithXLayersIf63SplitPoints() {
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

    @Nested
    @DisplayName("Schema with int row key")
    class SchemaWithIntRowKey {
        @Test
        void shouldCreateTreeWithOneRootNodeIfNoSplitPoints() {
            // Given
            Schema schema = Schema.builder().rowKeyFields(new Field("id", new IntType())).build();
            RangeFactory rangeFactory = new RangeFactory(schema);
            List<Object> splitPoints = List.of();
            PartitionsFromSplitPoints partitionsFromSplitPoints = new PartitionsFromSplitPoints(schema, splitPoints);

            // When / Then
            assertThat(partitionsFromSplitPoints.construct())
                    .usingRecursiveFieldByFieldElementComparator(IGNORE_IDS)
                    .containsExactlyInAnyOrderElementsOf(new PartitionsBuilder(schema)
                            .rootFirst("root")
                            .buildList());
        }

        @Test
        void shouldCreateTreeWithIntKeyAndOneSplitPoint() {
            // Given
            Schema schema = Schema.builder().rowKeyFields(new Field("id", new IntType())).build();
            List<Object> splitPoints = List.of(-10);
            PartitionsFromSplitPoints partitionsFromSplitPoints = new PartitionsFromSplitPoints(schema, splitPoints);

            // When / Then
            assertThat(partitionsFromSplitPoints.construct())
                    .usingRecursiveFieldByFieldElementComparator(IGNORE_IDS)
                    .containsExactlyInAnyOrderElementsOf(new PartitionsBuilder(schema)
                            .rootFirst("root")
                            .splitToNewChildren("root", "L", "R", -10)
                            .buildList());
        }

        @Test
        void shouldCreateTreeWithMultipleSplitPoints() {
            // Given
            Schema schema = Schema.builder().rowKeyFields(new Field("id", new IntType())).build();
            List<Object> splitPoints = List.of(-10, 0, 1000);
            PartitionsFromSplitPoints partitionsFromSplitPoints = new PartitionsFromSplitPoints(schema, splitPoints);

            // When / Then
            assertThat(partitionsFromSplitPoints.construct())
                    .usingRecursiveFieldByFieldElementComparator(IGNORE_IDS)
                    .containsExactlyInAnyOrderElementsOf(new PartitionsBuilder(schema)
                            .rootFirst("root")
                            .splitToNewChildren("root", "L", "R", 0)
                            .splitToNewChildren("R", "RL", "RR", 1000)
                            .splitToNewChildren("L", "LL", "LR", -10)
                            .buildList());
        }

        @Test
        void shouldCreateTreeWithMultipleSplitPointAndMultiDimRowKey() {
            // Given
            Schema schema = Schema.builder()
                    .rowKeyFields(
                            new Field("key0", new IntType()),
                            new Field("key1", new LongType()),
                            new Field("key2", new StringType()),
                            new Field("key3", new ByteArrayType()))
                    .build();
            List<Object> splitPoints = List.of(-10, 0, 1000);
            PartitionsFromSplitPoints partitionsFromSplitPoints = new PartitionsFromSplitPoints(schema, splitPoints);

            // When / Then
            assertThat(partitionsFromSplitPoints.construct())
                    .usingRecursiveFieldByFieldElementComparator(IGNORE_IDS)
                    .containsExactlyInAnyOrderElementsOf(new PartitionsBuilder(schema)
                            .rootFirst("root")
                            .splitToNewChildrenOnDimension("root", "L", "R", 0, 0)
                            .splitToNewChildrenOnDimension("R", "RL", "RR", 0, 1000)
                            .splitToNewChildrenOnDimension("L", "LL", "LR", 0, -10)
                            .buildList());
        }
    }
}
