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
package sleeper.bulkimport.job.runner.rdd;

import org.apache.spark.broadcast.Broadcast;
import org.junit.jupiter.api.Test;

import sleeper.core.key.Key;
import sleeper.core.partition.Partition;
import sleeper.core.partition.PartitionTree;
import sleeper.core.partition.PartitionsFromSplitPoints;
import sleeper.core.range.Range;
import sleeper.core.range.Range.RangeFactory;
import sleeper.core.range.Region;
import sleeper.core.schema.Field;
import sleeper.core.schema.Schema;
import sleeper.core.schema.SchemaSerDe;
import sleeper.core.schema.type.ByteArrayType;
import sleeper.core.schema.type.IntType;
import sleeper.core.schema.type.StringType;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class SleeperPartitionerTest {

    private static Schema getSchema() {
        return Schema.builder()
                .rowKeyFields(new Field("key1", new IntType()), new Field("key2", new ByteArrayType()))
                .sortKeyFields(new Field("sort1", new StringType()), new Field("sort2", new ByteArrayType()), new Field("sort3", new StringType()))
                .valueFields(new Field("value", new StringType()))
                .build();
    }
    

    @Test
    public void shouldGiveCorrectResultsWith1LeafPartition() {
        // Given
        Schema schema = getSchema();
        String schemaAsString = new SchemaSerDe().toJson(schema);
        Broadcast<List<Partition>> mockedBroadcast = mock(Broadcast.class);
        PartitionTree partitionTree = PartitionsFromSplitPoints.treeFrom(schema, Collections.emptyList());
        when(mockedBroadcast.getValue()).thenReturn(partitionTree.getAllPartitions());

        // When
        SleeperPartitioner partitioner = new SleeperPartitioner(schemaAsString, mockedBroadcast);

        // Then
        assertThat(partitioner.numPartitions()).isEqualTo(1);
        List<Key> keys = new ArrayList<>();
        for (int i = -1_000_000; i < 1_000_000; i += 10_000_000) {
            List<Object> objs = new ArrayList<>();
            objs.add(i);
            objs.add(new byte[]{(byte) i});
            Key key = Key.create(objs);
            keys.add(key);
        }
        Set<Integer> partitionIds = getPartitionNumbers(keys, partitioner);
        assertThat(partitionIds).hasSize(1);
        assertThat(partitionIds).containsExactly(0);
    }

    @Test
    public void shouldGiveCorrectResultsWith2LeafPartitions() {
        // Given
        Schema schema = getSchema();
        String schemaAsString = new SchemaSerDe().toJson(schema);
        Broadcast<List<Partition>> mockedBroadcast = mock(Broadcast.class);
        PartitionTree partitionTree = PartitionsFromSplitPoints.treeFrom(schema, Collections.singletonList(50));
        when(mockedBroadcast.getValue()).thenReturn(partitionTree.getAllPartitions());

        // When
        SleeperPartitioner partitioner = new SleeperPartitioner(schemaAsString, mockedBroadcast);

        // Then
        assertThat(partitioner.numPartitions()).isEqualTo(2);
        //  - Keys in left-hand partition
        List<Key> keys = new ArrayList<>();
        for (int i = -1000; i < 50; i++) {
            List<Object> objs = new ArrayList<>();
            objs.add(i);
            objs.add(new byte[]{(byte) i});
            Key key = Key.create(objs);
            keys.add(key);
        }
        Set<Integer> leftPartitionIds = getPartitionNumbers(keys, partitioner);
        assertThat(leftPartitionIds).hasSize(1);
        //  - Keys in right-hand partition
        keys.clear();
        for (int i = 50; i < 10_000; i += 500) {
            List<Object> objs = new ArrayList<>();
            objs.add(i);
            objs.add(new byte[]{(byte) i});
            Key key = Key.create(objs);
            keys.add(key);
        }
        Set<Integer> rightPartitionIds = getPartitionNumbers(keys, partitioner);
        assertThat(rightPartitionIds).hasSize(1);
        //  - Partition ids should be 0 and 1
        Set<Integer> allIds = new HashSet<>();
        allIds.addAll(leftPartitionIds);
        allIds.addAll(rightPartitionIds);
        assertThat(allIds).containsExactlyInAnyOrder(0, 1);
    }

    @Test
    public void shouldGiveCorrectResultWhenSplitHasHappenedOnSecondDimension() {
        // Given
        Schema schema = getSchema();
        String schemaAsString = new SchemaSerDe().toJson(schema);
        RangeFactory rangeFactory = new RangeFactory(schema);

        PartitionTree partitionTree = PartitionsFromSplitPoints.treeFrom(schema, Collections.singletonList(50));
        Partition leftLeafPartition = partitionTree.getLeafPartition(Key.create(Arrays.asList(0, new byte[]{0})));
        //  - Split the left-hand partition on the second dimension
        Region leftLeafRegion = leftLeafPartition.getRegion();

        List<Range> leftUpperRanges = new ArrayList<>();
        leftUpperRanges.add(leftLeafRegion.getRange("key1"));
        leftUpperRanges.add(rangeFactory.createRange(schema.getRowKeyFields().get(1), new byte[]{20}, null));
        Region leftUpperRegion = new Region(leftUpperRanges);
        Partition leftUpperPartition = Partition.builder()
                .id("leftupper")
                .parentPartitionId(leftLeafPartition.getId())
                .region(leftUpperRegion)
                .rowKeyTypes(schema.getRowKeyTypes())
                .leafPartition(true)
                .build();

        List<Range> leftLowerRanges = new ArrayList<>();
        leftLowerRanges.add(leftLeafRegion.getRange("key1"));
        leftLowerRanges.add(rangeFactory.createRange(schema.getRowKeyFields().get(1), new byte[]{}, new byte[]{20}));
        Region leftLowerRegion = new Region(leftLowerRanges);
        Partition leftLowerPartition = Partition.builder()
                .id("leftlower")
                .parentPartitionId(leftLeafPartition.getId())
                .region(leftLowerRegion)
                .rowKeyTypes(schema.getRowKeyTypes())
                .leafPartition(true)
                .build();

        leftLeafPartition = leftLeafPartition.toBuilder().leafPartition(false).childPartitionIds(Arrays.asList("leftupper", "leftlower")).build();

        List<Partition> partitionsAfterSplit = new ArrayList<>();
        partitionsAfterSplit.add(partitionTree.getRootPartition());
        partitionsAfterSplit.add(leftLeafPartition);
        partitionsAfterSplit.add(partitionTree.getLeafPartition(Key.create(Arrays.asList(100, new byte[]{0}))));
        partitionsAfterSplit.add(leftUpperPartition);
        partitionsAfterSplit.add(leftLowerPartition);
        PartitionTree newPartitionTree = new PartitionTree(schema, partitionsAfterSplit);

        Broadcast<List<Partition>> mockedBroadcast = mock(Broadcast.class);
        when(mockedBroadcast.getValue()).thenReturn(newPartitionTree.getAllPartitions());

        // When
        SleeperPartitioner partitioner = new SleeperPartitioner(schemaAsString, mockedBroadcast);

        // Then
        assertThat(partitioner.numPartitions()).isEqualTo(3);
        //  - Keys in left-hand upper partition
        List<Key> keys = new ArrayList<>();
        for (int i = -1000; i < 50; i += 100) {
            for (byte b = 20; b < 100; b += 20) {
                List<Object> objs = new ArrayList<>();
                objs.add(i);
                objs.add(new byte[]{b});
                Key key = Key.create(objs);
                keys.add(key);
            }
        }
        Set<Integer> leftUpperPartitionIds = getPartitionNumbers(keys, partitioner);
        assertThat(leftUpperPartitionIds).hasSize(1);
        //  - Keys in left-hand lower partition
        keys.clear();
        for (int i = -1000; i < 50; i += 100) {
            for (byte b = -50; b < 2; b += 20) {
                List<Object> objs = new ArrayList<>();
                objs.add(i);
                objs.add(new byte[]{b});
                Key key = Key.create(objs);
                keys.add(key);
            }
        }
        Set<Integer> leftLowerPartitionIds = getPartitionNumbers(keys, partitioner);
        assertThat(leftLowerPartitionIds).hasSize(1);
        //  - Keys in right-hand partition
        keys.clear();
        for (int i = 50; i < 1000; i += 100) {
            for (byte b = -50; b < 100; b += 20) {
                List<Object> objs = new ArrayList<>();
                objs.add(i);
                objs.add(new byte[]{b});
                Key key = Key.create(objs);
                keys.add(key);
            }
        }
        Set<Integer> rightPartitionIds = getPartitionNumbers(keys, partitioner);
        assertThat(rightPartitionIds).hasSize(1);

        Set<Integer> allPartitionNumbers = new HashSet<>();
        allPartitionNumbers.addAll(leftUpperPartitionIds);
        allPartitionNumbers.addAll(leftLowerPartitionIds);
        allPartitionNumbers.addAll(rightPartitionIds);
        assertThat(allPartitionNumbers).containsExactlyInAnyOrder(0, 1, 2);
    }

    private Set<Integer> getPartitionNumbers(List<Key> keys, SleeperPartitioner partitioner) {
        Set<Integer> partitionIds = new HashSet<>();
        for (Key key : keys) {
            partitionIds.add(partitioner.getPartition(key));
        }
        return partitionIds;
    }
}
