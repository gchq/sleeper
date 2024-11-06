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
package sleeper.bulkimport.runner.rdd;

import org.apache.spark.broadcast.Broadcast;
import org.junit.jupiter.api.Test;

import sleeper.core.key.Key;
import sleeper.core.partition.Partition;
import sleeper.core.partition.PartitionTree;
import sleeper.core.partition.PartitionsBuilder;
import sleeper.core.schema.Field;
import sleeper.core.schema.Schema;
import sleeper.core.schema.SchemaSerDe;
import sleeper.core.schema.type.ByteArrayType;
import sleeper.core.schema.type.IntType;
import sleeper.core.schema.type.StringType;

import java.util.ArrayList;
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
        PartitionTree partitionTree = new PartitionsBuilder(schema)
                .rootFirst("root")
                .buildTree();
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
            keys.add(Key.create(objs));
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
        PartitionTree partitionTree = new PartitionsBuilder(schema)
                .rootFirst("root")
                .splitToNewChildren("root", "left", "right", 50)
                .buildTree();
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
            keys.add(Key.create(objs));
        }
        Set<Integer> leftPartitionIds = getPartitionNumbers(keys, partitioner);
        assertThat(leftPartitionIds).hasSize(1);
        //  - Keys in right-hand partition
        keys.clear();
        for (int i = 50; i < 10_000; i += 500) {
            List<Object> objs = new ArrayList<>();
            objs.add(i);
            objs.add(new byte[]{(byte) i});
            keys.add(Key.create(objs));
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
        PartitionTree tree = new PartitionsBuilder(schema)
                .rootFirst("root")
                .splitToNewChildrenOnDimension("root", "leftPartition", "rightPartition", 0, 50)
                .splitToNewChildrenOnDimension("leftPartition", "leftLower", "leftUpper", 1, new byte[]{20})
                .buildTree();

        Broadcast<List<Partition>> mockedBroadcast = mock(Broadcast.class);
        when(mockedBroadcast.getValue()).thenReturn(tree.getAllPartitions());

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
                keys.add(Key.create(objs));
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
                keys.add(Key.create(objs));
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
                keys.add(Key.create(objs));
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
