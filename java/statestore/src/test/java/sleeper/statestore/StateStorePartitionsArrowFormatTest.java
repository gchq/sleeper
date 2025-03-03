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
package sleeper.statestore;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.junit.jupiter.api.Test;

import sleeper.core.partition.Partition;
import sleeper.core.partition.PartitionTree;
import sleeper.core.partition.PartitionsBuilder;
import sleeper.core.schema.Field;
import sleeper.core.schema.Schema;
import sleeper.core.schema.type.ByteArrayType;
import sleeper.core.schema.type.IntType;
import sleeper.core.schema.type.LongType;
import sleeper.core.schema.type.StringType;
import sleeper.statestore.StateStorePartitionsArrowFormat.ReadResult;
import sleeper.statestore.StateStorePartitionsArrowFormat.WriteResult;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.nio.channels.Channels;
import java.util.List;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.core.schema.SchemaTestHelper.schemaWithKey;

public class StateStorePartitionsArrowFormatTest {

    private final BufferAllocator allocator = new RootAllocator();

    @Test
    void shouldWritePartitionsSplitOnOneStringField() throws Exception {
        // Given
        Schema schema = schemaWithKey("key", new StringType());
        PartitionTree tree = new PartitionsBuilder(schema)
                .rootFirst("root")
                .splitToNewChildren("root", "L", "R", "mmm")
                .buildTree();
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();

        // When
        write(tree.getAllPartitions(), bytes);

        // Then
        assertThat(read(bytes)).isEqualTo(tree.getAllPartitions());
    }

    @Test
    void shouldWritePartitionsSplitOnOneLongField() throws Exception {
        // Given
        Schema schema = schemaWithKey("key", new LongType());
        PartitionTree tree = new PartitionsBuilder(schema)
                .rootFirst("root")
                .splitToNewChildren("root", "L", "R", 123L)
                .buildTree();
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();

        // When
        write(tree.getAllPartitions(), bytes);

        // Then
        assertThat(read(bytes)).isEqualTo(tree.getAllPartitions());
    }

    @Test
    void shouldWritePartitionsSplitOnOneIntField() throws Exception {
        // Given
        Schema schema = schemaWithKey("key", new IntType());
        PartitionTree tree = new PartitionsBuilder(schema)
                .rootFirst("root")
                .splitToNewChildren("root", "L", "R", 123)
                .buildTree();
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();

        // When
        write(tree.getAllPartitions(), bytes);

        // Then
        assertThat(read(bytes)).isEqualTo(tree.getAllPartitions());
    }

    @Test
    void shouldWritePartitionsSplitOnOneByteArrayField() throws Exception {
        // Given
        Schema schema = schemaWithKey("key", new ByteArrayType());
        PartitionTree tree = new PartitionsBuilder(schema)
                .rootFirst("root")
                .splitToNewChildren("root", "L", "R", new byte[]{123})
                .buildTree();
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();

        // When
        write(tree.getAllPartitions(), bytes);

        // Then
        assertThat(read(bytes)).isEqualTo(tree.getAllPartitions());
    }

    @Test
    void shouldWriteOnePartitionWithMultipleStringFields() throws Exception {
        // Given
        Schema schema = Schema.builder().rowKeyFields(
                new Field("key1", new StringType()),
                new Field("key2", new StringType())).build();
        PartitionTree tree = new PartitionsBuilder(schema)
                .rootFirst("root")
                .buildTree();
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();

        // When
        write(tree.getAllPartitions(), bytes);

        // Then
        assertThat(read(bytes)).isEqualTo(tree.getAllPartitions());
    }

    @Test
    void shouldWriteOnePartitionWithMultipleFieldsOfDifferentTypes() throws Exception {
        // Given
        Schema schema = Schema.builder().rowKeyFields(
                new Field("key1", new StringType()),
                new Field("key2", new LongType()),
                new Field("key3", new IntType()),
                new Field("key4", new ByteArrayType())).build();
        PartitionTree tree = new PartitionsBuilder(schema)
                .rootFirst("root")
                .buildTree();
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();

        // When
        write(tree.getAllPartitions(), bytes);

        // Then
        assertThat(read(bytes)).isEqualTo(tree.getAllPartitions());
    }

    @Test
    void shouldWritePartitionsSplitOnOneStringFieldOverMultipleLevels() throws Exception {
        // Given
        Schema schema = schemaWithKey("key", new StringType());
        PartitionTree tree = new PartitionsBuilder(schema)
                .rootFirst("root")
                .splitToNewChildren("root", "L", "R", "mmm")
                .splitToNewChildren("L", "LL", "LR", "ccc")
                .splitToNewChildren("R", "RL", "RR", "ttt")
                .buildTree();
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();

        // When
        write(tree.getAllPartitions(), bytes);

        // Then
        assertThat(read(bytes)).isEqualTo(tree.getAllPartitions());
    }

    @Test
    void shouldWritePartitionsSplitOnDifferentDimensions() throws Exception {
        // Given
        Schema schema = Schema.builder().rowKeyFields(
                new Field("key1", new StringType()),
                new Field("key2", new StringType())).build();
        PartitionTree tree = new PartitionsBuilder(schema)
                .rootFirst("root")
                .splitToNewChildrenOnDimension("root", "L", "R", 0, "mmm")
                .splitToNewChildrenOnDimension("L", "LL", "LR", 1, "ccc")
                .splitToNewChildrenOnDimension("R", "RL", "RR", 1, "ttt")
                .buildTree();
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();

        // When
        write(tree.getAllPartitions(), bytes);

        // Then
        assertThat(read(bytes)).isEqualTo(tree.getAllPartitions());
    }

    @Test
    void shouldWriteNoPartitions() throws Exception {
        // Given
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();

        // When
        write(List.of(), bytes);

        // Then
        assertThat(read(bytes)).isEmpty();
    }

    @Test
    void shouldWriteMorePartitionsThanBatchSize() throws Exception {
        // Given
        List<Partition> partitions = new PartitionsBuilder(schemaWithKey("key", new StringType()))
                .rootFirst("root")
                .splitToNewChildren("root", "L", "R", "m")
                .buildList();
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();

        // When
        WriteResult writeResult = writeWithMaxElementsInBatch(2, partitions, bytes);
        ReadResult readResult = readResult(bytes);

        // Then
        assertThat(readResult.partitions()).isEqualTo(partitions);
        assertThat(writeResult.numBatches()).isEqualTo(2).isEqualTo(readResult.numBatches());
    }

    @Test
    void shouldWritePartitionTreeInBatches() throws Exception {
        // Given
        PartitionTree tree = new PartitionsBuilder(schemaWithKey("key", new StringType()))
                .rootFirst("root")
                .splitToNewChildren("root", "L", "R", "c")
                .splitToNewChildren("L", "LL", "LR", "b")
                .buildTree();
        // List<Partition> partitions = Stream.of("LL", "LR", "R", "L", "root").map(tree::getPartition).toList();
        List<Partition> partitions = Stream.of("root", "L", "R", "L", "LL", "LR").map(tree::getPartition).toList();
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();

        // When
        WriteResult writeResult = writeWithMaxElementsInBatch(2, partitions, bytes);
        ReadResult readResult = readResult(bytes);

        // Then
        assertThat(readResult.partitions()).isEqualTo(partitions);
        assertThat(writeResult.numBatches()).isEqualTo(3).isEqualTo(readResult.numBatches());
    }

    private void write(List<Partition> partitions, ByteArrayOutputStream stream) throws Exception {
        writeWithMaxElementsInBatch(10, partitions, stream);
    }

    private WriteResult writeWithMaxElementsInBatch(int maxElementsInBatch, List<Partition> partitions, ByteArrayOutputStream stream) throws Exception {
        return StateStorePartitionsArrowFormat.write(partitions, allocator, Channels.newChannel(stream), maxElementsInBatch);
    }

    private List<Partition> read(ByteArrayOutputStream stream) throws Exception {
        return readResult(stream).partitions();
    }

    private ReadResult readResult(ByteArrayOutputStream stream) throws Exception {
        return StateStorePartitionsArrowFormat.read(allocator,
                Channels.newChannel(new ByteArrayInputStream(stream.toByteArray())));
    }
}
