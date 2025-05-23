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
import static sleeper.core.schema.SchemaTestHelper.createSchemaWithKey;

public class StateStorePartitionsArrowFormatTest {

    private final BufferAllocator allocator = new RootAllocator();

    @Test
    void shouldWritePartitionsSplitOnOneStringField() throws Exception {
        // Given
        Schema schema = createSchemaWithKey("key", new StringType());
        List<Partition> partitions = new PartitionsBuilder(schema)
                .rootFirst("root")
                .splitToNewChildren("root", "L", "R", "mmm")
                .buildTree().traverseLeavesFirst().toList();
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();

        // When
        write(partitions, bytes);

        // Then
        assertThat(read(bytes)).isEqualTo(partitions);
    }

    @Test
    void shouldWritePartitionsSplitOnOneLongField() throws Exception {
        // Given
        Schema schema = createSchemaWithKey("key", new LongType());
        List<Partition> partitions = new PartitionsBuilder(schema)
                .rootFirst("root")
                .splitToNewChildren("root", "L", "R", 123L)
                .buildTree().traverseLeavesFirst().toList();
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();

        // When
        write(partitions, bytes);

        // Then
        assertThat(read(bytes)).isEqualTo(partitions);
    }

    @Test
    void shouldWritePartitionsSplitOnOneIntField() throws Exception {
        // Given
        Schema schema = createSchemaWithKey("key", new IntType());
        List<Partition> partitions = new PartitionsBuilder(schema)
                .rootFirst("root")
                .splitToNewChildren("root", "L", "R", 123)
                .buildTree().traverseLeavesFirst().toList();
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();

        // When
        write(partitions, bytes);

        // Then
        assertThat(read(bytes)).isEqualTo(partitions);
    }

    @Test
    void shouldWritePartitionsSplitOnOneByteArrayField() throws Exception {
        // Given
        Schema schema = createSchemaWithKey("key", new ByteArrayType());
        List<Partition> partitions = new PartitionsBuilder(schema)
                .rootFirst("root")
                .splitToNewChildren("root", "L", "R", new byte[]{123})
                .buildTree().traverseLeavesFirst().toList();
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();

        // When
        write(partitions, bytes);

        // Then
        assertThat(read(bytes)).isEqualTo(partitions);
    }

    @Test
    void shouldWriteOnePartitionWithMultipleStringFields() throws Exception {
        // Given
        Schema schema = Schema.builder().rowKeyFields(
                new Field("key1", new StringType()),
                new Field("key2", new StringType())).build();
        List<Partition> partitions = new PartitionsBuilder(schema)
                .rootFirst("root")
                .buildList();
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();

        // When
        write(partitions, bytes);

        // Then
        assertThat(read(bytes)).isEqualTo(partitions);
    }

    @Test
    void shouldWriteOnePartitionWithMultipleFieldsOfDifferentTypes() throws Exception {
        // Given
        Schema schema = Schema.builder().rowKeyFields(
                new Field("key1", new StringType()),
                new Field("key2", new LongType()),
                new Field("key3", new IntType()),
                new Field("key4", new ByteArrayType())).build();
        List<Partition> partitions = new PartitionsBuilder(schema)
                .rootFirst("root")
                .buildList();
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();

        // When
        write(partitions, bytes);

        // Then
        assertThat(read(bytes)).isEqualTo(partitions);
    }

    @Test
    void shouldWritePartitionsSplitOnOneStringFieldOverMultipleLevels() throws Exception {
        // Given
        Schema schema = createSchemaWithKey("key", new StringType());
        List<Partition> partitions = new PartitionsBuilder(schema)
                .rootFirst("root")
                .splitToNewChildren("root", "L", "R", "mmm")
                .splitToNewChildren("L", "LL", "LR", "ccc")
                .splitToNewChildren("R", "RL", "RR", "ttt")
                .buildList();
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();

        // When
        write(partitions, bytes);

        // Then
        assertThat(read(bytes)).isEqualTo(partitions);
    }

    @Test
    void shouldWritePartitionsSplitOnDifferentDimensions() throws Exception {
        // Given
        Schema schema = Schema.builder().rowKeyFields(
                new Field("key1", new StringType()),
                new Field("key2", new StringType())).build();
        List<Partition> partitions = new PartitionsBuilder(schema)
                .rootFirst("root")
                .splitToNewChildrenOnDimension("root", "L", "R", 0, "mmm")
                .splitToNewChildrenOnDimension("L", "LL", "LR", 1, "ccc")
                .splitToNewChildrenOnDimension("R", "RL", "RR", 1, "ttt")
                .buildList();
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();

        // When
        write(partitions, bytes);

        // Then
        assertThat(read(bytes)).isEqualTo(partitions);
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
        List<Partition> partitions = new PartitionsBuilder(createSchemaWithKey("key", new StringType()))
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
        PartitionTree tree = new PartitionsBuilder(createSchemaWithKey("key", new StringType()))
                .rootFirst("root")
                .splitToNewChildren("root", "L", "R", "c")
                .buildTree();
        List<Partition> partitions = Stream.of("root", "L", "R").map(tree::getPartition).toList();
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();

        // When
        WriteResult writeResult = writeWithMaxElementsInBatch(2, partitions, bytes);
        ReadResult readResult = readResult(bytes);

        // Then
        assertThat(readResult.partitions()).isEqualTo(partitions);
        assertThat(writeResult.numBatches()).isEqualTo(2).isEqualTo(readResult.numBatches());
    }

    @Test
    void shouldWritePartitionTreeInBatchesWithRootLast() throws Exception {
        // Given
        PartitionTree tree = new PartitionsBuilder(createSchemaWithKey("key", new StringType()))
                .rootFirst("root")
                .splitToNewChildren("root", "L", "R", "c")
                .buildTree();
        List<Partition> partitions = Stream.of("L", "R", "root").map(tree::getPartition).toList();
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();

        // When
        WriteResult writeResult = writeWithMaxElementsInBatch(2, partitions, bytes);
        ReadResult readResult = readResult(bytes);

        // Then
        assertThat(readResult.partitions()).isEqualTo(partitions);
        assertThat(writeResult.numBatches()).isEqualTo(2).isEqualTo(readResult.numBatches());
    }

    @Test
    void shouldWriteLargerPartitionTreeInBatchesInTreeOrder() throws Exception {
        // Given
        List<Partition> partitions = new PartitionsBuilder(createSchemaWithKey("key", new StringType()))
                .rootFirst("root")
                .splitToNewChildren("root", "L", "R", "c")
                .splitToNewChildren("L", "LL", "LR", "b")
                .splitToNewChildren("LL", "LLL", "LLR", "a")
                .buildTree().traverseLeavesFirst().toList();
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();

        // When
        WriteResult writeResult = writeWithMaxElementsInBatch(2, partitions, bytes);
        ReadResult readResult = readResult(bytes);

        // Then
        assertThat(readResult.partitions()).isEqualTo(partitions);
        assertThat(writeResult.numBatches()).isEqualTo(4).isEqualTo(readResult.numBatches());
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
