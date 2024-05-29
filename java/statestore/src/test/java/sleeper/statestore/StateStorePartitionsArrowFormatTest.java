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
import sleeper.core.schema.type.StringType;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.nio.channels.Channels;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.core.schema.SchemaTestHelper.schemaWithKey;

public class StateStorePartitionsArrowFormatTest {

    private final BufferAllocator allocator = new RootAllocator();

    @Test
    void shouldWriteOnePartitionWithOneStringField() throws Exception {
        // Given
        Schema schema = schemaWithKey("key", new StringType());
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

    private void write(List<Partition> partitions, ByteArrayOutputStream stream) throws Exception {
        StateStorePartitionsArrowFormat.write(partitions, allocator, Channels.newChannel(stream));
    }

    private List<Partition> read(ByteArrayOutputStream stream) throws Exception {
        return StateStorePartitionsArrowFormat.read(allocator,
                Channels.newChannel(new ByteArrayInputStream(stream.toByteArray())));
    }
}
