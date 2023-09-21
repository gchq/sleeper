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

import sleeper.core.range.Range.RangeFactory;
import sleeper.core.range.Region;
import sleeper.core.schema.Field;
import sleeper.core.schema.Schema;
import sleeper.core.schema.type.ByteArrayType;
import sleeper.core.schema.type.IntType;
import sleeper.core.schema.type.LongType;
import sleeper.core.schema.type.StringType;

import static org.assertj.core.api.Assertions.assertThat;

public class PartitionSerDeTest {

    @Test
    public void shouldSerialiseAndDeserialiseWithIntKeyCorrectly() {
        // Given
        Field field = new Field("key", new IntType());
        Schema schema = Schema.builder().rowKeyFields(field).build();
        RangeFactory rangeFactory = new RangeFactory(schema);
        Region region = new Region(rangeFactory.createRange(field, 0, true, 10, false));
        Partition partition = Partition.builder()
                .id("id")
                .rowKeyTypes(schema.getRowKeyTypes())
                .leafPartition(true)
                .region(region)
                .parentPartitionId(null)
                .childPartitionIds("id1", "id2")
                .build();
        PartitionSerDe partitionSerDe = new PartitionSerDe(schema);

        // When
        String serialisedPartition = partitionSerDe.toJson(partition);
        Partition deserialisedPartition = partitionSerDe.fromJson(serialisedPartition);

        // Then
        assertThat(deserialisedPartition).isEqualTo(partition);
    }

    @Test
    public void shouldSerialiseAndDeserialiseWithLongKeyCorrectly() {
        // Given
        Field field = new Field("key", new LongType());
        Schema schema = Schema.builder().rowKeyFields(field).build();
        RangeFactory rangeFactory = new RangeFactory(schema);
        Region region = new Region(rangeFactory.createRange(field, 1L, true, 10L, false));
        Partition partition = Partition.builder()
                .id("id")
                .rowKeyTypes(schema.getRowKeyTypes())
                .leafPartition(true)
                .region(region)
                .parentPartitionId(null)
                .childPartitionIds("id1", "id2")
                .build();
        PartitionSerDe partitionSerDe = new PartitionSerDe(schema);

        // When
        String serialisedPartition = partitionSerDe.toJson(partition);
        Partition deserialisedPartition = partitionSerDe.fromJson(serialisedPartition);

        // Then
        assertThat(deserialisedPartition).isEqualTo(partition);
    }

    @Test
    public void shouldSerialiseAndDeserialiseWithStringKeyCorrectly() {
        // Given
        Field field = new Field("key", new StringType());
        Schema schema = Schema.builder().rowKeyFields(field).build();
        RangeFactory rangeFactory = new RangeFactory(schema);
        Region region = new Region(rangeFactory.createRange(field, "A", true, "Z", false));
        Partition partition = Partition.builder()
                .id("id")
                .rowKeyTypes(schema.getRowKeyTypes())
                .leafPartition(true)
                .region(region)
                .parentPartitionId(null)
                .childPartitionIds("id1", "id2")
                .build();
        PartitionSerDe partitionSerDe = new PartitionSerDe(schema);

        // When
        String serialisedPartition = partitionSerDe.toJson(partition);
        Partition deserialisedPartition = partitionSerDe.fromJson(serialisedPartition);

        // Then
        assertThat(deserialisedPartition).isEqualTo(partition);
    }

    @Test
    public void shouldSerialiseAndDeserialiseWithStringKeyWithNullMaxCorrectly() {
        // Given
        Field field = new Field("key", new StringType());
        Schema schema = Schema.builder().rowKeyFields(field).build();
        RangeFactory rangeFactory = new RangeFactory(schema);
        Region region = new Region(rangeFactory.createRange(field, "", true, null, false));
        Partition partition = Partition.builder()
                .id("id")
                .rowKeyTypes(schema.getRowKeyTypes())
                .leafPartition(true)
                .region(region)
                .parentPartitionId(null)
                .childPartitionIds("id1", "id2")
                .build();
        PartitionSerDe partitionSerDe = new PartitionSerDe(schema);

        // When
        String serialisedPartition = partitionSerDe.toJson(partition);
        Partition deserialisedPartition = partitionSerDe.fromJson(serialisedPartition);

        // Then
        assertThat(deserialisedPartition).isEqualTo(partition);
    }

    @Test
    public void shouldSerialiseAndDeserialiseWithByteArrayKeyCorrectly() {
        // Given
        Field field = new Field("key", new ByteArrayType());
        Schema schema = Schema.builder().rowKeyFields(field).build();
        RangeFactory rangeFactory = new RangeFactory(schema);
        Region region = new Region(rangeFactory.createRange(field, new byte[]{0}, true, new byte[]{64, 64}, false));
        Partition partition = Partition.builder()
                .id("id")
                .rowKeyTypes(schema.getRowKeyTypes())
                .leafPartition(true)
                .region(region)
                .parentPartitionId(null)
                .childPartitionIds("id1", "id2")
                .build();
        PartitionSerDe partitionSerDe = new PartitionSerDe(schema);

        // When
        String serialisedPartition = partitionSerDe.toJson(partition);
        Partition deserialisedPartition = partitionSerDe.fromJson(serialisedPartition);

        // Then
        assertThat(deserialisedPartition).isEqualTo(partition);
    }

    @Test
    public void shouldSerialiseAndDeserialiseWithByteArrayKeyWithNullMaxCorrectly() {
        // Given
        Field field = new Field("key", new ByteArrayType());
        Schema schema = Schema.builder().rowKeyFields(field).build();
        RangeFactory rangeFactory = new RangeFactory(schema);
        Region region = new Region(rangeFactory.createRange(field, new byte[]{}, true, null, false));
        Partition partition = Partition.builder()
                .id("id")
                .rowKeyTypes(schema.getRowKeyTypes())
                .leafPartition(true)
                .region(region)
                .parentPartitionId(null)
                .childPartitionIds("id1", "id2")
                .build();
        PartitionSerDe partitionSerDe = new PartitionSerDe(schema);

        // When
        String serialisedPartition = partitionSerDe.toJson(partition);
        Partition deserialisedPartition = partitionSerDe.fromJson(serialisedPartition);

        // Then
        assertThat(deserialisedPartition).isEqualTo(partition);
    }
}
