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
import sleeper.core.range.Range.RangeFactory;
import sleeper.core.range.Region;
import sleeper.core.schema.Field;
import sleeper.core.schema.Schema;
import sleeper.core.schema.type.ByteArrayType;
import sleeper.core.schema.type.IntType;
import sleeper.core.schema.type.LongType;
import sleeper.core.schema.type.StringType;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

public class PartitionSerDeTest {

    @Test
    public void shouldSerialiseAndDeserialiseWithIntKeyCorrectly() throws IOException {
        // Given
        Field field = new Field("key", new IntType());
        Schema schema = Schema.builder().rowKeyFields(field).build();
        RangeFactory rangeFactory = new RangeFactory(schema);
        Partition partition = new Partition();
        partition.setId("id");
        partition.setRowKeyTypes(new IntType());
        partition.setLeafPartition(true);
        Region region = new Region(rangeFactory.createRange(field, 0, true, 10, false));
        partition.setRegion(region);
        partition.setParentPartitionId(null);
        List<String> childPartitionIds = new ArrayList<>();
        childPartitionIds.add("id1");
        childPartitionIds.add("id2");
        partition.setChildPartitionIds(childPartitionIds);
        PartitionSerDe partitionSerDe = new PartitionSerDe(schema);

        // When
        String serialisedPartition = partitionSerDe.toJson(partition);
        Partition deserialisedPartition = partitionSerDe.fromJson(serialisedPartition);

        // Then
        assertThat(deserialisedPartition).isEqualTo(partition);
    }

    @Test
    public void shouldSerialiseAndDeserialiseWithLongKeyCorrectly() throws IOException {
        // Given
        Field field = new Field("key", new LongType());
        Schema schema = Schema.builder().rowKeyFields(field).build();
        RangeFactory rangeFactory = new RangeFactory(schema);
        Partition partition = new Partition();
        partition.setId("id");
        partition.setRowKeyTypes(new LongType());
        partition.setLeafPartition(true);
        Region region = new Region(rangeFactory.createRange(field, 1L, true, 10L, false));
        partition.setRegion(region);
        partition.setParentPartitionId(null);
        List<String> childPartitionIds = new ArrayList<>();
        childPartitionIds.add("id1");
        childPartitionIds.add("id2");
        partition.setChildPartitionIds(childPartitionIds);
        PartitionSerDe partitionSerDe = new PartitionSerDe(schema);

        // When
        String serialisedPartition = partitionSerDe.toJson(partition);
        Partition deserialisedPartition = partitionSerDe.fromJson(serialisedPartition);

        // Then
        assertThat(deserialisedPartition).isEqualTo(partition);
    }

    @Test
    public void shouldSerialiseAndDeserialiseWithStringKeyCorrectly() throws IOException {
        // Given
        Field field = new Field("key", new StringType());
        Schema schema = Schema.builder().rowKeyFields(field).build();
        RangeFactory rangeFactory = new RangeFactory(schema);
        Partition partition = new Partition();
        partition.setId("id");
        partition.setRowKeyTypes(new StringType());
        partition.setLeafPartition(true);
        Region region = new Region(rangeFactory.createRange(field, "A", true, "Z", false));
        partition.setRegion(region);
        partition.setParentPartitionId(null);
        List<String> childPartitionIds = new ArrayList<>();
        childPartitionIds.add("id1");
        childPartitionIds.add("id2");
        partition.setChildPartitionIds(childPartitionIds);
        PartitionSerDe partitionSerDe = new PartitionSerDe(schema);

        // When
        String serialisedPartition = partitionSerDe.toJson(partition);
        Partition deserialisedPartition = partitionSerDe.fromJson(serialisedPartition);

        // Then
        assertThat(deserialisedPartition).isEqualTo(partition);
    }

    @Test
    public void shouldSerialiseAndDeserialiseWithStringKeyWithNullMaxCorrectly() throws IOException {
        // Given
        Field field = new Field("key", new StringType());
        Schema schema = Schema.builder().rowKeyFields(field).build();
        RangeFactory rangeFactory = new RangeFactory(schema);
        Partition partition = new Partition();
        partition.setId("id");
        partition.setRowKeyTypes(new StringType());
        partition.setLeafPartition(true);
        Region region = new Region(rangeFactory.createRange(field, "", true, null, false));
        partition.setRegion(region);
        partition.setParentPartitionId(null);
        List<String> childPartitionIds = new ArrayList<>();
        childPartitionIds.add("id1");
        childPartitionIds.add("id2");
        partition.setChildPartitionIds(childPartitionIds);
        PartitionSerDe partitionSerDe = new PartitionSerDe(schema);

        // When
        String serialisedPartition = partitionSerDe.toJson(partition);
        Partition deserialisedPartition = partitionSerDe.fromJson(serialisedPartition);

        // Then
        assertThat(deserialisedPartition).isEqualTo(partition);
    }

    @Test
    public void shouldSerialiseAndDeserialiseWithByteArrayKeyCorrectly() throws IOException {
        // Given
        Field field = new Field("key", new ByteArrayType());
        Schema schema = Schema.builder().rowKeyFields(field).build();
        RangeFactory rangeFactory = new RangeFactory(schema);
        Partition partition = new Partition();
        partition.setId("id");
        partition.setRowKeyTypes(new ByteArrayType());
        partition.setLeafPartition(true);
        Region region = new Region(rangeFactory.createRange(field, new byte[]{0}, true, new byte[]{64, 64}, false));
        partition.setRegion(region);
        partition.setParentPartitionId(null);
        List<String> childPartitionIds = new ArrayList<>();
        childPartitionIds.add("id1");
        childPartitionIds.add("id2");
        partition.setChildPartitionIds(childPartitionIds);
        PartitionSerDe partitionSerDe = new PartitionSerDe(schema);

        // When
        String serialisedPartition = partitionSerDe.toJson(partition);
        Partition deserialisedPartition = partitionSerDe.fromJson(serialisedPartition);

        // Then
        assertThat(deserialisedPartition).isEqualTo(partition);
    }

    @Test
    public void shouldSerialiseAndDeserialiseWithByteArrayKeyWithNullMaxCorrectly() throws IOException {
        // Given
        Field field = new Field("key", new ByteArrayType());
        Schema schema = Schema.builder().rowKeyFields(field).build();
        RangeFactory rangeFactory = new RangeFactory(schema);
        Partition partition = new Partition();
        partition.setId("id");
        partition.setRowKeyTypes(new ByteArrayType());
        partition.setLeafPartition(true);
        Region region = new Region(rangeFactory.createRange(field, new byte[]{}, true, null, false));
        partition.setRegion(region);
        partition.setParentPartitionId(null);
        List<String> childPartitionIds = new ArrayList<>();
        childPartitionIds.add("id1");
        childPartitionIds.add("id2");
        partition.setChildPartitionIds(childPartitionIds);
        PartitionSerDe partitionSerDe = new PartitionSerDe(schema);

        // When
        String serialisedPartition = partitionSerDe.toJson(partition);
        Partition deserialisedPartition = partitionSerDe.fromJson(serialisedPartition);

        // Then
        assertThat(deserialisedPartition).isEqualTo(partition);
    }
}
