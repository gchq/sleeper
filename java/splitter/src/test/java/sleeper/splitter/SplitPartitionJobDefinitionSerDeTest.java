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
package sleeper.splitter;

import org.junit.jupiter.api.Test;

import sleeper.core.key.Key;
import sleeper.core.partition.Partition;
import sleeper.core.range.Range;
import sleeper.core.range.Range.RangeFactory;
import sleeper.core.range.Region;
import sleeper.core.schema.Field;
import sleeper.core.schema.Schema;
import sleeper.core.schema.type.ByteArrayType;
import sleeper.core.schema.type.IntType;
import sleeper.core.schema.type.LongType;
import sleeper.core.schema.type.StringType;
import sleeper.core.statestore.FileInfo;
import sleeper.splitter.FindPartitionsToSplitIT.TestTablePropertiesProvider;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

public class SplitPartitionJobDefinitionSerDeTest {

    @Test
    public void shouldSerialiseAndDeserialiseWithIntKey() {
        // Given
        Field field = new Field("key", new IntType());
        Schema schema = Schema.builder().rowKeyFields(field).build();
        Range range = new RangeFactory(schema).createRange(field, 1, 10);
        Partition partition = Partition.builder()
                .rowKeyTypes(new IntType())
                .id("123")
                .leafPartition(true)
                .parentPartitionId(null)
                .childPartitionIds(new ArrayList<>())
                .region(new Region(range))
                .build();
        FileInfo fileInfo1 = FileInfo.builder()
                .rowKeyTypes(new IntType())
                .filename("f1")
                .fileStatus(FileInfo.FileStatus.ACTIVE)
                .minRowKey(Key.create(1))
                .maxRowKey(Key.create(2))
                .numberOfRecords(100L)
                .partitionId("123")
                .build();
        FileInfo fileInfo2 = FileInfo.builder()
                .rowKeyTypes(new IntType())
                .filename("f2")
                .fileStatus(FileInfo.FileStatus.ACTIVE)
                .minRowKey(Key.create(1))
                .maxRowKey(Key.create(10))
                .numberOfRecords(1000L)
                .partitionId("123")
                .build();
        List<String> fileNames = new ArrayList<>();
        fileNames.add(fileInfo1.getFilename());
        fileNames.add(fileInfo2.getFilename());
        SplitPartitionJobDefinition jobDefinition = new SplitPartitionJobDefinition("myTable", partition, fileNames);
        SplitPartitionJobDefinitionSerDe jobDefinitionSerDe = new SplitPartitionJobDefinitionSerDe(new TestTablePropertiesProvider(schema));

        // When
        String serialised = jobDefinitionSerDe.toJson(jobDefinition);
        SplitPartitionJobDefinition deserialised = jobDefinitionSerDe.fromJson(serialised);

        // Then
        assertThat(deserialised).isEqualTo(jobDefinition);
    }

    @Test
    public void shouldSerialiseAndDeserialiseWithLongKey() {
        // Given
        Field field = new Field("key", new LongType());
        Schema schema = Schema.builder().rowKeyFields(field).build();
        Range range = new RangeFactory(schema).createRange(field, 1L, 10L);
        Partition partition = Partition.builder()
                .rowKeyTypes(new LongType())
                .id("123")
                .leafPartition(true)
                .parentPartitionId(null)
                .childPartitionIds(new ArrayList<>())
                .region(new Region(range))
                .build();
        FileInfo fileInfo1 = FileInfo.builder()
                .rowKeyTypes(new LongType())
                .filename("f1")
                .fileStatus(FileInfo.FileStatus.ACTIVE)
                .minRowKey(Key.create(1L))
                .maxRowKey(Key.create(2L))
                .numberOfRecords(100L)
                .partitionId("123")
                .build();
        FileInfo fileInfo2 = FileInfo.builder()
                .rowKeyTypes(new LongType())
                .filename("f2")
                .fileStatus(FileInfo.FileStatus.ACTIVE)
                .minRowKey(Key.create(1L))
                .maxRowKey(Key.create(10L))
                .numberOfRecords(1000L)
                .partitionId("123")
                .build();
        List<String> fileNames = new ArrayList<>();
        fileNames.add(fileInfo1.getFilename());
        fileNames.add(fileInfo2.getFilename());
        SplitPartitionJobDefinition jobDefinition = new SplitPartitionJobDefinition("myTable", partition, fileNames);
        SplitPartitionJobDefinitionSerDe jobDefinitionSerDe = new SplitPartitionJobDefinitionSerDe(new TestTablePropertiesProvider(schema));

        // When
        String serialised = jobDefinitionSerDe.toJson(jobDefinition);
        SplitPartitionJobDefinition deserialised = jobDefinitionSerDe.fromJson(serialised);

        // Then
        assertThat(deserialised).isEqualTo(jobDefinition);
    }

    @Test
    public void shouldSerialiseAndDeserialiseWithStringKey() {
        // Given
        Field field = new Field("key", new StringType());
        Schema schema = Schema.builder().rowKeyFields(field).build();
        Range range = new RangeFactory(schema).createRange(field, "A", "Z");
        Partition partition = Partition.builder()
                .rowKeyTypes(new StringType())
                .id("123")
                .leafPartition(true)
                .parentPartitionId(null)
                .childPartitionIds(new ArrayList<>())
                .region(new Region(range))
                .build();
        FileInfo fileInfo1 = FileInfo.builder()
                .rowKeyTypes(new StringType())
                .filename("f1")
                .fileStatus(FileInfo.FileStatus.ACTIVE)
                .minRowKey(Key.create("A"))
                .maxRowKey(Key.create("Z"))
                .numberOfRecords(100L)
                .partitionId("123")
                .build();
        FileInfo fileInfo2 = FileInfo.builder()
                .rowKeyTypes(new StringType())
                .filename("f2")
                .fileStatus(FileInfo.FileStatus.ACTIVE)
                .minRowKey(Key.create("A"))
                .maxRowKey(Key.create("Z"))
                .numberOfRecords(1000L)
                .partitionId("123")
                .build();
        List<String> fileNames = new ArrayList<>();
        fileNames.add(fileInfo1.getFilename());
        fileNames.add(fileInfo2.getFilename());
        SplitPartitionJobDefinition jobDefinition = new SplitPartitionJobDefinition("myTable", partition, fileNames);
        SplitPartitionJobDefinitionSerDe jobDefinitionSerDe = new SplitPartitionJobDefinitionSerDe(new TestTablePropertiesProvider(schema));

        // When
        String serialised = jobDefinitionSerDe.toJson(jobDefinition);
        SplitPartitionJobDefinition deserialised = jobDefinitionSerDe.fromJson(serialised);

        // Then
        assertThat(deserialised).isEqualTo(jobDefinition);
    }

    @Test
    public void shouldSerialiseAndDeserialiseWithStringKeyWithNullMax() {
        // Given
        Field field = new Field("key", new StringType());
        Schema schema = Schema.builder().rowKeyFields(field).build();
        Range range = new RangeFactory(schema).createRange(field, "", null);
        Partition partition = Partition.builder()
                .rowKeyTypes(new StringType())
                .id("123")
                .leafPartition(true)
                .parentPartitionId(null)
                .childPartitionIds(new ArrayList<>())
                .region(new Region(range))
                .build();
        FileInfo fileInfo1 = FileInfo.builder()
                .rowKeyTypes(new StringType())
                .filename("f1")
                .fileStatus(FileInfo.FileStatus.ACTIVE)
                .minRowKey(Key.create("A"))
                .maxRowKey(Key.create("Z"))
                .numberOfRecords(100L)
                .partitionId("123")
                .build();
        FileInfo fileInfo2 = FileInfo.builder()
                .rowKeyTypes(new StringType())
                .filename("f2")
                .fileStatus(FileInfo.FileStatus.ACTIVE)
                .minRowKey(Key.create("A"))
                .maxRowKey(Key.create("Z"))
                .numberOfRecords(1000L)
                .partitionId("123")
                .build();
        List<String> fileNames = new ArrayList<>();
        fileNames.add(fileInfo1.getFilename());
        fileNames.add(fileInfo2.getFilename());
        SplitPartitionJobDefinition jobDefinition = new SplitPartitionJobDefinition("myTable", partition, fileNames);
        SplitPartitionJobDefinitionSerDe jobDefinitionSerDe = new SplitPartitionJobDefinitionSerDe(new TestTablePropertiesProvider(schema));

        // When
        String serialised = jobDefinitionSerDe.toJson(jobDefinition);
        SplitPartitionJobDefinition deserialised = jobDefinitionSerDe.fromJson(serialised);

        // Then
        assertThat(deserialised).isEqualTo(jobDefinition);
    }

    @Test
    public void shouldSerialiseAndDeserialiseWithByteArrayKey() {
        // Given
        Field field = new Field("key", new ByteArrayType());
        Schema schema = Schema.builder().rowKeyFields(field).build();
        Range range = new RangeFactory(schema).createRange(field, new byte[]{}, new byte[]{64, 64});
        Partition partition = Partition.builder()
                .rowKeyTypes(new ByteArrayType())
                .id("123")
                .leafPartition(true)
                .parentPartitionId(null)
                .childPartitionIds(new ArrayList<>())
                .region(new Region(range))
                .build();
        FileInfo fileInfo1 = FileInfo.builder()
                .rowKeyTypes(new ByteArrayType())
                .filename("f1")
                .fileStatus(FileInfo.FileStatus.ACTIVE)
                .minRowKey(Key.create(new byte[]{}))
                .maxRowKey(Key.create(new byte[]{64, 64}))
                .numberOfRecords(100L)
                .partitionId("123")
                .build();
        FileInfo fileInfo2 = FileInfo.builder()
                .rowKeyTypes(new ByteArrayType())
                .filename("f2")
                .fileStatus(FileInfo.FileStatus.ACTIVE)
                .minRowKey(Key.create(new byte[]{}))
                .maxRowKey(Key.create(new byte[]{64, 64}))
                .numberOfRecords(1000L)
                .partitionId("123")
                .build();
        List<String> fileNames = new ArrayList<>();
        fileNames.add(fileInfo1.getFilename());
        fileNames.add(fileInfo2.getFilename());
        SplitPartitionJobDefinition jobDefinition = new SplitPartitionJobDefinition("myTable", partition, fileNames);
        SplitPartitionJobDefinitionSerDe jobDefinitionSerDe = new SplitPartitionJobDefinitionSerDe(new TestTablePropertiesProvider(schema));

        // When
        String serialised = jobDefinitionSerDe.toJson(jobDefinition);
        SplitPartitionJobDefinition deserialised = jobDefinitionSerDe.fromJson(serialised);

        // Then
        assertThat(deserialised).isEqualTo(jobDefinition);
    }

    @Test
    public void shouldSerialiseAndDeserialiseWithByteArrayKeyWithNullMax() {
        // Given
        Field field = new Field("key", new ByteArrayType());
        Schema schema = Schema.builder().rowKeyFields(field).build();
        Range range = new RangeFactory(schema).createRange(field, new byte[]{}, null);
        Partition partition = Partition.builder()
                .rowKeyTypes(new ByteArrayType())
                .id("123")
                .leafPartition(true)
                .parentPartitionId(null)
                .childPartitionIds(new ArrayList<>())
                .region(new Region(range))
                .build();
        FileInfo fileInfo1 = FileInfo.builder()
                .rowKeyTypes(new ByteArrayType())
                .filename("f1")
                .fileStatus(FileInfo.FileStatus.ACTIVE)
                .minRowKey(Key.create(new byte[]{}))
                .maxRowKey(Key.create(new byte[]{64, 64}))
                .numberOfRecords(100L)
                .partitionId("123")
                .build();
        FileInfo fileInfo2 = FileInfo.builder()
                .rowKeyTypes(new ByteArrayType())
                .filename("f2")
                .fileStatus(FileInfo.FileStatus.ACTIVE)
                .minRowKey(Key.create(new byte[]{}))
                .maxRowKey(Key.create(new byte[]{64, 64}))
                .numberOfRecords(1000L)
                .partitionId("123")
                .build();
        List<String> fileInfos = new ArrayList<>();
        fileInfos.add(fileInfo1.getFilename());
        fileInfos.add(fileInfo2.getFilename());
        SplitPartitionJobDefinition jobDefinition = new SplitPartitionJobDefinition("myTable", partition, fileInfos);
        SplitPartitionJobDefinitionSerDe jobDefinitionSerDe = new SplitPartitionJobDefinitionSerDe(new TestTablePropertiesProvider(schema));

        // When
        String serialised = jobDefinitionSerDe.toJson(jobDefinition);
        SplitPartitionJobDefinition deserialised = jobDefinitionSerDe.fromJson(serialised);

        // Then
        assertThat(deserialised).isEqualTo(jobDefinition);
    }
}
