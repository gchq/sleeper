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
package sleeper.splitter;

import org.junit.Test;
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
import sleeper.splitter.FindPartitionsToSplitIT.TestTablePropertiesProvider;
import sleeper.statestore.FileInfo;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

public class SplitPartitionJobDefinitionSerDeTest {

    @Test
    public void shouldSerialiseAndDeserialiseWithIntKey() throws IOException {
        // Given
        Field field = new Field("key", new IntType());
        Schema schema = Schema.builder().rowKeyFields(field).build();
        Partition partition = new Partition();
        partition.setRowKeyTypes(new IntType());
        partition.setId("123");
        partition.setLeafPartition(true);
        partition.setParentPartitionId(null);
        partition.setChildPartitionIds(new ArrayList<>());
        Range range = new RangeFactory(schema).createRange(field, 1, 10);
        partition.setRegion(new Region(range));
        FileInfo fileInfo1 = new FileInfo();
        fileInfo1.setRowKeyTypes(new IntType());
        fileInfo1.setFilename("f1");
        fileInfo1.setFileStatus(FileInfo.FileStatus.ACTIVE);
        fileInfo1.setMinRowKey(Key.create(1));
        fileInfo1.setMaxRowKey(Key.create(2));
        fileInfo1.setNumberOfRecords(100L);
        fileInfo1.setPartitionId("123");
        FileInfo fileInfo2 = new FileInfo();
        fileInfo2.setRowKeyTypes(new IntType());
        fileInfo2.setFilename("f2");
        fileInfo2.setFileStatus(FileInfo.FileStatus.ACTIVE);
        fileInfo2.setMinRowKey(Key.create(1));
        fileInfo2.setMaxRowKey(Key.create(10));
        fileInfo2.setNumberOfRecords(1000L);
        fileInfo2.setPartitionId("123");
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
    public void shouldSerialiseAndDeserialiseWithLongKey() throws IOException {
        // Given
        Field field = new Field("key", new LongType());
        Schema schema = Schema.builder().rowKeyFields(field).build();
        Partition partition = new Partition();
        partition.setRowKeyTypes(new LongType());
        partition.setId("123");
        partition.setLeafPartition(true);
        partition.setParentPartitionId(null);
        partition.setChildPartitionIds(new ArrayList<>());
        Range range = new RangeFactory(schema).createRange(field, 1L, 10L);
        partition.setRegion(new Region(range));
        FileInfo fileInfo1 = new FileInfo();
        fileInfo1.setRowKeyTypes(new LongType());
        fileInfo1.setFilename("f1");
        fileInfo1.setFileStatus(FileInfo.FileStatus.ACTIVE);
        fileInfo1.setMinRowKey(Key.create(1L));
        fileInfo1.setMaxRowKey(Key.create(2L));
        fileInfo1.setNumberOfRecords(100L);
        fileInfo1.setPartitionId("123");
        FileInfo fileInfo2 = new FileInfo();
        fileInfo2.setRowKeyTypes(new LongType());
        fileInfo2.setFilename("f2");
        fileInfo2.setFileStatus(FileInfo.FileStatus.ACTIVE);
        fileInfo2.setMinRowKey(Key.create(1L));
        fileInfo2.setMaxRowKey(Key.create(10L));
        fileInfo2.setNumberOfRecords(1000L);
        fileInfo2.setPartitionId("123");
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
    public void shouldSerialiseAndDeserialiseWithStringKey() throws IOException {
        // Given
        Field field = new Field("key", new StringType());
        Schema schema = Schema.builder().rowKeyFields(field).build();
        Partition partition = new Partition();
        partition.setRowKeyTypes(new StringType());
        partition.setId("123");
        partition.setLeafPartition(true);
        partition.setParentPartitionId(null);
        partition.setChildPartitionIds(new ArrayList<>());
        Range range = new RangeFactory(schema).createRange(field, "A", "Z");
        partition.setRegion(new Region(range));
        FileInfo fileInfo1 = new FileInfo();
        fileInfo1.setRowKeyTypes(new StringType());
        fileInfo1.setFilename("f1");
        fileInfo1.setFileStatus(FileInfo.FileStatus.ACTIVE);
        fileInfo1.setMinRowKey(Key.create("A"));
        fileInfo1.setMaxRowKey(Key.create("Z"));
        fileInfo1.setNumberOfRecords(100L);
        fileInfo1.setPartitionId("123");
        FileInfo fileInfo2 = new FileInfo();
        fileInfo2.setRowKeyTypes(new StringType());
        fileInfo2.setFilename("f2");
        fileInfo2.setFileStatus(FileInfo.FileStatus.ACTIVE);
        fileInfo2.setMinRowKey(Key.create("A"));
        fileInfo2.setMaxRowKey(Key.create("Z"));
        fileInfo2.setNumberOfRecords(1000L);
        fileInfo2.setPartitionId("123");
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
    public void shouldSerialiseAndDeserialiseWithStringKeyWithNullMax() throws IOException {
        // Given
        Field field = new Field("key", new StringType());
        Schema schema = Schema.builder().rowKeyFields(field).build();
        Partition partition = new Partition();
        partition.setRowKeyTypes(new StringType());
        partition.setId("123");
        partition.setLeafPartition(true);
        partition.setParentPartitionId(null);
        partition.setChildPartitionIds(new ArrayList<>());
        Range range = new RangeFactory(schema).createRange(field, "", null);
        partition.setRegion(new Region(range));
        FileInfo fileInfo1 = new FileInfo();
        fileInfo1.setRowKeyTypes(new StringType());
        fileInfo1.setFilename("f1");
        fileInfo1.setFileStatus(FileInfo.FileStatus.ACTIVE);
        fileInfo1.setMinRowKey(Key.create("A"));
        fileInfo1.setMaxRowKey(Key.create("Z"));
        fileInfo1.setNumberOfRecords(100L);
        fileInfo1.setPartitionId("123");
        FileInfo fileInfo2 = new FileInfo();
        fileInfo2.setRowKeyTypes(new StringType());
        fileInfo2.setFilename("f2");
        fileInfo2.setFileStatus(FileInfo.FileStatus.ACTIVE);
        fileInfo2.setMinRowKey(Key.create("A"));
        fileInfo2.setMaxRowKey(Key.create("Z"));
        fileInfo2.setNumberOfRecords(1000L);
        fileInfo2.setPartitionId("123");
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
    public void shouldSerialiseAndDeserialiseWithByteArrayKey() throws IOException {
        // Given
        Field field = new Field("key", new ByteArrayType());
        Schema schema = Schema.builder().rowKeyFields(field).build();
        Partition partition = new Partition();
        partition.setRowKeyTypes(new ByteArrayType());
        partition.setId("123");
        partition.setLeafPartition(true);
        partition.setParentPartitionId(null);
        partition.setChildPartitionIds(new ArrayList<>());
        Range range = new RangeFactory(schema).createRange(field, new byte[]{}, new byte[]{64, 64});
        partition.setRegion(new Region(range));
        FileInfo fileInfo1 = new FileInfo();
        fileInfo1.setRowKeyTypes(new ByteArrayType());
        fileInfo1.setFilename("f1");
        fileInfo1.setFileStatus(FileInfo.FileStatus.ACTIVE);
        fileInfo1.setMinRowKey(Key.create(new byte[]{}));
        fileInfo1.setMaxRowKey(Key.create(new byte[]{64, 64}));
        fileInfo1.setNumberOfRecords(100L);
        fileInfo1.setPartitionId("123");
        FileInfo fileInfo2 = new FileInfo();
        fileInfo2.setRowKeyTypes(new ByteArrayType());
        fileInfo2.setFilename("f2");
        fileInfo2.setFileStatus(FileInfo.FileStatus.ACTIVE);
        fileInfo2.setMinRowKey(Key.create(new byte[]{}));
        fileInfo2.setMaxRowKey(Key.create(new byte[]{64, 64}));
        fileInfo2.setNumberOfRecords(1000L);
        fileInfo2.setPartitionId("123");
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
    public void shouldSerialiseAndDeserialiseWithByteArrayKeyWithNullMax() throws IOException {
        // Given
        Field field = new Field("key", new ByteArrayType());
        Schema schema = Schema.builder().rowKeyFields(field).build();
        Partition partition = new Partition();
        partition.setRowKeyTypes(new ByteArrayType());
        partition.setId("123");
        partition.setLeafPartition(true);
        partition.setParentPartitionId(null);
        partition.setChildPartitionIds(new ArrayList<>());
        Range range = new RangeFactory(schema).createRange(field, new byte[]{}, null);
        partition.setRegion(new Region(range));
        FileInfo fileInfo1 = new FileInfo();
        fileInfo1.setRowKeyTypes(new ByteArrayType());
        fileInfo1.setFilename("f1");
        fileInfo1.setFileStatus(FileInfo.FileStatus.ACTIVE);
        fileInfo1.setMinRowKey(Key.create(new byte[]{}));
        fileInfo1.setMaxRowKey(Key.create(new byte[]{64, 64}));
        fileInfo1.setNumberOfRecords(100L);
        fileInfo1.setPartitionId("123");
        FileInfo fileInfo2 = new FileInfo();
        fileInfo2.setRowKeyTypes(new ByteArrayType());
        fileInfo2.setFilename("f2");
        fileInfo2.setFileStatus(FileInfo.FileStatus.ACTIVE);
        fileInfo2.setMinRowKey(Key.create(new byte[]{}));
        fileInfo2.setMaxRowKey(Key.create(new byte[]{64, 64}));
        fileInfo2.setNumberOfRecords(1000L);
        fileInfo2.setPartitionId("123");
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
