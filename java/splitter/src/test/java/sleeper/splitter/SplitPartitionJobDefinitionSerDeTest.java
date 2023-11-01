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

import sleeper.configuration.properties.instance.InstanceProperties;
import sleeper.configuration.properties.table.FixedTablePropertiesProvider;
import sleeper.configuration.properties.table.TableProperties;
import sleeper.configuration.properties.table.TableProperty;
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

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.configuration.properties.InstancePropertiesTestHelper.createTestInstanceProperties;
import static sleeper.configuration.properties.table.TablePropertiesTestHelper.createTestTableProperties;

public class SplitPartitionJobDefinitionSerDeTest {

    private final InstanceProperties instanceProperties = createTestInstanceProperties();

    private TableProperties createTable(Schema schema) {
        return createTestTableProperties(instanceProperties, schema);
    }

    private SplitPartitionJobDefinition createJob(TableProperties table, Partition partition, List<String> filenames) {
        return new SplitPartitionJobDefinition(table.get(TableProperty.TABLE_ID), partition, filenames);
    }

    private SplitPartitionJobDefinitionSerDe createSerDe(TableProperties table) {
        return new SplitPartitionJobDefinitionSerDe(new FixedTablePropertiesProvider(table));
    }

    @Test
    public void shouldSerialiseAndDeserialiseWithIntKey() {
        // Given
        Field field = new Field("key", new IntType());
        Schema schema = Schema.builder().rowKeyFields(field).build();
        TableProperties tableProperties = createTable(schema);
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
                .filename("f1")
                .fileStatus(FileInfo.FileStatus.ACTIVE)
                .numberOfRecords(100L)
                .partitionId("123")
                .build();
        FileInfo fileInfo2 = FileInfo.builder()
                .filename("f2")
                .fileStatus(FileInfo.FileStatus.ACTIVE)
                .numberOfRecords(1000L)
                .partitionId("123")
                .build();
        List<String> fileNames = new ArrayList<>();
        fileNames.add(fileInfo1.getFilename());
        fileNames.add(fileInfo2.getFilename());
        SplitPartitionJobDefinition jobDefinition = createJob(tableProperties, partition, fileNames);
        SplitPartitionJobDefinitionSerDe jobDefinitionSerDe = createSerDe(tableProperties);

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
        TableProperties tableProperties = createTable(schema);
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
                .filename("f1")
                .fileStatus(FileInfo.FileStatus.ACTIVE)
                .numberOfRecords(100L)
                .partitionId("123")
                .build();
        FileInfo fileInfo2 = FileInfo.builder()
                .filename("f2")
                .fileStatus(FileInfo.FileStatus.ACTIVE)
                .numberOfRecords(1000L)
                .partitionId("123")
                .build();
        List<String> fileNames = new ArrayList<>();
        fileNames.add(fileInfo1.getFilename());
        fileNames.add(fileInfo2.getFilename());
        SplitPartitionJobDefinition jobDefinition = createJob(tableProperties, partition, fileNames);
        SplitPartitionJobDefinitionSerDe jobDefinitionSerDe = createSerDe(tableProperties);

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
        TableProperties tableProperties = createTable(schema);
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
                .filename("f1")
                .fileStatus(FileInfo.FileStatus.ACTIVE)
                .numberOfRecords(100L)
                .partitionId("123")
                .build();
        FileInfo fileInfo2 = FileInfo.builder()
                .filename("f2")
                .fileStatus(FileInfo.FileStatus.ACTIVE)
                .numberOfRecords(1000L)
                .partitionId("123")
                .build();
        List<String> fileNames = new ArrayList<>();
        fileNames.add(fileInfo1.getFilename());
        fileNames.add(fileInfo2.getFilename());
        SplitPartitionJobDefinition jobDefinition = createJob(tableProperties, partition, fileNames);
        SplitPartitionJobDefinitionSerDe jobDefinitionSerDe = createSerDe(tableProperties);

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
        TableProperties tableProperties = createTable(schema);
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
                .filename("f1")
                .fileStatus(FileInfo.FileStatus.ACTIVE)
                .numberOfRecords(100L)
                .partitionId("123")
                .build();
        FileInfo fileInfo2 = FileInfo.builder()
                .filename("f2")
                .fileStatus(FileInfo.FileStatus.ACTIVE)
                .numberOfRecords(1000L)
                .partitionId("123")
                .build();
        List<String> fileNames = new ArrayList<>();
        fileNames.add(fileInfo1.getFilename());
        fileNames.add(fileInfo2.getFilename());
        SplitPartitionJobDefinition jobDefinition = createJob(tableProperties, partition, fileNames);
        SplitPartitionJobDefinitionSerDe jobDefinitionSerDe = createSerDe(tableProperties);

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
        TableProperties tableProperties = createTable(schema);
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
                .filename("f1")
                .fileStatus(FileInfo.FileStatus.ACTIVE)
                .numberOfRecords(100L)
                .partitionId("123")
                .build();
        FileInfo fileInfo2 = FileInfo.builder()
                .filename("f2")
                .fileStatus(FileInfo.FileStatus.ACTIVE)
                .numberOfRecords(1000L)
                .partitionId("123")
                .build();
        List<String> fileNames = new ArrayList<>();
        fileNames.add(fileInfo1.getFilename());
        fileNames.add(fileInfo2.getFilename());
        SplitPartitionJobDefinition jobDefinition = createJob(tableProperties, partition, fileNames);
        SplitPartitionJobDefinitionSerDe jobDefinitionSerDe = createSerDe(tableProperties);

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
        TableProperties tableProperties = createTable(schema);
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
                .filename("f1")
                .fileStatus(FileInfo.FileStatus.ACTIVE)
                .numberOfRecords(100L)
                .partitionId("123")
                .build();
        FileInfo fileInfo2 = FileInfo.builder()
                .filename("f2")
                .fileStatus(FileInfo.FileStatus.ACTIVE)
                .numberOfRecords(1000L)
                .partitionId("123")
                .build();
        List<String> fileNames = new ArrayList<>();
        fileNames.add(fileInfo1.getFilename());
        fileNames.add(fileInfo2.getFilename());
        SplitPartitionJobDefinition jobDefinition = createJob(tableProperties, partition, fileNames);
        SplitPartitionJobDefinitionSerDe jobDefinitionSerDe = createSerDe(tableProperties);

        // When
        String serialised = jobDefinitionSerDe.toJson(jobDefinition);
        SplitPartitionJobDefinition deserialised = jobDefinitionSerDe.fromJson(serialised);

        // Then
        assertThat(deserialised).isEqualTo(jobDefinition);
    }
}
