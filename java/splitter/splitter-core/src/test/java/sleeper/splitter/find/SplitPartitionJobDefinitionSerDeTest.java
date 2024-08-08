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
package sleeper.splitter.find;

import org.junit.jupiter.api.Test;

import sleeper.configuration.properties.instance.InstanceProperties;
import sleeper.configuration.properties.table.FixedTablePropertiesProvider;
import sleeper.configuration.properties.table.TableProperties;
import sleeper.configuration.properties.table.TableProperty;
import sleeper.core.partition.Partition;
import sleeper.core.partition.PartitionsBuilder;
import sleeper.core.schema.Field;
import sleeper.core.schema.Schema;
import sleeper.core.schema.type.ByteArrayType;
import sleeper.core.schema.type.IntType;
import sleeper.core.schema.type.LongType;
import sleeper.core.schema.type.StringType;

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
        PartitionsBuilder partitions = new PartitionsBuilder(schema)
                .singlePartition("root")
                .splitToNewChildren("root", "L", "R", 10)
                .splitToNewChildren("L", "LL", "LR", 1);
        Partition partition = partitions.buildTree().getPartition("LR");
        SplitPartitionJobDefinition jobDefinition = createJob(tableProperties, partition, List.of("file1", "file2"));
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
        PartitionsBuilder partitions = new PartitionsBuilder(schema)
                .singlePartition("root")
                .splitToNewChildren("root", "L", "R", 10L)
                .splitToNewChildren("L", "LL", "LR", 1L);
        Partition partition = partitions.buildTree().getPartition("LR");
        SplitPartitionJobDefinition jobDefinition = createJob(tableProperties, partition, List.of("file1", "file2"));
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
        PartitionsBuilder partitions = new PartitionsBuilder(schema)
                .singlePartition("root")
                .splitToNewChildren("root", "L", "R", "Z")
                .splitToNewChildren("L", "LL", "LR", "A");
        Partition partition = partitions.buildTree().getPartition("LR");
        SplitPartitionJobDefinition jobDefinition = createJob(tableProperties, partition, List.of("file1", "file2"));
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
        Partition partition = new PartitionsBuilder(schema)
                .singlePartition("root")
                .buildTree().getRootPartition();
        SplitPartitionJobDefinition jobDefinition = createJob(tableProperties, partition, List.of("file1", "file2"));
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
        PartitionsBuilder partitions = new PartitionsBuilder(schema)
                .singlePartition("root")
                .splitToNewChildren("root", "L", "R", new byte[]{64, 64});
        Partition partition = partitions.buildTree().getPartition("L");
        SplitPartitionJobDefinition jobDefinition = createJob(tableProperties, partition, List.of("file1", "file2"));
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
        Partition partition = new PartitionsBuilder(schema)
                .singlePartition("root")
                .buildTree().getRootPartition();
        SplitPartitionJobDefinition jobDefinition = createJob(tableProperties, partition, List.of("file1", "file2"));
        SplitPartitionJobDefinitionSerDe jobDefinitionSerDe = createSerDe(tableProperties);

        // When
        String serialised = jobDefinitionSerDe.toJson(jobDefinition);
        SplitPartitionJobDefinition deserialised = jobDefinitionSerDe.fromJson(serialised);

        // Then
        assertThat(deserialised).isEqualTo(jobDefinition);
    }
}
