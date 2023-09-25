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
package sleeper.statestore;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.facebook.collections.ByteArray;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import sleeper.configuration.properties.instance.InstanceProperties;
import sleeper.configuration.properties.table.TableProperties;
import sleeper.core.CommonTestConstants;
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
import sleeper.core.statestore.StateStore;
import sleeper.core.statestore.StateStoreException;
import sleeper.statestore.dynamodb.DynamoDBStateStoreCreator;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static sleeper.configuration.properties.InstancePropertiesTestHelper.createTestInstanceProperties;
import static sleeper.configuration.properties.table.TablePropertiesTestHelper.createTestTablePropertiesWithNoSchema;
import static sleeper.dynamodb.tools.GenericContainerAwsV1ClientHelper.buildAwsV1Client;

@Testcontainers
public class InitialiseStateStoreIT {
    private static final int DYNAMO_PORT = 8000;
    private static AmazonDynamoDB dynamoDBClient;
    private final InstanceProperties instanceProperties = createTestInstanceProperties();
    private final TableProperties tableProperties = createTestTablePropertiesWithNoSchema(instanceProperties);

    @Container
    public static GenericContainer dynamoDb = new GenericContainer(CommonTestConstants.DYNAMODB_LOCAL_CONTAINER)
            .withExposedPorts(DYNAMO_PORT);

    @BeforeAll
    public static void initDynamoClient() {
        dynamoDBClient = buildAwsV1Client(dynamoDb, DYNAMO_PORT, AmazonDynamoDBClientBuilder.standard());
    }

    @AfterAll
    public static void shutdownDynamoClient() {
        dynamoDBClient.shutdown();
    }

    private StateStore getStateStore(Schema schema) {
        tableProperties.setSchema(schema);
        return new DynamoDBStateStoreCreator(instanceProperties, tableProperties, dynamoDBClient).create(tableProperties);
    }

    private final Field field = new Field("key", new IntType());
    private final Schema schema = schemaWithRowKeys(field);

    private Schema schemaWithRowKeys(Field... rowKeys) {
        return Schema.builder()
                .rowKeyFields(rowKeys)
                .sortKeyFields(new Field("sort", new LongType()))
                .valueFields(new Field("value", new ByteArrayType()))
                .build();
    }

    @Test
    public void shouldInitialiseStateStoreCorrectlyWithIntKeyAndNoSplitPoints() throws StateStoreException {
        // Given
        StateStore dynamoDBStateStore = getStateStore(schema);
        InitialiseStateStore initialiseStateStore = InitialiseStateStore.createInitialiseStateStoreFromSplitPoints(schema, dynamoDBStateStore, Collections.emptyList());

        // When
        initialiseStateStore.run();

        // Then
        Region expectedRegion = new Region(new RangeFactory(schema).createRange(field, Integer.MIN_VALUE, null));
        Partition expectedPartition = Partition.builder()
                .rowKeyTypes(schema.getRowKeyTypes())
                .region(expectedRegion)
                .id("root")
                .leafPartition(true)
                .parentPartitionId(null)
                .childPartitionIds(Collections.emptyList())
                .dimension(-1)
                .build();
        assertThat(dynamoDBStateStore.getAllPartitions()).containsExactly(expectedPartition);
    }

    @Test
    public void shouldInitialiseStateStoreCorrectlyWithIntKeyAndOneSplitPoint() throws StateStoreException {
        // Given
        StateStore dynamoDBStateStore = getStateStore(schema);
        List<Object> splitPoints = new ArrayList<>();
        splitPoints.add(-10);
        InitialiseStateStore initialiseStateStore = InitialiseStateStore.createInitialiseStateStoreFromSplitPoints(schema, dynamoDBStateStore, splitPoints);

        // When
        initialiseStateStore.run();

        // Then
        List<Partition> partitions = dynamoDBStateStore.getAllPartitions();
        assertThat(partitions).hasSize(3);

        Partition rootPartition = partitions.stream().filter(p -> null == p.getParentPartitionId()).collect(Collectors.toList()).get(0);
        List<Partition> leafPartitions = partitions.stream().filter(Partition::isLeafPartition).collect(Collectors.toList());
        leafPartitions.sort(Comparator.comparing(p -> (int) p.getRegion().getRange("key").getMin()));

        Region expectedRootRegion = new Region(new RangeFactory(schema).createRange(field, Integer.MIN_VALUE, null));
        Partition expectedRootPartition = Partition.builder()
                .rowKeyTypes(schema.getRowKeyTypes())
                .region(expectedRootRegion)
                .id(rootPartition.getId())
                .leafPartition(false)
                .parentPartitionId(null)
                .childPartitionIds(Arrays.asList(leafPartitions.get(0).getId(), leafPartitions.get(1).getId()))
                .dimension(0)
                .build();
        Region expectedLeafRegion0 = new Region(new RangeFactory(schema).createRange(field, Integer.MIN_VALUE, -10));
        Partition expectedLeafPartition0 = Partition.builder()
                .rowKeyTypes(schema.getRowKeyTypes())
                .region(expectedLeafRegion0)
                .id(leafPartitions.get(0).getId())
                .leafPartition(true)
                .parentPartitionId(rootPartition.getId())
                .childPartitionIds(Collections.emptyList())
                .dimension(-1)
                .build();
        Region expectedLeafRegion1 = new Region(new RangeFactory(schema).createRange(field, -10, null));
        Partition expectedLeafPartition1 = Partition.builder()
                .rowKeyTypes(schema.getRowKeyTypes())
                .region(expectedLeafRegion1)
                .id(leafPartitions.get(1).getId())
                .leafPartition(true)
                .parentPartitionId(rootPartition.getId())
                .childPartitionIds(Collections.emptyList())
                .dimension(-1)
                .build();

        assertThat(rootPartition).isEqualTo(expectedRootPartition);
        assertThat(leafPartitions).containsExactly(expectedLeafPartition0, expectedLeafPartition1);
    }

    @Test
    public void shouldInitialiseStateStoreCorrectlyWithIntKeyAndMultipleSplitPoints() throws StateStoreException {
        // Given
        StateStore dynamoDBStateStore = getStateStore(schema);
        List<Object> splitPoints = new ArrayList<>();
        splitPoints.add(-10);
        splitPoints.add(0);
        splitPoints.add(1000);
        InitialiseStateStore initialiseStateStore = InitialiseStateStore.createInitialiseStateStoreFromSplitPoints(schema, dynamoDBStateStore, splitPoints);

        // When
        initialiseStateStore.run();

        // Then
        List<Partition> partitions = dynamoDBStateStore.getAllPartitions();
        assertThat(partitions).hasSize(7);

        Partition rootPartition = partitions.stream().filter(p -> null == p.getParentPartitionId()).collect(Collectors.toList()).get(0);
        List<Partition> internalPartitions = partitions.stream()
                .filter(p -> null != p.getParentPartitionId())
                .filter(p -> !p.isLeafPartition())
                .collect(Collectors.toList());
        internalPartitions.sort(Comparator.comparing(p -> (int) p.getRegion().getRange("key").getMin()));
        List<Partition> leafPartitions = partitions.stream().filter(Partition::isLeafPartition).collect(Collectors.toList());
        leafPartitions.sort(Comparator.comparing(p -> (int) p.getRegion().getRange("key").getMin()));

        Region expectedRootRegion = new Region(new RangeFactory(schema).createRange(field, Integer.MIN_VALUE, null));
        Partition expectedRootPartition = Partition.builder()
                .rowKeyTypes(schema.getRowKeyTypes())
                .region(expectedRootRegion)
                .id(rootPartition.getId())
                .leafPartition(false)
                .parentPartitionId(null)
                .childPartitionIds(Arrays.asList(internalPartitions.get(0).getId(), internalPartitions.get(1).getId()))
                .dimension(0)
                .build();
        Region expectedInternalRegion0 = new Region(new RangeFactory(schema).createRange(field, Integer.MIN_VALUE, 0));
        Partition expectedInternalPartition0 = Partition.builder()
                .rowKeyTypes(schema.getRowKeyTypes())
                .region(expectedInternalRegion0)
                .id(internalPartitions.get(0).getId())
                .leafPartition(false)
                .parentPartitionId(rootPartition.getId())
                .childPartitionIds(Arrays.asList(leafPartitions.get(0).getId(), leafPartitions.get(1).getId()))
                .dimension(0)
                .build();
        Region expectedInternalRegion1 = new Region(new RangeFactory(schema).createRange(field, 0, null));
        Partition expectedInternalPartition1 = Partition.builder()
                .rowKeyTypes(schema.getRowKeyTypes())
                .region(expectedInternalRegion1)
                .id(internalPartitions.get(1).getId())
                .leafPartition(false)
                .parentPartitionId(rootPartition.getId())
                .childPartitionIds(Arrays.asList(leafPartitions.get(2).getId(), leafPartitions.get(3).getId()))
                .dimension(0)
                .build();
        Region expectedLeafRegion0 = new Region(new RangeFactory(schema).createRange(field, Integer.MIN_VALUE, -10));
        Partition expectedLeafPartition0 = Partition.builder()
                .rowKeyTypes(schema.getRowKeyTypes())
                .region(expectedLeafRegion0)
                .id(leafPartitions.get(0).getId())
                .leafPartition(true)
                .parentPartitionId(internalPartitions.get(0).getId())
                .childPartitionIds(Collections.emptyList())
                .dimension(-1)
                .build();
        Region expectedLeafRegion1 = new Region(new RangeFactory(schema).createRange(field, -10, 0));
        Partition expectedLeafPartition1 = Partition.builder()
                .rowKeyTypes(schema.getRowKeyTypes())
                .region(expectedLeafRegion1)
                .id(leafPartitions.get(1).getId())
                .leafPartition(true)
                .parentPartitionId(internalPartitions.get(0).getId())
                .childPartitionIds(Collections.emptyList())
                .dimension(-1)
                .build();
        Region expectedLeafRegion2 = new Region(new RangeFactory(schema).createRange(field, 0, 1000));
        Partition expectedLeafPartition2 = Partition.builder()
                .rowKeyTypes(schema.getRowKeyTypes())
                .region(expectedLeafRegion2)
                .id(leafPartitions.get(2).getId())
                .leafPartition(true)
                .parentPartitionId(internalPartitions.get(1).getId())
                .childPartitionIds(Collections.emptyList())
                .dimension(-1)
                .build();
        Region expectedLeafRegion3 = new Region(new RangeFactory(schema).createRange(field, 1000, null));
        Partition expectedLeafPartition3 = Partition.builder()
                .rowKeyTypes(schema.getRowKeyTypes())
                .region(expectedLeafRegion3)
                .id(leafPartitions.get(3).getId())
                .leafPartition(true)
                .parentPartitionId(internalPartitions.get(1).getId())
                .childPartitionIds(Collections.emptyList())
                .dimension(-1)
                .build();

        assertThat(rootPartition).isEqualTo(expectedRootPartition);
        assertThat(internalPartitions).containsExactly(expectedInternalPartition0, expectedInternalPartition1);
        assertThat(leafPartitions).containsExactly(
                expectedLeafPartition0, expectedLeafPartition1, expectedLeafPartition2, expectedLeafPartition3);
    }

    @Test
    public void shouldInitialiseStateStoreCorrectlyWithIntKeyAndMultipleSplitPointsAndMultiDimRowKey() throws StateStoreException {
        // Given
        Field field0 = new Field("key0", new IntType());
        Field field1 = new Field("key1", new LongType());
        Field field2 = new Field("key2", new StringType());
        Field field3 = new Field("key3", new ByteArrayType());
        Schema schema = schemaWithRowKeys(field0, field1, field2, field3);
        StateStore dynamoDBStateStore = getStateStore(schema);
        List<Object> splitPoints = new ArrayList<>();
        splitPoints.add(-10);
        splitPoints.add(0);
        splitPoints.add(1000);
        InitialiseStateStore initialiseStateStore = InitialiseStateStore.createInitialiseStateStoreFromSplitPoints(schema, dynamoDBStateStore, splitPoints);

        // When
        initialiseStateStore.run();

        // Then
        List<Partition> partitions = dynamoDBStateStore.getAllPartitions();
        assertThat(partitions).hasSize(7);

        Partition rootPartition = partitions.stream().filter(p -> null == p.getParentPartitionId()).collect(Collectors.toList()).get(0);
        List<Partition> internalPartitions = partitions.stream()
                .filter(p -> null != p.getParentPartitionId())
                .filter(p -> !p.isLeafPartition())
                .collect(Collectors.toList());
        internalPartitions.sort(Comparator.comparing(p -> (int) p.getRegion().getRange("key0").getMin()));
        List<Partition> leafPartitions = partitions.stream().filter(Partition::isLeafPartition).collect(Collectors.toList());
        leafPartitions.sort(Comparator.comparing(p -> (int) p.getRegion().getRange("key0").getMin()));

        Range rangeForDim0 = new RangeFactory(schema).createRange(field0, Integer.MIN_VALUE, null);
        Range rangeForDim1 = new RangeFactory(schema).createRange(field1, Long.MIN_VALUE, null);
        Range rangeForDim2 = new RangeFactory(schema).createRange(field2, "", null);
        Range rangeForDim3 = new RangeFactory(schema).createRange(field3, new byte[]{}, null);

        Region expectedRootRegion = new Region(Arrays.asList(rangeForDim0, rangeForDim1, rangeForDim2, rangeForDim3));
        Partition expectedRootPartition = Partition.builder()
                .rowKeyTypes(schema.getRowKeyTypes())
                .region(expectedRootRegion)
                .id(rootPartition.getId())
                .leafPartition(false)
                .parentPartitionId(null)
                .childPartitionIds(Arrays.asList(internalPartitions.get(0).getId(), internalPartitions.get(1).getId()))
                .dimension(0)
                .build();

        Region expectedInternalRegion0 = new Region(Arrays.asList(new RangeFactory(schema).createRange(field0, Integer.MIN_VALUE, 0), rangeForDim1, rangeForDim2, rangeForDim3));
        Partition expectedInternalPartition0 = Partition.builder()
                .rowKeyTypes(schema.getRowKeyTypes())
                .region(expectedInternalRegion0)
                .id(internalPartitions.get(0).getId())
                .leafPartition(false)
                .parentPartitionId(rootPartition.getId())
                .childPartitionIds(Arrays.asList(leafPartitions.get(0).getId(), leafPartitions.get(1).getId()))
                .dimension(0)
                .build();

        Region expectedInternalRegion1 = new Region(Arrays.asList(new RangeFactory(schema).createRange(field0, 0, null), rangeForDim1, rangeForDim2, rangeForDim3));
        Partition expectedInternalPartition1 = Partition.builder()
                .rowKeyTypes(schema.getRowKeyTypes())
                .region(expectedInternalRegion1)
                .id(internalPartitions.get(1).getId())
                .leafPartition(false)
                .parentPartitionId(rootPartition.getId())
                .childPartitionIds(Arrays.asList(leafPartitions.get(2).getId(), leafPartitions.get(3).getId()))
                .dimension(0)
                .build();

        Region expectedLeafRegion0 = new Region(Arrays.asList(new RangeFactory(schema).createRange(field0, Integer.MIN_VALUE, -10), rangeForDim1, rangeForDim2, rangeForDim3));
        Partition expectedLeafPartition0 = Partition.builder()
                .rowKeyTypes(schema.getRowKeyTypes())
                .region(expectedLeafRegion0)
                .id(leafPartitions.get(0).getId())
                .leafPartition(true)
                .parentPartitionId(internalPartitions.get(0).getId())
                .childPartitionIds(Collections.emptyList())
                .dimension(-1)
                .build();

        Region expectedLeafRegion1 = new Region(Arrays.asList(new RangeFactory(schema).createRange(field0, -10, 0), rangeForDim1, rangeForDim2, rangeForDim3));
        Partition expectedLeafPartition1 = Partition.builder()
                .rowKeyTypes(schema.getRowKeyTypes())
                .region(expectedLeafRegion1)
                .id(leafPartitions.get(1).getId())
                .leafPartition(true)
                .parentPartitionId(internalPartitions.get(0).getId())
                .childPartitionIds(Collections.emptyList())
                .dimension(-1)
                .build();

        Region expectedLeafRegion2 = new Region(Arrays.asList(new RangeFactory(schema).createRange(field0, 0, 1000), rangeForDim1, rangeForDim2, rangeForDim3));
        Partition expectedLeafPartition2 = Partition.builder()
                .rowKeyTypes(schema.getRowKeyTypes())
                .region(expectedLeafRegion2)
                .id(leafPartitions.get(2).getId())
                .leafPartition(true)
                .parentPartitionId(internalPartitions.get(1).getId())
                .childPartitionIds(Collections.emptyList())
                .dimension(-1)
                .build();

        Region expectedLeafRegion3 = new Region(Arrays.asList(new RangeFactory(schema).createRange(field0, 1000, null), rangeForDim1, rangeForDim2, rangeForDim3));
        Partition expectedLeafPartition3 = Partition.builder()
                .rowKeyTypes(schema.getRowKeyTypes())
                .region(expectedLeafRegion3)
                .id(leafPartitions.get(3).getId())
                .leafPartition(true)
                .parentPartitionId(internalPartitions.get(1).getId())
                .childPartitionIds(Collections.emptyList())
                .dimension(-1)
                .build();

        assertThat(rootPartition).isEqualTo(expectedRootPartition);
        assertThat(internalPartitions).containsExactly(expectedInternalPartition0, expectedInternalPartition1);
        assertThat(leafPartitions).containsExactly(
                expectedLeafPartition0, expectedLeafPartition1, expectedLeafPartition2, expectedLeafPartition3);
    }

    @Test
    public void shouldInitialiseStateStoreCorrectlyWithStringKeyAndNoSplitPoints() throws StateStoreException {
        // Given
        Field field = new Field("key", new StringType());
        Schema schema = schemaWithRowKeys(field);
        StateStore dynamoDBStateStore = getStateStore(schema);
        InitialiseStateStore initialiseStateStore = InitialiseStateStore.createInitialiseStateStoreFromSplitPoints(schema, dynamoDBStateStore, Collections.emptyList());

        // When
        initialiseStateStore.run();

        // Then
        List<Partition> partitions = dynamoDBStateStore.getAllPartitions();
        assertThat(partitions).hasSize(1);
        Region expectedRegion = new Region(new RangeFactory(schema).createRange(field, "", null));
        Partition expectedPartition = Partition.builder()
                .rowKeyTypes(schema.getRowKeyTypes())
                .region(expectedRegion)
                .id(partitions.get(0).getId())
                .leafPartition(true)
                .parentPartitionId(null)
                .childPartitionIds(Collections.emptyList())
                .dimension(-1)
                .build();
        assertThat(partitions).containsExactly(expectedPartition);
    }

    @Test
    public void shouldInitialiseStateStoreCorrectlyWithStringKeyAndOneSplitPoint() throws StateStoreException {
        // Given
        Field field = new Field("key", new StringType());
        Schema schema = schemaWithRowKeys(field);
        StateStore dynamoDBStateStore = getStateStore(schema);
        List<Object> splitPoints = new ArrayList<>();
        splitPoints.add("E");
        InitialiseStateStore initialiseStateStore = InitialiseStateStore.createInitialiseStateStoreFromSplitPoints(schema, dynamoDBStateStore, splitPoints);

        // When
        initialiseStateStore.run();

        // Then
        List<Partition> partitions = dynamoDBStateStore.getAllPartitions();
        assertThat(partitions).hasSize(3);

        Partition rootPartition = partitions.stream().filter(p -> null == p.getParentPartitionId()).collect(Collectors.toList()).get(0);
        List<Partition> leafPartitions = partitions.stream().filter(Partition::isLeafPartition).collect(Collectors.toList());
        leafPartitions.sort(Comparator.comparing(p -> (String) p.getRegion().getRange("key").getMin()));

        Region expectedRootRegion = new Region(new RangeFactory(schema).createRange(field, "", null));
        Partition expectedRootPartition = Partition.builder()
                .rowKeyTypes(schema.getRowKeyTypes())
                .region(expectedRootRegion)
                .id(rootPartition.getId())
                .leafPartition(false)
                .parentPartitionId(null)
                .childPartitionIds(Arrays.asList(leafPartitions.get(0).getId(), leafPartitions.get(1).getId()))
                .dimension(0)
                .build();
        Region expectedLeafRegion0 = new Region(new RangeFactory(schema).createRange(field, "", "E"));
        Partition expectedLeafPartition0 = Partition.builder()
                .rowKeyTypes(schema.getRowKeyTypes())
                .region(expectedLeafRegion0)
                .id(leafPartitions.get(0).getId())
                .leafPartition(true)
                .parentPartitionId(rootPartition.getId())
                .childPartitionIds(Collections.emptyList())
                .dimension(-1)
                .build();
        Region expectedLeafRegion1 = new Region(new RangeFactory(schema).createRange(field, "E", null));
        Partition expectedLeafPartition1 = Partition.builder()
                .rowKeyTypes(schema.getRowKeyTypes())
                .region(expectedLeafRegion1)
                .id(leafPartitions.get(1).getId())
                .leafPartition(true)
                .parentPartitionId(rootPartition.getId())
                .childPartitionIds(Collections.emptyList())
                .dimension(-1)
                .build();

        assertThat(rootPartition).isEqualTo(expectedRootPartition);
        assertThat(leafPartitions).containsExactly(expectedLeafPartition0, expectedLeafPartition1);
    }

    @Test
    public void shouldInitialiseStateStoreCorrectlyWithStringKeyAndMultipleSplitPoints() throws StateStoreException {
        // Given
        Field field = new Field("key", new StringType());
        Schema schema = schemaWithRowKeys(field);
        StateStore dynamoDBStateStore = getStateStore(schema);
        List<Object> splitPoints = new ArrayList<>();
        splitPoints.add("E");
        splitPoints.add("P");
        splitPoints.add("T");
        InitialiseStateStore initialiseStateStore = InitialiseStateStore.createInitialiseStateStoreFromSplitPoints(schema, dynamoDBStateStore, splitPoints);

        // When
        initialiseStateStore.run();

        // Then
        List<Partition> partitions = dynamoDBStateStore.getAllPartitions();
        assertThat(partitions).hasSize(7);

        Partition rootPartition = partitions.stream().filter(p -> null == p.getParentPartitionId()).collect(Collectors.toList()).get(0);
        List<Partition> internalPartitions = partitions.stream()
                .filter(p -> null != p.getParentPartitionId())
                .filter(p -> !p.isLeafPartition())
                .collect(Collectors.toList());
        internalPartitions.sort(Comparator.comparing(p -> (String) p.getRegion().getRange("key").getMin()));
        List<Partition> leafPartitions = partitions.stream().filter(Partition::isLeafPartition).collect(Collectors.toList());
        leafPartitions.sort(Comparator.comparing(p -> (String) p.getRegion().getRange("key").getMin()));

        Region expectedRootRegion = new Region(new RangeFactory(schema).createRange(field, "", null));
        Partition expectedRootPartition = Partition.builder()
                .rowKeyTypes(schema.getRowKeyTypes())
                .region(expectedRootRegion)
                .id(rootPartition.getId())
                .leafPartition(false)
                .parentPartitionId(null)
                .childPartitionIds(Arrays.asList(internalPartitions.get(0).getId(), internalPartitions.get(1).getId()))
                .dimension(0)
                .build();
        Region expectedInternalfRegion0 = new Region(new RangeFactory(schema).createRange(field, "", "P"));
        Partition expectedInternalPartition0 = Partition.builder()
                .rowKeyTypes(schema.getRowKeyTypes())
                .region(expectedInternalfRegion0)
                .id(internalPartitions.get(0).getId())
                .leafPartition(false)
                .parentPartitionId(rootPartition.getId())
                .childPartitionIds(Arrays.asList(leafPartitions.get(0).getId(), leafPartitions.get(1).getId()))
                .dimension(0)
                .build();
        Region expectedInternalfRegion1 = new Region(new RangeFactory(schema).createRange(field, "P", null));
        Partition expectedInternalPartition1 = Partition.builder()
                .rowKeyTypes(schema.getRowKeyTypes())
                .region(expectedInternalfRegion1)
                .id(internalPartitions.get(1).getId())
                .leafPartition(false)
                .parentPartitionId(rootPartition.getId())
                .childPartitionIds(Arrays.asList(leafPartitions.get(2).getId(), leafPartitions.get(3).getId()))
                .dimension(0)
                .build();
        Region expectedLeafRegion0 = new Region(new RangeFactory(schema).createRange(field, "", "E"));
        Partition expectedLeafPartition0 = Partition.builder()
                .rowKeyTypes(schema.getRowKeyTypes())
                .region(expectedLeafRegion0)
                .id(leafPartitions.get(0).getId())
                .leafPartition(true)
                .parentPartitionId(internalPartitions.get(0).getId())
                .childPartitionIds(Collections.emptyList())
                .dimension(-1)
                .build();
        Region expectedLeafRegion1 = new Region(new RangeFactory(schema).createRange(field, "E", "P"));
        Partition expectedLeafPartition1 = Partition.builder()
                .rowKeyTypes(schema.getRowKeyTypes())
                .region(expectedLeafRegion1)
                .id(leafPartitions.get(1).getId())
                .leafPartition(true)
                .parentPartitionId(internalPartitions.get(0).getId())
                .childPartitionIds(Collections.emptyList())
                .dimension(-1)
                .build();
        Region expectedLeafRegion2 = new Region(new RangeFactory(schema).createRange(field, "P", "T"));
        Partition expectedLeafPartition2 = Partition.builder()
                .rowKeyTypes(schema.getRowKeyTypes())
                .region(expectedLeafRegion2)
                .id(leafPartitions.get(2).getId())
                .leafPartition(true)
                .parentPartitionId(internalPartitions.get(1).getId())
                .childPartitionIds(Collections.emptyList())
                .dimension(-1)
                .build();
        Region expectedLeafRegion3 = new Region(new RangeFactory(schema).createRange(field, "T", null));
        Partition expectedLeafPartition3 = Partition.builder()
                .rowKeyTypes(schema.getRowKeyTypes())
                .region(expectedLeafRegion3)
                .id(leafPartitions.get(3).getId())
                .leafPartition(true)
                .parentPartitionId(internalPartitions.get(1).getId())
                .childPartitionIds(Collections.emptyList())
                .dimension(-1)
                .build();

        assertThat(rootPartition).isEqualTo(expectedRootPartition);
        assertThat(internalPartitions).containsExactly(expectedInternalPartition0, expectedInternalPartition1);
        assertThat(leafPartitions).containsExactly(
                expectedLeafPartition0, expectedLeafPartition1, expectedLeafPartition2, expectedLeafPartition3);
    }

    @Test
    public void shouldInitialiseStateStoreCorrectlyWithStringKeyAndMultipleSplitPointsAndMultiDimRowKey() throws StateStoreException {
        // Given
        Field field0 = new Field("key0", new StringType());
        Field field1 = new Field("key1", new LongType());
        Field field2 = new Field("key2", new StringType());
        Field field3 = new Field("key3", new ByteArrayType());
        Schema schema = schemaWithRowKeys(field0, field1, field2, field3);
        StateStore dynamoDBStateStore = getStateStore(schema);
        List<Object> splitPoints = new ArrayList<>();
        splitPoints.add("E");
        splitPoints.add("P");
        splitPoints.add("T");
        InitialiseStateStore initialiseStateStore = InitialiseStateStore.createInitialiseStateStoreFromSplitPoints(schema, dynamoDBStateStore, splitPoints);

        // When
        initialiseStateStore.run();

        // Then
        List<Partition> partitions = dynamoDBStateStore.getAllPartitions();
        assertThat(partitions).hasSize(7);

        Partition rootPartition = partitions.stream().filter(p -> null == p.getParentPartitionId()).collect(Collectors.toList()).get(0);
        List<Partition> internalPartitions = partitions.stream()
                .filter(p -> null != p.getParentPartitionId())
                .filter(p -> !p.isLeafPartition())
                .collect(Collectors.toList());
        internalPartitions.sort(Comparator.comparing(p -> (String) p.getRegion().getRange("key0").getMin()));
        List<Partition> leafPartitions = partitions.stream().filter(Partition::isLeafPartition).collect(Collectors.toList());
        leafPartitions.sort(Comparator.comparing(p -> (String) p.getRegion().getRange("key0").getMin()));

        Range rangeForDim0 = new RangeFactory(schema).createRange(field0, "", null);
        Range rangeForDim1 = new RangeFactory(schema).createRange(field1, Long.MIN_VALUE, null);
        Range rangeForDim2 = new RangeFactory(schema).createRange(field2, "", null);
        Range rangeForDim3 = new RangeFactory(schema).createRange(field3, new byte[]{}, null);

        Region expectedRootRegion = new Region(Arrays.asList(rangeForDim0, rangeForDim1, rangeForDim2, rangeForDim3));
        Partition expectedRootPartition = Partition.builder()
                .rowKeyTypes(schema.getRowKeyTypes())
                .region(expectedRootRegion)
                .id(rootPartition.getId())
                .leafPartition(false)
                .parentPartitionId(null)
                .childPartitionIds(Arrays.asList(internalPartitions.get(0).getId(), internalPartitions.get(1).getId()))
                .dimension(0)
                .build();

        Region expectedInternalRegion0 = new Region(Arrays.asList(new RangeFactory(schema).createRange(field0, "", "P"), rangeForDim1, rangeForDim2, rangeForDim3));
        Partition expectedInternalPartition0 = Partition.builder()
                .rowKeyTypes(schema.getRowKeyTypes())
                .region(expectedInternalRegion0)
                .id(internalPartitions.get(0).getId())
                .leafPartition(false)
                .parentPartitionId(rootPartition.getId())
                .childPartitionIds(Arrays.asList(leafPartitions.get(0).getId(), leafPartitions.get(1).getId()))
                .dimension(0)
                .build();

        Region expectedInternalRegion1 = new Region(Arrays.asList(new RangeFactory(schema).createRange(field0, "P", null), rangeForDim1, rangeForDim2, rangeForDim3));
        Partition expectedInternalPartition1 = Partition.builder()
                .rowKeyTypes(schema.getRowKeyTypes())
                .region(expectedInternalRegion1)
                .id(internalPartitions.get(1).getId())
                .leafPartition(false)
                .parentPartitionId(rootPartition.getId())
                .childPartitionIds(Arrays.asList(leafPartitions.get(2).getId(), leafPartitions.get(3).getId()))
                .dimension(0)
                .build();

        Region expectedLeafRegion0 = new Region(Arrays.asList(new RangeFactory(schema).createRange(field0, "", "E"), rangeForDim1, rangeForDim2, rangeForDim3));
        Partition expectedLeafPartition0 = Partition.builder()
                .rowKeyTypes(schema.getRowKeyTypes())
                .region(expectedLeafRegion0)
                .id(leafPartitions.get(0).getId())
                .leafPartition(true)
                .parentPartitionId(internalPartitions.get(0).getId())
                .childPartitionIds(Collections.emptyList())
                .dimension(-1)
                .build();

        Region expectedLeafRegion1 = new Region(Arrays.asList(new RangeFactory(schema).createRange(field0, "E", "P"), rangeForDim1, rangeForDim2, rangeForDim3));
        Partition expectedLeafPartition1 = Partition.builder()
                .rowKeyTypes(schema.getRowKeyTypes())
                .region(expectedLeafRegion1)
                .id(leafPartitions.get(1).getId())
                .leafPartition(true)
                .parentPartitionId(internalPartitions.get(0).getId())
                .childPartitionIds(Collections.emptyList())
                .dimension(-1)
                .build();

        Region expectedLeafRegion2 = new Region(Arrays.asList(new RangeFactory(schema).createRange(field0, "P", "T"), rangeForDim1, rangeForDim2, rangeForDim3));
        Partition expectedLeafPartition2 = Partition.builder()
                .rowKeyTypes(schema.getRowKeyTypes())
                .region(expectedLeafRegion2)
                .id(leafPartitions.get(2).getId())
                .leafPartition(true)
                .parentPartitionId(internalPartitions.get(1).getId())
                .childPartitionIds(Collections.emptyList())
                .dimension(-1)
                .build();

        Region expectedLeafRegion3 = new Region(Arrays.asList(new RangeFactory(schema).createRange(field0, "T", null), rangeForDim1, rangeForDim2, rangeForDim3));
        Partition expectedLeafPartition3 = Partition.builder()
                .rowKeyTypes(schema.getRowKeyTypes())
                .region(expectedLeafRegion3)
                .id(leafPartitions.get(3).getId())
                .leafPartition(true)
                .parentPartitionId(internalPartitions.get(1).getId())
                .childPartitionIds(Collections.emptyList())
                .dimension(-1)
                .build();

        assertThat(rootPartition).isEqualTo(expectedRootPartition);
        assertThat(internalPartitions).containsExactly(expectedInternalPartition0, expectedInternalPartition1);
        assertThat(leafPartitions).containsExactly(
                expectedLeafPartition0, expectedLeafPartition1, expectedLeafPartition2, expectedLeafPartition3);
    }

    @Test
    public void shouldInitialiseStateStoreCorrectlyWithByteArrayKeyAndNoSplitPoints() throws StateStoreException {
        // Given
        Field field = new Field("key", new ByteArrayType());
        Schema schema = schemaWithRowKeys(field);
        StateStore dynamoDBStateStore = getStateStore(schema);
        InitialiseStateStore initialiseStateStore = InitialiseStateStore.createInitialiseStateStoreFromSplitPoints(schema, dynamoDBStateStore, Collections.emptyList());

        // When
        initialiseStateStore.run();

        // Then
        List<Partition> partitions = dynamoDBStateStore.getAllPartitions();
        assertThat(partitions).hasSize(1);

        Region expectedRegion = new Region(new RangeFactory(schema).createRange(field, new byte[]{}, null));
        Partition expectedPartition = Partition.builder()
                .rowKeyTypes(schema.getRowKeyTypes())
                .region(expectedRegion)
                .id(partitions.get(0).getId())
                .leafPartition(true)
                .parentPartitionId(null)
                .childPartitionIds(Collections.emptyList())
                .dimension(-1)
                .build();
        assertThat(partitions).containsExactly(expectedPartition);
    }

    @Test
    public void shouldInitialiseStateStoreCorrectlyWithByteArrayKeyAndOneSplitPoint() throws StateStoreException {
        // Given
        Field field = new Field("key", new ByteArrayType());
        Schema schema = schemaWithRowKeys(field);
        StateStore dynamoDBStateStore = getStateStore(schema);
        List<Object> splitPoints = new ArrayList<>();
        splitPoints.add(new byte[]{10});
        InitialiseStateStore initialiseStateStore = InitialiseStateStore.createInitialiseStateStoreFromSplitPoints(schema, dynamoDBStateStore, splitPoints);

        // When
        initialiseStateStore.run();

        // Then
        List<Partition> partitions = dynamoDBStateStore.getAllPartitions();
        assertThat(partitions).hasSize(3);

        Partition rootPartition = partitions.stream().filter(p -> null == p.getParentPartitionId()).collect(Collectors.toList()).get(0);
        List<Partition> leafPartitions = partitions.stream().filter(Partition::isLeafPartition).collect(Collectors.toList());
        leafPartitions.sort(Comparator.comparing(p -> ByteArray.wrap((byte[]) p.getRegion().getRange("key").getMin())));

        Region expectedRegion = new Region(new RangeFactory(schema).createRange(field, new byte[]{}, null));
        Partition expectedRootPartition = Partition.builder()
                .rowKeyTypes(schema.getRowKeyTypes())
                .region(expectedRegion)
                .id(rootPartition.getId())
                .leafPartition(false)
                .parentPartitionId(null)
                .childPartitionIds(Arrays.asList(leafPartitions.get(0).getId(), leafPartitions.get(1).getId()))
                .dimension(0)
                .build();
        Region expectedLeafRegion0 = new Region(new RangeFactory(schema).createRange(field, new byte[]{}, new byte[]{10}));
        Partition expectedLeafPartition0 = Partition.builder()
                .rowKeyTypes(schema.getRowKeyTypes())
                .region(expectedLeafRegion0)
                .id(leafPartitions.get(0).getId())
                .leafPartition(true)
                .parentPartitionId(rootPartition.getId())
                .childPartitionIds(Collections.emptyList())
                .dimension(-1)
                .build();
        Region expectedLeafRegion1 = new Region(new RangeFactory(schema).createRange(field, new byte[]{10}, null));
        Partition expectedLeafPartition1 = Partition.builder()
                .rowKeyTypes(schema.getRowKeyTypes())
                .region(expectedLeafRegion1)
                .id(leafPartitions.get(1).getId())
                .leafPartition(true)
                .parentPartitionId(rootPartition.getId())
                .childPartitionIds(Collections.emptyList())
                .dimension(-1)
                .build();

        assertThat(rootPartition).isEqualTo(expectedRootPartition);
        assertThat(leafPartitions).containsExactly(expectedLeafPartition0, expectedLeafPartition1);
    }

    @Test
    public void shouldInitialiseStateStoreCorrectlyWithByteArrayKeyAndMultipleSplitPoints() throws StateStoreException {
        // Given
        Field field = new Field("key", new ByteArrayType());
        Schema schema = schemaWithRowKeys(field);
        StateStore dynamoDBStateStore = getStateStore(schema);
        List<Object> splitPoints = new ArrayList<>();
        splitPoints.add(new byte[]{10});
        splitPoints.add(new byte[]{50});
        splitPoints.add(new byte[]{99});
        InitialiseStateStore initialiseStateStore = InitialiseStateStore.createInitialiseStateStoreFromSplitPoints(schema, dynamoDBStateStore, splitPoints);

        // When
        initialiseStateStore.run();

        // Then
        List<Partition> partitions = dynamoDBStateStore.getAllPartitions();
        assertThat(partitions).hasSize(7);

        Partition rootPartition = partitions.stream().filter(p -> null == p.getParentPartitionId()).collect(Collectors.toList()).get(0);
        List<Partition> internalPartitions = partitions.stream()
                .filter(p -> null != p.getParentPartitionId())
                .filter(p -> !p.isLeafPartition())
                .collect(Collectors.toList());
        internalPartitions.sort(Comparator.comparing(p -> ByteArray.wrap((byte[]) p.getRegion().getRange("key").getMin())));
        List<Partition> leafPartitions = partitions.stream().filter(Partition::isLeafPartition).collect(Collectors.toList());
        leafPartitions.sort(Comparator.comparing(p -> ByteArray.wrap((byte[]) p.getRegion().getRange("key").getMin())));

        Region expectedRootRegion = new Region(new RangeFactory(schema).createRange(field, new byte[]{}, null));
        Partition expectedRootPartition = Partition.builder()
                .rowKeyTypes(schema.getRowKeyTypes())
                .region(expectedRootRegion)
                .id(rootPartition.getId())
                .leafPartition(false)
                .parentPartitionId(null)
                .childPartitionIds(Arrays.asList(internalPartitions.get(0).getId(), internalPartitions.get(1).getId()))
                .dimension(0)
                .build();

        Region expectedInternalRegion0 = new Region(new RangeFactory(schema).createRange(field, new byte[]{}, new byte[]{50}));
        Partition expectedInternalPartition0 = Partition.builder()
                .rowKeyTypes(schema.getRowKeyTypes())
                .region(expectedInternalRegion0)
                .id(internalPartitions.get(0).getId())
                .leafPartition(false)
                .parentPartitionId(rootPartition.getId())
                .childPartitionIds(Arrays.asList(leafPartitions.get(0).getId(), leafPartitions.get(1).getId()))
                .dimension(0)
                .build();

        Region expectedInternalRegion1 = new Region(new RangeFactory(schema).createRange(field, new byte[]{50}, null));
        Partition expectedInternalPartition1 = Partition.builder()
                .rowKeyTypes(schema.getRowKeyTypes())
                .region(expectedInternalRegion1)
                .id(internalPartitions.get(1).getId())
                .leafPartition(false)
                .parentPartitionId(rootPartition.getId())
                .childPartitionIds(Arrays.asList(leafPartitions.get(2).getId(), leafPartitions.get(3).getId()))
                .dimension(0)
                .build();

        Region expectedLeafRegion0 = new Region(new RangeFactory(schema).createRange(field, new byte[]{}, new byte[]{10}));
        Partition expectedLeafPartition0 = Partition.builder()
                .rowKeyTypes(schema.getRowKeyTypes())
                .region(expectedLeafRegion0)
                .id(leafPartitions.get(0).getId())
                .leafPartition(true)
                .parentPartitionId(internalPartitions.get(0).getId())
                .childPartitionIds(Collections.emptyList())
                .dimension(-1)
                .build();

        Region expectedLeafRegion1 = new Region(new RangeFactory(schema).createRange(field, new byte[]{10}, new byte[]{50}));
        Partition expectedLeafPartition1 = Partition.builder()
                .rowKeyTypes(schema.getRowKeyTypes())
                .region(expectedLeafRegion1)
                .id(leafPartitions.get(1).getId())
                .leafPartition(true)
                .parentPartitionId(internalPartitions.get(0).getId())
                .childPartitionIds(Collections.emptyList())
                .dimension(-1)
                .build();

        Region expectedLeafRegion2 = new Region(new RangeFactory(schema).createRange(field, new byte[]{50}, new byte[]{99}));
        Partition expectedLeafPartition2 = Partition.builder()
                .rowKeyTypes(schema.getRowKeyTypes())
                .region(expectedLeafRegion2)
                .id(leafPartitions.get(2).getId())
                .leafPartition(true)
                .parentPartitionId(internalPartitions.get(1).getId())
                .childPartitionIds(Collections.emptyList())
                .dimension(-1)
                .build();

        Region expectedLeafRegion3 = new Region(new RangeFactory(schema).createRange(field, new byte[]{99}, null));
        Partition expectedLeafPartition3 = Partition.builder()
                .rowKeyTypes(schema.getRowKeyTypes())
                .region(expectedLeafRegion3)
                .id(leafPartitions.get(3).getId())
                .leafPartition(true)
                .parentPartitionId(internalPartitions.get(1).getId())
                .childPartitionIds(Collections.emptyList())
                .dimension(-1)
                .build();

        assertThat(rootPartition).isEqualTo(expectedRootPartition);
        assertThat(internalPartitions).containsExactly(expectedInternalPartition0, expectedInternalPartition1);
        assertThat(leafPartitions).containsExactly(
                expectedLeafPartition0, expectedLeafPartition1, expectedLeafPartition2, expectedLeafPartition3);
    }

    @Test
    public void shouldInitialiseStateStoreCorrectlyWithByteArrayKeyAndMultipleSplitPointsAndMultiDimRowKey() throws StateStoreException {
        // Given
        Field field0 = new Field("key0", new ByteArrayType());
        Field field1 = new Field("key1", new LongType());
        Field field2 = new Field("key2", new StringType());
        Field field3 = new Field("key3", new ByteArrayType());
        Schema schema = schemaWithRowKeys(field0, field1, field2, field3);
        StateStore dynamoDBStateStore = getStateStore(schema);
        List<Object> splitPoints = new ArrayList<>();
        splitPoints.add(new byte[]{10});
        splitPoints.add(new byte[]{50});
        splitPoints.add(new byte[]{99});
        InitialiseStateStore initialiseStateStore = InitialiseStateStore.createInitialiseStateStoreFromSplitPoints(schema, dynamoDBStateStore, splitPoints);

        // When
        initialiseStateStore.run();

        // Then
        List<Partition> partitions = dynamoDBStateStore.getAllPartitions();
        assertThat(partitions).hasSize(7);

        Partition rootPartition = partitions.stream().filter(p -> null == p.getParentPartitionId()).collect(Collectors.toList()).get(0);
        List<Partition> internalPartitions = partitions.stream()
                .filter(p -> null != p.getParentPartitionId())
                .filter(p -> !p.isLeafPartition())
                .collect(Collectors.toList());
        internalPartitions.sort(Comparator.comparing(p -> ByteArray.wrap((byte[]) p.getRegion().getRange("key0").getMin())));
        List<Partition> leafPartitions = partitions.stream().filter(Partition::isLeafPartition).collect(Collectors.toList());
        leafPartitions.sort(Comparator.comparing(p -> ByteArray.wrap((byte[]) p.getRegion().getRange("key0").getMin())));

        Range rangeForDim0 = new RangeFactory(schema).createRange(field0, new byte[]{}, null);
        Range rangeForDim1 = new RangeFactory(schema).createRange(field1, Long.MIN_VALUE, null);
        Range rangeForDim2 = new RangeFactory(schema).createRange(field2, "", null);
        Range rangeForDim3 = new RangeFactory(schema).createRange(field3, new byte[]{}, null);

        Region expectedRootRegion = new Region(Arrays.asList(rangeForDim0, rangeForDim1, rangeForDim2, rangeForDim3));
        Partition expectedRootPartition = Partition.builder()
                .rowKeyTypes(schema.getRowKeyTypes())
                .region(expectedRootRegion)
                .id(rootPartition.getId())
                .leafPartition(false)
                .parentPartitionId(null)
                .childPartitionIds(Arrays.asList(internalPartitions.get(0).getId(), internalPartitions.get(1).getId()))
                .dimension(0)
                .build();

        Region expectedInternalRegion0 = new Region(Arrays.asList(new RangeFactory(schema).createRange(field0, new byte[]{}, new byte[]{50}), rangeForDim1, rangeForDim2, rangeForDim3));
        Partition expectedInternalPartition0 = Partition.builder()
                .rowKeyTypes(schema.getRowKeyTypes())
                .region(expectedInternalRegion0)
                .id(internalPartitions.get(0).getId())
                .leafPartition(false)
                .parentPartitionId(rootPartition.getId())
                .childPartitionIds(Arrays.asList(leafPartitions.get(0).getId(), leafPartitions.get(1).getId()))
                .dimension(0)
                .build();

        Region expectedInternalRegion1 = new Region(Arrays.asList(new RangeFactory(schema).createRange(field0, new byte[]{50}, null), rangeForDim1, rangeForDim2, rangeForDim3));
        Partition expectedInternalPartition1 = Partition.builder()
                .rowKeyTypes(schema.getRowKeyTypes())
                .region(expectedInternalRegion1)
                .id(internalPartitions.get(1).getId())
                .leafPartition(false)
                .parentPartitionId(rootPartition.getId())
                .childPartitionIds(Arrays.asList(leafPartitions.get(2).getId(), leafPartitions.get(3).getId()))
                .dimension(0)
                .build();

        Region expectedLeafRegion0 = new Region(Arrays.asList(new RangeFactory(schema).createRange(field0, new byte[]{}, new byte[]{10}), rangeForDim1, rangeForDim2, rangeForDim3));
        Partition expectedLeafPartition0 = Partition.builder()
                .rowKeyTypes(schema.getRowKeyTypes())
                .region(expectedLeafRegion0)
                .id(leafPartitions.get(0).getId())
                .leafPartition(true)
                .parentPartitionId(internalPartitions.get(0).getId())
                .childPartitionIds(Collections.emptyList())
                .dimension(-1)
                .build();

        Region expectedLeafRegion1 = new Region(Arrays.asList(new RangeFactory(schema).createRange(field0, new byte[]{10}, new byte[]{50}), rangeForDim1, rangeForDim2, rangeForDim3));
        Partition expectedLeafPartition1 = Partition.builder()
                .rowKeyTypes(schema.getRowKeyTypes())
                .region(expectedLeafRegion1)
                .id(leafPartitions.get(1).getId())
                .leafPartition(true)
                .parentPartitionId(internalPartitions.get(0).getId())
                .childPartitionIds(Collections.emptyList())
                .dimension(-1)
                .build();

        Region expectedLeafRegion2 = new Region(Arrays.asList(new RangeFactory(schema).createRange(field0, new byte[]{50}, new byte[]{99}), rangeForDim1, rangeForDim2, rangeForDim3));
        Partition expectedLeafPartition2 = Partition.builder()
                .rowKeyTypes(schema.getRowKeyTypes())
                .region(expectedLeafRegion2)
                .id(leafPartitions.get(2).getId())
                .leafPartition(true)
                .parentPartitionId(internalPartitions.get(1).getId())
                .childPartitionIds(Collections.emptyList())
                .dimension(-1)
                .build();

        Region expectedLeafRegion3 = new Region(Arrays.asList(new RangeFactory(schema).createRange(field0, new byte[]{99}, null), rangeForDim1, rangeForDim2, rangeForDim3));
        Partition expectedLeafPartition3 = Partition.builder()
                .rowKeyTypes(schema.getRowKeyTypes())
                .region(expectedLeafRegion3)
                .id(leafPartitions.get(3).getId())
                .leafPartition(true)
                .parentPartitionId(internalPartitions.get(1).getId())
                .childPartitionIds(Collections.emptyList())
                .dimension(-1)
                .build();

        assertThat(rootPartition).isEqualTo(expectedRootPartition);
        assertThat(internalPartitions).containsExactly(expectedInternalPartition0, expectedInternalPartition1);
        assertThat(leafPartitions).containsExactly(
                expectedLeafPartition0, expectedLeafPartition1, expectedLeafPartition2, expectedLeafPartition3);
    }

    @Test
    public void shouldThrowExceptionIfSplitPointIsOfWrongType() {
        // Given
        Schema schema = schemaWithRowKeys(new Field("key", new IntType()));
        StateStore dynamoDBStateStore = getStateStore(schema);
        List<Object> splitPoints = new ArrayList<>();
        splitPoints.add(Long.MIN_VALUE);

        // When / Then
        assertThatThrownBy(() -> InitialiseStateStore.createInitialiseStateStoreFromSplitPoints(schema, dynamoDBStateStore, splitPoints))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    public void shouldThrowExceptionIfDuplicateSplitPoints() {
        // Given
        Schema schema = schemaWithRowKeys(new Field("key", new IntType()));
        StateStore dynamoDBStateStore = getStateStore(schema);
        List<Object> splitPoints = new ArrayList<>();
        splitPoints.add(0);
        splitPoints.add(0);

        // When / Then
        assertThatThrownBy(() -> InitialiseStateStore.createInitialiseStateStoreFromSplitPoints(schema, dynamoDBStateStore, splitPoints))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    public void shouldThrowExceptionIfSplitPointsAreInWrongOrder() {
        // Given
        Schema schema = schemaWithRowKeys(new Field("key", new IntType()));
        StateStore dynamoDBStateStore = getStateStore(schema);
        List<Object> splitPoints = new ArrayList<>();
        splitPoints.add(1);
        splitPoints.add(0);

        // When / Then
        assertThatThrownBy(() -> InitialiseStateStore.createInitialiseStateStoreFromSplitPoints(schema, dynamoDBStateStore, splitPoints))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
