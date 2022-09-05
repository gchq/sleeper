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
package sleeper.statestore;

import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.facebook.collections.ByteArray;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.testcontainers.containers.GenericContainer;
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
import sleeper.statestore.dynamodb.DynamoDBStateStoreCreator;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class InitialiseStateStoreIT {
    private static final int DYNAMO_PORT = 8000;
    private static AmazonDynamoDB dynamoDBClient;

    @ClassRule
    public static GenericContainer dynamoDb = new GenericContainer(CommonTestConstants.DYNAMODB_LOCAL_CONTAINER)
            .withExposedPorts(DYNAMO_PORT);

    @BeforeClass
    public static void initDynamoClient() {
        AwsClientBuilder.EndpointConfiguration endpointConfiguration =
                new AwsClientBuilder.EndpointConfiguration("http://" + dynamoDb.getContainerIpAddress() + ":"
                        + dynamoDb.getMappedPort(DYNAMO_PORT), "us-west-2");
        dynamoDBClient = AmazonDynamoDBClientBuilder.standard()
                .withCredentials(new AWSStaticCredentialsProvider(new BasicAWSCredentials("12345", "6789")))
                .withEndpointConfiguration(endpointConfiguration)
                .build();
    }

    @AfterClass
    public static void shutdownDynamoClient() {
        dynamoDBClient.shutdown();
    }

    private static StateStore getStateStore(Schema schema) throws StateStoreException {
        String id = UUID.randomUUID().toString();
        DynamoDBStateStoreCreator dynamoDBStateStoreCreator = new DynamoDBStateStoreCreator(id, schema, dynamoDBClient);
        return dynamoDBStateStoreCreator.create();
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
        InitialiseStateStore initialiseStateStore = new InitialiseStateStore(schema, dynamoDBStateStore, Collections.emptyList());

        // When
        initialiseStateStore.run();

        // Then
        Region expectedRegion = new Region(new RangeFactory(schema).createRange(field, Integer.MIN_VALUE, null));
        Partition expectedPartition = new Partition(
                schema.getRowKeyTypes(),
                expectedRegion,
                "root",
                true,
                null,
                Collections.emptyList(),
                -1
        );
        assertThat(dynamoDBStateStore.getAllPartitions()).containsExactly(expectedPartition);
    }

    @Test
    public void shouldInitialiseStateStoreCorrectlyWithIntKeyAndOneSplitPoint() throws StateStoreException {
        // Given
        StateStore dynamoDBStateStore = getStateStore(schema);
        List<Object> splitPoints = new ArrayList<>();
        splitPoints.add(-10);
        InitialiseStateStore initialiseStateStore = new InitialiseStateStore(schema, dynamoDBStateStore, splitPoints);

        // When
        initialiseStateStore.run();

        // Then
        List<Partition> partitions = dynamoDBStateStore.getAllPartitions();
        assertThat(partitions).hasSize(3);

        Partition rootPartition = partitions.stream().filter(p -> null == p.getParentPartitionId()).collect(Collectors.toList()).get(0);
        List<Partition> leafPartitions = partitions.stream().filter(Partition::isLeafPartition).collect(Collectors.toList());
        leafPartitions.sort(Comparator.comparing(p -> (int) p.getRegion().getRange("key").getMin()));

        Region expectedRootRegion = new Region(new RangeFactory(schema).createRange(field, Integer.MIN_VALUE, null));
        Partition expectedRootPartition = new Partition(
                schema.getRowKeyTypes(),
                expectedRootRegion,
                rootPartition.getId(),
                false,
                null,
                Arrays.asList(leafPartitions.get(0).getId(), leafPartitions.get(1).getId()),
                0
        );
        Region expectedLeafRegion0 = new Region(new RangeFactory(schema).createRange(field, Integer.MIN_VALUE, -10));
        Partition expectedLeafPartition0 = new Partition(
                schema.getRowKeyTypes(),
                expectedLeafRegion0,
                leafPartitions.get(0).getId(),
                true,
                rootPartition.getId(),
                Collections.emptyList(),
                -1
        );
        Region expectedLeafRegion1 = new Region(new RangeFactory(schema).createRange(field, -10, null));
        Partition expectedLeafPartition1 = new Partition(
                schema.getRowKeyTypes(),
                expectedLeafRegion1,
                leafPartitions.get(1).getId(),
                true,
                rootPartition.getId(),
                Collections.emptyList(),
                -1
        );

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
        InitialiseStateStore initialiseStateStore = new InitialiseStateStore(schema, dynamoDBStateStore, splitPoints);

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
        Partition expectedRootPartition = new Partition(
                schema.getRowKeyTypes(),
                expectedRootRegion,
                rootPartition.getId(),
                false,
                null,
                Arrays.asList(internalPartitions.get(0).getId(), internalPartitions.get(1).getId()),
                0
        );
        Region expectedInternalRegion0 = new Region(new RangeFactory(schema).createRange(field, Integer.MIN_VALUE, 0));
        Partition expectedInternalPartition0 = new Partition(
                schema.getRowKeyTypes(),
                expectedInternalRegion0,
                internalPartitions.get(0).getId(),
                false,
                rootPartition.getId(),
                Arrays.asList(leafPartitions.get(0).getId(), leafPartitions.get(1).getId()),
                0
        );
        Region expectedInternalRegion1 = new Region(new RangeFactory(schema).createRange(field, 0, null));
        Partition expectedInternalPartition1 = new Partition(
                schema.getRowKeyTypes(),
                expectedInternalRegion1,
                internalPartitions.get(1).getId(),
                false,
                rootPartition.getId(),
                Arrays.asList(leafPartitions.get(2).getId(), leafPartitions.get(3).getId()),
                0
        );
        Region expectedLeafRegion0 = new Region(new RangeFactory(schema).createRange(field, Integer.MIN_VALUE, -10));
        Partition expectedLeafPartition0 = new Partition(
                schema.getRowKeyTypes(),
                expectedLeafRegion0,
                leafPartitions.get(0).getId(),
                true,
                internalPartitions.get(0).getId(),
                Collections.emptyList(),
                -1
        );
        Region expectedLeafRegion1 = new Region(new RangeFactory(schema).createRange(field, -10, 0));
        Partition expectedLeafPartition1 = new Partition(
                schema.getRowKeyTypes(),
                expectedLeafRegion1,
                leafPartitions.get(1).getId(),
                true,
                internalPartitions.get(0).getId(),
                Collections.emptyList(),
                -1
        );
        Region expectedLeafRegion2 = new Region(new RangeFactory(schema).createRange(field, 0, 1000));
        Partition expectedLeafPartition2 = new Partition(
                schema.getRowKeyTypes(),
                expectedLeafRegion2,
                leafPartitions.get(2).getId(),
                true,
                internalPartitions.get(1).getId(),
                Collections.emptyList(),
                -1
        );
        Region expectedLeafRegion3 = new Region(new RangeFactory(schema).createRange(field, 1000, null));
        Partition expectedLeafPartition3 = new Partition(
                schema.getRowKeyTypes(),
                expectedLeafRegion3,
                leafPartitions.get(3).getId(),
                true,
                internalPartitions.get(1).getId(),
                Collections.emptyList(),
                -1
        );

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
        InitialiseStateStore initialiseStateStore = new InitialiseStateStore(schema, dynamoDBStateStore, splitPoints);

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
        Partition expectedRootPartition = new Partition(
                schema.getRowKeyTypes(),
                expectedRootRegion,
                rootPartition.getId(),
                false,
                null,
                Arrays.asList(internalPartitions.get(0).getId(), internalPartitions.get(1).getId()),
                0
        );

        Region expectedInternalRegion0 = new Region(Arrays.asList(new RangeFactory(schema).createRange(field0, Integer.MIN_VALUE, 0), rangeForDim1, rangeForDim2, rangeForDim3));
        Partition expectedInternalPartition0 = new Partition(
                schema.getRowKeyTypes(),
                expectedInternalRegion0,
                internalPartitions.get(0).getId(),
                false,
                rootPartition.getId(),
                Arrays.asList(leafPartitions.get(0).getId(), leafPartitions.get(1).getId()),
                0
        );

        Region expectedInternalRegion1 = new Region(Arrays.asList(new RangeFactory(schema).createRange(field0, 0, null), rangeForDim1, rangeForDim2, rangeForDim3));
        Partition expectedInternalPartition1 = new Partition(
                schema.getRowKeyTypes(),
                expectedInternalRegion1,
                internalPartitions.get(1).getId(),
                false,
                rootPartition.getId(),
                Arrays.asList(leafPartitions.get(2).getId(), leafPartitions.get(3).getId()),
                0
        );

        Region expectedLeafRegion0 = new Region(Arrays.asList(new RangeFactory(schema).createRange(field0, Integer.MIN_VALUE, -10), rangeForDim1, rangeForDim2, rangeForDim3));
        Partition expectedLeafPartition0 = new Partition(
                schema.getRowKeyTypes(),
                expectedLeafRegion0,
                leafPartitions.get(0).getId(),
                true,
                internalPartitions.get(0).getId(),
                Collections.emptyList(),
                -1
        );

        Region expectedLeafRegion1 = new Region(Arrays.asList(new RangeFactory(schema).createRange(field0, -10, 0), rangeForDim1, rangeForDim2, rangeForDim3));
        Partition expectedLeafPartition1 = new Partition(
                schema.getRowKeyTypes(),
                expectedLeafRegion1,
                leafPartitions.get(1).getId(),
                true,
                internalPartitions.get(0).getId(),
                Collections.emptyList(),
                -1
        );

        Region expectedLeafRegion2 = new Region(Arrays.asList(new RangeFactory(schema).createRange(field0, 0, 1000), rangeForDim1, rangeForDim2, rangeForDim3));
        Partition expectedLeafPartition2 = new Partition(
                schema.getRowKeyTypes(),
                expectedLeafRegion2,
                leafPartitions.get(2).getId(),
                true,
                internalPartitions.get(1).getId(),
                Collections.emptyList(),
                -1
        );

        Region expectedLeafRegion3 = new Region(Arrays.asList(new RangeFactory(schema).createRange(field0, 1000, null), rangeForDim1, rangeForDim2, rangeForDim3));
        Partition expectedLeafPartition3 = new Partition(
                schema.getRowKeyTypes(),
                expectedLeafRegion3,
                leafPartitions.get(3).getId(),
                true,
                internalPartitions.get(1).getId(),
                Collections.emptyList(),
                -1
        );

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
        InitialiseStateStore initialiseStateStore = new InitialiseStateStore(schema, dynamoDBStateStore, Collections.emptyList());

        // When
        initialiseStateStore.run();

        // Then
        List<Partition> partitions = dynamoDBStateStore.getAllPartitions();
        assertThat(partitions).hasSize(1);
        Region expectedRegion = new Region(new RangeFactory(schema).createRange(field, "", null));
        Partition expectedPartition = new Partition(
                schema.getRowKeyTypes(),
                expectedRegion,
                partitions.get(0).getId(),
                true,
                null,
                Collections.emptyList(),
                -1
        );
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
        InitialiseStateStore initialiseStateStore = new InitialiseStateStore(schema, dynamoDBStateStore, splitPoints);

        // When
        initialiseStateStore.run();

        // Then
        List<Partition> partitions = dynamoDBStateStore.getAllPartitions();
        assertThat(partitions).hasSize(3);

        Partition rootPartition = partitions.stream().filter(p -> null == p.getParentPartitionId()).collect(Collectors.toList()).get(0);
        List<Partition> leafPartitions = partitions.stream().filter(Partition::isLeafPartition).collect(Collectors.toList());
        leafPartitions.sort(Comparator.comparing(p -> (String) p.getRegion().getRange("key").getMin()));

        Region expectedRootRegion = new Region(new RangeFactory(schema).createRange(field, "", null));
        Partition expectedRootPartition = new Partition(
                schema.getRowKeyTypes(),
                expectedRootRegion,
                rootPartition.getId(),
                false,
                null,
                Arrays.asList(leafPartitions.get(0).getId(), leafPartitions.get(1).getId()),
                0
        );
        Region expectedLeafRegion0 = new Region(new RangeFactory(schema).createRange(field, "", "E"));
        Partition expectedLeafPartition0 = new Partition(
                schema.getRowKeyTypes(),
                expectedLeafRegion0,
                leafPartitions.get(0).getId(),
                true,
                rootPartition.getId(),
                Collections.emptyList(),
                -1
        );
        Region expectedLeafRegion1 = new Region(new RangeFactory(schema).createRange(field, "E", null));
        Partition expectedLeafPartition1 = new Partition(
                schema.getRowKeyTypes(),
                expectedLeafRegion1,
                leafPartitions.get(1).getId(),
                true,
                rootPartition.getId(),
                Collections.emptyList(),
                -1
        );

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
        InitialiseStateStore initialiseStateStore = new InitialiseStateStore(schema, dynamoDBStateStore, splitPoints);

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
        Partition expectedRootPartition = new Partition(
                schema.getRowKeyTypes(),
                expectedRootRegion,
                rootPartition.getId(),
                false,
                null,
                Arrays.asList(internalPartitions.get(0).getId(), internalPartitions.get(1).getId()),
                0
        );
        Region expectedInternalfRegion0 = new Region(new RangeFactory(schema).createRange(field, "", "P"));
        Partition expectedInternalPartition0 = new Partition(
                schema.getRowKeyTypes(),
                expectedInternalfRegion0,
                internalPartitions.get(0).getId(),
                false,
                rootPartition.getId(),
                Arrays.asList(leafPartitions.get(0).getId(), leafPartitions.get(1).getId()),
                0
        );
        Region expectedInternalfRegion1 = new Region(new RangeFactory(schema).createRange(field, "P", null));
        Partition expectedInternalPartition1 = new Partition(
                schema.getRowKeyTypes(),
                expectedInternalfRegion1,
                internalPartitions.get(1).getId(),
                false,
                rootPartition.getId(),
                Arrays.asList(leafPartitions.get(2).getId(), leafPartitions.get(3).getId()),
                0
        );
        Region expectedLeafRegion0 = new Region(new RangeFactory(schema).createRange(field, "", "E"));
        Partition expectedLeafPartition0 = new Partition(
                schema.getRowKeyTypes(),
                expectedLeafRegion0,
                leafPartitions.get(0).getId(),
                true,
                internalPartitions.get(0).getId(),
                Collections.emptyList(),
                -1
        );
        Region expectedLeafRegion1 = new Region(new RangeFactory(schema).createRange(field, "E", "P"));
        Partition expectedLeafPartition1 = new Partition(
                schema.getRowKeyTypes(),
                expectedLeafRegion1,
                leafPartitions.get(1).getId(),
                true,
                internalPartitions.get(0).getId(),
                Collections.emptyList(),
                -1
        );
        Region expectedLeafRegion2 = new Region(new RangeFactory(schema).createRange(field, "P", "T"));
        Partition expectedLeafPartition2 = new Partition(
                schema.getRowKeyTypes(),
                expectedLeafRegion2,
                leafPartitions.get(2).getId(),
                true,
                internalPartitions.get(1).getId(),
                Collections.emptyList(),
                -1
        );
        Region expectedLeafRegion3 = new Region(new RangeFactory(schema).createRange(field, "T", null));
        Partition expectedLeafPartition3 = new Partition(
                schema.getRowKeyTypes(),
                expectedLeafRegion3,
                leafPartitions.get(3).getId(),
                true,
                internalPartitions.get(1).getId(),
                Collections.emptyList(),
                -1
        );

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
        InitialiseStateStore initialiseStateStore = new InitialiseStateStore(schema, dynamoDBStateStore, splitPoints);

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
        Partition expectedRootPartition = new Partition(
                schema.getRowKeyTypes(),
                expectedRootRegion,
                rootPartition.getId(),
                false,
                null,
                Arrays.asList(internalPartitions.get(0).getId(), internalPartitions.get(1).getId()),
                0
        );

        Region expectedInternalRegion0 = new Region(Arrays.asList(new RangeFactory(schema).createRange(field0, "", "P"), rangeForDim1, rangeForDim2, rangeForDim3));
        Partition expectedInternalPartition0 = new Partition(
                schema.getRowKeyTypes(),
                expectedInternalRegion0,
                internalPartitions.get(0).getId(),
                false,
                rootPartition.getId(),
                Arrays.asList(leafPartitions.get(0).getId(), leafPartitions.get(1).getId()),
                0
        );

        Region expectedInternalRegion1 = new Region(Arrays.asList(new RangeFactory(schema).createRange(field0, "P", null), rangeForDim1, rangeForDim2, rangeForDim3));
        Partition expectedInternalPartition1 = new Partition(
                schema.getRowKeyTypes(),
                expectedInternalRegion1,
                internalPartitions.get(1).getId(),
                false,
                rootPartition.getId(),
                Arrays.asList(leafPartitions.get(2).getId(), leafPartitions.get(3).getId()),
                0
        );

        Region expectedLeafRegion0 = new Region(Arrays.asList(new RangeFactory(schema).createRange(field0, "", "E"), rangeForDim1, rangeForDim2, rangeForDim3));
        Partition expectedLeafPartition0 = new Partition(
                schema.getRowKeyTypes(),
                expectedLeafRegion0,
                leafPartitions.get(0).getId(),
                true,
                internalPartitions.get(0).getId(),
                Collections.emptyList(),
                -1
        );

        Region expectedLeafRegion1 = new Region(Arrays.asList(new RangeFactory(schema).createRange(field0, "E", "P"), rangeForDim1, rangeForDim2, rangeForDim3));
        Partition expectedLeafPartition1 = new Partition(
                schema.getRowKeyTypes(),
                expectedLeafRegion1,
                leafPartitions.get(1).getId(),
                true,
                internalPartitions.get(0).getId(),
                Collections.emptyList(),
                -1
        );

        Region expectedLeafRegion2 = new Region(Arrays.asList(new RangeFactory(schema).createRange(field0, "P", "T"), rangeForDim1, rangeForDim2, rangeForDim3));
        Partition expectedLeafPartition2 = new Partition(
                schema.getRowKeyTypes(),
                expectedLeafRegion2,
                leafPartitions.get(2).getId(),
                true,
                internalPartitions.get(1).getId(),
                Collections.emptyList(),
                -1
        );

        Region expectedLeafRegion3 = new Region(Arrays.asList(new RangeFactory(schema).createRange(field0, "T", null), rangeForDim1, rangeForDim2, rangeForDim3));
        Partition expectedLeafPartition3 = new Partition(
                schema.getRowKeyTypes(),
                expectedLeafRegion3,
                leafPartitions.get(3).getId(),
                true,
                internalPartitions.get(1).getId(),
                Collections.emptyList(),
                -1
        );

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
        InitialiseStateStore initialiseStateStore = new InitialiseStateStore(schema, dynamoDBStateStore, Collections.emptyList());

        // When
        initialiseStateStore.run();

        // Then
        List<Partition> partitions = dynamoDBStateStore.getAllPartitions();
        assertThat(partitions).hasSize(1);

        Region expectedRegion = new Region(new RangeFactory(schema).createRange(field, new byte[]{}, null));
        Partition expectedPartition = new Partition(
                schema.getRowKeyTypes(),
                expectedRegion,
                partitions.get(0).getId(),
                true,
                null,
                Collections.emptyList(),
                -1
        );
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
        InitialiseStateStore initialiseStateStore = new InitialiseStateStore(schema, dynamoDBStateStore, splitPoints);

        // When
        initialiseStateStore.run();

        // Then
        List<Partition> partitions = dynamoDBStateStore.getAllPartitions();
        assertThat(partitions).hasSize(3);

        Partition rootPartition = partitions.stream().filter(p -> null == p.getParentPartitionId()).collect(Collectors.toList()).get(0);
        List<Partition> leafPartitions = partitions.stream().filter(Partition::isLeafPartition).collect(Collectors.toList());
        leafPartitions.sort(Comparator.comparing(p -> ByteArray.wrap((byte[]) p.getRegion().getRange("key").getMin())));

        Region expectedRegion = new Region(new RangeFactory(schema).createRange(field, new byte[]{}, null));
        Partition expectedRootPartition = new Partition(
                schema.getRowKeyTypes(),
                expectedRegion,
                rootPartition.getId(),
                false,
                null,
                Arrays.asList(leafPartitions.get(0).getId(), leafPartitions.get(1).getId()),
                0
        );
        Region expectedLeafRegion0 = new Region(new RangeFactory(schema).createRange(field, new byte[]{}, new byte[]{10}));
        Partition expectedLeafPartition0 = new Partition(
                schema.getRowKeyTypes(),
                expectedLeafRegion0,
                leafPartitions.get(0).getId(),
                true,
                rootPartition.getId(),
                Collections.emptyList(),
                -1
        );
        Region expectedLeafRegion1 = new Region(new RangeFactory(schema).createRange(field, new byte[]{10}, null));
        Partition expectedLeafPartition1 = new Partition(
                schema.getRowKeyTypes(),
                expectedLeafRegion1,
                leafPartitions.get(1).getId(),
                true,
                rootPartition.getId(),
                Collections.emptyList(),
                -1
        );

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
        InitialiseStateStore initialiseStateStore = new InitialiseStateStore(schema, dynamoDBStateStore, splitPoints);

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
        Partition expectedRootPartition = new Partition(
                schema.getRowKeyTypes(),
                expectedRootRegion,
                rootPartition.getId(),
                false,
                null,
                Arrays.asList(internalPartitions.get(0).getId(), internalPartitions.get(1).getId()),
                0
        );

        Region expectedInternalRegion0 = new Region(new RangeFactory(schema).createRange(field, new byte[]{}, new byte[]{50}));
        Partition expectedInternalPartition0 = new Partition(
                schema.getRowKeyTypes(),
                expectedInternalRegion0,
                internalPartitions.get(0).getId(),
                false,
                rootPartition.getId(),
                Arrays.asList(leafPartitions.get(0).getId(), leafPartitions.get(1).getId()),
                0
        );

        Region expectedInternalRegion1 = new Region(new RangeFactory(schema).createRange(field, new byte[]{50}, null));
        Partition expectedInternalPartition1 = new Partition(
                schema.getRowKeyTypes(),
                expectedInternalRegion1,
                internalPartitions.get(1).getId(),
                false,
                rootPartition.getId(),
                Arrays.asList(leafPartitions.get(2).getId(), leafPartitions.get(3).getId()),
                0
        );

        Region expectedLeafRegion0 = new Region(new RangeFactory(schema).createRange(field, new byte[]{}, new byte[]{10}));
        Partition expectedLeafPartition0 = new Partition(
                schema.getRowKeyTypes(),
                expectedLeafRegion0,
                leafPartitions.get(0).getId(),
                true,
                internalPartitions.get(0).getId(),
                Collections.emptyList(),
                -1
        );

        Region expectedLeafRegion1 = new Region(new RangeFactory(schema).createRange(field, new byte[]{10}, new byte[]{50}));
        Partition expectedLeafPartition1 = new Partition(
                schema.getRowKeyTypes(),
                expectedLeafRegion1,
                leafPartitions.get(1).getId(),
                true,
                internalPartitions.get(0).getId(),
                Collections.emptyList(),
                -1
        );

        Region expectedLeafRegion2 = new Region(new RangeFactory(schema).createRange(field, new byte[]{50}, new byte[]{99}));
        Partition expectedLeafPartition2 = new Partition(
                schema.getRowKeyTypes(),
                expectedLeafRegion2,
                leafPartitions.get(2).getId(),
                true,
                internalPartitions.get(1).getId(),
                Collections.emptyList(),
                -1
        );

        Region expectedLeafRegion3 = new Region(new RangeFactory(schema).createRange(field, new byte[]{99}, null));
        Partition expectedLeafPartition3 = new Partition(
                schema.getRowKeyTypes(),
                expectedLeafRegion3,
                leafPartitions.get(3).getId(),
                true,
                internalPartitions.get(1).getId(),
                Collections.emptyList(),
                -1
        );

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
        InitialiseStateStore initialiseStateStore = new InitialiseStateStore(schema, dynamoDBStateStore, splitPoints);

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
        Partition expectedRootPartition = new Partition(
                schema.getRowKeyTypes(),
                expectedRootRegion,
                rootPartition.getId(),
                false,
                null,
                Arrays.asList(internalPartitions.get(0).getId(), internalPartitions.get(1).getId()),
                0
        );

        Region expectedInternalRegion0 = new Region(Arrays.asList(new RangeFactory(schema).createRange(field0, new byte[]{}, new byte[]{50}), rangeForDim1, rangeForDim2, rangeForDim3));
        Partition expectedInternalPartition0 = new Partition(
                schema.getRowKeyTypes(),
                expectedInternalRegion0,
                internalPartitions.get(0).getId(),
                false,
                rootPartition.getId(),
                Arrays.asList(leafPartitions.get(0).getId(), leafPartitions.get(1).getId()),
                0
        );

        Region expectedInternalRegion1 = new Region(Arrays.asList(new RangeFactory(schema).createRange(field0, new byte[]{50}, null), rangeForDim1, rangeForDim2, rangeForDim3));
        Partition expectedInternalPartition1 = new Partition(
                schema.getRowKeyTypes(),
                expectedInternalRegion1,
                internalPartitions.get(1).getId(),
                false,
                rootPartition.getId(),
                Arrays.asList(leafPartitions.get(2).getId(), leafPartitions.get(3).getId()),
                0
        );

        Region expectedLeafRegion0 = new Region(Arrays.asList(new RangeFactory(schema).createRange(field0, new byte[]{}, new byte[]{10}), rangeForDim1, rangeForDim2, rangeForDim3));
        Partition expectedLeafPartition0 = new Partition(
                schema.getRowKeyTypes(),
                expectedLeafRegion0,
                leafPartitions.get(0).getId(),
                true,
                internalPartitions.get(0).getId(),
                Collections.emptyList(),
                -1
        );

        Region expectedLeafRegion1 = new Region(Arrays.asList(new RangeFactory(schema).createRange(field0, new byte[]{10}, new byte[]{50}), rangeForDim1, rangeForDim2, rangeForDim3));
        Partition expectedLeafPartition1 = new Partition(
                schema.getRowKeyTypes(),
                expectedLeafRegion1,
                leafPartitions.get(1).getId(),
                true,
                internalPartitions.get(0).getId(),
                Collections.emptyList(),
                -1
        );

        Region expectedLeafRegion2 = new Region(Arrays.asList(new RangeFactory(schema).createRange(field0, new byte[]{50}, new byte[]{99}), rangeForDim1, rangeForDim2, rangeForDim3));
        Partition expectedLeafPartition2 = new Partition(
                schema.getRowKeyTypes(),
                expectedLeafRegion2,
                leafPartitions.get(2).getId(),
                true,
                internalPartitions.get(1).getId(),
                Collections.emptyList(),
                -1
        );

        Region expectedLeafRegion3 = new Region(Arrays.asList(new RangeFactory(schema).createRange(field0, new byte[]{99}, null), rangeForDim1, rangeForDim2, rangeForDim3));
        Partition expectedLeafPartition3 = new Partition(
                schema.getRowKeyTypes(),
                expectedLeafRegion3,
                leafPartitions.get(3).getId(),
                true,
                internalPartitions.get(1).getId(),
                Collections.emptyList(),
                -1
        );

        assertThat(rootPartition).isEqualTo(expectedRootPartition);
        assertThat(internalPartitions).containsExactly(expectedInternalPartition0, expectedInternalPartition1);
        assertThat(leafPartitions).containsExactly(
                expectedLeafPartition0, expectedLeafPartition1, expectedLeafPartition2, expectedLeafPartition3);
    }

    @Test
    public void shouldThrowExceptionIfSplitPointIsOfWrongType() throws StateStoreException {
        // Given
        Schema schema = schemaWithRowKeys(new Field("key", new IntType()));
        StateStore dynamoDBStateStore = getStateStore(schema);
        List<Object> splitPoints = new ArrayList<>();
        splitPoints.add(Long.MIN_VALUE);
        InitialiseStateStore initialiseStateStore = new InitialiseStateStore(schema, dynamoDBStateStore, splitPoints);

        // When / Then
        assertThatThrownBy(initialiseStateStore::run)
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    public void shouldThrowExceptionIfDuplicateSplitPoints() throws StateStoreException {
        // Given
        Schema schema = schemaWithRowKeys(new Field("key", new IntType()));
        StateStore dynamoDBStateStore = getStateStore(schema);
        List<Object> splitPoints = new ArrayList<>();
        splitPoints.add(0);
        splitPoints.add(0);
        InitialiseStateStore initialiseStateStore = new InitialiseStateStore(schema, dynamoDBStateStore, splitPoints);

        // When / Then
        assertThatThrownBy(initialiseStateStore::run)
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    public void shouldThrowExceptionIfSplitPointsAreInWrongOrder() throws StateStoreException {
        // Given
        Schema schema = schemaWithRowKeys(new Field("key", new IntType()));
        StateStore dynamoDBStateStore = getStateStore(schema);
        List<Object> splitPoints = new ArrayList<>();
        splitPoints.add(1);
        splitPoints.add(0);
        InitialiseStateStore initialiseStateStore = new InitialiseStateStore(schema, dynamoDBStateStore, splitPoints);

        // When / Then
        assertThatThrownBy(initialiseStateStore::run)
                .isInstanceOf(IllegalArgumentException.class);
    }
}
