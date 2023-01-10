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
package sleeper.status.partitions;

import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.testcontainers.containers.GenericContainer;

import sleeper.core.CommonTestConstants;
import sleeper.core.schema.Field;
import sleeper.core.schema.Schema;
import sleeper.core.schema.type.ByteArrayType;
import sleeper.core.schema.type.IntType;
import sleeper.core.schema.type.LongType;
import sleeper.core.schema.type.PrimitiveType;
import sleeper.core.schema.type.StringType;
import sleeper.statestore.InitialiseStateStore;
import sleeper.statestore.StateStore;
import sleeper.statestore.StateStoreException;
import sleeper.statestore.dynamodb.DynamoDBStateStoreCreator;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

public class ExportSplitPointsIT {
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

    private StateStore getStateStore(Schema schema) throws StateStoreException {
        String id = UUID.randomUUID().toString();
        DynamoDBStateStoreCreator dynamoDBStateStoreCreator = new DynamoDBStateStoreCreator(id, schema, 10000, dynamoDBClient);
        return dynamoDBStateStoreCreator.create();
    }

    private Schema schemaWithKeyType(PrimitiveType type) {
        return Schema.builder()
                .rowKeyFields(new Field("key", type))
                .sortKeyFields(new Field("sort", new LongType()))
                .valueFields(new Field("value", new ByteArrayType()))
                .build();
    }

    @Test
    public void shouldExportCorrectSplitPointsIntType() throws StateStoreException {
        // Given
        Schema schema = schemaWithKeyType(new IntType());
        StateStore stateStore = getStateStore(schema);
        List<Object> splitPoints = new ArrayList<>();
        splitPoints.add(-10);
        splitPoints.add(1000);
        InitialiseStateStore initialiseStateStore = new InitialiseStateStore(schema, stateStore, splitPoints);
        initialiseStateStore.run();
        ExportSplitPoints exportSplitPoints = new ExportSplitPoints(stateStore, schema);

        // When
        List<Object> exportedSplitPoints = exportSplitPoints.getSplitPoints();

        // Then
        assertThat(exportedSplitPoints).containsExactly(-10, 1000);
    }

    @Test
    public void shouldExportCorrectSplitPointsLongType() throws StateStoreException {
        // Given
        Schema schema = schemaWithKeyType(new LongType());
        StateStore stateStore = getStateStore(schema);
        List<Object> splitPoints = new ArrayList<>();
        splitPoints.add(-10L);
        splitPoints.add(1000L);
        InitialiseStateStore initialiseStateStore = new InitialiseStateStore(schema, stateStore, splitPoints);
        initialiseStateStore.run();
        ExportSplitPoints exportSplitPoints = new ExportSplitPoints(stateStore, schema);

        // When
        List<Object> exportedSplitPoints = exportSplitPoints.getSplitPoints();

        // Then
        assertThat(exportedSplitPoints).containsExactly(-10L, 1000L);
    }

    @Test
    public void shouldExportCorrectSplitPointsStringType() throws StateStoreException {
        // Given
        Schema schema = schemaWithKeyType(new StringType());
        StateStore stateStore = getStateStore(schema);
        List<Object> splitPoints = new ArrayList<>();
        splitPoints.add("A");
        splitPoints.add("T");
        InitialiseStateStore initialiseStateStore = new InitialiseStateStore(schema, stateStore, splitPoints);
        initialiseStateStore.run();
        ExportSplitPoints exportSplitPoints = new ExportSplitPoints(stateStore, schema);

        // When
        List<Object> exportedSplitPoints = exportSplitPoints.getSplitPoints();

        // Then
        assertThat(exportedSplitPoints).containsExactly("A", "T");
    }

    @Test
    public void shouldExportCorrectSplitPointsByteArrayType() throws StateStoreException {
        // Given
        Schema schema = schemaWithKeyType(new ByteArrayType());
        StateStore stateStore = getStateStore(schema);
        List<Object> splitPoints = new ArrayList<>();
        splitPoints.add(new byte[]{10});
        splitPoints.add(new byte[]{100});
        InitialiseStateStore initialiseStateStore = new InitialiseStateStore(schema, stateStore, splitPoints);
        initialiseStateStore.run();
        ExportSplitPoints exportSplitPoints = new ExportSplitPoints(stateStore, schema);

        // When
        List<Object> exportedSplitPoints = exportSplitPoints.getSplitPoints();

        // Then
        assertThat(exportedSplitPoints).containsExactly(new byte[]{10}, new byte[]{100});
    }
}
