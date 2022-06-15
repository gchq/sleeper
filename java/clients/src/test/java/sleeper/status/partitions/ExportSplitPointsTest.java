package sleeper.status.partitions;

import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import org.junit.AfterClass;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
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
import sleeper.core.schema.type.StringType;
import sleeper.statestore.InitialiseStateStore;
import sleeper.statestore.StateStore;
import sleeper.statestore.StateStoreException;
import sleeper.statestore.dynamodb.DynamoDBStateStoreCreator;

public class ExportSplitPointsTest {
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
    
    @Test
    public void shouldExportCorrectSplitPointsIntType() throws StateStoreException {
        // Given
        Schema schema = new Schema();
        schema.setRowKeyFields(new Field("key", new IntType()));
        schema.setSortKeyFields(new Field("sort", new LongType()));
        schema.setValueFields(new Field("value", new ByteArrayType()));
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
        assertEquals(Arrays.asList(-10, 1000), exportedSplitPoints);
    }
    
    @Test
    public void shouldExportCorrectSplitPointsLongType() throws StateStoreException {
        // Given
        Schema schema = new Schema();
        schema.setRowKeyFields(new Field("key", new LongType()));
        schema.setSortKeyFields(new Field("sort", new LongType()));
        schema.setValueFields(new Field("value", new ByteArrayType()));
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
        assertEquals(Arrays.asList(-10L, 1000L), exportedSplitPoints);
    }
    
    @Test
    public void shouldExportCorrectSplitPointsStringType() throws StateStoreException {
        // Given
        Schema schema = new Schema();
        schema.setRowKeyFields(new Field("key", new StringType()));
        schema.setSortKeyFields(new Field("sort", new LongType()));
        schema.setValueFields(new Field("value", new ByteArrayType()));
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
        assertEquals(Arrays.asList("A", "T"), exportedSplitPoints);
    }
    
    @Test
    public void shouldExportCorrectSplitPointsByteArrayType() throws StateStoreException {
        // Given
        Schema schema = new Schema();
        schema.setRowKeyFields(new Field("key", new ByteArrayType()));
        schema.setSortKeyFields(new Field("sort", new LongType()));
        schema.setValueFields(new Field("value", new ByteArrayType()));
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
        assertEquals(2, exportedSplitPoints.size());
        assertArrayEquals(new byte[]{10}, (byte[]) exportedSplitPoints.get(0));
        assertArrayEquals(new byte[]{100}, (byte[]) exportedSplitPoints.get(1));
    }
}
