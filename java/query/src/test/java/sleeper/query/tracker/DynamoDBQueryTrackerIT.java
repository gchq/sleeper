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
package sleeper.query.tracker;

import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.model.AttributeDefinition;
import com.amazonaws.services.dynamodbv2.model.BillingMode;
import com.amazonaws.services.dynamodbv2.model.CreateTableRequest;
import com.amazonaws.services.dynamodbv2.model.KeySchemaElement;
import com.amazonaws.services.dynamodbv2.model.KeyType;
import com.amazonaws.services.dynamodbv2.model.ScalarAttributeType;
import com.google.common.collect.Lists;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.testcontainers.containers.GenericContainer;
import sleeper.query.model.LeafPartitionQuery;
import sleeper.query.model.Query;
import sleeper.query.model.output.ResultsOutputInfo;

import static sleeper.query.tracker.QueryState.COMPLETED;
import static sleeper.query.tracker.QueryState.FAILED;
import static sleeper.query.tracker.QueryState.IN_PROGRESS;
import static sleeper.query.tracker.QueryState.PARTIALLY_FAILED;
import sleeper.query.tracker.exception.QueryTrackerException;
import sleeper.configuration.properties.InstanceProperties;
import static sleeper.configuration.properties.SystemDefinedInstanceProperty.QUERY_TRACKER_TABLE_NAME;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.QUERY_TRACKER_ITEM_TTL_IN_DAYS;
import sleeper.core.CommonTestConstants;
import sleeper.core.range.Range;
import sleeper.core.range.Range.RangeFactory;
import sleeper.core.range.Region;
import sleeper.core.schema.Field;
import sleeper.core.schema.Schema;
import sleeper.core.schema.type.IntType;

public class DynamoDBQueryTrackerIT {
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

    private InstanceProperties instanceProperties;

    @Before
    public void createDynamoTable() {
        String tableName = UUID.randomUUID().toString();
        dynamoDBClient.createTable(new CreateTableRequest(tableName, createKeySchema())
                .withAttributeDefinitions(createAttributeDefinitions())
                .withBillingMode(BillingMode.PAY_PER_REQUEST)
        );
        instanceProperties = new InstanceProperties();
        instanceProperties.set(QUERY_TRACKER_TABLE_NAME, tableName);
    }

    @Test
    public void shouldReturnNullWhenGettingItemThatDoesNotExist() throws QueryTrackerException {
        // Given
        DynamoDBQueryTracker queryTracker = new DynamoDBQueryTracker(instanceProperties, dynamoDBClient);

        // When / Then
        assertNull(queryTracker.getStatus("non-existent"));
    }

    @Test
    public void shouldReturnQueriesFromDynamoIfTheyExist() throws QueryTrackerException {
        // Given
        DynamoDBQueryTracker queryTracker = new DynamoDBQueryTracker(instanceProperties, dynamoDBClient);

        // When
        queryTracker.queryCompleted(createQueryWithId("my-id"), new ResultsOutputInfo(10, Collections.emptyList()));

        // Then
        TrackedQuery status = queryTracker.getStatus("my-id");
        assertEquals(COMPLETED, status.getLastKnownState());
        assertEquals(Long.valueOf(10), status.getRecordCount());
    }

    @Test
    public void shouldCreateEntryInTableIfIdDoesNotExist() throws QueryTrackerException {
        // Given
        DynamoDBQueryTracker queryTracker = new DynamoDBQueryTracker(instanceProperties, dynamoDBClient);

        // When
        queryTracker.queryInProgress(createQueryWithId("my-id"));

        // Then
        assertEquals(IN_PROGRESS, queryTracker.getStatus("my-id").getLastKnownState());
    }

    @Test
    public void shouldSetAgeOffTimeAccordingToInstanceProperty() throws QueryTrackerException {
        // Given
        instanceProperties.setNumber(QUERY_TRACKER_ITEM_TTL_IN_DAYS, 3);
        DynamoDBQueryTracker queryTracker = new DynamoDBQueryTracker(instanceProperties, dynamoDBClient);

        // When
        queryTracker.queryInProgress(createQueryWithId("my-id"));
        TrackedQuery status = queryTracker.getStatus("my-id");

        // Then
        assertEquals(3 * 24 * 3600, status.getExpiryDate() - status.getLastUpdateTime());
    }

    @Test
    public void shouldUpdateStateInTableIfIdDoesExist() throws QueryTrackerException {
        // Given
        DynamoDBQueryTracker queryTracker = new DynamoDBQueryTracker(instanceProperties, dynamoDBClient);

        // When
        queryTracker.queryQueued(createQueryWithId("my-id"));
        queryTracker.queryFailed(createQueryWithId("my-id"), new Exception("fail"));

        // Then
        assertEquals(FAILED, queryTracker.getStatus("my-id").getLastKnownState());
    }

    @Test
    public void shouldUpdateParentStateInTableWhenTheChildIsTheLastOneToComplete() throws QueryTrackerException {
        // Given
        DynamoDBQueryTracker queryTracker = new DynamoDBQueryTracker(instanceProperties, dynamoDBClient);

        // When
        queryTracker.queryInProgress(createQueryWithId("parent"));
        queryTracker.queryCompleted(createSubQueryWithId("parent", "my-id"), new ResultsOutputInfo(10, Collections.emptyList()));

        // Then
        TrackedQuery parent = queryTracker.getStatus("parent");
        TrackedQuery child = queryTracker.getStatus("parent", "my-id");
        assertEquals(COMPLETED, parent.getLastKnownState());
        assertEquals(COMPLETED, child.getLastKnownState());
        assertEquals(Long.valueOf(10), parent.getRecordCount());
        assertEquals(Long.valueOf(10), child.getRecordCount());
    }

    @Test
    public void shouldNotUpdateParentStateInTableWhenMoreChildrenAreYetToComplete() throws QueryTrackerException {
        // Given
        DynamoDBQueryTracker queryTracker = new DynamoDBQueryTracker(instanceProperties, dynamoDBClient);

        // When
        queryTracker.queryInProgress(createQueryWithId("parent"));
        queryTracker.queryInProgress(createSubQueryWithId("parent", "my-id"));
        queryTracker.queryCompleted(createSubQueryWithId("parent", "my-other-id"), new ResultsOutputInfo(10, Collections.emptyList()));

        // Then
        assertEquals(IN_PROGRESS, queryTracker.getStatus("parent").getLastKnownState());
        assertEquals(COMPLETED, queryTracker.getStatus("parent", "my-other-id").getLastKnownState());
        assertEquals(IN_PROGRESS, queryTracker.getStatus("parent", "my-id").getLastKnownState());
    }

    @Test
    public void shouldUpdateParentStateToFailedInTableWhenAllChildrenFail() throws QueryTrackerException {
        // Given
        DynamoDBQueryTracker queryTracker = new DynamoDBQueryTracker(instanceProperties, dynamoDBClient);

        // When
        queryTracker.queryInProgress(createQueryWithId("parent"));
        queryTracker.queryFailed(createSubQueryWithId("parent", "my-id"), new Exception("Fail"));
        queryTracker.queryFailed(createSubQueryWithId("parent", "my-other-id"), new Exception("Fail"));

        // Then
        assertEquals(FAILED, queryTracker.getStatus("parent").getLastKnownState());
        assertEquals(FAILED, queryTracker.getStatus("parent", "my-id").getLastKnownState());
        assertEquals(FAILED, queryTracker.getStatus("parent", "my-other-id").getLastKnownState());
    }

    @Test
    public void shouldUpdateParentStateToPartiallyFailedInTableWhenSomeChildrenFail() throws QueryTrackerException {
        // Given
        DynamoDBQueryTracker queryTracker = new DynamoDBQueryTracker(instanceProperties, dynamoDBClient);

        // When
        queryTracker.queryInProgress(createQueryWithId("parent"));
        queryTracker.queryCompleted(createSubQueryWithId("parent", "my-id"), new ResultsOutputInfo(10, Collections.emptyList()));
        queryTracker.queryFailed(createSubQueryWithId("parent", "my-other-id"), new Exception("Fail"));

        // Then
        assertEquals(PARTIALLY_FAILED, queryTracker.getStatus("parent").getLastKnownState());
        assertEquals(COMPLETED, queryTracker.getStatus("parent", "my-id").getLastKnownState());
        assertEquals(FAILED, queryTracker.getStatus("parent", "my-other-id").getLastKnownState());
    }

    @Test
    public void shouldUpdateParentStateWithTotalRecordsReturnedByAllChildren() throws QueryTrackerException {
        // Given
        DynamoDBQueryTracker queryTracker = new DynamoDBQueryTracker(instanceProperties, dynamoDBClient);

        // When
        queryTracker.queryInProgress(createQueryWithId("parent"));
        queryTracker.queryCompleted(createSubQueryWithId("parent", "my-id"), new ResultsOutputInfo(10, Collections.emptyList()));
        queryTracker.queryCompleted(createSubQueryWithId("parent", "my-other-id"), new ResultsOutputInfo(25, Collections.emptyList()));

        // Then
        assertEquals(COMPLETED, queryTracker.getStatus("parent").getLastKnownState());
        assertEquals(COMPLETED, queryTracker.getStatus("parent", "my-id").getLastKnownState());
        assertEquals(COMPLETED, queryTracker.getStatus("parent", "my-other-id").getLastKnownState());
        assertEquals(Long.valueOf(35), queryTracker.getStatus("parent").getRecordCount());
        assertEquals(Long.valueOf(10), queryTracker.getStatus("parent", "my-id").getRecordCount());
        assertEquals(Long.valueOf(25), queryTracker.getStatus("parent", "my-other-id").getRecordCount());
    }

    private Collection<AttributeDefinition> createAttributeDefinitions() {
        return Lists.newArrayList(
                new AttributeDefinition(DynamoDBQueryTracker.QUERY_ID, ScalarAttributeType.S),
                new AttributeDefinition(DynamoDBQueryTracker.SUB_QUERY_ID, ScalarAttributeType.S)
        );
    }

    private Query createQueryWithId(String id) {
        Schema schema = new Schema();
        Field field = new Field("field1", new IntType());
        schema.setRowKeyFields(field);
        RangeFactory rangeFactory = new RangeFactory(schema);
        Range range = rangeFactory.createExactRange(field, 1);
        Region region = new Region(range);
        return new Query.Builder("myTable", id, region).build();
    }

    private LeafPartitionQuery createSubQueryWithId(String parentId, String subId) {
        Schema schema = new Schema();
        Field field = new Field("field1", new IntType());
        schema.setRowKeyFields(field);
        RangeFactory rangeFactory = new RangeFactory(schema);
        Range range = rangeFactory.createExactRange(field, 1);
        Region region = new Region(range);
        Range partitionRange = rangeFactory.createRange(field, 0, 1000);
        Region partitionRegion = new Region(partitionRange);
        return new LeafPartitionQuery.Builder("myTable", parentId, subId, region, "leafId", partitionRegion, new ArrayList<>()).build();
    }

    private List<KeySchemaElement> createKeySchema() {
        return Lists.newArrayList(
                new KeySchemaElement()
                        .withAttributeName(DynamoDBQueryTracker.QUERY_ID)
                        .withKeyType(KeyType.HASH),
                new KeySchemaElement()
                        .withAttributeName(DynamoDBQueryTracker.SUB_QUERY_ID)
                        .withKeyType(KeyType.RANGE)
        );
    }
}
