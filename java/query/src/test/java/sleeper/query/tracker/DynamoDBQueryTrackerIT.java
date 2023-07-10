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
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import sleeper.configuration.properties.InstanceProperties;
import sleeper.core.CommonTestConstants;
import sleeper.core.range.Range;
import sleeper.core.range.Range.RangeFactory;
import sleeper.core.range.Region;
import sleeper.core.schema.Field;
import sleeper.core.schema.Schema;
import sleeper.core.schema.type.IntType;
import sleeper.query.model.LeafPartitionQuery;
import sleeper.query.model.Query;
import sleeper.query.model.output.ResultsOutputInfo;
import sleeper.query.tracker.exception.QueryTrackerException;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.configuration.properties.SystemDefinedInstanceProperty.QUERY_TRACKER_TABLE_NAME;
import static sleeper.configuration.properties.QueryProperties.QUERY_TRACKER_ITEM_TTL_IN_DAYS;
import static sleeper.query.tracker.QueryState.COMPLETED;
import static sleeper.query.tracker.QueryState.FAILED;
import static sleeper.query.tracker.QueryState.IN_PROGRESS;
import static sleeper.query.tracker.QueryState.PARTIALLY_FAILED;

@Testcontainers
public class DynamoDBQueryTrackerIT {
    private static final int DYNAMO_PORT = 8000;
    private static AmazonDynamoDB dynamoDBClient;

    @Container
    public static GenericContainer dynamoDb = new GenericContainer(CommonTestConstants.DYNAMODB_LOCAL_CONTAINER)
            .withExposedPorts(DYNAMO_PORT);

    @BeforeAll
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

    @BeforeEach
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
        assertThat(queryTracker.getStatus("non-existent")).isNull();
    }

    @Test
    public void shouldReturnQueriesFromDynamoIfTheyExist() throws QueryTrackerException {
        // Given
        DynamoDBQueryTracker queryTracker = new DynamoDBQueryTracker(instanceProperties, dynamoDBClient);

        // When
        queryTracker.queryCompleted(createQueryWithId("my-id"), new ResultsOutputInfo(10, Collections.emptyList()));

        // Then
        TrackedQuery status = queryTracker.getStatus("my-id");
        assertThat(status.getLastKnownState()).isEqualTo(COMPLETED);
        assertThat(status.getRecordCount()).isEqualTo(Long.valueOf(10));
    }

    @Test
    public void shouldCreateEntryInTableIfIdDoesNotExist() throws QueryTrackerException {
        // Given
        DynamoDBQueryTracker queryTracker = new DynamoDBQueryTracker(instanceProperties, dynamoDBClient);

        // When
        queryTracker.queryInProgress(createQueryWithId("my-id"));

        // Then
        assertThat(queryTracker.getStatus("my-id").getLastKnownState()).isEqualTo(IN_PROGRESS);
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
        assertThat(status.getExpiryDate() - status.getLastUpdateTime()).isEqualTo(3 * 24 * 3600);
    }

    @Test
    public void shouldUpdateStateInTableIfIdDoesExist() throws QueryTrackerException {
        // Given
        DynamoDBQueryTracker queryTracker = new DynamoDBQueryTracker(instanceProperties, dynamoDBClient);

        // When
        queryTracker.queryQueued(createQueryWithId("my-id"));
        queryTracker.queryFailed(createQueryWithId("my-id"), new Exception("fail"));

        // Then
        assertThat(queryTracker.getStatus("my-id").getLastKnownState()).isEqualTo(FAILED);
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
        assertThat(parent.getLastKnownState()).isEqualTo(COMPLETED);
        assertThat(child.getLastKnownState()).isEqualTo(COMPLETED);
        assertThat(parent.getRecordCount()).isEqualTo(Long.valueOf(10));
        assertThat(child.getRecordCount()).isEqualTo(Long.valueOf(10));
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
        assertThat(queryTracker.getStatus("parent").getLastKnownState()).isEqualTo(IN_PROGRESS);
        assertThat(queryTracker.getStatus("parent", "my-other-id").getLastKnownState()).isEqualTo(COMPLETED);
        assertThat(queryTracker.getStatus("parent", "my-id").getLastKnownState()).isEqualTo(IN_PROGRESS);
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
        assertThat(queryTracker.getStatus("parent").getLastKnownState()).isEqualTo(FAILED);
        assertThat(queryTracker.getStatus("parent", "my-id").getLastKnownState()).isEqualTo(FAILED);
        assertThat(queryTracker.getStatus("parent", "my-other-id").getLastKnownState()).isEqualTo(FAILED);
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
        assertThat(queryTracker.getStatus("parent").getLastKnownState()).isEqualTo(PARTIALLY_FAILED);
        assertThat(queryTracker.getStatus("parent", "my-id").getLastKnownState()).isEqualTo(COMPLETED);
        assertThat(queryTracker.getStatus("parent", "my-other-id").getLastKnownState()).isEqualTo(FAILED);
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
        assertThat(queryTracker.getStatus("parent").getLastKnownState()).isEqualTo(COMPLETED);
        assertThat(queryTracker.getStatus("parent", "my-id").getLastKnownState()).isEqualTo(COMPLETED);
        assertThat(queryTracker.getStatus("parent", "my-other-id").getLastKnownState()).isEqualTo(COMPLETED);
        assertThat(queryTracker.getStatus("parent").getRecordCount()).isEqualTo(Long.valueOf(35));
        assertThat(queryTracker.getStatus("parent", "my-id").getRecordCount()).isEqualTo(Long.valueOf(10));
        assertThat(queryTracker.getStatus("parent", "my-other-id").getRecordCount()).isEqualTo(Long.valueOf(25));
    }

    private Collection<AttributeDefinition> createAttributeDefinitions() {
        return Lists.newArrayList(
                new AttributeDefinition(DynamoDBQueryTracker.QUERY_ID, ScalarAttributeType.S),
                new AttributeDefinition(DynamoDBQueryTracker.SUB_QUERY_ID, ScalarAttributeType.S)
        );
    }

    private Query createQueryWithId(String id) {
        Field field = new Field("field1", new IntType());
        Schema schema = Schema.builder().rowKeyFields(field).build();
        RangeFactory rangeFactory = new RangeFactory(schema);
        Range range = rangeFactory.createExactRange(field, 1);
        Region region = new Region(range);
        return new Query.Builder("myTable", id, region).build();
    }

    private LeafPartitionQuery createSubQueryWithId(String parentId, String subId) {
        Field field = new Field("field1", new IntType());
        Schema schema = Schema.builder().rowKeyFields(field).build();
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
