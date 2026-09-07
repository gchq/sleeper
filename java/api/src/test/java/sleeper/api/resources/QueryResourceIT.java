/*
 * Copyright 2022-2026 Crown Copyright
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
package sleeper.api.resources;

import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.QuarkusTestProfile;
import io.quarkus.test.junit.TestProfile;
import jakarta.inject.Inject;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.CreateBucketRequest;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.Message;

import sleeper.configuration.properties.S3InstanceProperties;
import sleeper.configuration.properties.S3TableProperties;
import sleeper.configuration.table.index.DynamoDBTableIndexCreator;
import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.model.OptionalStack;
import sleeper.core.properties.table.TableProperties;
import sleeper.core.range.Range.RangeFactory;
import sleeper.core.range.Region;
import sleeper.core.schema.Schema;
import sleeper.core.schema.type.StringType;
import sleeper.query.core.model.LeafPartitionQuery;
import sleeper.query.core.model.Query;
import sleeper.query.core.model.QueryOrLeafPartitionQuery;
import sleeper.query.core.model.QuerySerDe;
import sleeper.query.core.output.ResultsOutputInfo;
import sleeper.query.core.output.ResultsOutputLocation;
import sleeper.query.runner.tracker.DynamoDBQueryTracker;
import sleeper.query.runner.tracker.DynamoDBQueryTrackerCreator;

import java.util.List;
import java.util.Map;

import static io.restassured.RestAssured.given;
import static org.assertj.core.api.Assertions.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.CONFIG_BUCKET;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.QUERY_QUEUE_URL;
import static sleeper.core.properties.instance.CommonProperty.OPTIONAL_STACKS;
import static sleeper.core.properties.table.TableProperty.TABLE_ID;
import static sleeper.core.properties.table.TableProperty.TABLE_NAME;
import static sleeper.core.properties.testutils.InstancePropertiesTestHelper.createTestInstancePropertiesWithId;
import static sleeper.core.properties.testutils.TablePropertiesTestHelper.createTestTableProperties;
import static sleeper.core.schema.SchemaTestHelper.createSchemaWithKey;

@QuarkusTest
@TestProfile(QueryResourceIT.Profile.class)
class QueryResourceIT {

    static final String INSTANCE_ID = "query-it";
    static final String ACCOUNT_NAME = "test-account";

    @Inject
    S3Client s3Client;
    @Inject
    DynamoDbClient dynamoDbClient;
    @Inject
    SqsClient sqsClient;

    public static class Profile implements QuarkusTestProfile {
        @Override
        public Map<String, String> getConfigOverrides() {
            return Map.of(
                    "sleeper.instance.id", INSTANCE_ID,
                    "sleeper.account.name", ACCOUNT_NAME);
        }
    }

    @BeforeEach
    void clearState() {
        LocalStackTestResources.deleteDynamoTables(dynamoDbClient);
        LocalStackTestResources.deleteS3Buckets(s3Client);
        LocalStackTestResources.deleteSqsQueues(sqsClient);
    }

    private InstanceProperties setUpInstance(boolean queryEnabled) {
        InstanceProperties instanceProperties = createTestInstancePropertiesWithId(INSTANCE_ID);
        instanceProperties.setEnumList(OPTIONAL_STACKS, queryEnabled ? List.of(OptionalStack.QueryStack) : List.of());
        String queueUrl = sqsClient.createQueue(builder -> builder.queueName("query-it-queue")).queueUrl();
        instanceProperties.set(QUERY_QUEUE_URL, queueUrl);
        s3Client.createBucket(CreateBucketRequest.builder().bucket(instanceProperties.get(CONFIG_BUCKET)).build());
        S3InstanceProperties.saveToS3(s3Client, instanceProperties);
        DynamoDBTableIndexCreator.create(dynamoDbClient, instanceProperties);
        if (queryEnabled) {
            new DynamoDBQueryTrackerCreator(instanceProperties, dynamoDbClient).create();
        }
        return instanceProperties;
    }

    private TableProperties createTable(InstanceProperties instanceProperties, String tableName) {
        TableProperties tableProperties = createTestTableProperties(instanceProperties, createSchemaWithKey("key", new StringType()));
        tableProperties.set(TABLE_NAME, tableName);
        S3TableProperties.createStore(instanceProperties, s3Client, dynamoDbClient).createTable(tableProperties);
        return tableProperties;
    }

    private Query queryFor(TableProperties table, String queryId) {
        Schema schema = table.getSchema();
        RangeFactory rangeFactory = new RangeFactory(schema);
        Region region = new Region(rangeFactory.createExactRange(schema.getRowKeyFields().get(0), "abc"));
        return Query.builder()
                .queryId(queryId)
                .tableName(table.get(TABLE_NAME))
                .tableId(table.get(TABLE_ID))
                .regions(List.of(region))
                .build();
    }

    private LeafPartitionQuery subQueryFor(TableProperties table, Query query, String subQueryId) {
        return LeafPartitionQuery.builder()
                .parentQuery(query)
                .tableId(table.get(TABLE_ID))
                .subQueryId(subQueryId)
                .regions(query.getRegions())
                .leafPartitionId("leaf-1")
                .partitionRegion(query.getRegions().get(0))
                .files(List.of())
                .build();
    }

    @Test
    void shouldReturn404WhenQueryStackNotEnabled() {
        setUpInstance(false);

        given()
                .when().get("/api/queries")
                .then()
                .statusCode(404)
                .body("error", is("query_not_enabled"));
    }

    @Test
    void shouldListTrackedQueriesNewestFirst() {
        InstanceProperties instanceProperties = setUpInstance(true);
        TableProperties table = createTable(instanceProperties, "table-1");
        DynamoDBQueryTracker tracker = new DynamoDBQueryTracker(instanceProperties, dynamoDbClient);
        tracker.queryCompleted(queryFor(table, "query-older"), new ResultsOutputInfo(5, List.of()));
        tracker.queryInProgress(queryFor(table, "query-newer"));

        given()
                .when().get("/api/queries")
                .then()
                .statusCode(200)
                .body("queries.queryId", contains("query-newer", "query-older"))
                .body("queries[0].tableName", is("table-1"))
                .body("queries[0].tableId", is(table.get(TABLE_ID)));
    }

    @Test
    void shouldFilterQueriesByTable() {
        InstanceProperties instanceProperties = setUpInstance(true);
        TableProperties table1 = createTable(instanceProperties, "table-1");
        TableProperties table2 = createTable(instanceProperties, "table-2");
        DynamoDBQueryTracker tracker = new DynamoDBQueryTracker(instanceProperties, dynamoDbClient);
        tracker.queryInProgress(queryFor(table1, "query-1"));
        tracker.queryInProgress(queryFor(table2, "query-2"));

        given()
                .when().get("/api/queries?tableId=" + table1.get(TABLE_ID))
                .then()
                .statusCode(200)
                .body("queries", hasSize(1))
                .body("queries[0].queryId", is("query-1"));
    }

    @Test
    void shouldGetQueryDetailWithSubQueries() {
        InstanceProperties instanceProperties = setUpInstance(true);
        TableProperties table = createTable(instanceProperties, "table-1");
        DynamoDBQueryTracker tracker = new DynamoDBQueryTracker(instanceProperties, dynamoDbClient);
        Query query = queryFor(table, "query-1");
        tracker.queryInProgress(query);
        LeafPartitionQuery subQuery = LeafPartitionQuery.builder()
                .parentQuery(query)
                .tableId(table.get(TABLE_ID))
                .subQueryId("sub-1")
                .regions(query.getRegions())
                .leafPartitionId("leaf-1")
                .partitionRegion(query.getRegions().get(0))
                .files(List.of())
                .build();
        tracker.queryInProgress(subQuery);
        tracker.queryCompleted(subQuery, new ResultsOutputInfo(5, List.of(
                new ResultsOutputLocation("s3", "s3a://results-bucket/query-query-1/sub-1.parquet"))));

        given()
                .when().get("/api/query/query-1")
                .then()
                .statusCode(200)
                .body("queryId", is("query-1"))
                .body("tableName", is("table-1"))
                .body("firstUpdateTime", notNullValue())
                .body("lastUpdateTime", notNullValue())
                .body("subQueries", hasSize(1))
                .body("subQueries[0].subQueryId", is("sub-1"))
                .body("subQueries[0].firstUpdateTime", notNullValue())
                .body("subQueries[0].lastUpdateTime", notNullValue())
                .body("subQueries[0].resultsLocations", hasSize(1))
                .body("subQueries[0].resultsLocations[0].type", is("s3"))
                .body("subQueries[0].resultsLocations[0].location", is("s3a://results-bucket/query-query-1/sub-1.parquet"));
    }

    @Test
    void shouldRejectResultsLocationThatTheQueryDidNotWriteTo() {
        InstanceProperties instanceProperties = setUpInstance(true);
        TableProperties table = createTable(instanceProperties, "table-1");
        DynamoDBQueryTracker tracker = new DynamoDBQueryTracker(instanceProperties, dynamoDbClient);
        Query query = queryFor(table, "query-1");
        tracker.queryInProgress(query);
        LeafPartitionQuery subQuery = LeafPartitionQuery.builder()
                .parentQuery(query)
                .tableId(table.get(TABLE_ID))
                .subQueryId("sub-1")
                .regions(query.getRegions())
                .leafPartitionId("leaf-1")
                .partitionRegion(query.getRegions().get(0))
                .files(List.of())
                .build();
        tracker.queryInProgress(subQuery);
        tracker.queryCompleted(subQuery, new ResultsOutputInfo(5, List.of(
                new ResultsOutputLocation("s3", "s3a://results-bucket/query-query-1/sub-1.parquet"))));

        given()
                .when().get("/api/query/query-1/sub-1/results"
                        + "?location=s3a://results-bucket/query-other/secret.parquet")
                .then()
                .statusCode(404)
                .body("error", is("results_location_not_found"));
    }

    @Test
    void shouldReturn404ForUnknownQuery() {
        setUpInstance(true);

        given()
                .when().get("/api/query/does-not-exist")
                .then()
                .statusCode(404)
                .body("error", is("query_not_found"));
    }

    @Test
    void shouldGetSubQueryDetail() {
        InstanceProperties instanceProperties = setUpInstance(true);
        TableProperties table = createTable(instanceProperties, "table-1");
        DynamoDBQueryTracker tracker = new DynamoDBQueryTracker(instanceProperties, dynamoDbClient);
        Query query = queryFor(table, "query-1");
        tracker.queryInProgress(query);
        LeafPartitionQuery subQuery = subQueryFor(table, query, "sub-1");
        tracker.queryInProgress(subQuery);
        tracker.queryCompleted(subQuery, new ResultsOutputInfo(5, List.of(
                new ResultsOutputLocation("s3", "s3a://results-bucket/query-query-1/sub-1.parquet"))));

        given()
                .when().get("/api/query/query-1/sub-1")
                .then()
                .statusCode(200)
                .body("queryId", is("query-1"))
                .body("subQueryId", is("sub-1"))
                .body("tableId", is(table.get(TABLE_ID)))
                .body("tableName", is("table-1"))
                .body("state", is("COMPLETED"))
                .body("rowCount", is(5))
                .body("firstUpdateTime", notNullValue())
                .body("lastUpdateTime", notNullValue())
                .body("resultsLocations", hasSize(1))
                .body("resultsLocations[0].type", is("s3"))
                .body("resultsLocations[0].location", is("s3a://results-bucket/query-query-1/sub-1.parquet"))
                .body("maxResultRows", is(5000));
    }

    @Test
    void shouldGetSubQueryDetailWhenParentQueryIsNotTracked() {
        InstanceProperties instanceProperties = setUpInstance(true);
        TableProperties table = createTable(instanceProperties, "table-1");
        DynamoDBQueryTracker tracker = new DynamoDBQueryTracker(instanceProperties, dynamoDbClient);
        Query query = queryFor(table, "query-1");
        tracker.queryInProgress(subQueryFor(table, query, "sub-1"));

        given()
                .when().get("/api/query/query-1/sub-1")
                .then()
                .statusCode(200)
                .body("subQueryId", is("sub-1"))
                .body("state", is("IN_PROGRESS"))
                .body("tableName", is("table-1"));

        given()
                .when().get("/api/query/query-1")
                .then()
                .statusCode(404);
    }

    @Test
    void shouldGetSubQueryDetailWithNoResultsLocations() {
        InstanceProperties instanceProperties = setUpInstance(true);
        TableProperties table = createTable(instanceProperties, "table-1");
        DynamoDBQueryTracker tracker = new DynamoDBQueryTracker(instanceProperties, dynamoDbClient);
        Query query = queryFor(table, "query-1");
        tracker.queryInProgress(query);
        tracker.queryInProgress(subQueryFor(table, query, "sub-1"));

        given()
                .when().get("/api/query/query-1/sub-1")
                .then()
                .statusCode(200)
                .body("resultsLocations", hasSize(0))
                .body("errorMessage", nullValue());
    }

    @Test
    void shouldReportErrorMessageForFailedSubQuery() {
        InstanceProperties instanceProperties = setUpInstance(true);
        TableProperties table = createTable(instanceProperties, "table-1");
        DynamoDBQueryTracker tracker = new DynamoDBQueryTracker(instanceProperties, dynamoDbClient);
        Query query = queryFor(table, "query-1");
        tracker.queryInProgress(query);
        LeafPartitionQuery subQuery = subQueryFor(table, query, "sub-1");
        tracker.queryInProgress(subQuery);
        tracker.queryFailed(subQuery, new RuntimeException("boom"));

        given()
                .when().get("/api/query/query-1/sub-1")
                .then()
                .statusCode(200)
                .body("state", is("FAILED"))
                .body("errorMessage", notNullValue());
    }

    @Test
    void shouldReturn404ForUnknownSubQueryOnKnownQuery() {
        InstanceProperties instanceProperties = setUpInstance(true);
        TableProperties table = createTable(instanceProperties, "table-1");
        DynamoDBQueryTracker tracker = new DynamoDBQueryTracker(instanceProperties, dynamoDbClient);
        Query query = queryFor(table, "query-1");
        tracker.queryInProgress(query);
        tracker.queryInProgress(subQueryFor(table, query, "sub-1"));

        given()
                .when().get("/api/query/query-1/does-not-exist")
                .then()
                .statusCode(404)
                .body("error", is("query_not_found"));
    }

    @Test
    void shouldReturn404ForSubQueryOfUnknownQuery() {
        setUpInstance(true);

        given()
                .when().get("/api/query/does-not-exist/sub-1")
                .then()
                .statusCode(404)
                .body("error", is("query_not_found"));
    }

    @Test
    void shouldReturn404ForSubQueryWhenQueryStackNotEnabled() {
        setUpInstance(false);

        given()
                .when().get("/api/query/query-1/sub-1")
                .then()
                .statusCode(404)
                .body("error", is("query_not_enabled"));
    }

    @Test
    void shouldStillRouteResultsRequestsToTheResultsEndpoint() {
        InstanceProperties instanceProperties = setUpInstance(true);
        TableProperties table = createTable(instanceProperties, "table-1");
        DynamoDBQueryTracker tracker = new DynamoDBQueryTracker(instanceProperties, dynamoDbClient);
        Query query = queryFor(table, "query-1");
        tracker.queryInProgress(query);
        tracker.queryInProgress(subQueryFor(table, query, "sub-1"));

        given()
                .when().get("/api/query/query-1/sub-1/results")
                .then()
                .statusCode(400);
    }

    @Test
    void shouldSubmitQueryToTheQueryQueue() {
        InstanceProperties instanceProperties = setUpInstance(true);
        TableProperties table = createTable(instanceProperties, "table-1");

        given().contentType("application/json")
                .body(Map.of(
                        "tableId", table.get(TABLE_ID),
                        "conditions", List.of(Map.of("field", "key", "min", "abc")),
                        "valueFields", List.of()))
                .when().post("/api/query/submit")
                .then()
                .statusCode(201);

        List<Message> messages = sqsClient.receiveMessage(builder -> builder
                .queueUrl(instanceProperties.get(QUERY_QUEUE_URL))
                .maxNumberOfMessages(10))
                .messages();
        assertThat(messages).hasSize(1);
        QueryOrLeafPartitionQuery sent = new QuerySerDe(S3TableProperties.createProvider(instanceProperties, s3Client, dynamoDbClient))
                .fromJsonOrLeafQuery(messages.get(0).body());
        assertThat(sent.asParentQuery().getTableId()).isEqualTo(table.get(TABLE_ID));
    }

    @Test
    void shouldSubmitQueryWithMultipleConditionsOnTheSameField() {
        InstanceProperties instanceProperties = setUpInstance(true);
        TableProperties table = createTable(instanceProperties, "table-1");

        given().contentType("application/json")
                .body(Map.of(
                        "tableId", table.get(TABLE_ID),
                        "conditions", List.of(
                                Map.of("field", "key", "min", "abc"),
                                Map.of("field", "key", "min", "def")),
                        "valueFields", List.of()))
                .when().post("/api/query/submit")
                .then()
                .statusCode(201);

        List<Message> messages = sqsClient.receiveMessage(builder -> builder
                .queueUrl(instanceProperties.get(QUERY_QUEUE_URL)).maxNumberOfMessages(10)).messages();
        assertThat(messages).hasSize(1);
        QueryOrLeafPartitionQuery sent = new QuerySerDe(S3TableProperties.createProvider(instanceProperties, s3Client, dynamoDbClient))
                .fromJsonOrLeafQuery(messages.get(0).body());
        assertThat(sent.asParentQuery().getRegions()).hasSize(2);
    }

    @Test
    void shouldReturn404OnSubmitWhenQueryStackNotEnabled() {
        InstanceProperties instanceProperties = setUpInstance(false);
        TableProperties table = createTable(instanceProperties, "table-1");

        given().contentType("application/json")
                .body(Map.of(
                        "tableId", table.get(TABLE_ID),
                        "conditions", List.of(Map.of("field", "key", "min", "abc")),
                        "valueFields", List.of()))
                .when().post("/api/query/submit")
                .then()
                .statusCode(404)
                .body("error", is("query_not_enabled"));
    }

}
