/*
 * Copyright 2026 Crown Copyright
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
package sleeper.query.lambda;

import com.amazonaws.services.lambda.runtime.events.APIGatewayV2WebSocketEvent;
import com.amazonaws.services.lambda.runtime.events.APIGatewayV2WebSocketResponse;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import sleeper.configuration.properties.S3InstanceProperties;
import sleeper.configuration.properties.S3TableProperties;
import sleeper.configuration.table.index.DynamoDBTableIndexCreator;
import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.table.TableProperties;
import sleeper.core.range.Range.RangeFactory;
import sleeper.core.range.Region;
import sleeper.core.schema.Schema;
import sleeper.core.schema.type.LongType;
import sleeper.localstack.test.LocalStackTestBase;
import sleeper.query.core.model.Query;
import sleeper.query.core.model.QuerySerDe;
import sleeper.query.runner.output.S3ResultsOutput;
import sleeper.query.runner.output.WebSocketOutput;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.CONFIG_BUCKET;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.DNS_SUFFIX;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.QUERY_QUEUE_URL;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.REGION;
import static sleeper.core.properties.table.TableProperty.TABLE_NAME;
import static sleeper.core.properties.testutils.InstancePropertiesTestHelper.createTestInstanceProperties;
import static sleeper.core.properties.testutils.TablePropertiesTestHelper.createTestTableProperties;
import static sleeper.core.schema.SchemaTestHelper.createSchemaWithKey;
import static sleeper.query.core.output.ResultsOutput.DESTINATION;

class WebSocketQueryProcessorLambdaIT extends LocalStackTestBase {

    private static final Schema SCHEMA = createSchemaWithKey("key", new LongType());
    private static final String CONNECTION_ID = "current-connection";
    private static final String ENDPOINT = "https://test-api.execute-api.eu-west-1.amazonaws.com/test-stage";

    private final InstanceProperties instanceProperties = createTestInstanceProperties();
    private final TableProperties tableProperties = createTestTableProperties(instanceProperties, SCHEMA);
    private final QuerySerDe serde = new QuerySerDe(SCHEMA);
    private WebSocketQueryProcessorLambda lambda;

    @BeforeEach
    void setUp() {
        instanceProperties.set(REGION, "eu-west-1");
        instanceProperties.set(DNS_SUFFIX, "amazonaws.com");
        instanceProperties.set(QUERY_QUEUE_URL, createSqsQueueGetUrl());
        createBucket(instanceProperties.get(CONFIG_BUCKET));
        S3InstanceProperties.saveToS3(s3Client, instanceProperties);
        DynamoDBTableIndexCreator.create(dynamoClient, instanceProperties);
        S3TableProperties.createStore(instanceProperties, s3Client, dynamoClient).save(tableProperties);
        lambda = new WebSocketQueryProcessorLambda(s3Client, dynamoClient, sqsClient, instanceProperties.get(CONFIG_BUCKET));
    }

    @Test
    void shouldDefaultEmptyResultsConfigurationToWebSocket() {
        // Given
        Query query = query();

        // When
        Query submitted = submit(serde.toJson(query));

        // Then
        assertThat(submitted).isEqualTo(query
                .withResultsPublisherConfig(webSocketDestination())
                .withStatusReportDestination(webSocketDestination()));
    }

    @Test
    void shouldDefaultOmittedResultsConfigurationToWebSocket() {
        // Given
        Query query = query();
        JsonObject json = JsonParser.parseString(serde.toJson(query)).getAsJsonObject();
        json.remove("resultsPublisherConfig");

        // When
        Query submitted = submit(json.toString());

        // Then
        assertThat(submitted).isEqualTo(query
                .withResultsPublisherConfig(webSocketDestination())
                .withStatusReportDestination(webSocketDestination()));
    }

    @Test
    void shouldPreserveOtherPublishingOptionsWhenDefaultingDestination() {
        // Given
        Query query = query().withResultsPublisherConfig(Map.of("batchSize", "10"));

        // When
        Query submitted = submit(serde.toJson(query));

        // Then
        assertThat(submitted).isEqualTo(query
                .withResultsPublisherConfig(Map.of(
                        DESTINATION, WebSocketOutput.DESTINATION_NAME,
                        WebSocketOutput.ENDPOINT, ENDPOINT,
                        WebSocketOutput.CONNECTION_ID, CONNECTION_ID,
                        "batchSize", "10"))
                .withStatusReportDestination(webSocketDestination()));
    }

    @Test
    void shouldReplaceWebSocketConnectionDetailsWithCurrentRequest() {
        // Given
        Query query = query().withResultsPublisherConfig(Map.of(
                DESTINATION, WebSocketOutput.DESTINATION_NAME,
                WebSocketOutput.ENDPOINT, "https://old.example.com/old-stage",
                WebSocketOutput.CONNECTION_ID, "old-connection"));

        // When
        Query submitted = submit(serde.toJson(query));

        // Then
        assertThat(submitted).isEqualTo(query
                .withResultsPublisherConfig(webSocketDestination())
                .withStatusReportDestination(webSocketDestination()));
    }

    @Test
    void shouldPreserveNonWebSocketResultsDestination() {
        // Given
        Query query = query().withResultsPublisherConfig(Map.of(
                DESTINATION, S3ResultsOutput.S3,
                S3ResultsOutput.S3_BUCKET, "results-bucket"));

        // When
        Query submitted = submit(serde.toJson(query));

        // Then
        assertThat(submitted).isEqualTo(query.withStatusReportDestination(webSocketDestination()));
    }

    @Test
    void shouldPreserveExistingStatusDestinations() {
        // Given
        Query query = query().withStatusReportDestination(Map.of(DESTINATION, "LOGGER"));

        // When
        Query submitted = submit(serde.toJson(query));

        // Then
        assertThat(submitted).isEqualTo(query
                .withResultsPublisherConfig(webSocketDestination())
                .withStatusReportDestination(webSocketDestination()));
    }

    @Test
    void shouldNotSubmitQueryForConnectEvent() {
        assertNoQuerySubmittedForEvent("CONNECT");
    }

    @Test
    void shouldNotSubmitQueryForDisconnectEvent() {
        assertNoQuerySubmittedForEvent("DISCONNECT");
    }

    private void assertNoQuerySubmittedForEvent(String eventType) {
        // Given
        APIGatewayV2WebSocketEvent event = event(serde.toJson(query()));
        event.getRequestContext().setEventType(eventType);

        // When
        APIGatewayV2WebSocketResponse response = lambda.handleRequest(event, null);

        // Then
        assertThat(response.getStatusCode()).isEqualTo(200);
        assertThat(receiveMessages(instanceProperties.get(QUERY_QUEUE_URL))).isEmpty();
    }

    private Query query() {
        return Query.builder()
                .tableName(tableProperties.get(TABLE_NAME))
                .queryId("test-query")
                .regions(List.of(new Region(List.of(
                        new RangeFactory(SCHEMA).createRange(SCHEMA.getRowKeyFields().get(0), 1L, 10L)))))
                .build();
    }

    private Query submit(String body) {
        APIGatewayV2WebSocketResponse response = lambda.handleRequest(event(body), null);
        assertThat(response.getStatusCode()).isEqualTo(200);
        List<String> messages = receiveMessages(instanceProperties.get(QUERY_QUEUE_URL)).toList();
        assertThat(messages).hasSize(1);
        return serde.fromJson(messages.get(0));
    }

    private APIGatewayV2WebSocketEvent event(String body) {
        APIGatewayV2WebSocketEvent.RequestContext context = new APIGatewayV2WebSocketEvent.RequestContext();
        context.setEventType("MESSAGE");
        context.setApiId("test-api");
        context.setStage("test-stage");
        context.setConnectionId(CONNECTION_ID);
        APIGatewayV2WebSocketEvent event = new APIGatewayV2WebSocketEvent();
        event.setRequestContext(context);
        event.setBody(body);
        return event;
    }

    private Map<String, String> webSocketDestination() {
        return Map.of(
                DESTINATION, WebSocketOutput.DESTINATION_NAME,
                WebSocketOutput.ENDPOINT, ENDPOINT,
                WebSocketOutput.CONNECTION_ID, CONNECTION_ID);
    }
}
