/*
 * Copyright 2022-2025 Crown Copyright
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
package sleeper.query.runner.tracker;

import com.github.tomakehurst.wiremock.junit5.WireMockRuntimeInfo;
import com.github.tomakehurst.wiremock.junit5.WireMockTest;
import com.github.tomakehurst.wiremock.matching.UrlPattern;
import org.apache.curator.shaded.com.google.common.collect.Lists;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.apigatewaymanagementapi.ApiGatewayManagementApiClient;

import sleeper.core.range.Range;
import sleeper.core.range.Range.RangeFactory;
import sleeper.core.range.Region;
import sleeper.core.schema.Field;
import sleeper.core.schema.Schema;
import sleeper.core.schema.type.LongType;
import sleeper.core.schema.type.StringType;
import sleeper.query.core.model.LeafPartitionQuery;
import sleeper.query.core.model.Query;
import sleeper.query.core.output.ResultsOutputInfo;
import sleeper.query.core.output.ResultsOutputLocation;
import sleeper.query.runner.websocket.ApiGatewayWebSocketOutput;

import java.io.IOException;
import java.util.List;

import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static com.github.tomakehurst.wiremock.client.WireMock.equalToJson;
import static com.github.tomakehurst.wiremock.client.WireMock.post;
import static com.github.tomakehurst.wiremock.client.WireMock.postRequestedFor;
import static com.github.tomakehurst.wiremock.client.WireMock.stubFor;
import static com.github.tomakehurst.wiremock.client.WireMock.urlEqualTo;
import static com.github.tomakehurst.wiremock.client.WireMock.verify;
import static sleeper.localstack.test.WiremockAwsV2ClientHelper.wiremockAwsV2Client;

@WireMockTest
class WebSocketQueryStatusReportDestinationIT {
    private static final Schema SCHEMA = Schema.builder()
            .rowKeyFields(new Field("key", new StringType()))
            .valueFields(new Field("count", new LongType()))
            .build();
    private static WebSocketQueryConfig config;

    @BeforeAll
    public static void setup(WireMockRuntimeInfo runtimeInfo) {
        config = new WebSocketQueryConfig(runtimeInfo);
    }

    @Test
    void shouldNotSendQueryQueuedNotification() {
        // Given
        stubFor(post(config.getUrl()).willReturn(aResponse().withStatus(200)));
        Range range = config.getRangeFactory().createExactRange(SCHEMA.getRowKeyFields().get(0), "a");
        Query query = Query.builder()
                .tableName("tableName")
                .queryId("q1")
                .regions(List.of(new Region(range)))
                .build();

        // When
        config.getListener().queryQueued(query);

        // Then
        verify(0, postRequestedFor(config.getUrl()));
    }

    @Test
    void shouldNotSendQueryInProgressNotification() {
        // Given
        stubFor(post(config.getUrl()).willReturn(aResponse().withStatus(200)));
        Range range = config.getRangeFactory().createExactRange(SCHEMA.getRowKeyFields().get(0), "a");
        Query query = Query.builder()
                .tableName("tableName")
                .queryId("q1")
                .regions(List.of(new Region(range)))
                .build();

        // When
        config.getListener().queryInProgress(query);

        // Then
        verify(0, postRequestedFor(config.getUrl()));
    }

    @Test
    void shouldSendNotificationOfSubQueriesBeingCreated() {
        // Given
        stubFor(post(config.getUrl()).willReturn(aResponse().withStatus(200)));
        Range range = config.getRangeFactory().createExactRange(SCHEMA.getRowKeyFields().get(0), "a");
        Region region = new Region(range);
        Range partitionRange = config.getRangeFactory().createRange(SCHEMA.getRowKeyFields().get(0), "a", "b");
        Region partitionRegion = new Region(partitionRange);
        Query query = Query.builder()
                .tableName("tableName")
                .queryId("q1")
                .regions(List.of(new Region(range)))
                .build();
        List<LeafPartitionQuery> subQueries = List.of(
                LeafPartitionQuery.builder().parentQuery(query).tableId("tableId").subQueryId("s1").regions(List.of(region)).leafPartitionId("leaf1").partitionRegion(partitionRegion).files(List.of())
                        .build(),
                LeafPartitionQuery.builder().parentQuery(query).tableId("tableId").subQueryId("s2").regions(List.of(region)).leafPartitionId("leaf2").partitionRegion(partitionRegion).files(List.of())
                        .build(),
                LeafPartitionQuery.builder().parentQuery(query).tableId("tableId").subQueryId("s3").regions(List.of(region)).leafPartitionId("leaf3").partitionRegion(partitionRegion).files(List.of())
                        .build());

        // When
        config.getListener().subQueriesCreated(query, subQueries);

        // Then
        verify(1, postRequestedFor(config.getUrl()).withRequestBody(equalToJson("""
                {
                  "message": "subqueries",
                  "queryId": "q1",
                  "queryIds": ["s1", "s2", "s3"]
                }
                """)));
    }

    @Test
    void shouldSendParentQueryCompletedNotification() {
        // Given
        stubFor(post(config.getUrl()).willReturn(aResponse().withStatus(200)));
        Range range = config.getRangeFactory().createExactRange(SCHEMA.getRowKeyFields().get(0), "a");
        Query query = Query.builder()
                .tableName("tableName")
                .queryId("q1")
                .regions(List.of(new Region(range)))
                .build();
        ResultsOutputInfo result = new ResultsOutputInfo(1, Lists.newArrayList(
                new ResultsOutputLocation("s3", "s3://bucket/file1.parquet"),
                new ResultsOutputLocation("s3", "s3://bucket/file2.parquet")));

        // When
        config.getListener().queryCompleted(query, result);

        // Then
        verify(1, postRequestedFor(config.getUrl()).withRequestBody(equalToJson("""
                {
                  "message" : "completed",
                  "rowCount" : 1,
                  "queryId" : "q1",
                  "locations" : [
                    {
                      "type" : "s3",
                      "location" : "s3://bucket/file1.parquet"
                    },
                    {
                      "type" : "s3",
                      "location" : "s3://bucket/file2.parquet"
                    }
                  ]
                }
                """)));
    }

    @Test
    void shouldSendSubQueryCompletedNotification() {
        // Given
        stubFor(post(config.getUrl()).willReturn(aResponse().withStatus(200)));
        Range range = config.getRangeFactory().createExactRange(SCHEMA.getRowKeyFields().get(0), "a");
        Range partitionRange = config.getRangeFactory().createRange(SCHEMA.getRowKeyFields().get(0), "a", "b");
        Query query = Query.builder()
                .tableName("tableName")
                .queryId("q1")
                .regions(List.of(new Region(range)))
                .build();
        LeafPartitionQuery subQuery = LeafPartitionQuery.builder()
                .parentQuery(query)
                .tableId("tableId")
                .subQueryId("s1")
                .regions(List.of(new Region(range)))
                .leafPartitionId("leaf1")
                .partitionRegion(new Region(partitionRange))
                .files(List.of())
                .build();
        ResultsOutputInfo result = new ResultsOutputInfo(1, Lists.newArrayList(
                new ResultsOutputLocation("s3", "s3://bucket/file1.parquet"),
                new ResultsOutputLocation("s3", "s3://bucket/file2.parquet")));

        // When
        config.getListener().queryCompleted(subQuery, result);

        // Then
        verify(1, postRequestedFor(config.getUrl()).withRequestBody(equalToJson("""
                {
                  "message" : "completed",
                  "queryId" : "s1",
                  "rowCount" : 1,
                  "locations" : [
                    {
                      "type" : "s3",
                      "location" : "s3://bucket/file1.parquet"
                    },
                    {
                      "type" : "s3",
                      "location" : "s3://bucket/file2.parquet"
                    }
                  ]
                }
                """)));
    }

    @Test
    void shouldSendPartialQueryFailureNotification() {
        // Given
        stubFor(post(config.getUrl()).willReturn(aResponse().withStatus(200)));
        Range range = config.getRangeFactory().createExactRange(SCHEMA.getRowKeyFields().get(0), "a");
        Query query = Query.builder()
                .tableName("tableName")
                .queryId("q2")
                .regions(List.of(new Region(range)))
                .build();
        ResultsOutputInfo result = new ResultsOutputInfo(1, Lists.newArrayList(
                new ResultsOutputLocation("data", "s3://bucket/data/parquet"),
                new ResultsOutputLocation("sketches", "s3://bucket/sketches.parquet")), new IOException("error writing record #2"));

        // When
        config.getListener().queryCompleted(query, result);

        // Then
        verify(1, postRequestedFor(config.getUrl()).withRequestBody(equalToJson("""
                {
                  "message" : "error",
                  "queryId" : "q2",
                  "rowCount" : 1,
                  "locations" : [
                    {
                      "type" : "data",
                      "location" : "s3://bucket/data/parquet"
                    },
                    {
                      "type" : "sketches",
                      "location" : "s3://bucket/sketches.parquet"
                    }
                  ], "error" : "IOException: error writing record #2"
                }
                """)));
    }

    @Test
    void shouldSendQueryFailureNotificationOnException() {
        // Given
        stubFor(post(config.getUrl()).willReturn(aResponse().withStatus(200)));
        Range range = config.getRangeFactory().createExactRange(SCHEMA.getRowKeyFields().get(0), "a");
        Query query = Query.builder()
                .tableName("tableName")
                .queryId("q3")
                .regions(List.of(new Region(range)))
                .build();

        // When
        config.getListener().queryFailed(query, new IOException("fail"));

        // Then
        verify(1, postRequestedFor(config.getUrl()).withRequestBody(equalToJson("""
                {
                  "queryId" : "q3",
                  "message" : "error",
                  "error" : "IOException: fail"
                }
                """)));
    }

    @Test
    void shouldSendSubQueryFailureNotificationOnException() {
        // Given
        stubFor(post(config.getUrl()).willReturn(aResponse().withStatus(200)));
        Range range = config.getRangeFactory().createExactRange(SCHEMA.getRowKeyFields().get(0), "a");
        Range partitionRange = config.getRangeFactory().createRange(SCHEMA.getRowKeyFields().get(0), "a", "b");
        Query query = Query.builder()
                .tableName("tableName")
                .queryId("q1")
                .regions(List.of(new Region(range)))
                .build();
        LeafPartitionQuery subQuery = LeafPartitionQuery.builder()
                .parentQuery(query)
                .tableId("tableId")
                .subQueryId("s1")
                .regions(List.of(new Region(range)))
                .leafPartitionId("leaf1")
                .partitionRegion(new Region(partitionRange))
                .files(List.of())
                .build();

        // When
        config.getListener().queryFailed(subQuery, new IOException("fail"));

        // Then
        verify(1, postRequestedFor(config.getUrl()).withRequestBody(equalToJson("""
                {
                  "queryId" : "s1",
                  "message" : "error",
                  "error" : "IOException: fail"
                }
                """)));
    }

    private static class WebSocketQueryConfig {
        private final WebSocketQueryStatusReportDestination listener;
        private final UrlPattern url;
        private final RangeFactory rangeFactory;

        WebSocketQueryConfig(WireMockRuntimeInfo runtimeInfo) {
            String connectionId = "connection1";
            ApiGatewayManagementApiClient client = wiremockAwsV2Client(runtimeInfo, ApiGatewayManagementApiClient.builder());
            ApiGatewayWebSocketOutput output = new ApiGatewayWebSocketOutput(client, connectionId);
            this.listener = new WebSocketQueryStatusReportDestination(output);
            this.url = urlEqualTo("/@connections/" + connectionId);
            this.rangeFactory = new RangeFactory(SCHEMA);
        }

        public WebSocketQueryStatusReportDestination getListener() {
            return listener;
        }

        public UrlPattern getUrl() {
            return url;
        }

        public RangeFactory getRangeFactory() {
            return rangeFactory;
        }
    }
}
