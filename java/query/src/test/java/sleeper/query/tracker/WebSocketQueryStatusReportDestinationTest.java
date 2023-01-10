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

import com.amazonaws.auth.BasicAWSCredentials;
import com.github.tomakehurst.wiremock.junit.WireMockClassRule;
import com.github.tomakehurst.wiremock.matching.UrlPattern;
import org.apache.curator.shaded.com.google.common.collect.Lists;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.jupiter.api.Test;

import sleeper.core.range.Range;
import sleeper.core.range.Range.RangeFactory;
import sleeper.core.range.Region;
import sleeper.core.schema.Field;
import sleeper.core.schema.Schema;
import sleeper.core.schema.type.LongType;
import sleeper.core.schema.type.StringType;
import sleeper.query.model.LeafPartitionQuery;
import sleeper.query.model.Query;
import sleeper.query.model.output.ResultsOutputInfo;
import sleeper.query.model.output.ResultsOutputLocation;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;

import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static com.github.tomakehurst.wiremock.client.WireMock.absent;
import static com.github.tomakehurst.wiremock.client.WireMock.equalTo;
import static com.github.tomakehurst.wiremock.client.WireMock.equalToJson;
import static com.github.tomakehurst.wiremock.client.WireMock.matchingJsonPath;
import static com.github.tomakehurst.wiremock.client.WireMock.post;
import static com.github.tomakehurst.wiremock.client.WireMock.postRequestedFor;
import static com.github.tomakehurst.wiremock.client.WireMock.urlEqualTo;

public class WebSocketQueryStatusReportDestinationTest {
    private static final Schema SCHEMA = Schema.builder()
            .rowKeyFields(new Field("key", new StringType()))
            .valueFields(new Field("count", new LongType()))
            .build();

    @ClassRule
    public static WireMockClassRule wireMockRule = new WireMockClassRule();

    @Rule
    public WireMockClassRule wireMock = wireMockRule;

    private final WebSocketQueryStatusReportDestination listener;
    private final UrlPattern url;
    private final RangeFactory rangeFactory;

    public WebSocketQueryStatusReportDestinationTest() {
        String region = "eu-west-1";
        String connectionId = "connection1";
        this.listener = new WebSocketQueryStatusReportDestination(region, this.wireMock.baseUrl(), connectionId, new BasicAWSCredentials("accessKey", "secretKey"));
        this.url = urlEqualTo("/@connections/" + connectionId);
        this.wireMock.stubFor(post(this.url).willReturn(aResponse().withStatus(200)));
        this.rangeFactory = new RangeFactory(SCHEMA);
    }

    @Test
    public void shouldNotSendQueryQueuedNotification() {
        // Given
        Range range = rangeFactory.createExactRange(SCHEMA.getRowKeyFields().get(0), "a");
        Query query = new Query.Builder("tableName", "q1", new Region(range)).build();

        // When
        listener.queryQueued(query);

        // Then
        wireMock.verify(0, postRequestedFor(url));
    }

    @Test
    public void shouldNotSendQueryInProgressNotification() {
        // Given
        Range range = rangeFactory.createExactRange(SCHEMA.getRowKeyFields().get(0), "a");
        Query query = new Query.Builder("tableName", "q1", new Region(range)).build();

        // When
        listener.queryInProgress(query);

        // Then
        wireMock.verify(0, postRequestedFor(url));
    }

    @Test
    public void shouldSendNotificationOfSubQueriesBeingCreated() {
        // Given
        Range range = rangeFactory.createExactRange(SCHEMA.getRowKeyFields().get(0), "a");
        Region region = new Region(range);
        Range partitionRange = rangeFactory.createRange(SCHEMA.getRowKeyFields().get(0), "a", "b");
        Region partitionRegion = new Region(partitionRange);
        Query query = new Query.Builder("tableName", "q1", new Region(range)).build();
        ArrayList<LeafPartitionQuery> subQueries = Lists.newArrayList(
                new LeafPartitionQuery.Builder("tableName", "q1", "s1", region, "leaf1", partitionRegion, Collections.emptyList()).build(),
                new LeafPartitionQuery.Builder("tableName", "q1", "s2", region, "leaf2", partitionRegion, Collections.emptyList()).build(),
                new LeafPartitionQuery.Builder("tableName", "q1", "s3", region, "leaf3", partitionRegion, Collections.emptyList()).build()
        );

        // When
        listener.subQueriesCreated(query, subQueries);

        // Then
        wireMock.verify(1, postRequestedFor(url).withRequestBody(
                matchingJsonPath("$.queryId", equalTo("q1"))
                        .and(matchingJsonPath("$.message", equalTo("subqueries")))
                        .and(matchingJsonPath("$.error", absent()))
                        .and(matchingJsonPath("$.recordCount", absent()))
                        .and(matchingJsonPath("$.queryIds", equalToJson("[\"s1\",\"s2\",\"s3\"]")))
        ));
    }

    @Test
    public void shouldSendQueryCompletedNotification() {
        // Given
        Range range = rangeFactory.createExactRange(SCHEMA.getRowKeyFields().get(0), "a");
        Query query = new Query.Builder("tableName", "q1", new Region(range)).build();
        ResultsOutputInfo result = new ResultsOutputInfo(1, Lists.newArrayList(
                new ResultsOutputLocation("s3", "s3://bucket/file1.parquet"),
                new ResultsOutputLocation("s3", "s3://bucket/file2.parquet")
        ));

        // When
        listener.queryCompleted(query, result);

        // Then
        wireMock.verify(1, postRequestedFor(url).withRequestBody(
                matchingJsonPath("$.queryId", equalTo("q1"))
                        .and(matchingJsonPath("$.message", equalTo("completed")))
                        .and(matchingJsonPath("$.error", absent()))
                        .and(matchingJsonPath("$.recordCount", equalTo(String.valueOf(result.getRecordCount()))))
        ));
    }

    @Test
    public void shouldSendPartialQueryFailureNotification() {
        // Given
        Range range = rangeFactory.createExactRange(SCHEMA.getRowKeyFields().get(0), "a");
        Query query = new Query.Builder("tableName", "q2", new Region(range)).build();
        ResultsOutputInfo result = new ResultsOutputInfo(1, Lists.newArrayList(
                new ResultsOutputLocation("data", "s3://bucket/data/parquet"),
                new ResultsOutputLocation("sketches", "s3://bucket/sketches.parquet")
        ), new IOException("error writing record #2"));

        // When
        listener.queryCompleted(query, result);

        // Then
        wireMock.verify(1, postRequestedFor(url).withRequestBody(
                matchingJsonPath("$.queryId", equalTo("q2"))
                        .and(matchingJsonPath("$.message", equalTo("error")))
                        .and(matchingJsonPath("$.error", equalTo(result.getError().getClass().getSimpleName() + ": " + result.getError().getMessage())))
                        .and(matchingJsonPath("$.recordCount", equalTo(String.valueOf(result.getRecordCount()))))
        ));
    }

    @Test
    public void shouldSendQueryFailureNotificationOnException() {
        // Given
        Range range = rangeFactory.createExactRange(SCHEMA.getRowKeyFields().get(0), "a");
        Query query = new Query.Builder("tableName", "q3", new Region(range)).build();

        // When
        listener.queryFailed(query, new IOException("fail"));

        // Then
        wireMock.verify(1, postRequestedFor(url).withRequestBody(
                matchingJsonPath("$.queryId", equalTo("q3"))
                        .and(matchingJsonPath("$.message", equalTo("error")))
                        .and(matchingJsonPath("$.error", equalTo("IOException: fail")))
                        .and(matchingJsonPath("$.recordCount", absent()))
        ));
    }
}
