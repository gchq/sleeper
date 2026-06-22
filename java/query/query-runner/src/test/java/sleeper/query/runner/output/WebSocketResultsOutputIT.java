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
package sleeper.query.runner.output;

import com.github.tomakehurst.wiremock.junit5.WireMockRuntimeInfo;
import com.github.tomakehurst.wiremock.junit5.WireMockTest;
import com.github.tomakehurst.wiremock.matching.UrlPattern;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import sleeper.core.iterator.closeable.WrappedIterator;
import sleeper.core.row.Row;
import sleeper.core.schema.Field;
import sleeper.core.schema.Schema;
import sleeper.core.schema.type.StringType;
import sleeper.query.core.model.Query;
import sleeper.query.core.model.QueryOrLeafPartitionQuery;
import sleeper.query.core.output.ResultsOutputInfo;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static com.github.tomakehurst.wiremock.client.WireMock.equalTo;
import static com.github.tomakehurst.wiremock.client.WireMock.matchingJsonPath;
import static com.github.tomakehurst.wiremock.client.WireMock.post;
import static com.github.tomakehurst.wiremock.client.WireMock.postRequestedFor;
import static com.github.tomakehurst.wiremock.client.WireMock.stubFor;
import static com.github.tomakehurst.wiremock.client.WireMock.urlEqualTo;
import static com.github.tomakehurst.wiremock.client.WireMock.verify;
import static com.github.tomakehurst.wiremock.stubbing.Scenario.STARTED;
import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.core.util.ThreadSleepTestHelper.recordWaits;

@WireMockTest
public class WebSocketResultsOutputIT {
    private final Schema schema = Schema.builder()
            .rowKeyFields(List.of(new Field("id", new StringType())))
            .build();

    private final Query query = Query.builder()
            .tableName("table1")
            .queryId("query1")
            .regions(List.of())
            .build();

    private final List<Duration> foundWaits = new ArrayList<>();
    private final Map<String, String> config = new HashMap<>();
    private final UrlPattern url = urlEqualTo("/@connections/test-connection");

    @BeforeEach
    void setUp(WireMockRuntimeInfo wmRuntimeInfo) {
        config.put(WebSocketOutput.ENDPOINT, wmRuntimeInfo.getHttpBaseUrl());
        config.put(WebSocketOutput.REGION, "eu-west-1");
        config.put(WebSocketOutput.CONNECTION_ID, "test-connection");
        config.put(WebSocketOutput.ACCESS_KEY, "accessKey");
        config.put(WebSocketOutput.SECRET_KEY, "secretKey");
    }

    @Test
    public void shouldBatchResultsAccordingToConfig(WireMockRuntimeInfo wmRuntimeInfo) {
        // Given
        stubFor(post(url).willReturn(aResponse().withStatus(200)));

        config.put(WebSocketOutput.MAX_BATCH_SIZE, "1");

        List<Row> rows = new ArrayList<>();
        rows.add(new Row(Collections.singletonMap("id", "row1")));
        rows.add(new Row(Collections.singletonMap("id", "row2")));
        rows.add(new Row(Collections.singletonMap("id", "row3")));
        rows.add(new Row(Collections.singletonMap("id", "row4")));
        rows.add(new Row(Collections.singletonMap("id", "row5")));

        // When
        ResultsOutputInfo result = output().publish(new QueryOrLeafPartitionQuery(query), new WrappedIterator<>(rows.iterator()));

        // Then
        verify(rows.size(), postRequestedFor(url).withRequestBody(
                matchingJsonPath("$.queryId", equalTo("query1"))
                        .and(matchingJsonPath("$.message", equalTo("rows")))));
        assertThat(result.getRowCount()).isEqualTo(5);
    }

    // Test list for retries:
    // - Retries on LimitExceededException and succeeds
    // - Gives up after max attempts on persistent LimitExceededException
    // - Does not retry PayloadTooLargeException, status 413

    @Test
    public void shouldRetryWhenLimitExceededAndSucceed(WireMockRuntimeInfo wmRuntimeInfo) {
        // Given
        stubFor(post(url).inScenario("retry")
                .whenScenarioStateIs(STARTED)
                .willReturn(aResponse()
                        .withStatus(429)
                        .withHeader("x-amzn-ErrorType", "LimitExceededException"))
                .willSetStateTo("retried"));
        stubFor(post(url).inScenario("retry")
                .whenScenarioStateIs("retried")
                .willReturn(aResponse().withStatus(200)));

        config.put(WebSocketOutput.MAX_ATTEMPTS, "5");
        config.put(WebSocketOutput.LIMIT_EXCEEDED_FIRST_WAIT_CEILING_SECS, "0.001");
        config.put(WebSocketOutput.LIMIT_EXCEEDED_MAX_WAIT_CEILING_SECS, "0.01");

        List<Row> rows = new ArrayList<>();
        rows.add(new Row(Collections.singletonMap("id", "row1")));

        // When
        ResultsOutputInfo result = output().publish(new QueryOrLeafPartitionQuery(query), new WrappedIterator<>(rows.iterator()));

        // Then
        verify(2, postRequestedFor(url));
        assertThat(foundWaits).hasSize(1);
        assertThat(result.getRowCount()).isEqualTo(1);
    }

    @Test
    public void shouldGiveUpAfterMaxAttemptsOnPersistentLimitExceeded(WireMockRuntimeInfo wmRuntimeInfo) {
        // Given
        stubFor(post(url).willReturn(aResponse()
                .withStatus(429)
                .withHeader("x-amzn-ErrorType", "LimitExceededException")));

        config.put(WebSocketOutput.MAX_ATTEMPTS, "3");
        config.put(WebSocketOutput.LIMIT_EXCEEDED_FIRST_WAIT_CEILING_SECS, "0.001");
        config.put(WebSocketOutput.LIMIT_EXCEEDED_MAX_WAIT_CEILING_SECS, "0.01");

        List<Row> rows = new ArrayList<>();
        rows.add(new Row(Collections.singletonMap("id", "row1")));

        // When
        ResultsOutputInfo result = output().publish(new QueryOrLeafPartitionQuery(query), new WrappedIterator<>(rows.iterator()));

        // Then
        verify(3, postRequestedFor(url));
        assertThat(foundWaits).hasSize(2);
        assertThat(result.getRowCount()).isZero();
        assertThat(result.getError()).hasMessageContaining("LimitExceededException");
    }

    @Test
    public void shouldStopPublishingResultsWhenClientHasGone(WireMockRuntimeInfo wmRuntimeInfo) {
        // Given
        stubFor(post(url).willReturn(aResponse()
                .withStatus(410)
                .withHeader("x-amzn-ErrorType", "GoneException")));

        config.put(WebSocketOutput.MAX_BATCH_SIZE, "1");

        List<Row> rows = new ArrayList<>();
        rows.add(new Row(Collections.singletonMap("id", "row1")));
        rows.add(new Row(Collections.singletonMap("id", "row2")));
        rows.add(new Row(Collections.singletonMap("id", "row3")));
        rows.add(new Row(Collections.singletonMap("id", "row4")));
        rows.add(new Row(Collections.singletonMap("id", "row5")));

        // When
        ResultsOutputInfo result = output().publish(new QueryOrLeafPartitionQuery(query), new WrappedIterator<>(rows.iterator()));

        // Then
        verify(1, postRequestedFor(url).withRequestBody(
                matchingJsonPath("$.queryId", equalTo("query1"))
                        .and(matchingJsonPath("$.message", equalTo("rows")))));
        assertThat(result.getRowCount()).isZero();
        assertThat(result.getError()).hasMessageContaining("GoneException");
    }

    private WebSocketResultsOutput output() {
        return new WebSocketResultsOutput(schema, config, recordWaits(foundWaits));
    }
}
