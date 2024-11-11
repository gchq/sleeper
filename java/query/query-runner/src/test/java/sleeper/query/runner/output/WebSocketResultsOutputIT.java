/*
 * Copyright 2022-2024 Crown Copyright
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
import org.junit.jupiter.api.Test;

import sleeper.core.iterator.WrappedIterator;
import sleeper.core.record.Record;
import sleeper.core.schema.Field;
import sleeper.core.schema.Schema;
import sleeper.core.schema.type.StringType;
import sleeper.query.core.model.Query;
import sleeper.query.core.model.QueryOrLeafPartitionQuery;
import sleeper.query.core.output.ResultsOutputInfo;

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
import static org.assertj.core.api.Assertions.assertThat;

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

    @Test
    public void shouldStopPublishingResultsWhenClientHasGone(WireMockRuntimeInfo wmRuntimeInfo) {
        // Given
        String connectionId = "connection1";
        UrlPattern url = urlEqualTo("/@connections/" + connectionId);
        stubFor(post(url).willReturn(aResponse()
                .withStatus(410)
                .withHeader("x-amzn-ErrorType", "GoneException")));

        Map<String, String> config = new HashMap<>();
        config.put(WebSocketResultsOutput.ENDPOINT, wmRuntimeInfo.getHttpBaseUrl());
        config.put(WebSocketResultsOutput.REGION, "eu-west-1");
        config.put(WebSocketResultsOutput.CONNECTION_ID, connectionId);
        config.put(WebSocketResultsOutput.MAX_BATCH_SIZE, "1");
        config.put(WebSocketResultsOutput.ACCESS_KEY, "accessKey");
        config.put(WebSocketResultsOutput.SECRET_KEY, "secretKey");
        WebSocketResultsOutput out = new WebSocketResultsOutput(schema, config);

        List<Record> records = new ArrayList<>();
        records.add(new Record(Collections.singletonMap("id", "record1")));
        records.add(new Record(Collections.singletonMap("id", "record2")));
        records.add(new Record(Collections.singletonMap("id", "record3")));
        records.add(new Record(Collections.singletonMap("id", "record4")));
        records.add(new Record(Collections.singletonMap("id", "record5")));

        // When
        ResultsOutputInfo result = out.publish(new QueryOrLeafPartitionQuery(query), new WrappedIterator<>(records.iterator()));

        // Then
        verify(1, postRequestedFor(url).withRequestBody(
                matchingJsonPath("$.queryId", equalTo("query1"))
                        .and(matchingJsonPath("$.message", equalTo("records")))));
        assertThat(result.getRecordCount()).isZero();
        assertThat(result.getError()).hasMessageContaining("GoneException");
    }

    @Test
    public void shouldBatchResultsAccordingToConfig(WireMockRuntimeInfo wmRuntimeInfo) {
        // Given
        String connectionId = "connection1";
        UrlPattern url = urlEqualTo("/@connections/" + connectionId);
        stubFor(post(url).willReturn(aResponse().withStatus(200)));

        Map<String, String> config = new HashMap<>();
        config.put(WebSocketResultsOutput.ENDPOINT, wmRuntimeInfo.getHttpBaseUrl());
        config.put(WebSocketResultsOutput.REGION, "eu-west-1");
        config.put(WebSocketResultsOutput.CONNECTION_ID, connectionId);
        config.put(WebSocketResultsOutput.MAX_BATCH_SIZE, "1");
        config.put(WebSocketResultsOutput.ACCESS_KEY, "accessKey");
        config.put(WebSocketResultsOutput.SECRET_KEY, "secretKey");
        WebSocketResultsOutput out = new WebSocketResultsOutput(schema, config);

        List<Record> records = new ArrayList<>();
        records.add(new Record(Collections.singletonMap("id", "record1")));
        records.add(new Record(Collections.singletonMap("id", "record2")));
        records.add(new Record(Collections.singletonMap("id", "record3")));
        records.add(new Record(Collections.singletonMap("id", "record4")));
        records.add(new Record(Collections.singletonMap("id", "record5")));

        // When
        out.publish(new QueryOrLeafPartitionQuery(query), new WrappedIterator<>(records.iterator()));

        // Then
        verify(records.size(), postRequestedFor(url).withRequestBody(
                matchingJsonPath("$.queryId", equalTo("query1"))
                        .and(matchingJsonPath("$.message", equalTo("records")))));
    }
}
