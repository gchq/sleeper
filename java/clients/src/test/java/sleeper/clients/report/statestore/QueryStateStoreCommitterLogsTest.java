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
package sleeper.clients.report.statestore;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import software.amazon.awssdk.services.cloudwatchlogs.CloudWatchLogsClient;
import software.amazon.awssdk.services.cloudwatchlogs.model.GetQueryResultsRequest;
import software.amazon.awssdk.services.cloudwatchlogs.model.GetQueryResultsResponse;
import software.amazon.awssdk.services.cloudwatchlogs.model.QueryStatus;
import software.amazon.awssdk.services.cloudwatchlogs.model.ResultField;
import software.amazon.awssdk.services.cloudwatchlogs.model.StartQueryRequest;
import software.amazon.awssdk.services.cloudwatchlogs.model.StartQueryResponse;

import sleeper.core.properties.instance.InstanceProperties;

import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.STATESTORE_COMMITTER_LOG_GROUP;

public class QueryStateStoreCommitterLogsTest {
    private static final DateTimeFormatter TIMESTAMP_FORMATTER = ReadStateStoreCommitterLogs.TIMESTAMP_FORMATTER.withZone(ZoneOffset.UTC);

    // These are used to create a fixed query window, to keep the tests deterministic.
    private static final Instant START_TIME = Instant.parse("2026-07-09T14:54:00Z");
    private static final Instant END_TIME = Instant.parse("2026-07-09T14:55:00Z");
    // middle of the window
    private static final Instant LOG_TIME = Instant.parse("2026-07-09T14:54:30Z");

    @ParameterizedTest
    @ValueSource(ints = {0, 1, 2})
    void shouldReturnLogsWhenCloudWatchLogsQueryEventuallySucceeds(int failuresBeforeSuccess) throws Exception {
        // Given
        CloudWatchLogsClient cloudWatch = mock(CloudWatchLogsClient.class);
        int expectedAttempts = failuresBeforeSuccess + 1;
        givenStartQueries(cloudWatch, expectedAttempts);
        givenQueryResults(cloudWatch, failedQueries(failuresBeforeSuccess), completedQueryWithLambdaStartedLog());

        // When
        List<StateStoreCommitterLogEntry> logs = query(cloudWatch).getLogsInPeriod(START_TIME, END_TIME);

        // Then
        assertThat(logs).containsExactly(new StateStoreCommitterLambdaRunStarted("test-stream", LOG_TIME, START_TIME));
        verify(cloudWatch, times(expectedAttempts)).startQuery(expectedLogsQuery());
        verifyQueryResultsRequested(cloudWatch, expectedAttempts);
    }

    @Test
    void shouldThrowExceptionAfterThreeFailedCloudWatchLogsQueries() {
        // Given
        CloudWatchLogsClient cloudWatch = mock(CloudWatchLogsClient.class);
        givenStartQueries(cloudWatch, 3);
        givenQueryResults(cloudWatch, failedQueries(3));

        // When / Then
        assertThatThrownBy(() -> query(cloudWatch).getLogsInPeriod(START_TIME, END_TIME))
                .isInstanceOf(RuntimeException.class)
                .hasMessage("Logs query failed with status Failed");
        verify(cloudWatch, times(3)).startQuery(expectedLogsQuery());
        verifyQueryResultsRequested(cloudWatch, 3);
    }

    private static void givenStartQueries(CloudWatchLogsClient cloudWatch, int queryCount) {
        List<StartQueryResponse> responses = IntStream.rangeClosed(1, queryCount)
                .mapToObj(queryNumber -> StartQueryResponse.builder().queryId("query-" + queryNumber).build())
                .toList();
        when(cloudWatch.startQuery(anyStartQueryRequest()))
                .thenReturn(responses.get(0), responses.subList(1, responses.size()).toArray(StartQueryResponse[]::new));
    }

    private static void givenQueryResults(
            CloudWatchLogsClient cloudWatch, List<GetQueryResultsResponse> failedQueries, GetQueryResultsResponse successfulQuery) {
        List<GetQueryResultsResponse> responses = new ArrayList<>(failedQueries);
        responses.add(successfulQuery);
        givenQueryResults(cloudWatch, responses);
    }

    private static void givenQueryResults(CloudWatchLogsClient cloudWatch, List<GetQueryResultsResponse> responses) {
        when(cloudWatch.getQueryResults(anyGetQueryResultsRequest()))
                .thenReturn(responses.get(0), responses.subList(1, responses.size()).toArray(GetQueryResultsResponse[]::new));
    }

    private static void verifyQueryResultsRequested(CloudWatchLogsClient cloudWatch, int queryCount) {
        IntStream.rangeClosed(1, queryCount)
                .forEach(queryNumber -> verify(cloudWatch).getQueryResults(queryResultsRequestFor("query-" + queryNumber)));
    }

    private static QueryStateStoreCommitterLogs query(CloudWatchLogsClient cloudWatch) {
        return new QueryStateStoreCommitterLogs(instanceProperties(), cloudWatch);
    }

    private static InstanceProperties instanceProperties() {
        InstanceProperties instanceProperties = new InstanceProperties();
        instanceProperties.set(STATESTORE_COMMITTER_LOG_GROUP, "test-log-group");
        return instanceProperties;
    }

    private static GetQueryResultsResponse failedQuery() {
        return GetQueryResultsResponse.builder().status(QueryStatus.FAILED).build();
    }

    private static List<GetQueryResultsResponse> failedQueries(int count) {
        return IntStream.range(0, count)
                .mapToObj(ignored -> failedQuery())
                .toList();
    }

    // generate a fake query result
    private static GetQueryResultsResponse completedQueryWithLambdaStartedLog() {
        return GetQueryResultsResponse.builder()
                .status(QueryStatus.COMPLETE)
                .results(List.of(List.of(
                        logStreamField("test-stream"),
                        timestampField(LOG_TIME),
                        messageField("[main] lambda.committer.StateStoreCommitterLambda INFO - State store committer process started at 2026-07-09T14:54:00Z\n"))))
                .build();
    }

    private static ResultField logStreamField(String logStream) {
        return ResultField.builder().field("@logStream").value(logStream).build();
    }

    private static ResultField timestampField(Instant timestamp) {
        return ResultField.builder().field("@timestamp").value(TIMESTAMP_FORMATTER.format(timestamp)).build();
    }

    private static ResultField messageField(String message) {
        return ResultField.builder().field("@message").value(message).build();
    }

    @SuppressWarnings("unchecked")
    private static Consumer<StartQueryRequest.Builder> anyStartQueryRequest() {
        return any(Consumer.class);
    }

    private static Consumer<StartQueryRequest.Builder> expectedLogsQuery() {
        return argThat(query -> {
            StartQueryRequest.Builder builder = StartQueryRequest.builder();
            query.accept(builder);
            StartQueryRequest request = builder.build();
            return request.logGroupName().equals("test-log-group") &&
                    request.startTime().equals(START_TIME.getEpochSecond()) &&
                    request.endTime().equals(END_TIME.getEpochSecond());
        });
    }

    @SuppressWarnings("unchecked")
    private static Consumer<GetQueryResultsRequest.Builder> anyGetQueryResultsRequest() {
        return any(Consumer.class);
    }

    private static Consumer<GetQueryResultsRequest.Builder> queryResultsRequestFor(String queryId) {
        return argThat(query -> {
            GetQueryResultsRequest.Builder builder = GetQueryResultsRequest.builder();
            query.accept(builder);
            return builder.build().queryId().equals(queryId);
        });
    }
}
