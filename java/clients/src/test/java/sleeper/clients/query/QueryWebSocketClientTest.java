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
package sleeper.clients.query;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import sleeper.clients.query.exception.WebSocketClosedException;
import sleeper.clients.query.exception.WebSocketErrorException;
import sleeper.core.row.Row;
import sleeper.query.core.model.Query;
import sleeper.query.runner.websocket.QueryWebSocketMessageException;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

import static org.assertj.core.api.Assertions.assertThat;

public class QueryWebSocketClientTest extends QueryWebSocketClientTestBase {

    @Nested
    @DisplayName("Run queries")
    class RunQuery {

        @Test
        void shouldReturnResultsForQuery() throws Exception {
            // Given
            Query query = exactQuery("test-query-id", 123);
            Row expectedRow = new Row(Map.of("key", 123L));
            adapter.setFakeResponses(
                    message(queryResult("test-query-id", expectedRow)),
                    message(completedQuery("test-query-id", 1L)));

            // When / Then
            assertThat(runQueryFuture(query))
                    .isCompletedWithValue(List.of(expectedRow));
            assertThat(adapter.isConnected()).isFalse();
            assertThat(adapter.isClosed()).isTrue();
            assertThat(adapter.getSentMessages())
                    .containsExactly(querySerDe.toJson(query));
        }

        @Test
        void shouldReturnResultsForQueryWithOneSubquery() throws Exception {
            // Given
            Query query = exactQuery("test-query-id", 123);
            Row expectedRow = new Row(Map.of("key", 123L));
            adapter.setFakeResponses(
                    message(createdSubQueries("test-query-id", "test-subquery")),
                    message(queryResult("test-subquery", expectedRow)),
                    message(completedQuery("test-subquery", 1L)));

            // When / Then
            assertThat(runQueryFuture(query))
                    .isCompletedWithValue(List.of(expectedRow));
            assertThat(adapter.isConnected()).isFalse();
            assertThat(adapter.isClosed()).isTrue();
            assertThat(adapter.getSentMessages())
                    .containsExactly(querySerDe.toJson(query));
        }

        @Test
        void shouldReturnResultsForQueryWithMultipleSubqueries() throws Exception {
            // Given
            Query query = rangeQuery("test-query-id", 0L, 1000L);
            Row expectedRow1 = new Row(Map.of("key", 123L));
            Row expectedRow2 = new Row(Map.of("key", 456L));
            Row expectedRow3 = new Row(Map.of("key", 789L));
            adapter.setFakeResponses(
                    message(createdSubQueries("test-query-id", "subquery-1", "subquery-2", "subquery-3")),
                    message(queryResult("subquery-1", expectedRow1)),
                    message(completedQuery("subquery-1", 1L)),
                    message(queryResult("subquery-2", expectedRow2)),
                    message(completedQuery("subquery-2", 1L)),
                    message(queryResult("subquery-3", expectedRow3)),
                    message(completedQuery("subquery-3", 1L)));

            // When / Then
            assertThat(runQueryFuture(query))
                    .isCompletedWithValue(List.of(expectedRow1, expectedRow2, expectedRow3));
            assertThat(adapter.isConnected()).isFalse();
            assertThat(adapter.isClosed()).isTrue();
            assertThat(adapter.getSentMessages())
                    .containsExactly(querySerDe.toJson(query));
        }

        @Test
        void shouldReturnResultsWhenRecordCountDoesNotMatchRowsReceived() throws Exception {
            // Given
            Query query = exactQuery("test-query-id", 123);
            Row expectedRow = new Row(Map.of("key", 123L));
            adapter.setFakeResponses(
                    message(queryResult("test-query-id", expectedRow)),
                    message(completedQuery("test-query-id", 2L)));

            // When / Then
            assertThat(runQueryFuture(query))
                    .isCompletedWithValue(List.of(expectedRow));
            assertThat(adapter.isConnected()).isFalse();
            assertThat(adapter.isClosed()).isTrue();
            assertThat(adapter.getSentMessages())
                    .containsExactly(querySerDe.toJson(query));
        }
    }

    @Nested
    @DisplayName("Handle errors")
    class HandleErrors {

        @Test
        void shouldHandleErrorIfExceptionEncounteredThatDoesNotCloseConnection() throws Exception {
            // Given
            Query query = exactQuery("test-query-id", 123L);
            adapter.setFakeResponses(
                    error(new Exception("Exception that will not terminate connection")));

            // When / Then
            assertThat(runQueryFuture(query))
                    .isCompletedExceptionally()
                    .failsWithin(Duration.ofMillis(10))
                    .withThrowableOfType(ExecutionException.class)
                    .withCauseInstanceOf(WebSocketErrorException.class);
            assertThat(adapter.isConnected()).isFalse();
            assertThat(adapter.isClosed()).isTrue();
            assertThat(adapter.getSentMessages())
                    .containsExactly(querySerDe.toJson(query));
        }

        @Test
        void shouldHandleErrorIfExceptionEncounteredWhenQueryCompletesAfter() throws Exception {
            // Given
            Query query = exactQuery("test-query-id", 123L);
            adapter.setFakeResponses(
                    error(new Exception("Exception that will not terminate connection")),
                    message(completedQuery("test-query-id", 0L)));

            // When / Then
            assertThat(runQueryFuture(query))
                    .isCompletedExceptionally()
                    .failsWithin(Duration.ofMillis(10))
                    .withThrowableOfType(ExecutionException.class)
                    .withCauseInstanceOf(WebSocketErrorException.class);
            assertThat(adapter.isConnected()).isFalse();
            assertThat(adapter.isClosed()).isTrue();
            assertThat(adapter.getSentMessages())
                    .containsExactly(querySerDe.toJson(query));
        }

        @Test
        void shouldHandleErrorIfExceptionEncounteredThatClosesConnection() throws Exception {
            // Given
            Query query = exactQuery("test-query-id", 123L);
            adapter.setFakeResponses(
                    error(new Exception("Exception that will terminate connection")),
                    close("Exception caused connection to terminate"));

            // When / Then
            assertThat(runQueryFuture(query))
                    .isCompletedExceptionally()
                    .failsWithin(Duration.ofMillis(10))
                    .withThrowableOfType(ExecutionException.class)
                    .withCauseInstanceOf(WebSocketErrorException.class);
            assertThat(adapter.isConnected()).isFalse();
            assertThat(adapter.isClosed()).isTrue();
            assertThat(adapter.getSentMessages())
                    .containsExactly(querySerDe.toJson(query));
        }

        @Test
        void shouldHandleErrorIfMessageWithErrorIsReceived() throws Exception {
            // Given
            Query query = exactQuery("test-query-id", 123L);
            adapter.setFakeResponses(
                    message(errorMessage("test-query-id", "Query failed")));

            // When / Then
            assertThat(runQueryFuture(query))
                    .isCompletedExceptionally()
                    .failsWithin(Duration.ofMillis(10))
                    .withThrowableOfType(ExecutionException.class)
                    .withCauseInstanceOf(WebSocketErrorException.class);
            assertThat(adapter.isConnected()).isFalse();
            assertThat(adapter.isClosed()).isTrue();
            assertThat(adapter.getSentMessages())
                    .containsExactly(querySerDe.toJson(query));
        }

        @Test
        void shouldHandleMessageWithUnrecognisedType() throws Exception {
            // Given
            Query query = exactQuery("test-query-id", 123L);
            adapter.setFakeResponses(
                    message(unknownMessage("test-query-id")));

            // When / Then
            assertThat(runQueryFuture(query))
                    .isCompletedExceptionally()
                    .failsWithin(Duration.ofMillis(10))
                    .withThrowableOfType(ExecutionException.class)
                    .withCauseInstanceOf(QueryWebSocketMessageException.class);
            assertThat(adapter.isConnected()).isFalse();
            assertThat(adapter.isClosed()).isTrue();
            assertThat(adapter.getSentMessages())
                    .containsExactly(querySerDe.toJson(query));
        }

        @Test
        void shouldHandleMalformedJson() throws Exception {
            // Given
            Query query = exactQuery("test-query-id", 123L);
            adapter.setFakeResponses(
                    message("{"));

            // When / Then
            assertThat(runQueryFuture(query))
                    .isCompletedExceptionally()
                    .failsWithin(Duration.ofMillis(10))
                    .withThrowableOfType(ExecutionException.class)
                    .withCauseInstanceOf(QueryWebSocketMessageException.class);
            assertThat(adapter.isConnected()).isFalse();
            assertThat(adapter.isClosed()).isTrue();
            assertThat(adapter.getSentMessages())
                    .containsExactly(querySerDe.toJson(query));
        }

        @Test
        void shouldHandleMissingQueryIdInMessageFromApi() throws Exception {
            // Given
            Query query = exactQuery("test-query-id", 123L);
            adapter.setFakeResponses(
                    message("{\"message\":\"error\"}"));

            // When / Then
            assertThat(runQueryFuture(query))
                    .isCompletedExceptionally()
                    .failsWithin(Duration.ofMillis(10))
                    .withThrowableOfType(ExecutionException.class)
                    .withCauseInstanceOf(QueryWebSocketMessageException.class);
            assertThat(adapter.isConnected()).isFalse();
            assertThat(adapter.isClosed()).isTrue();
            assertThat(adapter.getSentMessages())
                    .containsExactly(querySerDe.toJson(query));
        }

        @Test
        void shouldHandleMissingMessageTypeInMessageFromApi() throws Exception {
            // Given
            Query query = exactQuery("test-query-id", 123L);
            adapter.setFakeResponses(
                    message("{\"queryId\":\"test-query-id\"}"));

            // When / Then
            assertThat(runQueryFuture(query))
                    .isCompletedExceptionally()
                    .failsWithin(Duration.ofMillis(10))
                    .withThrowableOfType(ExecutionException.class)
                    .withCauseInstanceOf(QueryWebSocketMessageException.class);
            assertThat(adapter.isConnected()).isFalse();
            assertThat(adapter.isClosed()).isTrue();
            assertThat(adapter.getSentMessages())
                    .containsExactly(querySerDe.toJson(query));
        }

        @Test
        void shouldHandleConnectionClosingUnexpectedly() throws Exception {
            // Given
            Query query = exactQuery("test-query-id", 123L);
            adapter.setFakeResponses(
                    close("Network error"));

            // When / Then
            assertThat(runQueryFuture(query))
                    .isCompletedExceptionally()
                    .failsWithin(Duration.ofMillis(10))
                    .withThrowableOfType(ExecutionException.class)
                    .withCauseInstanceOf(WebSocketClosedException.class);
            assertThat(adapter.isConnected()).isFalse();
            assertThat(adapter.isClosed()).isTrue();
            assertThat(adapter.getSentMessages())
                    .containsExactly(querySerDe.toJson(query));
        }

        @Test
        void shouldTimeOutWithNoReponse() throws Exception {
            // Given
            Query query = exactQuery("test-query-id", 123L);
            adapter.setFakeResponses();

            // When / Then
            assertThat(runQueryFuture(query))
                    .failsWithin(Duration.ofMillis(10))
                    .withThrowableOfType(ExecutionException.class)
                    .withCauseInstanceOf(TimeoutException.class);
            assertThat(adapter.isConnected()).isFalse();
            assertThat(adapter.isClosed()).isTrue();
            assertThat(adapter.getSentMessages())
                    .containsExactly(querySerDe.toJson(query));
        }
    }

    protected CompletableFuture<List<Row>> runQueryFuture(Query query) throws Exception {
        QueryWebSocketClient realClient = new QueryWebSocketClient(
                instanceProperties, tablePropertiesProvider, adapter.provider(), 0);
        return realClient.submitQuery(query);
    }
}
