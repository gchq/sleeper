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

import sleeper.clients.testutil.TestConsoleInput;
import sleeper.clients.testutil.ToStringConsoleOutput;
import sleeper.core.row.Row;
import sleeper.query.core.model.Query;

import java.time.Instant;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.clients.query.QueryClientTestConstants.EXACT_QUERY_OPTION;
import static sleeper.clients.query.QueryClientTestConstants.EXIT_OPTION;
import static sleeper.clients.query.QueryClientTestConstants.NO_OPTION;
import static sleeper.clients.query.QueryClientTestConstants.PROMPT_EXACT_KEY_LONG_TYPE;
import static sleeper.clients.query.QueryClientTestConstants.PROMPT_MAX_INCLUSIVE;
import static sleeper.clients.query.QueryClientTestConstants.PROMPT_MAX_ROW_KEY_LONG_TYPE;
import static sleeper.clients.query.QueryClientTestConstants.PROMPT_MIN_INCLUSIVE;
import static sleeper.clients.query.QueryClientTestConstants.PROMPT_MIN_ROW_KEY_LONG_TYPE;
import static sleeper.clients.query.QueryClientTestConstants.PROMPT_QUERY_TYPE;
import static sleeper.clients.query.QueryClientTestConstants.RANGE_QUERY_OPTION;
import static sleeper.clients.query.QueryClientTestConstants.YES_OPTION;

public class QueryWebSocketCommandLineClientTest extends QueryWebSocketClientTestBase {
    private static final String PROMPT_RANGE_QUERY = PROMPT_MIN_INCLUSIVE + PROMPT_MAX_INCLUSIVE +
            PROMPT_MIN_ROW_KEY_LONG_TYPE + PROMPT_MAX_ROW_KEY_LONG_TYPE;
    private static final Instant START_TIME = Instant.parse("2024-04-03T14:00:00Z");
    private static final Instant FINISH_TIME = Instant.parse("2024-04-03T14:00:01Z");
    private final ToStringConsoleOutput out = new ToStringConsoleOutput();
    private final TestConsoleInput in = new TestConsoleInput(out.consoleOut());

    @Nested
    @DisplayName("Run queries")
    class RunQuery {

        @Test
        void shouldReturnResultsForQuery() throws Exception {
            // Given
            Query expectedQuery = exactQuery("test-query-id", 123);
            Row expectedRow = new Row(Map.of("key", 123L));
            client.setFakeResponses(
                    message(queryResult("test-query-id", expectedRow)),
                    message(completedQuery("test-query-id", 1L)));

            // When
            in.enterNextPrompts(EXACT_QUERY_OPTION, "123", EXIT_OPTION);
            runQueryClient("test-query-id");

            // Then
            assertThat(out.toString())
                    .isEqualTo("Querying table test-table\n" +
                            "The table has the schema " + tableProperties.getSchema().toString() + "\n" +
                            PROMPT_QUERY_TYPE +
                            PROMPT_EXACT_KEY_LONG_TYPE +
                            "Submitting query with ID: test-query-id\n" +
                            "Query results:\n" +
                            asJson(expectedRow) + "\n" +
                            "Query took 1 second to return 1 records\n" +
                            PROMPT_QUERY_TYPE);
            assertThat(client.isConnected()).isFalse();
            assertThat(client.isClosed()).isTrue();
            assertThat(client.getSentMessages())
                    .containsExactly(querySerDe.toJson(expectedQuery));
        }

        @Test
        void shouldReturnResultsForQueryWithOneSubquery() throws Exception {
            // Given
            Query expectedQuery = exactQuery("test-query-id", 123);
            Row expectedRow = new Row(Map.of("key", 123L));
            client.setFakeResponses(
                    message(createdSubQueries("test-query-id", "test-subquery")),
                    message(queryResult("test-subquery", expectedRow)),
                    message(completedQuery("test-subquery", 1L)));

            // When
            in.enterNextPrompts(EXACT_QUERY_OPTION, "123", EXIT_OPTION);
            runQueryClient("test-query-id");

            // Then
            assertThat(out.toString())
                    .isEqualTo("Querying table test-table\n" +
                            "The table has the schema " + tableProperties.getSchema().toString() + "\n" +
                            PROMPT_QUERY_TYPE +
                            PROMPT_EXACT_KEY_LONG_TYPE +
                            "Submitting query with ID: test-query-id\n" +
                            "Query results:\n" +
                            asJson(expectedRow) + "\n" +
                            "Query took 1 second to return 1 records\n" +
                            PROMPT_QUERY_TYPE);
            assertThat(client.isConnected()).isFalse();
            assertThat(client.isClosed()).isTrue();
            assertThat(client.getSentMessages())
                    .containsExactly(querySerDe.toJson(expectedQuery));
        }

        @Test
        void shouldReturnResultsForQueryWithMultipleSubqueries() throws Exception {
            // Given
            Query expectedQuery = rangeQuery("test-query-id", 0L, 1000L);
            Row expectedRow1 = new Row(Map.of("key", 123L));
            Row expectedRow2 = new Row(Map.of("key", 456L));
            Row expectedRow3 = new Row(Map.of("key", 789L));
            client.setFakeResponses(
                    message(createdSubQueries("test-query-id", "subquery-1", "subquery-2", "subquery-3")),
                    message(queryResult("subquery-1", expectedRow1)),
                    message(completedQuery("subquery-1", 1L)),
                    message(queryResult("subquery-2", expectedRow2)),
                    message(completedQuery("subquery-2", 1L)),
                    message(queryResult("subquery-3", expectedRow3)),
                    message(completedQuery("subquery-3", 1L)));

            // When
            in.enterNextPrompts(RANGE_QUERY_OPTION, YES_OPTION, NO_OPTION, "0", "1000", EXIT_OPTION);
            runQueryClient("test-query-id");

            // Then
            assertThat(out.toString())
                    .isEqualTo("Querying table test-table\n" +
                            "The table has the schema " + tableProperties.getSchema().toString() + "\n" +
                            PROMPT_QUERY_TYPE +
                            PROMPT_RANGE_QUERY +
                            "Submitting query with ID: test-query-id\n" +
                            "Query results:\n" +
                            asJson(expectedRow1) + "\n" +
                            asJson(expectedRow2) + "\n" +
                            asJson(expectedRow3) + "\n" +
                            "Query took 1 second to return 3 records\n" +
                            PROMPT_QUERY_TYPE);
            assertThat(client.isConnected()).isFalse();
            assertThat(client.isClosed()).isTrue();
            assertThat(client.getSentMessages())
                    .containsExactly(querySerDe.toJson(expectedQuery));
        }

        @Test
        void shouldReturnResultsWhenRecordCountDoesNotMatchRowsReceived() throws Exception {
            // Given
            Query expectedQuery = exactQuery("test-query-id", 123);
            Row expectedRow = new Row(Map.of("key", 123L));
            client.setFakeResponses(
                    message(queryResult("test-query-id", expectedRow)),
                    message(completedQuery("test-query-id", 2L)));

            // When
            in.enterNextPrompts(EXACT_QUERY_OPTION, "123", EXIT_OPTION);
            runQueryClient("test-query-id");

            // Then
            assertThat(out.toString())
                    .isEqualTo("Querying table test-table\n" +
                            "The table has the schema " + tableProperties.getSchema().toString() + "\n" +
                            PROMPT_QUERY_TYPE +
                            PROMPT_EXACT_KEY_LONG_TYPE +
                            "Submitting query with ID: test-query-id\n" +
                            "Query results:\n" +
                            asJson(expectedRow) + "\n" +
                            "Query took 1 second to return 1 records\n" +
                            PROMPT_QUERY_TYPE);
            assertThat(client.isConnected()).isFalse();
            assertThat(client.isClosed()).isTrue();
            assertThat(client.getSentMessages())
                    .containsExactly(querySerDe.toJson(expectedQuery));
        }
    }

    @Nested
    @DisplayName("Handle errors")
    class HandleErrors {

        @Test
        void shouldHandleErrorIfExceptionEncounteredThatDoesNotCloseConnection() throws Exception {
            // Given
            Query expectedQuery = exactQuery("test-query-id", 123L);
            client.setFakeResponses(
                    error(new Exception("Exception that will not terminate connection")));

            // When
            in.enterNextPrompts(EXACT_QUERY_OPTION, "123", EXIT_OPTION);
            runQueryClient("test-query-id");

            // Then
            assertThat(out.toString())
                    .isEqualTo("Querying table test-table\n" +
                            "The table has the schema " + tableProperties.getSchema().toString() + "\n" +
                            PROMPT_QUERY_TYPE +
                            PROMPT_EXACT_KEY_LONG_TYPE +
                            "Submitting query with ID: test-query-id\n" +
                            "Query failed: Error while running queries\n" +
                            "Query took 1 second to return 0 records\n" +
                            PROMPT_QUERY_TYPE);
            assertThat(client.isConnected()).isFalse();
            assertThat(client.isClosed()).isTrue();
            assertThat(client.getSentMessages())
                    .containsExactly(querySerDe.toJson(expectedQuery));
        }

        @Test
        void shouldHandleErrorIfExceptionEncounteredThatClosesConnection() throws Exception {
            // Given
            Query expectedQuery = exactQuery("test-query-id", 123L);
            client.setFakeResponses(
                    error(new Exception("Exception that will terminate connection")),
                    close("Exception caused connection to terminate"));

            // When
            in.enterNextPrompts(EXACT_QUERY_OPTION, "123", EXIT_OPTION);
            runQueryClient("test-query-id");

            // Then
            assertThat(out.toString())
                    .isEqualTo("Querying table test-table\n" +
                            "The table has the schema " + tableProperties.getSchema().toString() + "\n" +
                            PROMPT_QUERY_TYPE +
                            PROMPT_EXACT_KEY_LONG_TYPE +
                            "Submitting query with ID: test-query-id\n" +
                            "Query failed: Error while running queries\n" +
                            "Query took 1 second to return 0 records\n" +
                            PROMPT_QUERY_TYPE);
            assertThat(client.isConnected()).isFalse();
            assertThat(client.isClosed()).isTrue();
            assertThat(client.getSentMessages())
                    .containsExactly(querySerDe.toJson(expectedQuery));
        }

        @Test
        void shouldHandleErrorIfMessageWithErrorIsReceived() throws Exception {
            // Given
            Query expectedQuery = exactQuery("test-query-id", 123L);
            client.setFakeResponses(
                    message(errorMessage("test-query-id", "Failure message")));

            // When
            in.enterNextPrompts(EXACT_QUERY_OPTION, "123", EXIT_OPTION);
            runQueryClient("test-query-id");

            // Then
            assertThat(out.toString())
                    .isEqualTo("Querying table test-table\n" +
                            "The table has the schema " + tableProperties.getSchema().toString() + "\n" +
                            PROMPT_QUERY_TYPE +
                            PROMPT_EXACT_KEY_LONG_TYPE +
                            "Submitting query with ID: test-query-id\n" +
                            "Query failed: Error while running queries: Failure message\n" +
                            "Query took 1 second to return 0 records\n" +
                            PROMPT_QUERY_TYPE);
            assertThat(client.isConnected()).isFalse();
            assertThat(client.isClosed()).isTrue();
            assertThat(client.getSentMessages())
                    .containsExactly(querySerDe.toJson(expectedQuery));
        }

        @Test
        void shouldHandleMessageWithUnrecognisedType() throws Exception {
            // Given
            Query expectedQuery = exactQuery("test-query-id", 123L);
            client.setFakeResponses(
                    message(unknownMessage("test-query-id")));

            // When
            in.enterNextPrompts(EXACT_QUERY_OPTION, "123", EXIT_OPTION);
            runQueryClient("test-query-id");

            // Then
            assertThat(out.toString())
                    .isEqualTo("Querying table test-table\n" +
                            "The table has the schema " + tableProperties.getSchema().toString() + "\n" +
                            PROMPT_QUERY_TYPE +
                            PROMPT_EXACT_KEY_LONG_TYPE +
                            "Submitting query with ID: test-query-id\n" +
                            "Query failed: Found invalid message: {\"queryId\": \"test-query-id\",\"message\": \"unknown\"}\n" +
                            "Query took 1 second to return 0 records\n" +
                            PROMPT_QUERY_TYPE);
            assertThat(client.isConnected()).isFalse();
            assertThat(client.isClosed()).isTrue();
            assertThat(client.getSentMessages())
                    .containsExactly(querySerDe.toJson(expectedQuery));
        }

        @Test
        void shouldHandleMalformedJson() throws Exception {
            // Given
            Query expectedQuery = exactQuery("test-query-id", 123L);
            client.setFakeResponses(
                    message("{"));

            // When
            in.enterNextPrompts(EXACT_QUERY_OPTION, "123", EXIT_OPTION);
            runQueryClient("test-query-id");

            // Then
            assertThat(out.toString())
                    .isEqualTo("Querying table test-table\n" +
                            "The table has the schema " + tableProperties.getSchema().toString() + "\n" +
                            PROMPT_QUERY_TYPE +
                            PROMPT_EXACT_KEY_LONG_TYPE +
                            "Submitting query with ID: test-query-id\n" +
                            "Query failed: Found invalid message: {\n" +
                            "Query took 1 second to return 0 records\n" +
                            PROMPT_QUERY_TYPE);
            assertThat(client.isConnected()).isFalse();
            assertThat(client.isClosed()).isTrue();
            assertThat(client.getSentMessages())
                    .containsExactly(querySerDe.toJson(expectedQuery));
        }

        @Test
        void shouldHandleMissingQueryIdInMessageFromApi() throws Exception {
            // Given
            Query expectedQuery = exactQuery("test-query-id", 123L);
            client.setFakeResponses(
                    message("{\"message\":\"error\"}"));

            // When
            in.enterNextPrompts(EXACT_QUERY_OPTION, "123", EXIT_OPTION);
            runQueryClient("test-query-id");

            // Then
            assertThat(out.toString())
                    .isEqualTo("Querying table test-table\n" +
                            "The table has the schema " + tableProperties.getSchema().toString() + "\n" +
                            PROMPT_QUERY_TYPE +
                            PROMPT_EXACT_KEY_LONG_TYPE +
                            "Submitting query with ID: test-query-id\n" +
                            "Query failed: Found invalid message: {\"message\":\"error\"}\n" +
                            "Query took 1 second to return 0 records\n" +
                            PROMPT_QUERY_TYPE);
            assertThat(client.isConnected()).isFalse();
            assertThat(client.isClosed()).isTrue();
            assertThat(client.getSentMessages())
                    .containsExactly(querySerDe.toJson(expectedQuery));
        }

        @Test
        void shouldHandleMissingMessageTypeInMessageFromApi() throws Exception {
            // Given
            Query expectedQuery = exactQuery("test-query-id", 123L);
            client.setFakeResponses(
                    message("{\"queryId\":\"test-query-id\"}"));

            // When
            in.enterNextPrompts(EXACT_QUERY_OPTION, "123", EXIT_OPTION);
            runQueryClient("test-query-id");

            // Then
            assertThat(out.toString())
                    .isEqualTo("Querying table test-table\n" +
                            "The table has the schema " + tableProperties.getSchema().toString() + "\n" +
                            PROMPT_QUERY_TYPE +
                            PROMPT_EXACT_KEY_LONG_TYPE +
                            "Submitting query with ID: test-query-id\n" +
                            "Query failed: Found invalid message: {\"queryId\":\"test-query-id\"}\n" +
                            "Query took 1 second to return 0 records\n" +
                            PROMPT_QUERY_TYPE);
            assertThat(client.isConnected()).isFalse();
            assertThat(client.isClosed()).isTrue();
            assertThat(client.getSentMessages())
                    .containsExactly(querySerDe.toJson(expectedQuery));
        }

        @Test
        void shouldHandleConnectionClosingUnexpectedly() throws Exception {
            // Given
            Query expectedQuery = exactQuery("test-query-id", 123L);
            client.setFakeResponses(
                    close("Network error"));

            // When
            in.enterNextPrompts(EXACT_QUERY_OPTION, "123", EXIT_OPTION);
            runQueryClient("test-query-id");

            // Then
            assertThat(out.toString())
                    .isEqualTo("Querying table test-table\n" +
                            "The table has the schema " + tableProperties.getSchema().toString() + "\n" +
                            PROMPT_QUERY_TYPE +
                            PROMPT_EXACT_KEY_LONG_TYPE +
                            "Submitting query with ID: test-query-id\n" +
                            "Query failed: WebSocket closed unexpectedly with reason: Network error\n" +
                            "Query took 1 second to return 0 records\n" +
                            PROMPT_QUERY_TYPE);
            assertThat(client.isConnected()).isFalse();
            assertThat(client.isClosed()).isTrue();
            assertThat(client.getSentMessages())
                    .containsExactly(querySerDe.toJson(expectedQuery));
        }
    }

    protected void runQueryClient(String queryId) throws Exception {
        new QueryWebSocketCommandLineClient(instanceProperties, tableIndex, tablePropertiesProvider,
                in.consoleIn(), out.consoleOut(),
                new QueryWebSocketClient(instanceProperties, tablePropertiesProvider, client.provider(), 0),
                () -> queryId, List.of(START_TIME, FINISH_TIME).iterator()::next)
                .run();
    }
}
