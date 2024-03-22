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
package sleeper.clients;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import sleeper.clients.FakeWebSocketClient.WebSocketResponse;
import sleeper.clients.QueryWebSocketClient.Client;
import sleeper.clients.testutil.TestConsoleInput;
import sleeper.clients.testutil.ToStringPrintStream;
import sleeper.configuration.properties.instance.InstanceProperties;
import sleeper.configuration.properties.table.FixedTablePropertiesProvider;
import sleeper.configuration.properties.table.TableProperties;
import sleeper.core.range.Range;
import sleeper.core.range.Region;
import sleeper.core.record.Record;
import sleeper.core.schema.Field;
import sleeper.core.schema.Schema;
import sleeper.core.table.InMemoryTableIndex;
import sleeper.core.table.TableIndex;
import sleeper.query.model.Query;
import sleeper.query.model.QuerySerDe;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.clients.QueryClientTestConstants.EXACT_QUERY_OPTION;
import static sleeper.clients.QueryClientTestConstants.EXIT_OPTION;
import static sleeper.clients.QueryClientTestConstants.NO_OPTION;
import static sleeper.clients.QueryClientTestConstants.PROMPT_EXACT_KEY_LONG_TYPE;
import static sleeper.clients.QueryClientTestConstants.PROMPT_MAX_INCLUSIVE;
import static sleeper.clients.QueryClientTestConstants.PROMPT_MAX_ROW_KEY_LONG_TYPE;
import static sleeper.clients.QueryClientTestConstants.PROMPT_MIN_INCLUSIVE;
import static sleeper.clients.QueryClientTestConstants.PROMPT_MIN_ROW_KEY_LONG_TYPE;
import static sleeper.clients.QueryClientTestConstants.PROMPT_QUERY_TYPE;
import static sleeper.clients.QueryClientTestConstants.RANGE_QUERY_OPTION;
import static sleeper.clients.QueryClientTestConstants.YES_OPTION;
import static sleeper.configuration.properties.InstancePropertiesTestHelper.createTestInstanceProperties;
import static sleeper.configuration.properties.instance.CdkDefinedInstanceProperty.QUERY_WEBSOCKET_API_URL;
import static sleeper.configuration.properties.table.TablePropertiesTestHelper.createTestTableProperties;
import static sleeper.configuration.properties.table.TableProperty.TABLE_NAME;
import static sleeper.core.schema.SchemaTestHelper.schemaWithKey;

public class QueryWebSocketClientTest {
    private static final String PROMPT_RANGE_QUERY = PROMPT_MIN_INCLUSIVE + PROMPT_MAX_INCLUSIVE +
            PROMPT_MIN_ROW_KEY_LONG_TYPE + PROMPT_MAX_ROW_KEY_LONG_TYPE;
    private final InstanceProperties instanceProperties = createInstance();
    private final Schema schema = schemaWithKey("key");
    private final Field rowKey = schema.getField("key").orElseThrow();
    private final TableIndex tableIndex = new InMemoryTableIndex();
    private final ToStringPrintStream out = new ToStringPrintStream();
    private final TestConsoleInput in = new TestConsoleInput(out.consoleOut());
    private final QuerySerDe querySerDe = new QuerySerDe(schema);
    private TableProperties tableProperties;
    private FakeWebSocketClient client;

    private static InstanceProperties createInstance() {
        InstanceProperties instanceProperties = createTestInstanceProperties();
        instanceProperties.set(QUERY_WEBSOCKET_API_URL, "websocket-endpoint");
        return instanceProperties;
    }

    private TableProperties createTable(String tableName) {
        TableProperties tableProperties = createTestTableProperties(instanceProperties, schema);
        tableProperties.set(TABLE_NAME, tableName);
        tableIndex.create(tableProperties.getStatus());
        return tableProperties;
    }

    @BeforeEach
    void setup() {
        tableProperties = createTable("test-table");
    }

    @Nested
    @DisplayName("Run queries")
    class RunQuery {

        @Test
        void shouldReturnResultsForQuery() throws Exception {
            // Given
            Query expectedQuery = exactQuery("test-query-id", 123);
            Record expectedRecord = new Record(Map.of("key", 123L));

            // When
            in.enterNextPrompts(EXACT_QUERY_OPTION, "123", EXIT_OPTION);
            runQueryClient("test-query-id",
                    withResponses(
                            message(queryResult("test-query-id", expectedRecord)),
                            message(completedQuery("test-query-id", 1L))));

            // Then
            assertThat(out.toString())
                    .startsWith("Querying table test-table")
                    .contains(PROMPT_QUERY_TYPE +
                            PROMPT_EXACT_KEY_LONG_TYPE +
                            "Connected to WebSocket API\n" +
                            "Submitting Query: " + querySerDe.toJson(expectedQuery) + "\n" +
                            "1 records returned by query: test-query-id. Remaining pending queries: 0\n" +
                            "Query results:\n" +
                            expectedRecord)
                    .containsSubsequence("Query took", "seconds to return 1 records");
            assertThat(client.isConnected()).isFalse();
            assertThat(client.isClosed()).isTrue();
            assertThat(client.getSentMessages())
                    .containsExactly(querySerDe.toJson(expectedQuery));
        }

        @Test
        void shouldReturnResultsForQueryWithOneSubquery() throws Exception {
            // Given
            Query expectedQuery = exactQuery("test-query-id", 123);
            Record expectedRecord = new Record(Map.of("key", 123L));

            // When
            in.enterNextPrompts(EXACT_QUERY_OPTION, "123", EXIT_OPTION);
            runQueryClient("test-query-id",
                    withResponses(
                            message(createdSubQueries("test-query-id", "test-subquery")),
                            message(queryResult("test-subquery", expectedRecord)),
                            message(completedQuery("test-subquery", 1L))));

            // Then
            assertThat(out.toString())
                    .startsWith("Querying table test-table")
                    .contains(PROMPT_QUERY_TYPE +
                            PROMPT_EXACT_KEY_LONG_TYPE +
                            "Connected to WebSocket API\n" +
                            "Submitting Query: " + querySerDe.toJson(expectedQuery) + "\n" +
                            "Query test-query-id split into the following subQueries:\n" +
                            "  test-subquery\n" +
                            "1 records returned by query: test-subquery. Remaining pending queries: 0\n" +
                            "Query results:\n" +
                            expectedRecord)
                    .containsSubsequence("Query took", "seconds to return 1 records");
            assertThat(client.isConnected()).isFalse();
            assertThat(client.isClosed()).isTrue();
            assertThat(client.getSentMessages())
                    .containsExactly(querySerDe.toJson(expectedQuery));
        }

        @Test
        void shouldReturnResultsForQueryWithMultipleSubqueries() throws Exception {
            // Given
            Query expectedQuery = rangeQuery("test-query-id", 0L, 1000L);
            Record expectedRecord1 = new Record(Map.of("key", 123L));
            Record expectedRecord2 = new Record(Map.of("key", 456L));
            Record expectedRecord3 = new Record(Map.of("key", 789L));

            // When
            in.enterNextPrompts(RANGE_QUERY_OPTION, YES_OPTION, NO_OPTION, "0", "1000", EXIT_OPTION);
            runQueryClient("test-query-id",
                    withResponses(
                            message(createdSubQueries("test-query-id", "subquery-1", "subquery-2", "subquery-3")),
                            message(queryResult("subquery-1", expectedRecord1)),
                            message(completedQuery("subquery-1", 1L)),
                            message(queryResult("subquery-2", expectedRecord2)),
                            message(completedQuery("subquery-2", 1L)),
                            message(queryResult("subquery-3", expectedRecord3)),
                            message(completedQuery("subquery-3", 1L))));

            // Then
            assertThat(out.toString())
                    .startsWith("Querying table test-table")
                    .contains(PROMPT_QUERY_TYPE +
                            PROMPT_RANGE_QUERY +
                            "Connected to WebSocket API\n" +
                            "Submitting Query: " + querySerDe.toJson(expectedQuery) + "\n" +
                            "Query test-query-id split into the following subQueries:\n" +
                            "  subquery-1\n" +
                            "  subquery-2\n" +
                            "  subquery-3\n" +
                            "1 records returned by query: subquery-1. Remaining pending queries: 2\n" +
                            "1 records returned by query: subquery-2. Remaining pending queries: 1\n" +
                            "1 records returned by query: subquery-3. Remaining pending queries: 0\n" +
                            "Query results:\n" +
                            expectedRecord1 + "\n" +
                            expectedRecord2 + "\n" +
                            expectedRecord3)
                    .containsSubsequence("Query took", "seconds to return 3 records");
            assertThat(client.isConnected()).isFalse();
            assertThat(client.isClosed()).isTrue();
            assertThat(client.getSentMessages())
                    .containsExactly(querySerDe.toJson(expectedQuery));
        }

        @Test
        void shouldReturnResultsWhenRecordCountDoesNotMatchRecordsReceived() throws Exception {
            // Given
            Query expectedQuery = exactQuery("test-query-id", 123);
            Record expectedRecord = new Record(Map.of("key", 123L));

            // When
            in.enterNextPrompts(EXACT_QUERY_OPTION, "123", EXIT_OPTION);
            runQueryClient("test-query-id",
                    withResponses(
                            message(queryResult("test-query-id", expectedRecord)),
                            message(completedQuery("test-query-id", 2L))));

            // Then
            assertThat(out.toString())
                    .startsWith("Querying table test-table")
                    .contains(PROMPT_QUERY_TYPE +
                            PROMPT_EXACT_KEY_LONG_TYPE +
                            "Connected to WebSocket API\n" +
                            "Submitting Query: " + querySerDe.toJson(expectedQuery) + "\n" +
                            "ERROR: API said it had returned 2 records for query test-query-id, but only received 1\n" +
                            "2 records returned by query: test-query-id. Remaining pending queries: 0\n" +
                            "Query results:\n" +
                            expectedRecord)
                    .containsSubsequence("Query took", "seconds to return 1 records");
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
        void shouldHandleErrorIfExceptionEncountered() throws Exception {
            // Given
            Query expectedQuery = exactQuery("test-query-id", 123L);

            // When
            in.enterNextPrompts(EXACT_QUERY_OPTION, "123", EXIT_OPTION);
            runQueryClient("test-query-id",
                    withResponses(
                            error(new Exception("Connection failed"))));

            // Then
            assertThat(out.toString())
                    .startsWith("Querying table test-table")
                    .contains(PROMPT_QUERY_TYPE +
                            PROMPT_EXACT_KEY_LONG_TYPE +
                            "Connected to WebSocket API\n" +
                            "Submitting Query: " + querySerDe.toJson(expectedQuery) + "\n" +
                            "Encountered an error: Connection failed\n")
                    .containsSubsequence("Query took", "seconds to return 0 records");
            assertThat(client.isConnected()).isFalse();
            assertThat(client.isClosed()).isTrue();
            assertThat(client.getSentMessages())
                    .containsExactly(querySerDe.toJson(expectedQuery));
        }

        @Test
        void shouldHandleErrorIfMessageWithErrorIsReceived() throws Exception {
            // Given
            Query expectedQuery = exactQuery("test-query-id", 123L);

            // When
            in.enterNextPrompts(EXACT_QUERY_OPTION, "123", EXIT_OPTION);
            runQueryClient("test-query-id",
                    withResponses(
                            message(errorMessage("test-query-id", "Query failed"))));

            // Then
            assertThat(out.toString())
                    .startsWith("Querying table test-table")
                    .contains(PROMPT_QUERY_TYPE +
                            PROMPT_EXACT_KEY_LONG_TYPE +
                            "Connected to WebSocket API\n" +
                            "Submitting Query: " + querySerDe.toJson(expectedQuery) + "\n" +
                            "Encountered an error while running query test-query-id: Query failed")
                    .containsSubsequence("Query took", "seconds to return 0 records");
            assertThat(client.isConnected()).isFalse();
            assertThat(client.isClosed()).isTrue();
            assertThat(client.getSentMessages())
                    .containsExactly(querySerDe.toJson(expectedQuery));
        }

        @Test
        void shouldHandleMessageWithUnrecognisedType() throws Exception {
            // Given
            Query expectedQuery = exactQuery("test-query-id", 123L);

            // When
            in.enterNextPrompts(EXACT_QUERY_OPTION, "123", EXIT_OPTION);
            runQueryClient("test-query-id",
                    withResponses(
                            message(unknownMessage("test-query-id"))));

            // Then
            assertThat(out.toString())
                    .startsWith("Querying table test-table")
                    .contains(PROMPT_QUERY_TYPE +
                            PROMPT_EXACT_KEY_LONG_TYPE +
                            "Connected to WebSocket API\n" +
                            "Submitting Query: " + querySerDe.toJson(expectedQuery) + "\n" +
                            "Received unrecognised message type: unknown")
                    .containsSubsequence("Query took", "seconds to return 0 records");
            assertThat(client.isConnected()).isFalse();
            assertThat(client.isClosed()).isTrue();
            assertThat(client.getSentMessages())
                    .containsExactly(querySerDe.toJson(expectedQuery));
        }

        @Test
        void shouldHandleMalformedJson() throws Exception {
            // Given
            Query expectedQuery = exactQuery("test-query-id", 123L);

            // When
            in.enterNextPrompts(EXACT_QUERY_OPTION, "123", EXIT_OPTION);
            runQueryClient("test-query-id",
                    withResponses(
                            message("{")));

            // Then
            assertThat(out.toString())
                    .startsWith("Querying table test-table")
                    .contains(PROMPT_QUERY_TYPE +
                            PROMPT_EXACT_KEY_LONG_TYPE +
                            "Connected to WebSocket API\n" +
                            "Submitting Query: " + querySerDe.toJson(expectedQuery) + "\n" +
                            "Received malformed JSON message from API:\n" +
                            "  {")
                    .containsSubsequence("Query took", "seconds to return 0 records");
            assertThat(client.isConnected()).isFalse();
            assertThat(client.isClosed()).isTrue();
            assertThat(client.getSentMessages())
                    .containsExactly(querySerDe.toJson(expectedQuery));
        }

        @Test
        void shouldHandleMissingQueryIdInMessageFromApi() throws Exception {
            // Given
            Query expectedQuery = exactQuery("test-query-id", 123L);

            // When
            in.enterNextPrompts(EXACT_QUERY_OPTION, "123", EXIT_OPTION);
            runQueryClient("test-query-id",
                    withResponses(
                            message("{\"message\":\"error\"}")));

            // Then
            assertThat(out.toString())
                    .startsWith("Querying table test-table")
                    .contains(PROMPT_QUERY_TYPE +
                            PROMPT_EXACT_KEY_LONG_TYPE +
                            "Connected to WebSocket API\n" +
                            "Submitting Query: " + querySerDe.toJson(expectedQuery) + "\n" +
                            "Received message without queryId from API:\n" +
                            "  {\"message\":\"error\"}")
                    .containsSubsequence("Query took", "seconds to return 0 records");
            assertThat(client.isConnected()).isFalse();
            assertThat(client.isClosed()).isTrue();
            assertThat(client.getSentMessages())
                    .containsExactly(querySerDe.toJson(expectedQuery));
        }

        @Test
        void shouldHandleMissingMessageTypeInMessageFromApi() throws Exception {
            // Given
            Query expectedQuery = exactQuery("test-query-id", 123L);

            // When
            in.enterNextPrompts(EXACT_QUERY_OPTION, "123", EXIT_OPTION);
            runQueryClient("test-query-id",
                    withResponses(
                            message("{\"queryId\":\"test-query-id\"}")));

            // Then
            assertThat(out.toString())
                    .startsWith("Querying table test-table")
                    .contains(PROMPT_QUERY_TYPE +
                            PROMPT_EXACT_KEY_LONG_TYPE +
                            "Connected to WebSocket API\n" +
                            "Submitting Query: " + querySerDe.toJson(expectedQuery) + "\n" +
                            "Received message without message type from API:\n" +
                            "  {\"queryId\":\"test-query-id\"}")
                    .containsSubsequence("Query took", "seconds to return 0 records");
            assertThat(client.isConnected()).isFalse();
            assertThat(client.isClosed()).isTrue();
            assertThat(client.getSentMessages())
                    .containsExactly(querySerDe.toJson(expectedQuery));
        }

        @Test
        void shouldHandleConnectionClosingUnexpectedly() throws Exception {
            // Given
            Query expectedQuery = exactQuery("test-query-id", 123L);

            // When
            in.enterNextPrompts(EXACT_QUERY_OPTION, "123", EXIT_OPTION);
            runQueryClient("test-query-id",
                    withResponses(
                            closeWithReason("Network error")));

            // Then
            assertThat(out.toString())
                    .startsWith("Querying table test-table")
                    .contains(PROMPT_QUERY_TYPE +
                            PROMPT_EXACT_KEY_LONG_TYPE +
                            "Connected to WebSocket API\n" +
                            "Submitting Query: " + querySerDe.toJson(expectedQuery) + "\n" +
                            "Disconnected from WebSocket API: Network error")
                    .containsSubsequence("Query took", "seconds to return 0 records");
            assertThat(client.isConnected()).isFalse();
            assertThat(client.isClosed()).isTrue();
            assertThat(client.getSentMessages())
                    .containsExactly(querySerDe.toJson(expectedQuery));
        }
    }

    private Query exactQuery(String queryId, long value) {
        return Query.builder()
                .tableName(tableProperties.get(TABLE_NAME))
                .queryId(queryId)
                .regions(List.of(new Region(new Range(rowKey, value, true, value, true))))
                .build();
    }

    private Query rangeQuery(String queryId, long min, long max) {
        return Query.builder()
                .tableName(tableProperties.get(TABLE_NAME))
                .queryId(queryId)
                .regions(List.of(new Region(new Range(rowKey, min, true, max, false))))
                .build();
    }

    private static String queryResult(String queryId, Record... records) {
        return "{" +
                "\"queryId\":\"" + queryId + "\", " +
                "\"message\":\"records\"," +
                "\"records\":[" + Stream.of(records).map(record -> "\"" + record + "\"").collect(Collectors.joining(",")) + "]" +
                "}";
    }

    private static String completedQuery(String queryId, long recordCount) {
        return "{" +
                "\"queryId\":\"" + queryId + "\", " +
                "\"message\":\"completed\"," +
                "\"recordCount\":\"" + recordCount + "\"," +
                "\"locations\":[{\"type\":\"websocket-endpoint\"}]" +
                "}";
    }

    private static String createdSubQueries(String queryId, String... subQueryIds) {
        return "{" +
                "\"queryId\":\"" + queryId + "\", " +
                "\"message\":\"subqueries\"," +
                "\"queryIds\":[" + Stream.of(subQueryIds).map(id -> "\"" + id + "\"").collect(Collectors.joining(",")) + "]" +
                "}";
    }

    private static String errorMessage(String queryId, String message) {
        return "{" +
                "\"queryId\":\"" + queryId + "\", " +
                "\"message\":\"error\"," +
                "\"error\":\"" + message + "\"" +
                "}";
    }

    private static String unknownMessage(String queryId) {
        return "{" +
                "\"queryId\":\"" + queryId + "\"," +
                "\"message\":\"unknown\"" +
                "}";
    }

    protected void runQueryClient(String queryId, Client webSocketClient) throws Exception {
        new QueryWebSocketClient(instanceProperties, tableIndex, new FixedTablePropertiesProvider(tableProperties),
                in.consoleIn(), out.consoleOut(), webSocketClient, () -> queryId)
                .run();
    }

    private FakeWebSocketClient withResponses(WebSocketResponse... responses) {
        client = new FakeWebSocketClient(new FixedTablePropertiesProvider(tableProperties), out.consoleOut());
        client.withResponses(responses);
        return client;
    }

    private WebSocketResponse message(String message) {
        return messageHandler -> messageHandler.onMessage(message);
    }

    public WebSocketResponse closeWithReason(String reason) {
        return messageHandler -> messageHandler.onClose(reason);
    }

    public WebSocketResponse error(Exception error) {
        return messageHandler -> messageHandler.onError(error);
    }
}
