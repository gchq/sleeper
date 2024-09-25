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
import sleeper.clients.testutil.ToStringConsoleOutput;
import sleeper.configuration.properties.table.FixedTablePropertiesProvider;
import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.table.TableProperties;
import sleeper.core.range.Range;
import sleeper.core.range.Region;
import sleeper.core.record.Record;
import sleeper.core.schema.Field;
import sleeper.core.schema.Schema;
import sleeper.core.table.InMemoryTableIndex;
import sleeper.core.table.TableIndex;
import sleeper.query.model.Query;
import sleeper.query.model.QuerySerDe;

import java.time.Instant;
import java.util.List;
import java.util.Map;

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
import static sleeper.clients.QueryWebSocketClientTestHelper.asJson;
import static sleeper.clients.QueryWebSocketClientTestHelper.close;
import static sleeper.clients.QueryWebSocketClientTestHelper.completedQuery;
import static sleeper.clients.QueryWebSocketClientTestHelper.createdSubQueries;
import static sleeper.clients.QueryWebSocketClientTestHelper.error;
import static sleeper.clients.QueryWebSocketClientTestHelper.errorMessage;
import static sleeper.clients.QueryWebSocketClientTestHelper.message;
import static sleeper.clients.QueryWebSocketClientTestHelper.queryResult;
import static sleeper.clients.QueryWebSocketClientTestHelper.unknownMessage;
import static sleeper.configuration.properties.InstancePropertiesTestHelper.createTestInstanceProperties;
import static sleeper.configuration.properties.table.TablePropertiesTestHelper.createTestTableProperties;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.QUERY_WEBSOCKET_API_URL;
import static sleeper.core.properties.table.TableProperty.TABLE_NAME;
import static sleeper.core.schema.SchemaTestHelper.schemaWithKey;

public class QueryWebSocketCommandLineClientTest {
    private static final String PROMPT_RANGE_QUERY = PROMPT_MIN_INCLUSIVE + PROMPT_MAX_INCLUSIVE +
            PROMPT_MIN_ROW_KEY_LONG_TYPE + PROMPT_MAX_ROW_KEY_LONG_TYPE;
    private static final Instant START_TIME = Instant.parse("2024-04-03T14:00:00Z");
    private static final Instant FINISH_TIME = Instant.parse("2024-04-03T14:00:01Z");
    private final InstanceProperties instanceProperties = createInstance();
    private final Schema schema = schemaWithKey("key");
    private final Field rowKey = schema.getField("key").orElseThrow();
    private final TableIndex tableIndex = new InMemoryTableIndex();
    private final ToStringConsoleOutput out = new ToStringConsoleOutput();
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
                    .isEqualTo("Querying table test-table\n" +
                            "The table has the schema " + tableProperties.getSchema().toString() + "\n" +
                            PROMPT_QUERY_TYPE +
                            PROMPT_EXACT_KEY_LONG_TYPE +
                            "Submitting query with ID: test-query-id\n" +
                            "Query results:\n" +
                            asJson(expectedRecord) + "\n" +
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
                    .isEqualTo("Querying table test-table\n" +
                            "The table has the schema " + tableProperties.getSchema().toString() + "\n" +
                            PROMPT_QUERY_TYPE +
                            PROMPT_EXACT_KEY_LONG_TYPE +
                            "Submitting query with ID: test-query-id\n" +
                            "Query results:\n" +
                            asJson(expectedRecord) + "\n" +
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
                    .isEqualTo("Querying table test-table\n" +
                            "The table has the schema " + tableProperties.getSchema().toString() + "\n" +
                            PROMPT_QUERY_TYPE +
                            PROMPT_RANGE_QUERY +
                            "Submitting query with ID: test-query-id\n" +
                            "Query results:\n" +
                            asJson(expectedRecord1) + "\n" +
                            asJson(expectedRecord2) + "\n" +
                            asJson(expectedRecord3) + "\n" +
                            "Query took 1 second to return 3 records\n" +
                            PROMPT_QUERY_TYPE);
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
                    .isEqualTo("Querying table test-table\n" +
                            "The table has the schema " + tableProperties.getSchema().toString() + "\n" +
                            PROMPT_QUERY_TYPE +
                            PROMPT_EXACT_KEY_LONG_TYPE +
                            "Submitting query with ID: test-query-id\n" +
                            "Query results:\n" +
                            asJson(expectedRecord) + "\n" +
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

            // When
            in.enterNextPrompts(EXACT_QUERY_OPTION, "123", EXIT_OPTION);
            runQueryClient("test-query-id",
                    withResponses(
                            error(new Exception("Exception that will not terminate connection"))));

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

            // When
            in.enterNextPrompts(EXACT_QUERY_OPTION, "123", EXIT_OPTION);
            runQueryClient("test-query-id",
                    withResponses(
                            error(new Exception("Exception that will terminate connection")),
                            close("Exception caused connection to terminate")));

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

            // When
            in.enterNextPrompts(EXACT_QUERY_OPTION, "123", EXIT_OPTION);
            runQueryClient("test-query-id",
                    withResponses(
                            message(errorMessage("test-query-id", "Failure message"))));

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

            // When
            in.enterNextPrompts(EXACT_QUERY_OPTION, "123", EXIT_OPTION);
            runQueryClient("test-query-id",
                    withResponses(
                            message(unknownMessage("test-query-id"))));

            // Then
            assertThat(out.toString())
                    .isEqualTo("Querying table test-table\n" +
                            "The table has the schema " + tableProperties.getSchema().toString() + "\n" +
                            PROMPT_QUERY_TYPE +
                            PROMPT_EXACT_KEY_LONG_TYPE +
                            "Submitting query with ID: test-query-id\n" +
                            "Query failed: Unknown message type received: unknown\n" +
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

            // When
            in.enterNextPrompts(EXACT_QUERY_OPTION, "123", EXIT_OPTION);
            runQueryClient("test-query-id",
                    withResponses(
                            message("{")));

            // Then
            assertThat(out.toString())
                    .isEqualTo("Querying table test-table\n" +
                            "The table has the schema " + tableProperties.getSchema().toString() + "\n" +
                            PROMPT_QUERY_TYPE +
                            PROMPT_EXACT_KEY_LONG_TYPE +
                            "Submitting query with ID: test-query-id\n" +
                            "Query failed: Received malformed message JSON: {\n" +
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

            // When
            in.enterNextPrompts(EXACT_QUERY_OPTION, "123", EXIT_OPTION);
            runQueryClient("test-query-id",
                    withResponses(
                            message("{\"message\":\"error\"}")));

            // Then
            assertThat(out.toString())
                    .isEqualTo("Querying table test-table\n" +
                            "The table has the schema " + tableProperties.getSchema().toString() + "\n" +
                            PROMPT_QUERY_TYPE +
                            PROMPT_EXACT_KEY_LONG_TYPE +
                            "Submitting query with ID: test-query-id\n" +
                            "Query failed: Message missing required field queryId: {\"message\":\"error\"}\n" +
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

            // When
            in.enterNextPrompts(EXACT_QUERY_OPTION, "123", EXIT_OPTION);
            runQueryClient("test-query-id",
                    withResponses(
                            message("{\"queryId\":\"test-query-id\"}")));

            // Then
            assertThat(out.toString())
                    .isEqualTo("Querying table test-table\n" +
                            "The table has the schema " + tableProperties.getSchema().toString() + "\n" +
                            PROMPT_QUERY_TYPE +
                            PROMPT_EXACT_KEY_LONG_TYPE +
                            "Submitting query with ID: test-query-id\n" +
                            "Query failed: Message missing required field message: {\"queryId\":\"test-query-id\"}\n" +
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

            // When
            in.enterNextPrompts(EXACT_QUERY_OPTION, "123", EXIT_OPTION);
            runQueryClient("test-query-id",
                    withResponses(
                            close("Network error")));

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

    protected void runQueryClient(String queryId, Client webSocketClient) throws Exception {
        new QueryWebSocketCommandLineClient(instanceProperties, tableIndex, new FixedTablePropertiesProvider(tableProperties),
                in.consoleIn(), out.consoleOut(), new QueryWebSocketClient(instanceProperties,
                        new FixedTablePropertiesProvider(tableProperties), () -> webSocketClient),
                () -> queryId, List.of(START_TIME, FINISH_TIME).iterator()::next)
                .run();
    }

    private FakeWebSocketClient withResponses(WebSocketResponse... responses) {
        client = new FakeWebSocketClient(new FixedTablePropertiesProvider(tableProperties));
        client.withResponses(responses);
        return client;
    }
}
