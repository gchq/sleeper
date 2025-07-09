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

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import sleeper.clients.query.FakeWebSocketClient.WebSocketResponse;
import sleeper.clients.query.QueryWebSocketClient.Client;
import sleeper.clients.query.exception.MessageMalformedException;
import sleeper.clients.query.exception.MessageMissingFieldException;
import sleeper.clients.query.exception.UnknownMessageTypeException;
import sleeper.clients.query.exception.WebSocketClosedException;
import sleeper.clients.query.exception.WebSocketErrorException;
import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.table.TableProperties;
import sleeper.core.properties.testutils.FixedTablePropertiesProvider;
import sleeper.core.range.Range.RangeFactory;
import sleeper.core.row.Record;
import sleeper.core.range.Region;
import sleeper.core.schema.Field;
import sleeper.core.schema.Schema;
import sleeper.core.table.InMemoryTableIndex;
import sleeper.core.table.TableIndex;
import sleeper.query.core.model.Query;
import sleeper.query.core.model.QuerySerDe;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.clients.query.QueryWebSocketClientTestHelper.asJson;
import static sleeper.clients.query.QueryWebSocketClientTestHelper.close;
import static sleeper.clients.query.QueryWebSocketClientTestHelper.completedQuery;
import static sleeper.clients.query.QueryWebSocketClientTestHelper.createdSubQueries;
import static sleeper.clients.query.QueryWebSocketClientTestHelper.error;
import static sleeper.clients.query.QueryWebSocketClientTestHelper.errorMessage;
import static sleeper.clients.query.QueryWebSocketClientTestHelper.message;
import static sleeper.clients.query.QueryWebSocketClientTestHelper.queryResult;
import static sleeper.clients.query.QueryWebSocketClientTestHelper.unknownMessage;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.QUERY_WEBSOCKET_API_URL;
import static sleeper.core.properties.table.TableProperty.TABLE_NAME;
import static sleeper.core.properties.testutils.InstancePropertiesTestHelper.createTestInstanceProperties;
import static sleeper.core.properties.testutils.TablePropertiesTestHelper.createTestTableProperties;
import static sleeper.core.schema.SchemaTestHelper.createSchemaWithKey;

public class QueryWebSocketClientTest {
    private final InstanceProperties instanceProperties = createInstance();
    private final Schema schema = createSchemaWithKey("key");
    private final Field rowKey = schema.getField("key").orElseThrow();
    private final TableIndex tableIndex = new InMemoryTableIndex();
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
            Query query = exactQuery("test-query-id", 123);
            Record expectedRecord = new Record(Map.of("key", 123L));

            // When / Then
            assertThat(runQueryFuture(query,
                    withResponses(
                            message(queryResult("test-query-id", expectedRecord)),
                            message(completedQuery("test-query-id", 1L)))))
                    .isCompletedWithValue(List.of(asJson(expectedRecord)));
            assertThat(client.isConnected()).isFalse();
            assertThat(client.isClosed()).isTrue();
            assertThat(client.getSentMessages())
                    .containsExactly(querySerDe.toJson(query));
            assertThat(client.getResults("test-query-id"))
                    .containsExactly(asJson(expectedRecord));
        }

        @Test
        void shouldReturnResultsForQueryWithOneSubquery() throws Exception {
            // Given
            Query query = exactQuery("test-query-id", 123);
            Record expectedRecord = new Record(Map.of("key", 123L));

            // When / Then
            assertThat(runQueryFuture(query,
                    withResponses(
                            message(createdSubQueries("test-query-id", "test-subquery")),
                            message(queryResult("test-subquery", expectedRecord)),
                            message(completedQuery("test-subquery", 1L)))))
                    .isCompletedWithValue(List.of(asJson(expectedRecord)));
            assertThat(client.isConnected()).isFalse();
            assertThat(client.isClosed()).isTrue();
            assertThat(client.getSentMessages())
                    .containsExactly(querySerDe.toJson(query));
            assertThat(client.getResults("test-query-id"))
                    .containsExactly(asJson(expectedRecord));
            assertThat(client.getResults("test-subquery"))
                    .containsExactly(asJson(expectedRecord));
        }

        @Test
        void shouldReturnResultsForQueryWithMultipleSubqueries() throws Exception {
            // Given
            Query query = rangeQuery("test-query-id", 0L, 1000L);
            Record expectedRecord1 = new Record(Map.of("key", 123L));
            Record expectedRecord2 = new Record(Map.of("key", 456L));
            Record expectedRecord3 = new Record(Map.of("key", 789L));

            // When / Then
            assertThat(runQueryFuture(query,
                    withResponses(
                            message(createdSubQueries("test-query-id", "subquery-1", "subquery-2", "subquery-3")),
                            message(queryResult("subquery-1", expectedRecord1)),
                            message(completedQuery("subquery-1", 1L)),
                            message(queryResult("subquery-2", expectedRecord2)),
                            message(completedQuery("subquery-2", 1L)),
                            message(queryResult("subquery-3", expectedRecord3)),
                            message(completedQuery("subquery-3", 1L)))))
                    .isCompletedWithValue(List.of(asJson(expectedRecord1), asJson(expectedRecord2), asJson(expectedRecord3)));
            assertThat(client.isConnected()).isFalse();
            assertThat(client.isClosed()).isTrue();
            assertThat(client.getSentMessages())
                    .containsExactly(querySerDe.toJson(query));
            assertThat(client.getResults("test-query-id"))
                    .containsExactly(asJson(expectedRecord1), asJson(expectedRecord2), asJson(expectedRecord3));
            assertThat(client.getResults("subquery-1"))
                    .containsExactly(asJson(expectedRecord1));
            assertThat(client.getResults("subquery-2"))
                    .containsExactly(asJson(expectedRecord2));
            assertThat(client.getResults("subquery-3"))
                    .containsExactly(asJson(expectedRecord3));
        }

        @Test
        void shouldReturnResultsWhenRecordCountDoesNotMatchRecordsReceived() throws Exception {
            // Given
            Query query = exactQuery("test-query-id", 123);
            Record expectedRecord = new Record(Map.of("key", 123L));

            // When / Then
            assertThat(runQueryFuture(query,
                    withResponses(
                            message(queryResult("test-query-id", expectedRecord)),
                            message(completedQuery("test-query-id", 2L)))))
                    .isCompletedWithValue(List.of(asJson(expectedRecord)));
            assertThat(client.isConnected()).isFalse();
            assertThat(client.isClosed()).isTrue();
            assertThat(client.getSentMessages())
                    .containsExactly(querySerDe.toJson(query));
            assertThat(client.getResults("test-query-id"))
                    .containsExactly(asJson(expectedRecord));
        }
    }

    @Nested
    @DisplayName("Handle errors")
    class HandleErrors {

        @Test
        void shouldHandleErrorIfExceptionEncounteredThatDoesNotCloseConnection() throws Exception {
            // Given
            Query query = exactQuery("test-query-id", 123L);

            // When / Then
            assertThat(runQueryFuture(query,
                    withResponses(
                            error(new Exception("Exception that will not terminate connection")))))
                    .isCompletedExceptionally()
                    .failsWithin(Duration.ofMillis(10))
                    .withThrowableOfType(ExecutionException.class)
                    .withCauseInstanceOf(WebSocketErrorException.class);
            assertThat(client.isConnected()).isFalse();
            assertThat(client.isClosed()).isTrue();
            assertThat(client.getSentMessages())
                    .containsExactly(querySerDe.toJson(query));
        }

        @Test
        void shouldHandleErrorIfExceptionEncounteredWhenQueryCompletesAfter() throws Exception {
            // Given
            Query query = exactQuery("test-query-id", 123L);

            // When / Then
            assertThat(runQueryFuture(query,
                    withResponses(
                            error(new Exception("Exception that will not terminate connection")),
                            message(completedQuery("test-query-id", 0L)))))
                    .isCompletedExceptionally()
                    .failsWithin(Duration.ofMillis(10))
                    .withThrowableOfType(ExecutionException.class)
                    .withCauseInstanceOf(WebSocketErrorException.class);
            assertThat(client.isConnected()).isFalse();
            assertThat(client.isClosed()).isTrue();
            assertThat(client.getSentMessages())
                    .containsExactly(querySerDe.toJson(query));
        }

        @Test
        void shouldHandleErrorIfExceptionEncounteredThatClosesConnection() throws Exception {
            // Given
            Query query = exactQuery("test-query-id", 123L);

            // When / Then
            assertThat(runQueryFuture(query,
                    withResponses(
                            error(new Exception("Exception that will terminate connection")),
                            close("Exception caused connection to terminate"))))
                    .isCompletedExceptionally()
                    .failsWithin(Duration.ofMillis(10))
                    .withThrowableOfType(ExecutionException.class)
                    .withCauseInstanceOf(WebSocketErrorException.class);
            assertThat(client.isConnected()).isFalse();
            assertThat(client.isClosed()).isTrue();
            assertThat(client.getSentMessages())
                    .containsExactly(querySerDe.toJson(query));
        }

        @Test
        void shouldHandleErrorIfMessageWithErrorIsReceived() throws Exception {
            // Given
            Query query = exactQuery("test-query-id", 123L);

            // When / Then
            assertThat(runQueryFuture(query,
                    withResponses(
                            message(errorMessage("test-query-id", "Query failed")))))
                    .isCompletedExceptionally()
                    .failsWithin(Duration.ofMillis(10))
                    .withThrowableOfType(ExecutionException.class)
                    .withCauseInstanceOf(WebSocketErrorException.class);
            assertThat(client.isConnected()).isFalse();
            assertThat(client.isClosed()).isTrue();
            assertThat(client.getSentMessages())
                    .containsExactly(querySerDe.toJson(query));
        }

        @Test
        void shouldHandleMessageWithUnrecognisedType() throws Exception {
            // Given
            Query query = exactQuery("test-query-id", 123L);

            // When / Then
            assertThat(runQueryFuture(query,
                    withResponses(
                            message(unknownMessage("test-query-id")))))
                    .isCompletedExceptionally()
                    .failsWithin(Duration.ofMillis(10))
                    .withThrowableOfType(ExecutionException.class)
                    .withCauseInstanceOf(UnknownMessageTypeException.class);
            assertThat(client.isConnected()).isFalse();
            assertThat(client.isClosed()).isTrue();
            assertThat(client.getSentMessages())
                    .containsExactly(querySerDe.toJson(query));
        }

        @Test
        void shouldHandleMalformedJson() throws Exception {
            // Given
            Query query = exactQuery("test-query-id", 123L);

            // When / Then
            assertThat(runQueryFuture(query,
                    withResponses(
                            message("{"))))
                    .isCompletedExceptionally()
                    .failsWithin(Duration.ofMillis(10))
                    .withThrowableOfType(ExecutionException.class)
                    .withCauseInstanceOf(MessageMalformedException.class);
            assertThat(client.isConnected()).isFalse();
            assertThat(client.isClosed()).isTrue();
            assertThat(client.getSentMessages())
                    .containsExactly(querySerDe.toJson(query));
        }

        @Test
        void shouldHandleMissingQueryIdInMessageFromApi() throws Exception {
            // Given
            Query query = exactQuery("test-query-id", 123L);

            // When / Then
            assertThat(runQueryFuture(query,
                    withResponses(
                            message("{\"message\":\"error\"}"))))
                    .isCompletedExceptionally()
                    .failsWithin(Duration.ofMillis(10))
                    .withThrowableOfType(ExecutionException.class)
                    .withCauseInstanceOf(MessageMissingFieldException.class);
            assertThat(client.isConnected()).isFalse();
            assertThat(client.isClosed()).isTrue();
            assertThat(client.getSentMessages())
                    .containsExactly(querySerDe.toJson(query));
        }

        @Test
        void shouldHandleMissingMessageTypeInMessageFromApi() throws Exception {
            // Given
            Query query = exactQuery("test-query-id", 123L);

            // When / Then
            assertThat(runQueryFuture(query,
                    withResponses(
                            message("{\"queryId\":\"test-query-id\"}"))))
                    .isCompletedExceptionally()
                    .failsWithin(Duration.ofMillis(10))
                    .withThrowableOfType(ExecutionException.class)
                    .withCauseInstanceOf(MessageMissingFieldException.class);
            assertThat(client.isConnected()).isFalse();
            assertThat(client.isClosed()).isTrue();
            assertThat(client.getSentMessages())
                    .containsExactly(querySerDe.toJson(query));
        }

        @Test
        void shouldHandleConnectionClosingUnexpectedly() throws Exception {
            // Given
            Query query = exactQuery("test-query-id", 123L);

            // When / Then
            assertThat(runQueryFuture(query,
                    withResponses(
                            close("Network error"))))
                    .isCompletedExceptionally()
                    .failsWithin(Duration.ofMillis(10))
                    .withThrowableOfType(ExecutionException.class)
                    .withCauseInstanceOf(WebSocketClosedException.class);
            assertThat(client.isConnected()).isFalse();
            assertThat(client.isClosed()).isTrue();
            assertThat(client.getSentMessages())
                    .containsExactly(querySerDe.toJson(query));
        }

        @Test
        void shouldTimeOutWithNoReponse() throws Exception {
            // Given
            Query query = exactQuery("test-query-id", 123L);

            // When / Then
            assertThat(runQueryFuture(query, withResponses()))
                    .failsWithin(Duration.ofMillis(10))
                    .withThrowableOfType(ExecutionException.class)
                    .withCauseInstanceOf(TimeoutException.class);
            assertThat(client.isConnected()).isFalse();
            assertThat(client.isClosed()).isTrue();
            assertThat(client.getSentMessages())
                    .containsExactly(querySerDe.toJson(query));
        }
    }

    private Query exactQuery(String queryId, long value) {
        return Query.builder()
                .tableName(tableProperties.get(TABLE_NAME))
                .queryId(queryId)
                .regions(List.of(new Region(rangeFactory().createExactRange(rowKey, value))))
                .build();
    }

    private Query rangeQuery(String queryId, long min, long max) {
        return Query.builder()
                .tableName(tableProperties.get(TABLE_NAME))
                .queryId(queryId)
                .regions(List.of(new Region(rangeFactory().createRange(rowKey, min, max))))
                .build();
    }

    private RangeFactory rangeFactory() {
        return new RangeFactory(tableProperties.getSchema());
    }

    protected CompletableFuture<List<String>> runQueryFuture(Query query, Client webSocketClient) throws Exception {
        QueryWebSocketClient client = new QueryWebSocketClient(instanceProperties,
                new FixedTablePropertiesProvider(tableProperties), () -> webSocketClient, 0);
        return client.submitQuery(query);
    }

    private FakeWebSocketClient withResponses(WebSocketResponse... responses) {
        client = new FakeWebSocketClient(new FixedTablePropertiesProvider(tableProperties));
        client.withResponses(responses);
        return client;
    }
}
