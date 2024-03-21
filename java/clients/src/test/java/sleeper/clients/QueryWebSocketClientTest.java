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

import org.junit.jupiter.api.Test;

import sleeper.clients.QueryWebSocketClient.BasicClient;
import sleeper.clients.QueryWebSocketClient.Client;
import sleeper.clients.testutil.TestConsoleInput;
import sleeper.clients.testutil.ToStringPrintStream;
import sleeper.clients.util.console.ConsoleOutput;
import sleeper.configuration.properties.instance.InstanceProperties;
import sleeper.configuration.properties.table.FixedTablePropertiesProvider;
import sleeper.configuration.properties.table.TableProperties;
import sleeper.configuration.properties.table.TablePropertiesProvider;
import sleeper.core.range.Range;
import sleeper.core.range.Region;
import sleeper.core.record.Record;
import sleeper.core.schema.Field;
import sleeper.core.schema.Schema;
import sleeper.core.statestore.StateStore;
import sleeper.core.statestore.inmemory.StateStoreTestHelper;
import sleeper.core.table.InMemoryTableIndex;
import sleeper.core.table.TableIndex;
import sleeper.query.model.Query;
import sleeper.query.model.QuerySerDe;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.clients.QueryClientTestConstants.EXACT_QUERY_OPTION;
import static sleeper.clients.QueryClientTestConstants.EXIT_OPTION;
import static sleeper.clients.QueryClientTestConstants.PROMPT_EXACT_KEY_LONG_TYPE;
import static sleeper.clients.QueryClientTestConstants.PROMPT_QUERY_TYPE;
import static sleeper.configuration.properties.InstancePropertiesTestHelper.createTestInstanceProperties;
import static sleeper.configuration.properties.instance.CdkDefinedInstanceProperty.QUERY_WEBSOCKET_API_URL;
import static sleeper.configuration.properties.table.TablePropertiesTestHelper.createTestTableProperties;
import static sleeper.configuration.properties.table.TableProperty.TABLE_NAME;
import static sleeper.core.schema.SchemaTestHelper.schemaWithKey;

public class QueryWebSocketClientTest {
    private static final String ENDPOINT_URL = "websocket-endpoint";
    private final InstanceProperties instanceProperties = createInstance();
    private final Schema schema = schemaWithKey("key");
    private final Field rowKey = schema.getField("key").orElseThrow();
    private final TableIndex tableIndex = new InMemoryTableIndex();
    private final ToStringPrintStream out = new ToStringPrintStream();
    private final TestConsoleInput in = new TestConsoleInput(out.consoleOut());
    private final QuerySerDe querySerDe = new QuerySerDe(schema);
    private FakeWebSocketClient client;

    private static InstanceProperties createInstance() {
        InstanceProperties instanceProperties = createTestInstanceProperties();
        instanceProperties.set(QUERY_WEBSOCKET_API_URL, ENDPOINT_URL);
        return instanceProperties;
    }

    private TableProperties createTable(String tableName) {
        TableProperties tableProperties = createTestTableProperties(instanceProperties, schema);
        tableProperties.set(TABLE_NAME, tableName);
        tableIndex.create(tableProperties.getStatus());
        return tableProperties;
    }

    @Test
    void shouldReturnOneRecordWhenExactRecordFound() throws Exception {
        // Given
        TableProperties tableProperties = createTable("test-table");
        StateStore stateStore = StateStoreTestHelper.inMemoryStateStoreWithSinglePartition(schema);
        Query expectedQuery = exactQuery("test-query-id", tableProperties, 123);
        Record expectedRecord = new Record(Map.of("key", 123L));
        setupWebSocketClient(tableProperties, expectedQuery)
                .withResponses(
                        message(queryResult("test-query-id", expectedRecord)),
                        message(completedQuery("test-query-id", 1L)),
                        closeWithReason("finished"));

        // When
        in.enterNextPrompts(EXACT_QUERY_OPTION, "123", EXIT_OPTION);
        runQueryClient(tableProperties, stateStore, List.of("test-query-id").iterator()::next);

        // Then
        assertThat(out.toString())
                .startsWith("Querying table test-table")
                .contains(PROMPT_QUERY_TYPE +
                        PROMPT_EXACT_KEY_LONG_TYPE +
                        "Connected to WebSocket API\n" +
                        "Submitting Query: " + querySerDe.toJson(expectedQuery) + "\n" +
                        "1 records returned by query: test-query-id Remaining pending queries: 0\n" +
                        "Query results:\n" +
                        expectedRecord + "\n" +
                        "Disconnected from WebSocket API: finished")
                .containsSubsequence("Query took", "seconds to return 1 records");
        assertThat(client.connected).isFalse();
        assertThat(client.closed).isTrue();
        assertThat(client.sentMessages)
                .containsExactly(querySerDe.toJson(expectedQuery));
    }

    private Query exactQuery(String queryId, TableProperties tableProperties, long value) {
        return Query.builder()
                .tableName(tableProperties.get(TABLE_NAME))
                .queryId(queryId)
                .regions(List.of(new Region(new Range(rowKey, value, true, value, true))))
                .build();
    }

    private static String queryResult(String queryId, Record... records) {
        String test = "{" +
                "\"queryId\":\"" + queryId + "\", " +
                "\"message\":\"records\"," +
                "\"records\":[" + Stream.of(records).map(record -> "\"" + record + "\"").collect(Collectors.joining(","))
                + "]" +
                "}";
        System.out.println(test);
        return test;
    }

    private static String completedQuery(String queryId, long recordCount) {
        return "{" +
                "\"queryId\":\"" + queryId + "\", " +
                "\"message\":\"completed\"," +
                "\"recordCount\":\"" + recordCount + "\"," +
                "\"locations\":[]" +
                "}";
    }

    protected void runQueryClient(TableProperties tableProperties, StateStore stateStore, Supplier<String> queryIdSupplier) throws Exception {
        new QueryWebSocketClient(instanceProperties, tableIndex, new FixedTablePropertiesProvider(tableProperties),
                in.consoleIn(), out.consoleOut(), client, queryIdSupplier)
                .run();
    }

    private FakeWebSocketClient setupWebSocketClient(TableProperties tableProperties, Query query) throws Exception {
        client = new FakeWebSocketClient(new FixedTablePropertiesProvider(tableProperties), out.consoleOut());
        return client;
    }

    private static class FakeWebSocketClient implements Client {
        private boolean connected = false;
        private boolean closed = false;
        private BasicClient basicClient;
        private List<String> sentMessages = new ArrayList<>();
        private List<WebSocketAction> actions;

        FakeWebSocketClient(TablePropertiesProvider tablePropertiesProvider, ConsoleOutput out) {
            this.basicClient = new BasicClient(new QuerySerDe(tablePropertiesProvider), out);
        }

        @Override
        public boolean connectBlocking() throws InterruptedException {
            connected = true;
            return connected;
        }

        @Override
        public void closeBlocking() throws InterruptedException {
            closed = true;
        }

        public void withResponses(WebSocketAction... actions) {
            this.actions = List.of(actions);
        }

        @Override
        public void startQuery(Query query) throws InterruptedException {
            basicClient.onOpen(query, sentMessages::add);
            actions.forEach(action -> action.run(basicClient));
        }

        @Override
        public boolean isQueryComplete() {
            return basicClient.isQueryComplete();
        }

        @Override
        public long getTotalRecordsReturned() {
            return basicClient.getTotalRecordsReturned();
        }
    }

    private interface WebSocketAction {
        void run(BasicClient client);
    }

    public WebSocketAction open(Query query) {
        return basicClient -> basicClient.onOpen(query, client.sentMessages::add);
    }

    private WebSocketAction message(String message) {
        return basicClient -> basicClient.onMessage(message);
    }

    public WebSocketAction closeWithReason(String reason) {
        return basicClient -> basicClient.onClose(reason);
    }

    public WebSocketAction error(Exception error) {
        return basicClient -> basicClient.onError(error);
    }

}
