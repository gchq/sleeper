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
import org.junit.jupiter.api.Test;

import sleeper.clients.QueryWebSocketClient.BasicClient;
import sleeper.clients.QueryWebSocketClient.Client;
import sleeper.clients.util.console.ConsoleOutput;
import sleeper.configuration.properties.table.FixedTablePropertiesProvider;
import sleeper.configuration.properties.table.TableProperties;
import sleeper.configuration.properties.table.TablePropertiesProvider;
import sleeper.core.schema.Schema;
import sleeper.core.statestore.StateStore;
import sleeper.core.statestore.inmemory.StateStoreTestHelper;
import sleeper.query.model.Query;
import sleeper.query.model.QuerySerDe;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;

import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.clients.QueryClientTestConstants.EXACT_QUERY_OPTION;
import static sleeper.clients.QueryClientTestConstants.EXIT_OPTION;
import static sleeper.clients.QueryClientTestConstants.PROMPT_EXACT_KEY_LONG_TYPE;
import static sleeper.clients.QueryClientTestConstants.PROMPT_QUERY_TYPE;
import static sleeper.configuration.properties.instance.CdkDefinedInstanceProperty.QUERY_WEBSOCKET_API_URL;
import static sleeper.core.schema.SchemaTestHelper.schemaWithKey;

public class QueryWebSocketClientIT extends QueryClientTestBase {
    private static final String ENDPOINT_URL = "websocket-endpoint";

    @BeforeEach
    void setUp() throws Exception {
        instanceProperties = createInstanceProperties(tempDir);
        instanceProperties.set(QUERY_WEBSOCKET_API_URL, ENDPOINT_URL);
    }

    @Test
    void shouldReturnNoRecordsWhenExactRecordNotFound() throws Exception {
        // Given
        Schema schema = schemaWithKey("key");
        TableProperties tableProperties = createTable("test-table", schema);
        StateStore stateStore = StateStoreTestHelper.inMemoryStateStoreWithSinglePartition(schema);

        // When
        in.enterNextPrompts(EXACT_QUERY_OPTION, "123", EXIT_OPTION);
        runQueryClient(tableProperties, stateStore);

        // Then
        assertThat(out.toString())
                .startsWith("Querying table test-table")
                .contains(PROMPT_QUERY_TYPE +
                        PROMPT_EXACT_KEY_LONG_TYPE +
                        "Returned Records:")
                .containsSubsequence("Query took", "seconds to return 0 records");

    }

    protected void runQueryClient(TableProperties tableProperties, StateStore stateStore) throws Exception {
        TablePropertiesProvider tablePropertiesProvider = new FixedTablePropertiesProvider(tableProperties);
        new QueryWebSocketClient(instanceProperties, tableIndex, tablePropertiesProvider,
                in.consoleIn(), out.consoleOut(), new FakeWebSocketClient(() -> true, tablePropertiesProvider, out.consoleOut()))
                .run();
    }

    private static class FakeWebSocketClient implements Client {
        private boolean connected = false;
        private boolean closed = false;
        private Supplier<Boolean> queryCompletionChecker;
        private BasicClient basicClient;
        private List<String> sentMessages = new ArrayList<>();

        FakeWebSocketClient(Supplier<Boolean> queryCompletionChecker, TablePropertiesProvider tablePropertiesProvider, ConsoleOutput out) {
            this.queryCompletionChecker = queryCompletionChecker;
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

        @Override
        public void startQuery(Query query) throws InterruptedException {
            basicClient.onOpen(query, sentMessages::add);
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
}
