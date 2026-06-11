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
package sleeper.clients.query;

import sleeper.clients.testutil.TestConsoleInput;
import sleeper.clients.testutil.ToStringConsoleOutput;
import sleeper.core.iterator.closeable.CloseableIterator;
import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.table.TableProperties;
import sleeper.core.properties.table.TablePropertiesProvider;
import sleeper.core.properties.table.TablePropertiesStore;
import sleeper.core.properties.testutils.InMemoryTableProperties;
import sleeper.core.row.Row;
import sleeper.core.row.testutils.InMemoryRowStore;
import sleeper.core.statestore.StateStoreProvider;
import sleeper.core.statestore.testutils.InMemoryTransactionLogStateStore;
import sleeper.core.statestore.testutils.InMemoryTransactionLogsPerTable;
import sleeper.core.table.InMemoryTableIndex;
import sleeper.core.table.TableIndex;
import sleeper.core.util.ObjectFactory;
import sleeper.query.core.model.Query;
import sleeper.query.core.model.QueryException;
import sleeper.query.core.rowretrieval.InMemoryLeafPartitionRowRetriever;
import sleeper.query.core.rowretrieval.LeafPartitionQueryExecutor;
import sleeper.query.core.rowretrieval.QueryExecutor;
import sleeper.query.core.rowretrieval.QueryPlanner;
import sleeper.query.core.tracker.InMemoryQueryTracker;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

import static sleeper.core.properties.testutils.InstancePropertiesTestHelper.createTestInstanceProperties;

public class QueryLambdaClientTest {
    private final InstanceProperties instanceProperties = createTestInstanceProperties();
    private final TableIndex tableIndex = new InMemoryTableIndex();
    private final ToStringConsoleOutput out = new ToStringConsoleOutput();
    private final TestConsoleInput in = new TestConsoleInput(out.consoleOut());
    private final InMemoryTransactionLogsPerTable transactionLogs = new InMemoryTransactionLogsPerTable();
    private final StateStoreProvider stateStoreProvider = InMemoryTransactionLogStateStore.createProvider(instanceProperties, transactionLogs);
    private final TablePropertiesStore tablePropertiesStore = InMemoryTableProperties.getStore(tableIndex);
    private final InMemoryRowStore rowStore = new InMemoryRowStore();
    private final List<Row> outputRows = new ArrayList<>();

    private void runQueryClient() throws Exception {
        new QueryLambdaClient(instanceProperties, tableIndex, tablePropertiesProvider(),
                this::runQuery, new InMemoryQueryTracker(instanceProperties, Instant::now),
                in.consoleIn(), out.consoleOut())
                .run();
    }

    private TablePropertiesProvider tablePropertiesProvider() {
        return new TablePropertiesProvider(instanceProperties, tablePropertiesStore);
    }

    private void runQuery(Query query) {
        TableProperties tableProperties = tablePropertiesStore.loadByName(query.getTableName());
        try (CloseableIterator<Row> rows = queryExecutor(tableProperties).execute(query)) {
            rows.forEachRemaining(outputRows::add);
        } catch (QueryException e) {
            throw new RuntimeException(e);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private QueryExecutor queryExecutor(TableProperties tableProperties) {
        return new QueryExecutor(
                QueryPlanner.initialiseNow(tableProperties, stateStoreProvider.getStateStore(tableProperties)),
                new LeafPartitionQueryExecutor(ObjectFactory.noUserJars(), tableProperties,
                        new InMemoryLeafPartitionRowRetriever(rowStore)));
    }

}
