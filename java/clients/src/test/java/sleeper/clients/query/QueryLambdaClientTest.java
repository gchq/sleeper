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

import org.junit.jupiter.api.Test;

import sleeper.clients.testutil.ExceptionWithOutput;
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
import sleeper.core.schema.Schema;
import sleeper.core.statestore.FileReference;
import sleeper.core.statestore.FileReferenceFactory;
import sleeper.core.statestore.StateStore;
import sleeper.core.statestore.StateStoreProvider;
import sleeper.core.statestore.testutils.InMemoryTransactionLogStateStore;
import sleeper.core.statestore.testutils.InMemoryTransactionLogsPerTable;
import sleeper.core.table.InMemoryTableIndex;
import sleeper.core.table.TableIndex;
import sleeper.core.util.ObjectFactory;
import sleeper.query.core.model.Query;
import sleeper.query.core.model.QueryException;
import sleeper.query.core.output.ResultsOutputInfo;
import sleeper.query.core.output.ResultsOutputLocation;
import sleeper.query.core.rowretrieval.InMemoryLeafPartitionRowRetriever;
import sleeper.query.core.rowretrieval.LeafPartitionQueryExecutor;
import sleeper.query.core.rowretrieval.QueryExecutor;
import sleeper.query.core.rowretrieval.QueryPlanner;
import sleeper.query.core.tracker.InMemoryQueryTracker;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.LongStream;

import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.clients.query.QueryClientTestConstants.EXIT_OPTION;
import static sleeper.clients.query.QueryClientTestConstants.NO_OPTION;
import static sleeper.clients.query.QueryClientTestConstants.PROMPT_MAX_INCLUSIVE;
import static sleeper.clients.query.QueryClientTestConstants.PROMPT_MAX_ROW_KEY_LONG_TYPE;
import static sleeper.clients.query.QueryClientTestConstants.PROMPT_MIN_INCLUSIVE;
import static sleeper.clients.query.QueryClientTestConstants.PROMPT_MIN_ROW_KEY_LONG_TYPE;
import static sleeper.clients.query.QueryClientTestConstants.PROMPT_QUERY_TYPE;
import static sleeper.clients.query.QueryClientTestConstants.RANGE_QUERY_OPTION;
import static sleeper.clients.query.QueryClientTestConstants.SEND_TO_S3_OPTION;
import static sleeper.clients.query.QueryClientTestConstants.YES_OPTION;
import static sleeper.core.properties.table.TableProperty.TABLE_NAME;
import static sleeper.core.properties.testutils.InstancePropertiesTestHelper.createTestInstanceProperties;
import static sleeper.core.properties.testutils.TablePropertiesTestHelper.createTestTableProperties;
import static sleeper.core.schema.SchemaTestHelper.createSchemaWithKey;
import static sleeper.core.statestore.testutils.StateStoreUpdatesWrapper.update;

public class QueryLambdaClientTest {
    private final InstanceProperties instanceProperties = createTestInstanceProperties();
    private final TableIndex tableIndex = new InMemoryTableIndex();
    private final ToStringConsoleOutput out = new ToStringConsoleOutput();
    private final TestConsoleInput in = new TestConsoleInput(out.consoleOut());
    private final InMemoryTransactionLogsPerTable transactionLogs = new InMemoryTransactionLogsPerTable();
    private final StateStoreProvider stateStoreProvider = InMemoryTransactionLogStateStore.createProvider(instanceProperties, transactionLogs);
    private final TablePropertiesStore tablePropertiesStore = InMemoryTableProperties.getStore(tableIndex);
    private final InMemoryRowStore rowStore = new InMemoryRowStore();
    private final InMemoryQueryTracker queryTracker = new InMemoryQueryTracker(instanceProperties);
    private final List<Row> outputRows = new ArrayList<>();

    @Test
    void shouldRunRangeQuery() throws Exception {
        // Given
        Schema schema = createSchemaWithKey("key");
        TableProperties tableProperties = createTable("test-table", schema);
        List<Row> rows = LongStream.rangeClosed(0, 10)
                .mapToObj(num -> new Row(Map.of("key", num)))
                .collect(Collectors.toList());
        ingestData(tableProperties, rows);

        // When
        in.enterNextPrompts(
                SEND_TO_S3_OPTION,
                RANGE_QUERY_OPTION,
                NO_OPTION, YES_OPTION,
                "3", "6",
                EXIT_OPTION);
        runQueryClient();

        // Then
        assertThat(out.toString())
                .startsWith("Querying table test-table")
                .contains(PROMPT_QUERY_TYPE +
                        PROMPT_MIN_INCLUSIVE +
                        PROMPT_MAX_INCLUSIVE +
                        PROMPT_MIN_ROW_KEY_LONG_TYPE +
                        PROMPT_MAX_ROW_KEY_LONG_TYPE +
                        "Submitting query with id")
                .contains("Polling query tracker\n" +
                        "Finished query processing with final state of: COMPLETED");
    }

    private TableProperties createTable(String tableName, Schema schema) {
        TableProperties tableProperties = createTestTableProperties(instanceProperties, schema);
        tableProperties.set(TABLE_NAME, tableName);
        tablePropertiesStore.save(tableProperties);
        update(stateStore(tableProperties)).initialise(tableProperties);
        return tableProperties;
    }

    private void ingestData(TableProperties tableProperties, List<Row> rows) throws Exception {
        FileReference file = fileFactory(tableProperties).rootFile(UUID.randomUUID().toString(), rows.size());
        rowStore.addFile(file.getFilename(), rows);
        update(stateStore(tableProperties)).addFile(file);
    }

    private FileReferenceFactory fileFactory(TableProperties tableProperties) {
        return FileReferenceFactory.from(instanceProperties, tableProperties, stateStore(tableProperties));
    }

    private StateStore stateStore(TableProperties tableProperties) {
        return stateStoreProvider.getStateStore(tableProperties);
    }

    private void runQueryClient() throws Exception {
        try {
            new QueryLambdaClient(instanceProperties, tableIndex, tablePropertiesProvider(),
                    this::runQuery, queryTracker,
                    in.consoleIn(), out.consoleOut())
                    .run();
        } catch (Exception e) {
            throw new ExceptionWithOutput(out, e);
        }
    }

    private TablePropertiesProvider tablePropertiesProvider() {
        return new TablePropertiesProvider(instanceProperties, tablePropertiesStore);
    }

    private void runQuery(Query query) {
        TableProperties tableProperties = tablePropertiesStore.loadByName(query.getTableName());
        try (CloseableIterator<Row> rowsIt = queryExecutor(tableProperties).execute(query)) {
            List<Row> rows = new ArrayList<>();
            rowsIt.forEachRemaining(rows::add);
            outputRows.addAll(rows);
            queryTracker.queryCompleted(query, new ResultsOutputInfo(rows.size(),
                    List.of(new ResultsOutputLocation("s3", "test.parquet"))));
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
