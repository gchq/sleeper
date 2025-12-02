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
import sleeper.core.partition.PartitionsBuilder;
import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.table.TableProperties;
import sleeper.core.properties.testutils.FixedTablePropertiesProvider;
import sleeper.core.row.Row;
import sleeper.core.row.testutils.InMemoryRowStore;
import sleeper.core.schema.Field;
import sleeper.core.schema.Schema;
import sleeper.core.schema.type.LongType;
import sleeper.core.schema.type.StringType;
import sleeper.core.statestore.FileReference;
import sleeper.core.statestore.FileReferenceFactory;
import sleeper.core.statestore.StateStoreProvider;
import sleeper.core.statestore.testutils.InMemoryTransactionLogStateStore;
import sleeper.core.statestore.testutils.InMemoryTransactionLogsPerTable;
import sleeper.core.statestore.testutils.StateStoreUpdatesWrapper;
import sleeper.core.table.InMemoryTableIndex;
import sleeper.core.table.TableIdGenerator;
import sleeper.core.table.TableIndex;
import sleeper.core.table.TableStatus;
import sleeper.core.table.TableStatusTestHelper;
import sleeper.core.util.ObjectFactory;
import sleeper.query.core.rowretrieval.InMemoryLeafPartitionRowRetriever;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.LongStream;

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
import static sleeper.core.properties.table.TableProperty.TABLE_ID;
import static sleeper.core.properties.table.TableProperty.TABLE_NAME;
import static sleeper.core.properties.testutils.InstancePropertiesTestHelper.createTestInstanceProperties;
import static sleeper.core.properties.testutils.TablePropertiesTestHelper.createTestTableProperties;
import static sleeper.core.schema.SchemaTestHelper.createSchemaWithKey;

public class QueryClientTest {
    private final InstanceProperties instanceProperties = createTestInstanceProperties();
    private final TableIndex tableIndex = new InMemoryTableIndex();
    private final ToStringConsoleOutput out = new ToStringConsoleOutput();
    private final TestConsoleInput in = new TestConsoleInput(out.consoleOut());
    private final InMemoryTransactionLogsPerTable transactionLogs = new InMemoryTransactionLogsPerTable();
    private final StateStoreProvider stateStoreProvider = InMemoryTransactionLogStateStore.createProvider(instanceProperties, transactionLogs);
    private final List<TableProperties> tablePropertiesList = new ArrayList<>();
    private final InMemoryRowStore rowStore = new InMemoryRowStore();

    @Nested
    @DisplayName("Exact query")
    class ExactQuery {
        @Test
        void shouldReturnNoRowsWhenExactRowNotFound() throws Exception {
            // Given
            Schema schema = createSchemaWithKey("key");
            createTable("test-table", schema);

            // When
            in.enterNextPrompts(EXACT_QUERY_OPTION, "123", EXIT_OPTION);
            runQueryClient();

            // Then
            assertThat(out.toString())
                    .startsWith("Querying table test-table")
                    .contains(PROMPT_QUERY_TYPE +
                            PROMPT_EXACT_KEY_LONG_TYPE +
                            "Returned Rows:")
                    .containsSubsequence("Query took", "seconds to return 0 rows");
        }

        @Test
        void shouldRunExactRowQuery() throws Exception {
            // Given
            Schema schema = Schema.builder()
                    .rowKeyFields(new Field("key", new LongType()))
                    .valueFields(new Field("value", new StringType()))
                    .build();
            TableProperties tableProperties = createTable("test-table", schema);
            Row row = new Row();
            row.put("key", 123L);
            row.put("value", "abc");
            ingestData(tableProperties, List.of(row));

            // When
            in.enterNextPrompts(EXACT_QUERY_OPTION, "123", EXIT_OPTION);
            runQueryClient();

            // Then
            assertThat(out.toString())
                    .startsWith("Querying table test-table")
                    .contains(PROMPT_QUERY_TYPE +
                            PROMPT_EXACT_KEY_LONG_TYPE +
                            "Returned Rows:\n" +
                            "Row{key=123, value=abc}")
                    .containsSubsequence("Query took", "seconds to return 1 row");
        }
    }

    @Nested
    @DisplayName("Range query")
    class RangeQuery {
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
            in.enterNextPrompts(RANGE_QUERY_OPTION,
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
                            "Returned Rows:\n" +
                            "Row{key=4}\n" +
                            "Row{key=5}\n" +
                            "Row{key=6}")
                    .containsSubsequence("Query took", "seconds to return 3 rows");
        }

        @Test
        void shouldDefaultToFullRangeWhenMinMaxPromptsAreIgnored() throws Exception {
            // Given
            Schema schema = createSchemaWithKey("key");
            TableProperties tableProperties = createTable("test-table", schema);
            List<Row> rows = LongStream.rangeClosed(0, 3)
                    .mapToObj(num -> new Row(Map.of("key", num)))
                    .collect(Collectors.toList());
            ingestData(tableProperties, rows);

            // When
            in.enterNextPrompts(RANGE_QUERY_OPTION,
                    NO_OPTION, YES_OPTION,
                    "", "",
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
                            "Returned Rows:\n" +
                            "Row{key=0}\n" +
                            "Row{key=1}\n" +
                            "Row{key=2}\n" +
                            "Row{key=3}")
                    .containsSubsequence("Query took", "seconds to return 4 rows");
        }

        @Test
        void shouldRunRangeQueryByMultipleKeys() throws Exception {
            // Given
            Schema schema = Schema.builder()
                    .rowKeyFields(new Field("key1", new LongType()), new Field("key2", new LongType()))
                    .valueFields(new Field("value", new StringType()))
                    .build();
            TableProperties tableProperties = createTable("test-table", schema);
            List<Row> rows = LongStream.rangeClosed(0, 10)
                    .mapToObj(num -> new Row(Map.of(
                            "key1", num,
                            "key2", num + 100L,
                            "value", "test-" + num)))
                    .collect(Collectors.toList());
            ingestData(tableProperties, rows);

            // When
            in.enterNextPrompts(RANGE_QUERY_OPTION,
                    NO_OPTION, YES_OPTION,
                    "1", "5",
                    YES_OPTION,
                    "102", "104",
                    EXIT_OPTION);
            runQueryClient();

            // Then
            assertThat(out.toString())
                    .startsWith("Querying table test-table")
                    .contains(PROMPT_QUERY_TYPE +
                            PROMPT_MIN_INCLUSIVE +
                            PROMPT_MAX_INCLUSIVE +
                            "Enter a minimum key for row key field key1 of type = LongType{} - hit return for no minimum: \n" +
                            "Enter a maximum key for row key field key1 of type = LongType{} - hit return for no maximum: \n" +
                            "Enter a value for row key field key2 of type = LongType{}? (y/n) \n" +
                            "Enter a minimum key for row key field key2 of type = LongType{} - hit return for no minimum: \n" +
                            "Enter a maximum key for row key field key2 of type = LongType{} - hit return for no maximum: \n" +
                            "Returned Rows:\n" +
                            "Row{key1=3, key2=103, value=test-3}\n" +
                            "Row{key1=4, key2=104, value=test-4}")
                    .containsSubsequence("Query took", "seconds to return 2 rows");
        }

        @Test
        void shouldRetryPromptWhenKeyTypeDoesNotMatchSchema() throws Exception {
            // Given
            Schema schema = createSchemaWithKey("key");
            createTable("test-table", schema);

            // When
            in.enterNextPrompts(RANGE_QUERY_OPTION,
                    NO_OPTION, YES_OPTION,
                    "abc",
                    "123", "456",
                    EXIT_OPTION);
            runQueryClient();

            // Then
            assertThat(out.toString())
                    .startsWith("Querying table test-table")
                    .contains(PROMPT_QUERY_TYPE +
                            PROMPT_MIN_INCLUSIVE +
                            PROMPT_MAX_INCLUSIVE +
                            PROMPT_MIN_ROW_KEY_LONG_TYPE +
                            "Failed to convert provided key \"abc\" to type LongType{}\n" +
                            PROMPT_MIN_ROW_KEY_LONG_TYPE +
                            PROMPT_MAX_ROW_KEY_LONG_TYPE +
                            "Returned Rows:\n")
                    .containsSubsequence("Query took", "seconds to return 0 rows");
        }

        @Test
        void shouldRunQueryForTableWhereMultipleTablesArePresent() throws Exception {
            // Given
            Schema schema = createSchemaWithKey("key");
            createTable("test-table-1", schema);
            createTable("test-table-2", schema);

            // When
            in.enterNextPrompts("test-table-2",
                    RANGE_QUERY_OPTION,
                    NO_OPTION, YES_OPTION,
                    "123", "456",
                    EXIT_OPTION);
            runQueryClient();

            // Then
            assertThat(out.toString())
                    .startsWith("The system contains the following tables:\n" +
                            "test-table-1\n" +
                            "test-table-2\n" +
                            "Which table do you wish to query? \n")
                    .contains(PROMPT_QUERY_TYPE +
                            PROMPT_MIN_INCLUSIVE +
                            PROMPT_MAX_INCLUSIVE +
                            PROMPT_MIN_ROW_KEY_LONG_TYPE +
                            PROMPT_MAX_ROW_KEY_LONG_TYPE +
                            "Returned Rows:\n")
                    .containsSubsequence("Query took", "seconds to return 0 rows");
        }
    }

    private TableProperties createTable(String tableName, Schema schema) {
        TableStatus tableStatus = TableStatusTestHelper.uniqueIdAndName(
                TableIdGenerator.fromRandomSeed(0).generateString(), tableName);
        TableProperties tableProperties = createTestTableProperties(instanceProperties, schema);
        tableProperties.set(TABLE_ID, tableStatus.getTableUniqueId());
        tableProperties.set(TABLE_NAME, tableStatus.getTableName());
        tableIndex.create(tableStatus);
        updateStateStore(tableProperties).initialise(tableProperties);
        tablePropertiesList.add(tableProperties);
        return tableProperties;
    }

    private void runQueryClient() throws Exception {
        new QueryClient(instanceProperties, tableIndex, new FixedTablePropertiesProvider(tablePropertiesList),
                in.consoleIn(), out.consoleOut(), ObjectFactory.noUserJars(),
                InMemoryTransactionLogStateStore.createProvider(instanceProperties, transactionLogs),
                new InMemoryLeafPartitionRowRetriever(rowStore))
                .run();
    }

    private void ingestData(TableProperties tableProperties, List<Row> rows) throws Exception {
        FileReference file = fileFactory(tableProperties).rootFile(UUID.randomUUID().toString(), rows.size());
        rowStore.addFile(file.getFilename(), rows);
        updateStateStore(tableProperties).addFile(file);
    }

    private StateStoreUpdatesWrapper updateStateStore(TableProperties tableProperties) {
        return StateStoreUpdatesWrapper.update(stateStoreProvider.getStateStore(tableProperties));
    }

    private FileReferenceFactory fileFactory(TableProperties tableProperties) {
        return FileReferenceFactory.from(instanceProperties, tableProperties, new PartitionsBuilder(tableProperties).singlePartition("root").buildTree());
    }
}
