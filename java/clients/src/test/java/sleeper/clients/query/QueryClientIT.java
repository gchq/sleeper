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
import org.junit.jupiter.api.io.TempDir;

import sleeper.clients.testutil.TestConsoleInput;
import sleeper.clients.testutil.ToStringConsoleOutput;
import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.table.TableProperties;
import sleeper.core.properties.testutils.FixedTablePropertiesProvider;
import sleeper.core.record.SleeperRow;
import sleeper.core.schema.Field;
import sleeper.core.schema.Schema;
import sleeper.core.schema.type.LongType;
import sleeper.core.schema.type.StringType;
import sleeper.core.statestore.testutils.InMemoryTransactionLogStateStore;
import sleeper.core.statestore.testutils.InMemoryTransactionLogsPerTable;
import sleeper.core.table.InMemoryTableIndex;
import sleeper.core.table.TableIdGenerator;
import sleeper.core.table.TableIndex;
import sleeper.core.table.TableStatus;
import sleeper.core.table.TableStatusTestHelper;
import sleeper.core.util.ObjectFactory;
import sleeper.ingest.runner.IngestFactory;
import sleeper.ingest.runner.testutils.IngestRecordsTestDataHelper;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.LongStream;

import static java.nio.file.Files.createTempDirectory;
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
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.DATA_BUCKET;
import static sleeper.core.properties.instance.CommonProperty.FILE_SYSTEM;
import static sleeper.core.properties.instance.TableDefaultProperty.DEFAULT_INGEST_PARTITION_FILE_WRITER_TYPE;
import static sleeper.core.properties.table.TableProperty.COMPRESSION_CODEC;
import static sleeper.core.properties.table.TableProperty.TABLE_ID;
import static sleeper.core.properties.table.TableProperty.TABLE_NAME;
import static sleeper.core.properties.testutils.InstancePropertiesTestHelper.createTestInstanceProperties;
import static sleeper.core.properties.testutils.TablePropertiesTestHelper.createTestTableProperties;
import static sleeper.core.schema.SchemaTestHelper.createSchemaWithKey;

public class QueryClientIT {
    @TempDir
    private Path tempDir;
    private InstanceProperties instanceProperties;
    private final TableIndex tableIndex = new InMemoryTableIndex();
    private final ToStringConsoleOutput out = new ToStringConsoleOutput();
    private final TestConsoleInput in = new TestConsoleInput(out.consoleOut());
    private final InMemoryTransactionLogsPerTable transactionLogs = new InMemoryTransactionLogsPerTable();
    private final List<TableProperties> tablePropertiesList = new ArrayList<>();

    @BeforeEach
    void setUp() throws Exception {
        instanceProperties = createInstanceProperties(tempDir);
    }

    @Nested
    @DisplayName("Exact query")
    class ExactQuery {
        @Test
        void shouldReturnNoRecordsWhenExactRecordNotFound() throws Exception {
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
                            "Returned Records:")
                    .containsSubsequence("Query took", "seconds to return 0 records");
        }

        @Test
        void shouldRunExactRecordQuery() throws Exception {
            // Given
            Schema schema = Schema.builder()
                    .rowKeyFields(new Field("key", new LongType()))
                    .valueFields(new Field("value", new StringType()))
                    .build();
            TableProperties tableProperties = createTable("test-table", schema);
            SleeperRow record = new SleeperRow();
            record.put("key", 123L);
            record.put("value", "abc");
            ingestData(tableProperties, List.of(record).iterator());

            // When
            in.enterNextPrompts(EXACT_QUERY_OPTION, "123", EXIT_OPTION);
            runQueryClient();

            // Then
            assertThat(out.toString())
                    .startsWith("Querying table test-table")
                    .contains(PROMPT_QUERY_TYPE +
                            PROMPT_EXACT_KEY_LONG_TYPE +
                            "Returned Records:\n" +
                            "Record{key=123, value=abc}")
                    .containsSubsequence("Query took", "seconds to return 1 records");
        }
    }

    @Nested
    @DisplayName("Range query")
    class RangeQuery {
        @Test
        void shouldRunRangeRecordQuery() throws Exception {
            // Given
            Schema schema = createSchemaWithKey("key");
            TableProperties tableProperties = createTable("test-table", schema);
            List<SleeperRow> records = LongStream.rangeClosed(0, 10)
                    .mapToObj(num -> new SleeperRow(Map.of("key", num)))
                    .collect(Collectors.toList());
            ingestData(tableProperties, records.iterator());

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
                            "Returned Records:\n" +
                            "Record{key=4}\n" +
                            "Record{key=5}\n" +
                            "Record{key=6}")
                    .containsSubsequence("Query took", "seconds to return 3 records");
        }

        @Test
        void shouldDefaultToFullRangeWhenMinMaxPromptsAreIgnored() throws Exception {
            // Given
            Schema schema = createSchemaWithKey("key");
            TableProperties tableProperties = createTable("test-table", schema);
            List<SleeperRow> records = LongStream.rangeClosed(0, 3)
                    .mapToObj(num -> new SleeperRow(Map.of("key", num)))
                    .collect(Collectors.toList());
            ingestData(tableProperties, records.iterator());

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
                            "Returned Records:\n" +
                            "Record{key=0}\n" +
                            "Record{key=1}\n" +
                            "Record{key=2}\n" +
                            "Record{key=3}")
                    .containsSubsequence("Query took", "seconds to return 4 records");
        }

        @Test
        void shouldRunRangeRecordQueryByMultipleKeys() throws Exception {
            // Given
            Schema schema = Schema.builder()
                    .rowKeyFields(new Field("key1", new LongType()), new Field("key2", new LongType()))
                    .valueFields(new Field("value", new StringType()))
                    .build();
            TableProperties tableProperties = createTable("test-table", schema);
            List<SleeperRow> records = LongStream.rangeClosed(0, 10)
                    .mapToObj(num -> new SleeperRow(Map.of(
                            "key1", num,
                            "key2", num + 100L,
                            "value", "test-" + num)))
                    .collect(Collectors.toList());
            ingestData(tableProperties, records.iterator());

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
                            "Returned Records:\n" +
                            "Record{key1=3, key2=103, value=test-3}\n" +
                            "Record{key1=4, key2=104, value=test-4}")
                    .containsSubsequence("Query took", "seconds to return 2 records");
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
                            "Returned Records:\n")
                    .containsSubsequence("Query took", "seconds to return 0 records");
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
                            "Returned Records:\n")
                    .containsSubsequence("Query took", "seconds to return 0 records");
        }
    }

    private static InstanceProperties createInstanceProperties(Path tempDir) throws Exception {
        String dataDir = createTempDirectory(tempDir, null).toString();
        InstanceProperties instanceProperties = createTestInstanceProperties();
        instanceProperties.set(FILE_SYSTEM, "file://");
        instanceProperties.set(DATA_BUCKET, dataDir);
        instanceProperties.set(DEFAULT_INGEST_PARTITION_FILE_WRITER_TYPE, "direct");
        return instanceProperties;
    }

    private TableProperties createTable(String tableName, Schema schema) {
        TableStatus tableStatus = TableStatusTestHelper.uniqueIdAndName(
                TableIdGenerator.fromRandomSeed(0).generateString(), tableName);
        TableProperties tableProperties = createTestTableProperties(instanceProperties, schema);
        tableProperties.set(TABLE_ID, tableStatus.getTableUniqueId());
        tableProperties.set(TABLE_NAME, tableStatus.getTableName());
        tableIndex.create(tableStatus);
        transactionLogs.initialiseTable(tableProperties);
        tablePropertiesList.add(tableProperties);
        return tableProperties;
    }

    private void runQueryClient() throws Exception {
        new QueryClient(instanceProperties, tableIndex, new FixedTablePropertiesProvider(tablePropertiesList),
                in.consoleIn(), out.consoleOut(), ObjectFactory.noUserJars(),
                InMemoryTransactionLogStateStore.createProvider(instanceProperties, transactionLogs))
                .run();
    }

    private void ingestData(TableProperties tableProperties, Iterator<SleeperRow> recordIterator) throws Exception {
        tableProperties.set(COMPRESSION_CODEC, "snappy");
        IngestFactory factory = IngestRecordsTestDataHelper.createIngestFactory(tempDir.toString(),
                InMemoryTransactionLogStateStore.createProvider(instanceProperties, transactionLogs),
                instanceProperties);
        factory.ingestFromRecordIterator(tableProperties, recordIterator);
    }
}
