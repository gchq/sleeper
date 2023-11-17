/*
 * Copyright 2022-2023 Crown Copyright
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
import org.junit.jupiter.api.io.TempDir;

import sleeper.clients.testutil.TestConsoleInput;
import sleeper.clients.testutil.ToStringPrintStream;
import sleeper.configuration.jars.ObjectFactory;
import sleeper.configuration.properties.instance.InstanceProperties;
import sleeper.configuration.properties.table.FixedTablePropertiesProvider;
import sleeper.configuration.properties.table.TableProperties;
import sleeper.core.iterator.IteratorException;
import sleeper.core.record.Record;
import sleeper.core.schema.Field;
import sleeper.core.schema.Schema;
import sleeper.core.schema.type.LongType;
import sleeper.core.schema.type.StringType;
import sleeper.core.statestore.StateStore;
import sleeper.core.statestore.StateStoreException;
import sleeper.core.statestore.inmemory.StateStoreTestHelper;
import sleeper.core.table.InMemoryTableIndex;
import sleeper.core.table.TableIdGenerator;
import sleeper.core.table.TableIdentity;
import sleeper.core.table.TableIndex;
import sleeper.ingest.IngestFactory;
import sleeper.ingest.testutils.IngestRecordsTestDataHelper;
import sleeper.statestore.FixedStateStoreProvider;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Iterator;
import java.util.List;

import static java.nio.file.Files.createTempDirectory;
import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.clients.QueryClientTestConstants.PROMPT_EXACT_KEY_LONG_TYPE;
import static sleeper.clients.QueryClientTestConstants.PROMPT_MAX_INCLUSIVE;
import static sleeper.clients.QueryClientTestConstants.PROMPT_MAX_ROW_KEY_LONG_TYPE;
import static sleeper.clients.QueryClientTestConstants.PROMPT_MIN_INCLUSIVE;
import static sleeper.clients.QueryClientTestConstants.PROMPT_MIN_ROW_KEY_LONG_TYPE;
import static sleeper.clients.QueryClientTestConstants.PROMPT_QUERY_TYPE;
import static sleeper.configuration.properties.InstancePropertiesTestHelper.createTestInstanceProperties;
import static sleeper.configuration.properties.instance.CdkDefinedInstanceProperty.DATA_BUCKET;
import static sleeper.configuration.properties.instance.CommonProperty.FILE_SYSTEM;
import static sleeper.configuration.properties.instance.IngestProperty.INGEST_PARTITION_FILE_WRITER_TYPE;
import static sleeper.configuration.properties.table.TablePropertiesTestHelper.createTestTableProperties;
import static sleeper.configuration.properties.table.TableProperty.COMPRESSION_CODEC;
import static sleeper.configuration.properties.table.TableProperty.TABLE_ID;
import static sleeper.configuration.properties.table.TableProperty.TABLE_NAME;
import static sleeper.core.schema.SchemaTestHelper.schemaWithKey;

public class QueryClientIT {
    @TempDir
    private Path tempDir;
    private InstanceProperties instanceProperties;
    private final TableIndex tableIndex = new InMemoryTableIndex();

    private final ToStringPrintStream out = new ToStringPrintStream();
    private final TestConsoleInput in = new TestConsoleInput(out.consoleOut());

    @BeforeEach
    void setUp() throws Exception {
        instanceProperties = createInstanceProperties();
    }

    @Test
    void shouldReturnNoRecordsWhenTableIsEmpty() throws StateStoreException {
        // Given
        Schema schema = schemaWithKey("key");
        TableProperties tableProperties = createTable("test-table", schema);
        StateStore stateStore = StateStoreTestHelper.inMemoryStateStoreWithSinglePartition(schema);

        // When
        in.enterNextPrompts("r", "n", "n", "123", "456", "");
        createQueryClient(tableProperties, stateStore).run();

        // Then
        assertThat(out.toString())
                .startsWith("Querying table test-table")
                .contains(PROMPT_QUERY_TYPE +
                        PROMPT_MIN_INCLUSIVE +
                        PROMPT_MAX_INCLUSIVE +
                        PROMPT_MIN_ROW_KEY_LONG_TYPE +
                        PROMPT_MAX_ROW_KEY_LONG_TYPE +
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
        StateStore stateStore = StateStoreTestHelper.inMemoryStateStoreWithSinglePartition(schema);
        Record record = new Record();
        record.put("key", 123L);
        record.put("value", "abc");
        ingestData(instanceProperties, stateStore, tableProperties, List.of(record).iterator());

        // When
        in.enterNextPrompts("e", "123", "");
        createQueryClient(tableProperties, stateStore).run();

        // Then
        assertThat(out.toString())
                .startsWith("Querying table test-table")
                .contains(PROMPT_QUERY_TYPE +
                        PROMPT_EXACT_KEY_LONG_TYPE +
                        "Returned Records:\n" +
                        "Record{key=123, value=abc}")
                .containsSubsequence("Query took", "seconds to return 1 records");
    }

    private InstanceProperties createInstanceProperties() throws Exception {
        String dataDir = createTempDirectory(tempDir, null).toString();
        InstanceProperties instanceProperties = createTestInstanceProperties();
        instanceProperties.set(FILE_SYSTEM, "file://");
        instanceProperties.set(DATA_BUCKET, dataDir);
        instanceProperties.set(INGEST_PARTITION_FILE_WRITER_TYPE, "direct");
        return instanceProperties;
    }

    private TableProperties createTable(String tableName, Schema schema) {
        TableIdentity tableIdentity = TableIdentity.uniqueIdAndName(
                TableIdGenerator.fromRandomSeed(0).generateString(), tableName);
        TableProperties tableProperties = createTestTableProperties(instanceProperties, schema);
        tableProperties.set(TABLE_ID, tableIdentity.getTableUniqueId());
        tableProperties.set(TABLE_NAME, tableIdentity.getTableName());
        tableIndex.create(tableIdentity);
        return tableProperties;
    }

    private QueryClient createQueryClient(TableProperties tableProperties, StateStore stateStore) {
        return new QueryClient(instanceProperties, tableIndex, new FixedTablePropertiesProvider(tableProperties),
                in.consoleIn(), out.consoleOut(),
                ObjectFactory.noUserJars(), new FixedStateStoreProvider(tableProperties, stateStore));
    }

    private void ingestData(InstanceProperties instanceProperties, StateStore stateStore,
                            TableProperties tableProperties, Iterator<Record> recordIterator) throws IOException, StateStoreException, IteratorException {
        tableProperties.set(COMPRESSION_CODEC, "snappy");
        IngestFactory factory = IngestRecordsTestDataHelper.createIngestFactory(tempDir.toString(),
                new FixedStateStoreProvider(tableProperties, stateStore), instanceProperties);
        factory.ingestFromRecordIterator(tableProperties, recordIterator);
    }
}
