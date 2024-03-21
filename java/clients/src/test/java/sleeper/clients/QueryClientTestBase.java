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
import org.junit.jupiter.api.io.TempDir;

import sleeper.clients.testutil.TestConsoleInput;
import sleeper.clients.testutil.ToStringPrintStream;
import sleeper.configuration.properties.instance.InstanceProperties;
import sleeper.configuration.properties.table.TableProperties;
import sleeper.core.record.Record;
import sleeper.core.schema.Schema;
import sleeper.core.statestore.StateStore;
import sleeper.core.table.InMemoryTableIndex;
import sleeper.core.table.TableIdGenerator;
import sleeper.core.table.TableIndex;
import sleeper.core.table.TableStatus;
import sleeper.core.table.TableStatusTestHelper;
import sleeper.ingest.IngestFactory;
import sleeper.ingest.testutils.IngestRecordsTestDataHelper;
import sleeper.statestore.FixedStateStoreProvider;

import java.nio.file.Path;
import java.util.Iterator;

import static java.nio.file.Files.createTempDirectory;
import static sleeper.configuration.properties.InstancePropertiesTestHelper.createTestInstanceProperties;
import static sleeper.configuration.properties.instance.CdkDefinedInstanceProperty.DATA_BUCKET;
import static sleeper.configuration.properties.instance.CommonProperty.FILE_SYSTEM;
import static sleeper.configuration.properties.instance.DefaultProperty.DEFAULT_INGEST_PARTITION_FILE_WRITER_TYPE;
import static sleeper.configuration.properties.table.TablePropertiesTestHelper.createTestTableProperties;
import static sleeper.configuration.properties.table.TableProperty.COMPRESSION_CODEC;
import static sleeper.configuration.properties.table.TableProperty.TABLE_ID;
import static sleeper.configuration.properties.table.TableProperty.TABLE_NAME;

public class QueryClientTestBase {
    @TempDir
    protected Path tempDir;
    protected InstanceProperties instanceProperties;
    protected final TableIndex tableIndex = new InMemoryTableIndex();
    protected final ToStringPrintStream out = new ToStringPrintStream();
    protected final TestConsoleInput in = new TestConsoleInput(out.consoleOut());

    @BeforeEach
    void setUp() throws Exception {
        instanceProperties = createInstanceProperties(tempDir);
    }

    protected static InstanceProperties createInstanceProperties(Path tempDir) throws Exception {
        String dataDir = createTempDirectory(tempDir, null).toString();
        InstanceProperties instanceProperties = createTestInstanceProperties();
        instanceProperties.set(FILE_SYSTEM, "file://");
        instanceProperties.set(DATA_BUCKET, dataDir);
        instanceProperties.set(DEFAULT_INGEST_PARTITION_FILE_WRITER_TYPE, "direct");
        return instanceProperties;
    }

    protected TableProperties createTable(String tableName, Schema schema) {
        TableStatus tableStatus = TableStatusTestHelper.uniqueIdAndName(
                TableIdGenerator.fromRandomSeed(0).generateString(), tableName);
        TableProperties tableProperties = createTestTableProperties(instanceProperties, schema);
        tableProperties.set(TABLE_ID, tableStatus.getTableUniqueId());
        tableProperties.set(TABLE_NAME, tableStatus.getTableName());
        tableIndex.create(tableStatus);
        return tableProperties;
    }

    protected void ingestData(TableProperties tableProperties, StateStore stateStore, Iterator<Record> recordIterator) throws Exception {
        tableProperties.set(COMPRESSION_CODEC, "snappy");
        IngestFactory factory = IngestRecordsTestDataHelper.createIngestFactory(tempDir.toString(),
                new FixedStateStoreProvider(tableProperties, stateStore), instanceProperties);
        factory.ingestFromRecordIterator(tableProperties, recordIterator);
    }
}
