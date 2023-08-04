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

package sleeper.ingest;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.io.TempDir;

import sleeper.configuration.properties.instance.InstanceProperties;
import sleeper.configuration.properties.table.TableProperties;
import sleeper.core.iterator.IteratorException;
import sleeper.core.record.Record;
import sleeper.core.schema.Field;
import sleeper.core.schema.Schema;
import sleeper.core.schema.type.LongType;
import sleeper.core.statestore.StateStore;
import sleeper.core.statestore.StateStoreException;
import sleeper.ingest.testutils.IngestRecordsTestDataHelper;
import sleeper.statestore.FixedStateStoreProvider;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Iterator;
import java.util.List;
import java.util.function.Consumer;

import static java.nio.file.Files.createTempDirectory;
import static sleeper.ingest.testutils.IngestRecordsTestDataHelper.TEST_TABLE_NAME;
import static sleeper.ingest.testutils.IngestRecordsTestDataHelper.defaultInstanceProperties;
import static sleeper.ingest.testutils.IngestRecordsTestDataHelper.schemaWithRowKeys;

public class IngestRecordsTestBase {
    @TempDir
    public Path folder;

    protected final Field field = new Field("key", new LongType());
    protected final Schema schema = schemaWithRowKeys(field);
    protected String inputFolderName;
    protected String sketchFolderName;

    @BeforeEach
    public void setUpBase() throws Exception {
        inputFolderName = createTempDirectory(folder, null).toString();
        sketchFolderName = createTempDirectory(folder, null).toString();
    }

    protected IngestResult ingestRecords(Schema schema, StateStore stateStore, List<Record> records) throws Exception {
        return ingestRecords(schema, stateStore, records, instanceProperties -> {
        }, tableProperties -> {
        });
    }

    protected IngestResult ingestRecordsWithTableProperties(
            Schema schema, StateStore stateStore, List<Record> records,
            Consumer<TableProperties> tablePropertiesConfig) throws Exception {
        return ingestRecords(schema, stateStore, records, instanceProperties -> {
        }, tablePropertiesConfig);
    }

    protected IngestResult ingestRecordsWithInstanceProperties(
            Schema schema, StateStore stateStore, List<Record> records,
            Consumer<InstanceProperties> instancePropertiesConfig) throws Exception {
        return ingestRecords(schema, stateStore, records, instancePropertiesConfig, tableProperties -> {
        });
    }

    protected IngestResult ingestRecords(
            Schema schema, StateStore stateStore, List<Record> records,
            Consumer<InstanceProperties> instancePropertiesConfig,
            Consumer<TableProperties> tablePropertiesConfig) throws Exception {

        InstanceProperties instanceProperties = defaultInstanceProperties();
        instancePropertiesConfig.accept(instanceProperties);
        TableProperties tableProperties = defaultTableProperties(schema, instanceProperties);
        tablePropertiesConfig.accept(tableProperties);
        IngestFactory factory = createIngestFactory(stateStore, tableProperties, instanceProperties);

        IngestRecords ingestRecords = factory.createIngestRecords(tableProperties);
        ingestRecords.init();
        for (Record record : records) {
            ingestRecords.write(record);
        }
        return ingestRecords.close();
    }

    protected IngestResult ingestFromRecordIterator(Schema schema, StateStore stateStore, Iterator<Record> iterator)
            throws StateStoreException, IteratorException, IOException {
        InstanceProperties instanceProperties = defaultInstanceProperties();
        TableProperties tableProperties = defaultTableProperties(schema, instanceProperties);
        IngestFactory factory = createIngestFactory(stateStore, tableProperties, instanceProperties);
        return factory.ingestFromRecordIterator(tableProperties, iterator);
    }

    private TableProperties defaultTableProperties(Schema schema, InstanceProperties instanceProperties) {
        return IngestRecordsTestDataHelper.defaultTableProperties(schema, TEST_TABLE_NAME, sketchFolderName, instanceProperties);
    }

    private IngestFactory createIngestFactory(StateStore stateStore, TableProperties tableProperties, InstanceProperties instanceProperties) {
        return IngestRecordsTestDataHelper.createIngestFactory(inputFolderName, new FixedStateStoreProvider(tableProperties, stateStore), instanceProperties);
    }
}
