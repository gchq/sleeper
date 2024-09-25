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

package sleeper.ingest;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.io.TempDir;

import sleeper.core.iterator.IteratorCreationException;
import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.table.TableProperties;
import sleeper.core.record.Record;
import sleeper.core.schema.Field;
import sleeper.core.schema.Schema;
import sleeper.core.schema.type.LongType;
import sleeper.core.statestore.FileReference;
import sleeper.core.statestore.StateStore;
import sleeper.core.statestore.StateStoreException;
import sleeper.core.statestore.testutils.FixedStateStoreProvider;
import sleeper.ingest.testutils.IngestRecordsTestDataHelper;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Iterator;
import java.util.List;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.nio.file.Files.createTempDirectory;
import static sleeper.ingest.testutils.IngestRecordsTestDataHelper.defaultInstanceProperties;
import static sleeper.ingest.testutils.IngestRecordsTestDataHelper.defaultTableProperties;
import static sleeper.ingest.testutils.IngestRecordsTestDataHelper.readRecordsFromParquetFile;
import static sleeper.ingest.testutils.IngestRecordsTestDataHelper.schemaWithRowKeys;

public class IngestRecordsTestBase {
    @TempDir
    public Path tempDir;

    protected final Field field = new Field("key", new LongType());
    protected final Schema schema = schemaWithRowKeys(field);
    protected String inputFolderName;
    protected String dataFolderName;
    protected InstanceProperties instanceProperties;
    protected TableProperties tableProperties;

    @BeforeEach
    public void setUpBase() throws Exception {
        inputFolderName = createTempDirectory(tempDir, null).toString();
        dataFolderName = createTempDirectory(tempDir, null).toString();
        instanceProperties = defaultInstanceProperties(dataFolderName);
        tableProperties = defaultTableProperties(schema, instanceProperties);
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

        instancePropertiesConfig.accept(instanceProperties);
        tableProperties.setSchema(schema);
        tablePropertiesConfig.accept(tableProperties);
        return ingestRecords(stateStore, records);
    }

    protected IngestResult ingestRecords(StateStore stateStore, List<Record> records) throws Exception {
        IngestFactory factory = createIngestFactory(stateStore);

        IngestRecords ingestRecords = factory.createIngestRecords(tableProperties);
        ingestRecords.init();
        for (Record record : records) {
            ingestRecords.write(record);
        }
        return ingestRecords.close();
    }

    protected IngestResult ingestFromRecordIterator(Schema schema, StateStore stateStore, Iterator<Record> iterator) throws StateStoreException, IteratorCreationException, IOException {
        tableProperties.setSchema(schema);
        IngestFactory factory = createIngestFactory(stateStore);
        return factory.ingestFromRecordIterator(tableProperties, iterator);
    }

    protected IngestResult ingestFromRecordIterator(StateStore stateStore, Iterator<Record> iterator) throws StateStoreException, IteratorCreationException, IOException {
        IngestFactory factory = createIngestFactory(stateStore);
        return factory.ingestFromRecordIterator(tableProperties, iterator);
    }

    private IngestFactory createIngestFactory(StateStore stateStore) {
        return IngestRecordsTestDataHelper.createIngestFactory(inputFolderName,
                new FixedStateStoreProvider(tableProperties, stateStore), instanceProperties);
    }

    protected static List<Record> readRecords(FileReference fileReference, Schema schema) throws Exception {
        return readRecordsFromParquetFile(fileReference.getFilename(), schema);
    }

    protected List<Record> readRecords(FileReference... fileReferences) {
        return readRecords(Stream.of(fileReferences).map(FileReference::getFilename));
    }

    protected List<Record> readRecords(Stream<String> filenames) {
        return filenames.map(filename -> {
            try {
                return readRecordsFromParquetFile(filename, schema);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        })
                .flatMap(List::stream)
                .collect(Collectors.toList());
    }
}
