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

package sleeper.ingest.runner;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.io.TempDir;

import sleeper.core.iterator.IteratorCreationException;
import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.table.TableProperties;
import sleeper.core.row.Row;
import sleeper.core.schema.Field;
import sleeper.core.schema.Schema;
import sleeper.core.schema.type.LongType;
import sleeper.core.statestore.FileReference;
import sleeper.core.statestore.StateStore;
import sleeper.core.statestore.testutils.FixedStateStoreProvider;
import sleeper.ingest.core.IngestResult;
import sleeper.ingest.runner.testutils.IngestRowsTestDataHelper;
import sleeper.sketches.store.LocalFileSystemSketchesStore;
import sleeper.sketches.store.SketchesStore;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.nio.file.Files.createTempDirectory;
import static sleeper.ingest.runner.testutils.IngestRowsTestDataHelper.defaultInstanceProperties;
import static sleeper.ingest.runner.testutils.IngestRowsTestDataHelper.defaultTableProperties;
import static sleeper.ingest.runner.testutils.IngestRowsTestDataHelper.readRowsFromParquetFile;
import static sleeper.ingest.runner.testutils.IngestRowsTestDataHelper.schemaWithRowKeys;

public class IngestRowsTestBase {
    @TempDir
    public Path tempDir;

    protected final Field field = new Field("key", new LongType());
    protected Schema schema = schemaWithRowKeys(field);
    protected String inputFolderName;
    protected String dataFolderName;
    protected InstanceProperties instanceProperties;
    protected TableProperties tableProperties;
    protected final SketchesStore sketchesStore = new LocalFileSystemSketchesStore();

    @BeforeEach
    public void setUpBase() throws Exception {
        inputFolderName = createTempDirectory(tempDir, null).toString();
        dataFolderName = createTempDirectory(tempDir, null).toString();
        instanceProperties = defaultInstanceProperties(dataFolderName);
        tableProperties = defaultTableProperties(schema, instanceProperties);
    }

    protected void setSchema(Schema schema) {
        tableProperties.setSchema(schema);
        this.schema = schema;
    }

    protected IngestResult ingestRows(StateStore stateStore, List<Row> rows) throws Exception {
        IngestFactory factory = createIngestFactory(stateStore);

        IngestRows ingestRows = factory.createIngestRows(tableProperties);
        ingestRows.init();
        for (Row row : rows) {
            ingestRows.write(row);
        }
        return ingestRows.close();
    }

    protected IngestResult ingestFromRowIterator(StateStore stateStore, Iterator<Row> iterator) throws IteratorCreationException, IOException {
        IngestFactory factory = createIngestFactory(stateStore);
        return factory.ingestFromRowIterator(tableProperties, iterator);
    }

    private IngestFactory createIngestFactory(StateStore stateStore) {
        return IngestRowsTestDataHelper.createIngestFactory(inputFolderName,
                FixedStateStoreProvider.singleTable(tableProperties, stateStore), instanceProperties);
    }

    protected static List<Row> readRows(FileReference fileReference, Schema schema) throws Exception {
        return readRowsFromParquetFile(fileReference.getFilename(), schema);
    }

    protected List<Row> readRows(FileReference... fileReferences) {
        return readRows(Stream.of(fileReferences).map(FileReference::getFilename));
    }

    protected List<Row> readRows(Stream<String> filenames) {
        return filenames.map(filename -> {
            try {
                return readRowsFromParquetFile(filename, schema);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        })
                .flatMap(List::stream)
                .collect(Collectors.toList());
    }
}
