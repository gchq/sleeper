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
import sleeper.core.util.ObjectFactory;
import sleeper.ingest.core.IngestResult;
import sleeper.localstack.test.LocalStackTestBase;
import sleeper.sketches.store.S3SketchesStore;
import sleeper.sketches.store.SketchesStore;
import sleeper.statestore.transactionlog.DynamoDBTransactionLogStateStore;
import sleeper.statestore.transactionlog.TransactionLogStateStoreCreator;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Iterator;
import java.util.List;

import static java.nio.file.Files.createTempDirectory;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.DATA_BUCKET;
import static sleeper.core.properties.testutils.InstancePropertiesTestHelper.createTestInstanceProperties;
import static sleeper.core.properties.testutils.TablePropertiesTestHelper.createTestTableProperties;
import static sleeper.core.statestore.testutils.StateStoreUpdatesWrapper.update;
import static sleeper.ingest.runner.testutils.IngestRowsTestDataHelper.schemaWithRowKeys;
import static sleeper.ingest.runner.testutils.ResultVerifier.readMergedRowsFromPartitionDataFiles;

public class IngestRowsLocalStackITBase extends LocalStackTestBase {
    @TempDir
    private static Path tempDir;

    private final Field field = new Field("key", new LongType());
    protected Schema schema = schemaWithRowKeys(field);
    protected String ingestLocalFiles;
    private final InstanceProperties instanceProperties = createTestInstanceProperties();
    private final TableProperties tableProperties = createTestTableProperties(instanceProperties, schema);
    protected final SketchesStore sketchesStore = new S3SketchesStore(s3Client, s3TransferManager);

    @BeforeEach
    void setUp() throws Exception {
        ingestLocalFiles = createTempDirectory(tempDir, null).toString();
        new TransactionLogStateStoreCreator(instanceProperties, dynamoClient).create();
        createBucket(instanceProperties.get(DATA_BUCKET));
    }

    protected StateStore initialiseStateStore() {
        StateStore stateStore = DynamoDBTransactionLogStateStore.builderFrom(instanceProperties, tableProperties, dynamoClient, s3Client).build();
        update(stateStore).initialise(tableProperties.getSchema());
        return stateStore;
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

    protected List<Row> readRows(List<FileReference> fileReferences) {
        return readMergedRowsFromPartitionDataFiles(schema, fileReferences, hadoopConf);
    }

    private IngestFactory createIngestFactory(StateStore stateStore) {
        return IngestFactory.builder()
                .objectFactory(ObjectFactory.noUserJars())
                .localDir(ingestLocalFiles)
                .stateStoreProvider(FixedStateStoreProvider.singleTable(tableProperties, stateStore))
                .instanceProperties(instanceProperties)
                .s3AsyncClient(s3AsyncClient)
                .build();
    }

}
