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
package sleeper.compaction.job.execution.testutils;

import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.io.TempDir;

import sleeper.compaction.job.CompactionJobFactory;
import sleeper.compaction.job.execution.DefaultSelector;
import sleeper.configuration.jars.ObjectFactory;
import sleeper.configuration.properties.instance.InstanceProperties;
import sleeper.configuration.properties.table.FixedTablePropertiesProvider;
import sleeper.configuration.properties.table.TableProperties;
import sleeper.configuration.statestore.FixedStateStoreProvider;
import sleeper.core.record.Record;
import sleeper.core.schema.Schema;
import sleeper.core.statestore.FileReference;
import sleeper.core.statestore.StateStore;
import sleeper.ingest.IngestFactory;
import sleeper.ingest.IngestResult;

import java.nio.file.Path;
import java.util.List;
import java.util.function.Consumer;

import static java.nio.file.Files.createTempDirectory;
import static sleeper.configuration.properties.InstancePropertiesTestHelper.createTestInstanceProperties;
import static sleeper.configuration.properties.instance.CdkDefinedInstanceProperty.DATA_BUCKET;
import static sleeper.configuration.properties.instance.CommonProperty.FILE_SYSTEM;
import static sleeper.configuration.properties.instance.DefaultProperty.DEFAULT_INGEST_PARTITION_FILE_WRITER_TYPE;
import static sleeper.configuration.properties.table.TablePropertiesTestHelper.createTestTableProperties;
import static sleeper.core.schema.SchemaTestHelper.schemaWithKey;
import static sleeper.core.statestore.inmemory.StateStoreTestHelper.inMemoryStateStoreWithNoPartitions;

public class CompactSortedFilesTestBase {
    public static final String DEFAULT_TASK_ID = "task-id";
    @TempDir
    public Path tempDir;
    protected String dataFolderName;
    protected final InstanceProperties instanceProperties = createTestInstanceProperties();
    protected final TableProperties tableProperties = createTestTableProperties(instanceProperties, schemaWithKey("key"));
    protected final StateStore stateStore = inMemoryStateStoreWithNoPartitions();

    @BeforeEach
    public void setUpBase() throws Exception {
        dataFolderName = createTempDirectory(tempDir, null).toString();
        instanceProperties.set(FILE_SYSTEM, "file://");
        instanceProperties.set(DATA_BUCKET, dataFolderName);
        instanceProperties.set(DEFAULT_INGEST_PARTITION_FILE_WRITER_TYPE, "direct");
    }

    protected CompactionJobFactory compactionFactory() {
        return new CompactionJobFactory(instanceProperties, tableProperties);
    }

    protected DefaultSelector createCompactionSelector(Schema schema, Configuration configuration) throws Exception {
        tableProperties.setSchema(schema);
        return new DefaultSelector(new FixedTablePropertiesProvider(tableProperties),
                new FixedStateStoreProvider(tableProperties, stateStore),
                ObjectFactory.noUserJars(),
                configuration);
    }

    protected FileReference ingestRecordsGetFile(List<Record> records) throws Exception {
        return ingestRecordsGetFile(records, builder -> {
        });
    }

    protected FileReference ingestRecordsGetFile(List<Record> records, Consumer<IngestFactory.Builder> config) throws Exception {
        String localDir = createTempDirectory(tempDir, null).toString();
        IngestFactory.Builder builder = IngestFactory.builder()
                .objectFactory(ObjectFactory.noUserJars())
                .localDir(localDir)
                .stateStoreProvider(new FixedStateStoreProvider(tableProperties, stateStore))
                .instanceProperties(instanceProperties);
        config.accept(builder);
        IngestResult result = builder.build().ingestFromRecordIterator(tableProperties, records.iterator());
        List<FileReference> files = result.getFileReferenceList();
        if (files.size() != 1) {
            throw new IllegalStateException("Expected 1 file ingested, found: " + files);
        }
        return files.get(0);
    }
}
