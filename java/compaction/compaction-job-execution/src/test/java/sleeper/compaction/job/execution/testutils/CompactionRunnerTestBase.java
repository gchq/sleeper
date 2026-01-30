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
package sleeper.compaction.job.execution.testutils;

import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.io.TempDir;

import sleeper.compaction.core.job.CompactionJob;
import sleeper.compaction.core.job.CompactionJobFactory;
import sleeper.compaction.core.job.CompactionRunner;
import sleeper.compaction.core.task.CompactionTaskTestHelper;
import sleeper.compaction.job.execution.DefaultCompactionRunnerFactory;
import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.model.DataEngine;
import sleeper.core.properties.table.TableProperties;
import sleeper.core.properties.testutils.FixedTablePropertiesProvider;
import sleeper.core.row.Row;
import sleeper.core.schema.Schema;
import sleeper.core.statestore.FileReference;
import sleeper.core.statestore.FileReferenceFactory;
import sleeper.core.statestore.StateStore;
import sleeper.core.statestore.testutils.FixedStateStoreProvider;
import sleeper.core.statestore.testutils.InMemoryTransactionLogStateStore;
import sleeper.core.statestore.testutils.InMemoryTransactionLogs;
import sleeper.core.tracker.compaction.job.CompactionJobTracker;
import sleeper.core.tracker.compaction.job.CompactionJobTrackerTestHelper;
import sleeper.core.tracker.compaction.job.InMemoryCompactionJobTracker;
import sleeper.core.tracker.job.run.RowsProcessed;
import sleeper.core.util.ObjectFactory;
import sleeper.ingest.core.IngestResult;
import sleeper.ingest.runner.IngestFactory;
import sleeper.parquet.utils.HadoopConfigurationProvider;
import sleeper.sketches.Sketches;
import sleeper.sketches.store.LocalFileSystemSketchesStore;
import sleeper.sketches.store.SketchesStore;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import java.util.function.Consumer;

import static java.nio.file.Files.createTempDirectory;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.DATA_BUCKET;
import static sleeper.core.properties.instance.CommonProperty.FILE_SYSTEM;
import static sleeper.core.properties.instance.TableDefaultProperty.DEFAULT_DATA_ENGINE;
import static sleeper.core.properties.instance.TableDefaultProperty.DEFAULT_INGEST_PARTITION_FILE_WRITER_TYPE;
import static sleeper.core.properties.testutils.InstancePropertiesTestHelper.createTestInstanceProperties;
import static sleeper.core.properties.testutils.TablePropertiesTestHelper.createTestTablePropertiesWithNoSchema;

public class CompactionRunnerTestBase {
    public static final String DEFAULT_TASK_ID = "task-id";
    @TempDir
    public Path tempDir;
    protected String dataFolderName;
    protected final InstanceProperties instanceProperties = createTestInstanceProperties();
    protected final TableProperties tableProperties = createTestTablePropertiesWithNoSchema(instanceProperties);
    protected StateStore stateStore = InMemoryTransactionLogStateStore.create(tableProperties, new InMemoryTransactionLogs());
    protected CompactionJobTracker jobTracker = new InMemoryCompactionJobTracker();

    @BeforeEach
    public void setUpBase() throws Exception {
        dataFolderName = createTempDirectory(tempDir, null).toString();
        instanceProperties.set(FILE_SYSTEM, "file://");
        instanceProperties.set(DATA_BUCKET, dataFolderName);
        instanceProperties.set(DEFAULT_INGEST_PARTITION_FILE_WRITER_TYPE, "direct");
        instanceProperties.set(DEFAULT_DATA_ENGINE, DataEngine.JAVA.toString());
        stateStore.fixFileUpdateTime(null);
    }

    protected CompactionJobFactory compactionFactory() {
        return new CompactionJobFactory(instanceProperties, tableProperties);
    }

    protected void runTask(CompactionJob job) throws Exception {
        runTask(job, HadoopConfigurationProvider.getConfigurationForECS(instanceProperties));
    }

    protected void runTask(CompactionJob job, Configuration hadoopConf) throws Exception {
        DefaultCompactionRunnerFactory selector = new DefaultCompactionRunnerFactory(ObjectFactory.noUserJars(), hadoopConf, createSketchesStore());
        CompactionRunner runner = selector.createCompactor(job, tableProperties);
        compactionTaskTestHelper().runTask(runner, List.of(job));
    }

    protected FileReference outputFileReference(CompactionJob job, long numberOfRows) {
        return FileReferenceFactory.from(stateStore).partitionFile(job.getPartitionId(), job.getOutputFile(), numberOfRows);
    }

    private CompactionTaskTestHelper compactionTaskTestHelper() {
        return new CompactionTaskTestHelper(
                instanceProperties, new FixedTablePropertiesProvider(tableProperties),
                FixedStateStoreProvider.singleTable(tableProperties, stateStore), jobTracker);
    }

    protected RowsProcessed getRowsProcessed(CompactionJob job) {
        return CompactionJobTrackerTestHelper.getRowsProcessed(jobTracker, job.getId());
    }

    protected SketchesStore createSketchesStore() {
        return new LocalFileSystemSketchesStore();
    }

    protected FileReference ingestRowsGetFile(List<Row> rows) throws Exception {
        return ingestRowsGetFile(rows, builder -> {
        });
    }

    protected FileReference ingestRowsGetFile(List<Row> rows, Consumer<IngestFactory.Builder> config) throws Exception {
        String localDir = createTempDirectory(tempDir, null).toString();
        IngestFactory.Builder builder = IngestFactory.builder()
                .objectFactory(ObjectFactory.noUserJars())
                .localDir(localDir)
                .stateStoreProvider(FixedStateStoreProvider.singleTable(tableProperties, stateStore))
                .instanceProperties(instanceProperties);
        config.accept(builder);
        IngestResult result = builder.build().ingestFromRowIterator(tableProperties, rows.iterator());
        List<FileReference> files = result.getFileReferenceList();
        if (files.size() != 1) {
            throw new IllegalStateException("Expected 1 file ingested, found: " + files);
        }
        return files.get(0);
    }

    protected Sketches readSketches(Schema schema, String filename) throws IOException {
        return createSketchesStore().loadFileSketches(filename, schema);
    }
}
