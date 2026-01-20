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
package sleeper.bulkimport.runner;

import org.apache.parquet.hadoop.ParquetWriter;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.io.TempDir;

import sleeper.core.partition.Partition;
import sleeper.core.partition.PartitionsBuilder;
import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.table.TableProperties;
import sleeper.core.properties.table.TablePropertiesStore;
import sleeper.core.properties.testutils.InMemoryTableProperties;
import sleeper.core.row.Row;
import sleeper.core.schema.Schema;
import sleeper.core.statestore.StateStore;
import sleeper.core.statestore.StateStoreProvider;
import sleeper.core.statestore.testutils.InMemoryTransactionLogStateStore;
import sleeper.core.statestore.testutils.InMemoryTransactionLogsPerTable;
import sleeper.parquet.row.ParquetRowWriterFactory;
import sleeper.sketches.testutils.SketchesDeciles;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.BULK_IMPORT_BUCKET;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.DATA_BUCKET;
import static sleeper.core.properties.instance.CommonProperty.FILE_SYSTEM;
import static sleeper.core.properties.testutils.InstancePropertiesTestHelper.createTestInstanceProperties;
import static sleeper.core.properties.testutils.TablePropertiesTestHelper.createTestTableProperties;
import static sleeper.core.statestore.testutils.StateStoreUpdatesWrapper.update;

/**
 * A base for tests running against Spark, holding files in the local file system.
 */
public abstract class SparkTestBase {
    protected final InstanceProperties instanceProperties = createTestInstanceProperties();
    private final TablePropertiesStore tablePropertiesStore = InMemoryTableProperties.getStoreReturningExactInstance();
    private final StateStoreProvider stateStoreProvider = InMemoryTransactionLogStateStore.createProvider(instanceProperties, new InMemoryTransactionLogsPerTable());
    private TableProperties lastTable;
    @TempDir
    public Path tempDir;
    private int nextFileNumber = 1;

    @BeforeEach
    void setUpBase() {
        instanceProperties.set(DATA_BUCKET, createDir("data"));
        instanceProperties.set(FILE_SYSTEM, "file://");
        instanceProperties.set(BULK_IMPORT_BUCKET, createDir("bulk-import"));
    }

    private String createDir(String name) {
        Path path = tempDir.resolve(name);
        try {
            Files.createDirectories(path);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        return path.toString();
    }

    @BeforeAll
    public static void setSparkProperties() {
        System.setProperty("spark.master", "local");
        System.setProperty("spark.app.name", "bulk import");
    }

    @AfterAll
    public static void clearSparkProperties() {
        System.clearProperty("spark.master");
        System.clearProperty("spark.app.name");
    }

    protected TableProperties createTable(Schema schema) {
        TableProperties tableProperties = createTestTableProperties(instanceProperties, schema);
        createTable(tableProperties);
        return tableProperties;
    }

    protected void createTable(TableProperties tableProperties) {
        tablePropertiesStore.createTable(tableProperties);
        update(stateStoreProvider.getStateStore(tableProperties)).initialise(tableProperties);
        lastTable = tableProperties;
    }

    protected PartitionsBuilder partitionsBuilder() {
        return new PartitionsBuilder(lastTable);
    }

    protected StateStore stateStore() {
        return stateStore(lastTable);
    }

    protected StateStore stateStore(TableProperties tableProperties) {
        return stateStoreProvider.getStateStore(tableProperties);
    }

    protected BulkImportSparkContext createBulkImportContext(List<String> filenames) {
        return createBulkImportContext(lastTable, filenames);
    }

    protected SketchesDeciles expectedSketchDeciles(List<Row> rows) {
        return SketchesDeciles.from(lastTable, rows);
    }

    protected BulkImportSparkContext createBulkImportContext(TableProperties tableProperties, List<String> filenames) {
        List<Partition> partitions = stateStoreProvider.getStateStore(tableProperties).getAllPartitions();
        return BulkImportSparkContext.create(
                instanceProperties, tableProperties, partitions, filenames);
    }

    protected String writeRowsToFile(List<Row> rows) {
        String filename = generateFilename();
        try (ParquetWriter<Row> writer = ParquetRowWriterFactory.createParquetRowWriter(
                new org.apache.hadoop.fs.Path(filename), lastTable.getSchema())) {
            for (Row row : rows) {
                writer.write(row);
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        return filename;
    }

    private String generateFilename() {
        String filename = tempDir + "/input/file-" + nextFileNumber + ".parquet";
        nextFileNumber++;
        return filename;
    }

}
