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
package sleeper.garbagecollector;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.hadoop.ParquetWriter;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import sleeper.configuration.properties.instance.InstanceProperties;
import sleeper.configuration.properties.table.FixedTablePropertiesProvider;
import sleeper.configuration.properties.table.TableProperties;
import sleeper.core.partition.PartitionTree;
import sleeper.core.partition.PartitionsBuilder;
import sleeper.core.record.Record;
import sleeper.core.schema.Field;
import sleeper.core.schema.Schema;
import sleeper.core.schema.type.IntType;
import sleeper.core.schema.type.StringType;
import sleeper.core.statestore.FileReference;
import sleeper.core.statestore.FileReferenceFactory;
import sleeper.core.statestore.StateStore;
import sleeper.io.parquet.record.ParquetRecordWriterFactory;
import sleeper.statestore.FixedStateStoreProvider;
import sleeper.statestore.StateStoreProvider;

import java.nio.file.Files;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.configuration.properties.InstancePropertiesTestHelper.createTestInstanceProperties;
import static sleeper.configuration.properties.instance.CdkDefinedInstanceProperty.DATA_BUCKET;
import static sleeper.configuration.properties.instance.CommonProperty.FILE_SYSTEM;
import static sleeper.configuration.properties.instance.GarbageCollectionProperty.GARBAGE_COLLECTOR_BATCH_SIZE;
import static sleeper.configuration.properties.table.TablePropertiesTestHelper.createTestTableProperties;
import static sleeper.configuration.properties.table.TableProperty.GARBAGE_COLLECTOR_DELAY_BEFORE_DELETION;
import static sleeper.configuration.properties.table.TableProperty.TABLE_NAME;
import static sleeper.core.statestore.AllReferencesToAFileTestHelper.fileWithNoReferences;
import static sleeper.core.statestore.AssignJobIdRequest.assignJobOnPartitionToFiles;
import static sleeper.core.statestore.FilesReportTestHelper.activeAndReadyForGCFilesReport;
import static sleeper.core.statestore.FilesReportTestHelper.activeFilesReport;
import static sleeper.core.statestore.inmemory.StateStoreTestHelper.inMemoryStateStoreWithSinglePartition;

public class GarbageCollectorIT {
    private static final Schema TEST_SCHEMA = getSchema();
    private static final String TEST_TABLE_NAME = "test-table";
    private static final String TEST_TABLE_NAME_1 = "test-table-1";
    private static final String TEST_TABLE_NAME_2 = "test-table-2";

    @TempDir
    public java.nio.file.Path tempDir;
    private final PartitionTree partitions = new PartitionsBuilder(TEST_SCHEMA).singlePartition("root").buildTree();
    private final List<TableProperties> tables = new ArrayList<>();

    @Nested
    @DisplayName("Collecting from single table")
    class SingleTable {
        private InstanceProperties instanceProperties;
        private TableProperties tableProperties;
        private StateStoreProvider stateStoreProvider;

        StateStore setupStateStoreAndFixTime(Instant fixedTime) {
            StateStore stateStore = inMemoryStateStoreWithSinglePartition(TEST_SCHEMA);
            stateStore.fixTime(fixedTime);
            stateStoreProvider = new FixedStateStoreProvider(tableProperties, stateStore);
            return stateStore;
        }

        @Test
        void shouldCollectFileWithNoReferencesAfterSpecifiedDelay() throws Exception {
            // Given
            instanceProperties = createInstanceProperties();
            tableProperties = createTableWithGCDelay(TEST_TABLE_NAME, instanceProperties, 10);
            Instant currentTime = Instant.parse("2023-06-28T13:46:00Z");
            Instant oldEnoughTime = currentTime.minus(Duration.ofMinutes(11));
            StateStore stateStore = setupStateStoreAndFixTime(oldEnoughTime);
            java.nio.file.Path oldFile = tempDir.resolve("old-file.parquet");
            java.nio.file.Path newFile = tempDir.resolve("new-file.parquet");
            createFileWithNoReferencesByCompaction(stateStore, oldFile, newFile);

            // When
            createGarbageCollector(instanceProperties, stateStoreProvider).runAtTime(currentTime);

            // Then
            assertThat(Files.exists(oldFile)).isFalse();
            assertThat(stateStore.getAllFilesWithMaxUnreferenced(10))
                    .isEqualTo(activeFilesReport(oldEnoughTime, activeReference(newFile)));
        }

        @Test
        void shouldNotCollectFileMarkedAsActive() throws Exception {
            // Given
            instanceProperties = createInstanceProperties();
            tableProperties = createTableWithGCDelay(TEST_TABLE_NAME, instanceProperties, 10);
            Instant currentTime = Instant.parse("2023-06-28T13:46:00Z");
            Instant oldEnoughTime = currentTime.minus(Duration.ofMinutes(11));
            StateStore stateStore = setupStateStoreAndFixTime(oldEnoughTime);
            java.nio.file.Path filePath = tempDir.resolve("test-file.parquet");
            createActiveFile(filePath, stateStore);

            // When
            createGarbageCollector(instanceProperties, stateStoreProvider).runAtTime(currentTime);

            // Then
            assertThat(Files.exists(filePath)).isTrue();
            assertThat(stateStore.getAllFilesWithMaxUnreferenced(10))
                    .isEqualTo(activeFilesReport(oldEnoughTime, activeReference(filePath)));
        }

        @Test
        void shouldNotCollectFileWithNoReferencesBeforeSpecifiedDelay() throws Exception {
            // Given
            instanceProperties = createInstanceProperties();
            tableProperties = createTableWithGCDelay(TEST_TABLE_NAME, instanceProperties, 10);
            Instant currentTime = Instant.parse("2023-06-28T13:46:00Z");
            Instant notOldEnoughTime = currentTime.minus(Duration.ofMinutes(5));
            StateStore stateStore = setupStateStoreAndFixTime(notOldEnoughTime);
            java.nio.file.Path oldFile = tempDir.resolve("old-file.parquet");
            java.nio.file.Path newFile = tempDir.resolve("new-file.parquet");
            createFileWithNoReferencesByCompaction(stateStore, oldFile, newFile);

            // When
            createGarbageCollector(instanceProperties, stateStoreProvider).runAtTime(currentTime);

            // Then
            assertThat(Files.exists(oldFile)).isTrue();
            assertThat(stateStore.getAllFilesWithMaxUnreferenced(10)).isEqualTo(
                    activeAndReadyForGCFilesReport(notOldEnoughTime,
                            List.of(activeReference(newFile)),
                            List.of(oldFile.toString())));
        }

        @Test
        void shouldCollectMultipleFilesInOneRun() throws Exception {
            // Given
            instanceProperties = createInstanceProperties();
            tableProperties = createTableWithGCDelay(TEST_TABLE_NAME, instanceProperties, 10);
            Instant currentTime = Instant.parse("2023-06-28T13:46:00Z");
            Instant oldEnoughTime = currentTime.minus(Duration.ofMinutes(11));
            StateStore stateStore = setupStateStoreAndFixTime(oldEnoughTime);
            java.nio.file.Path oldFile1 = tempDir.resolve("old-file-1.parquet");
            java.nio.file.Path oldFile2 = tempDir.resolve("old-file-2.parquet");
            java.nio.file.Path newFile1 = tempDir.resolve("new-file-1.parquet");
            java.nio.file.Path newFile2 = tempDir.resolve("new-file-2.parquet");
            createFileWithNoReferencesByCompaction(stateStore, oldFile1, newFile1);
            createFileWithNoReferencesByCompaction(stateStore, oldFile2, newFile2);

            // When
            stateStore.fixTime(currentTime);
            createGarbageCollector(instanceProperties, stateStoreProvider).runAtTime(currentTime);

            // Then
            assertThat(Files.exists(oldFile1)).isFalse();
            assertThat(Files.exists(oldFile2)).isFalse();
            assertThat(Files.exists(newFile1)).isTrue();
            assertThat(Files.exists(newFile2)).isTrue();
            assertThat(stateStore.getAllFilesWithMaxUnreferenced(10))
                    .isEqualTo(activeFilesReport(oldEnoughTime,
                            activeReference(newFile1),
                            activeReference(newFile2)));
        }

        @Test
        void shouldCollectFilesInBatchesIfBatchSizeExceeded() throws Exception {
            // Given
            instanceProperties = createInstancePropertiesWithGCBatchSize(2);
            tableProperties = createTableWithGCDelay(TEST_TABLE_NAME, instanceProperties, 10);
            Instant currentTime = Instant.parse("2023-06-28T13:46:00Z");
            Instant oldEnoughTime = currentTime.minus(Duration.ofMinutes(11));
            StateStore stateStore = setupStateStoreAndFixTime(oldEnoughTime);
            java.nio.file.Path oldFile1 = tempDir.resolve("old-file-1.parquet");
            java.nio.file.Path oldFile2 = tempDir.resolve("old-file-2.parquet");
            java.nio.file.Path newFile1 = tempDir.resolve("new-file-1.parquet");
            java.nio.file.Path newFile2 = tempDir.resolve("new-file-2.parquet");
            java.nio.file.Path oldFile3 = tempDir.resolve("old-file-3.parquet");
            java.nio.file.Path newFile3 = tempDir.resolve("new-file-3.parquet");
            createFileWithNoReferencesByCompaction(stateStore, oldFile1, newFile1);
            createFileWithNoReferencesByCompaction(stateStore, oldFile2, newFile2);
            createFileWithNoReferencesByCompaction(stateStore, oldFile3, newFile3);

            // When
            createGarbageCollector(instanceProperties, stateStoreProvider).runAtTime(currentTime);

            // Then
            assertThat(Files.exists(oldFile1)).isFalse();
            assertThat(Files.exists(oldFile2)).isFalse();
            assertThat(Files.exists(oldFile3)).isFalse();
            assertThat(Files.exists(newFile1)).isTrue();
            assertThat(Files.exists(newFile2)).isTrue();
            assertThat(Files.exists(newFile3)).isTrue();
            assertThat(stateStore.getAllFilesWithMaxUnreferenced(10)).isEqualTo(
                    activeFilesReport(oldEnoughTime,
                            activeReference(newFile1),
                            activeReference(newFile2),
                            activeReference(newFile3)));
        }

        @Test
        void shouldContinueCollectingFilesIfFileDoesNotExist() throws Exception {
            // Given
            instanceProperties = createInstanceProperties();
            tableProperties = createTableWithGCDelay(TEST_TABLE_NAME, instanceProperties, 10);
            Instant currentTime = Instant.parse("2023-06-28T13:46:00Z");
            Instant oldEnoughTime = currentTime.minus(Duration.ofMinutes(11));
            StateStore stateStore = setupStateStoreAndFixTime(oldEnoughTime);
            stateStore.addFilesWithReferences(List.of(
                    fileWithNoReferences("/tmp/not-a-file.parquet")));
            java.nio.file.Path oldFile2 = tempDir.resolve("old-file-2.parquet");
            java.nio.file.Path newFile2 = tempDir.resolve("new-file-2.parquet");
            createFileWithNoReferencesByCompaction(stateStore, oldFile2, newFile2);

            // When
            stateStore.fixTime(currentTime);
            createGarbageCollector(instanceProperties, stateStoreProvider).runAtTime(currentTime);

            // Then
            assertThat(Files.exists(oldFile2)).isFalse();
            assertThat(Files.exists(newFile2)).isTrue();
            assertThat(stateStore.getAllFilesWithMaxUnreferenced(10))
                    .isEqualTo(activeFilesReport(oldEnoughTime,
                            activeReference(newFile2)));
        }
    }

    @Nested
    @DisplayName("Collecting from multiple tables")
    class MultipleTables {
        private StateStoreProvider stateStoreProvider;

        void setupStateStoresAndFixTimes(Instant fixedTime) {
            StateStore stateStore1 = inMemoryStateStoreWithSinglePartition(TEST_SCHEMA);
            stateStore1.fixTime(fixedTime);
            StateStore stateStore2 = inMemoryStateStoreWithSinglePartition(TEST_SCHEMA);
            stateStore2.fixTime(fixedTime);
            stateStoreProvider = new FixedStateStoreProvider(Map.of(
                    TEST_TABLE_NAME_1, stateStore1, TEST_TABLE_NAME_2, stateStore2));
        }

        @Test
        void shouldCollectOneFileFromEachTable() throws Exception {
            // Given
            InstanceProperties instanceProperties = createInstancePropertiesWithGCBatchSize(2);
            TableProperties tableProperties1 = createTableWithGCDelay(TEST_TABLE_NAME_1, instanceProperties, 10);
            TableProperties tableProperties2 = createTableWithGCDelay(TEST_TABLE_NAME_2, instanceProperties, 10);
            Instant currentTime = Instant.parse("2023-06-28T13:46:00Z");
            Instant oldEnoughTime = currentTime.minus(Duration.ofMinutes(11));
            setupStateStoresAndFixTimes(oldEnoughTime);
            StateStore stateStore1 = stateStoreProvider.getStateStore(tableProperties1);
            StateStore stateStore2 = stateStoreProvider.getStateStore(tableProperties2);
            java.nio.file.Path oldFile1 = tempDir.resolve("old-file-1.parquet");
            java.nio.file.Path oldFile2 = tempDir.resolve("old-file-2.parquet");
            java.nio.file.Path newFile1 = tempDir.resolve("new-file-1.parquet");
            java.nio.file.Path newFile2 = tempDir.resolve("new-file-2.parquet");
            createFileWithNoReferencesByCompaction(stateStore1, oldFile1, newFile1);
            createFileWithNoReferencesByCompaction(stateStore2, oldFile2, newFile2);

            // When
            stateStore1.fixTime(currentTime);
            stateStore2.fixTime(currentTime);
            createGarbageCollector(instanceProperties, stateStoreProvider).runAtTime(currentTime);

            // Then
            assertThat(Files.exists(oldFile1)).isFalse();
            assertThat(Files.exists(oldFile2)).isFalse();
            assertThat(stateStore1.getAllFilesWithMaxUnreferenced(10)).isEqualTo(
                    activeFilesReport(oldEnoughTime, activeReference(newFile1)));
            assertThat(stateStore2.getAllFilesWithMaxUnreferenced(10)).isEqualTo(
                    activeFilesReport(oldEnoughTime, activeReference(newFile2)));
        }
    }

    private FileReference createActiveFile(java.nio.file.Path filePath, StateStore stateStore) throws Exception {
        String filename = filePath.toString();
        FileReference fileReference = FileReferenceFactory.from(partitions).rootFile(filename, 100L);
        writeFile(filename);
        stateStore.addFile(fileReference);
        return fileReference;
    }

    private void createFileWithNoReferencesByCompaction(StateStore stateStore,
                                                        java.nio.file.Path oldFilePath, java.nio.file.Path newFilePath) throws Exception {
        FileReference oldFile = createActiveFile(oldFilePath, stateStore);
        writeFile(newFilePath.toString());
        stateStore.assignJobIds(List.of(
                assignJobOnPartitionToFiles("job1", "root", List.of(oldFile.getFilename()))));
        stateStore.atomicallyReplaceFileReferencesWithNewOne("job1", "root", List.of(oldFile.getFilename()),
                FileReferenceFactory.from(partitions).rootFile(newFilePath.toString(), 100));
    }

    private FileReference activeReference(java.nio.file.Path filePath) {
        return FileReferenceFactory.from(partitions).rootFile(filePath.toString(), 100);
    }

    private void writeFile(String filename) throws Exception {
        ParquetWriter<Record> writer = ParquetRecordWriterFactory.createParquetRecordWriter(new Path(filename), TEST_SCHEMA);
        for (int i = 0; i < 100; i++) {
            Record record = new Record();
            record.put("key", i);
            record.put("value", "" + i);
            writer.write(record);
        }
        writer.close();
    }

    private InstanceProperties createInstancePropertiesWithGCBatchSize(int gcBatchSize) {
        return createInstanceProperties(properties ->
                properties.setNumber(GARBAGE_COLLECTOR_BATCH_SIZE, gcBatchSize));
    }

    private InstanceProperties createInstanceProperties() {
        return createInstanceProperties(properties -> {
        });
    }

    private InstanceProperties createInstanceProperties(Consumer<InstanceProperties> extraProperties) {
        InstanceProperties instanceProperties = createTestInstanceProperties();
        instanceProperties.set(FILE_SYSTEM, "file://");
        instanceProperties.set(DATA_BUCKET, tempDir.toString());
        extraProperties.accept(instanceProperties);
        return instanceProperties;
    }

    private TableProperties createTableWithGCDelay(String tableName, InstanceProperties instanceProperties, int gcDelay) {
        TableProperties tableProperties = createTestTableProperties(instanceProperties, TEST_SCHEMA);
        tableProperties.set(TABLE_NAME, tableName);
        tableProperties.setNumber(GARBAGE_COLLECTOR_DELAY_BEFORE_DELETION, gcDelay);
        tables.add(tableProperties);
        return tableProperties;
    }

    private GarbageCollector createGarbageCollector(InstanceProperties instanceProperties, StateStoreProvider stateStoreProvider) {
        return new GarbageCollector(new Configuration(),
                new FixedTablePropertiesProvider(tables), stateStoreProvider,
                instanceProperties.getInt(GARBAGE_COLLECTOR_BATCH_SIZE));
    }

    private static Schema getSchema() {
        return Schema.builder()
                .rowKeyFields(new Field("key", new IntType()))
                .valueFields(new Field("value", new StringType()))
                .build();
    }
}
