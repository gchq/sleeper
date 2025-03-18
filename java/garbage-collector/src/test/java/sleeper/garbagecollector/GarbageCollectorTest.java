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
package sleeper.garbagecollector;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import sleeper.core.partition.PartitionTree;
import sleeper.core.partition.PartitionsBuilder;
import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.table.TableProperties;
import sleeper.core.schema.Field;
import sleeper.core.schema.Schema;
import sleeper.core.schema.type.IntType;
import sleeper.core.schema.type.StringType;
import sleeper.core.statestore.FileReference;
import sleeper.core.statestore.FileReferenceFactory;
import sleeper.core.statestore.StateStore;
import sleeper.core.statestore.StateStoreProvider;
import sleeper.core.statestore.commit.StateStoreCommitRequest;
import sleeper.core.statestore.testutils.InMemoryTransactionLogStateStore;
import sleeper.core.statestore.testutils.InMemoryTransactionLogsPerTable;
import sleeper.core.statestore.transactionlog.transaction.impl.DeleteFilesTransaction;
import sleeper.garbagecollector.FailedGarbageCollectionException.FileFailure;
import sleeper.garbagecollector.FailedGarbageCollectionException.TableFailures;
import sleeper.garbagecollector.GarbageCollector.DeleteFiles;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static sleeper.core.properties.instance.CommonProperty.FILE_SYSTEM;
import static sleeper.core.properties.instance.GarbageCollectionProperty.GARBAGE_COLLECTOR_BATCH_SIZE;
import static sleeper.core.properties.table.TableProperty.GARBAGE_COLLECTOR_ASYNC_COMMIT;
import static sleeper.core.properties.table.TableProperty.GARBAGE_COLLECTOR_DELAY_BEFORE_DELETION;
import static sleeper.core.properties.table.TableProperty.TABLE_ID;
import static sleeper.core.properties.testutils.InstancePropertiesTestHelper.createTestInstanceProperties;
import static sleeper.core.properties.testutils.TablePropertiesTestHelper.createTestTableProperties;
import static sleeper.core.statestore.AllReferencesToAFileTestHelper.fileWithNoReferences;
import static sleeper.core.statestore.AssignJobIdRequest.assignJobOnPartitionToFiles;
import static sleeper.core.statestore.FilesReportTestHelper.activeAndReadyForGCFilesReport;
import static sleeper.core.statestore.FilesReportTestHelper.activeFilesReport;
import static sleeper.core.statestore.ReplaceFileReferencesRequest.replaceJobFileReferences;
import static sleeper.core.statestore.testutils.StateStoreUpdatesWrapper.update;

public class GarbageCollectorTest {
    private static final Schema TEST_SCHEMA = getSchema();

    private final PartitionTree partitions = new PartitionsBuilder(TEST_SCHEMA).singlePartition("root").buildTree();
    private final List<TableProperties> tables = new ArrayList<>();
    private final InstanceProperties instanceProperties = createTestInstanceProperties();
    private final StateStoreProvider stateStoreProvider = InMemoryTransactionLogStateStore
            .createProvider(instanceProperties, new InMemoryTransactionLogsPerTable());

    private final List<StateStoreCommitRequest> sentCommits = new ArrayList<>();
    private final Set<String> filesInBucket = new HashSet<>();

    @BeforeEach
    void setUp() throws Exception {
        instanceProperties.set(FILE_SYSTEM, "file://");
    }

    @Nested
    @DisplayName("Collecting from single table")
    class SingleTable {
        private final TableProperties table = createTable();
        private final StateStore stateStore = stateStore(table);

        @Test
        void shouldCollectFileWithNoReferencesAfterSpecifiedDelay() throws Exception {
            // Given
            Instant currentTime = Instant.parse("2023-06-28T13:46:00Z");
            Instant oldEnoughTime = currentTime.minus(Duration.ofMinutes(11));
            stateStore.fixFileUpdateTime(oldEnoughTime);
            table.setNumber(GARBAGE_COLLECTOR_DELAY_BEFORE_DELETION, 10);
            createFileWithNoReferencesByCompaction(stateStore, "old-file.parquet", "new-file.parquet");

            // When
            collectGarbageAtTime(currentTime);

            // Then
            assertThat(filesInBucket).containsExactly("new-file.parquet");
            assertThat(stateStore.getAllFilesWithMaxUnreferenced(10))
                    .isEqualTo(activeFilesReport(oldEnoughTime, activeReference("new-file.parquet")));
        }

        @Test
        void shouldNotCollectFileMarkedAsActive() throws Exception {
            // Given
            Instant currentTime = Instant.parse("2023-06-28T13:46:00Z");
            Instant oldEnoughTime = currentTime.minus(Duration.ofMinutes(11));
            stateStore.fixFileUpdateTime(oldEnoughTime);
            table.setNumber(GARBAGE_COLLECTOR_DELAY_BEFORE_DELETION, 10);
            createActiveFile("test-file.parquet", stateStore);

            // When
            collectGarbageAtTime(currentTime);

            // Then
            assertThat(filesInBucket).containsExactly("test-file.parquet");
            assertThat(stateStore.getAllFilesWithMaxUnreferenced(10))
                    .isEqualTo(activeFilesReport(oldEnoughTime, activeReference("test-file.parquet")));
        }

        @Test
        void shouldNotCollectFileWithNoReferencesBeforeSpecifiedDelay() throws Exception {
            // Given
            Instant currentTime = Instant.parse("2023-06-28T13:46:00Z");
            Instant notOldEnoughTime = currentTime.minus(Duration.ofMinutes(5));
            stateStore.fixFileUpdateTime(notOldEnoughTime);
            table.setNumber(GARBAGE_COLLECTOR_DELAY_BEFORE_DELETION, 10);
            createFileWithNoReferencesByCompaction(stateStore, "old-file.parquet", "new-file.parquet");

            // When
            collectGarbageAtTime(currentTime);

            // Then
            assertThat(filesInBucket).isEqualTo(Set.of("old-file.parquet", "new-file.parquet"));
            assertThat(stateStore.getAllFilesWithMaxUnreferenced(10)).isEqualTo(
                    activeAndReadyForGCFilesReport(notOldEnoughTime,
                            List.of(activeReference("new-file.parquet")),
                            List.of("old-file.parquet")));
        }

        @Test
        void shouldCollectMultipleFilesInOneRun() throws Exception {
            // Given
            Instant currentTime = Instant.parse("2023-06-28T13:46:00Z");
            Instant oldEnoughTime = currentTime.minus(Duration.ofMinutes(11));
            stateStore.fixFileUpdateTime(oldEnoughTime);
            table.setNumber(GARBAGE_COLLECTOR_DELAY_BEFORE_DELETION, 10);
            createFileWithNoReferencesByCompaction(stateStore, "old-file-1.parquet", "new-file-1.parquet");
            createFileWithNoReferencesByCompaction(stateStore, "old-file-2.parquet", "new-file-2.parquet");

            // When
            collectGarbageAtTime(currentTime);

            // Then
            assertThat(filesInBucket).isEqualTo(Set.of("new-file-1.parquet", "new-file-2.parquet"));
            assertThat(stateStore.getAllFilesWithMaxUnreferenced(10))
                    .isEqualTo(activeFilesReport(oldEnoughTime,
                            activeReference("new-file-1.parquet"),
                            activeReference("new-file-2.parquet")));
        }

        @Test
        void shouldCollectMoreFilesThanBatchSize() throws Exception {
            // Given
            instanceProperties.setNumber(GARBAGE_COLLECTOR_BATCH_SIZE, 2);
            Instant currentTime = Instant.parse("2023-06-28T13:46:00Z");
            Instant oldEnoughTime = currentTime.minus(Duration.ofMinutes(11));
            stateStore.fixFileUpdateTime(oldEnoughTime);
            table.setNumber(GARBAGE_COLLECTOR_DELAY_BEFORE_DELETION, 10);
            createFileWithNoReferencesByCompaction(stateStore, "old-file-1.parquet", "new-file-1.parquet");
            createFileWithNoReferencesByCompaction(stateStore, "old-file-2.parquet", "new-file-2.parquet");
            createFileWithNoReferencesByCompaction(stateStore, "old-file-3.parquet", "new-file-3.parquet");

            // When
            collectGarbageAtTime(currentTime);

            // Then
            assertThat(filesInBucket).isEqualTo(Set.of("new-file-1.parquet", "new-file-2.parquet", "new-file-3.parquet"));
            assertThat(stateStore.getAllFilesWithMaxUnreferenced(10)).isEqualTo(
                    activeFilesReport(oldEnoughTime,
                            activeReference("new-file-1.parquet"),
                            activeReference("new-file-2.parquet"),
                            activeReference("new-file-3.parquet")));
        }

        @Test
        void shouldContinueCollectingFilesIfFileDoesNotExist() throws Exception {
            // Given
            Instant currentTime = Instant.parse("2023-06-28T13:46:00Z");
            Instant oldEnoughTime = currentTime.minus(Duration.ofMinutes(11));
            stateStore.fixFileUpdateTime(oldEnoughTime);
            table.setNumber(GARBAGE_COLLECTOR_DELAY_BEFORE_DELETION, 10);
            update(stateStore).addFilesWithReferences(List.of(
                    fileWithNoReferences("not-a-file.parquet")));
            createFileWithNoReferencesByCompaction(stateStore, "old-file.parquet", "new-file.parquet");

            // When
            collectGarbageAtTime(currentTime);

            // Then
            assertThat(filesInBucket).isEqualTo(Set.of("new-file.parquet"));
            assertThat(stateStore.getAllFilesWithMaxUnreferenced(10))
                    .isEqualTo(activeFilesReport(oldEnoughTime,
                            activeReference("new-file.parquet")));
        }
    }

    @Nested
    @DisplayName("Collecting from multiple tables")
    class MultipleTables {

        @Test
        void shouldCollectOneFileFromEachTable() throws Exception {
            // Given
            instanceProperties.setNumber(GARBAGE_COLLECTOR_BATCH_SIZE, 2);
            TableProperties table1 = createTableWithGcDelayMinutes(10);
            TableProperties table2 = createTableWithGcDelayMinutes(10);
            Instant currentTime = Instant.parse("2023-06-28T13:46:00Z");
            Instant oldEnoughTime = currentTime.minus(Duration.ofMinutes(11));
            StateStore stateStore1 = stateStoreWithFixedTime(table1, oldEnoughTime);
            StateStore stateStore2 = stateStoreWithFixedTime(table2, oldEnoughTime);
            createFileWithNoReferencesByCompaction(stateStore1, "old-file-1.parquet", "new-file-1.parquet");
            createFileWithNoReferencesByCompaction(stateStore2, "old-file-2.parquet", "new-file-2.parquet");

            // When
            collectGarbageAtTime(currentTime);

            // Then
            assertThat(filesInBucket).isEqualTo(Set.of("new-file-1.parquet", "new-file-2.parquet"));
            assertThat(stateStore1.getAllFilesWithMaxUnreferenced(10)).isEqualTo(
                    activeFilesReport(oldEnoughTime, activeReference("new-file-1.parquet")));
            assertThat(stateStore2.getAllFilesWithMaxUnreferenced(10)).isEqualTo(
                    activeFilesReport(oldEnoughTime, activeReference("new-file-2.parquet")));
        }

        @Test
        void shouldFailOneFileAndFinishBatch() throws Exception {
            // Given
            instanceProperties.setNumber(GARBAGE_COLLECTOR_BATCH_SIZE, 2);
            TableProperties table1 = createTableWithGcDelayMinutes(10);
            TableProperties table2 = createTableWithGcDelayMinutes(10);
            Instant currentTime = Instant.parse("2023-06-28T13:46:00Z");
            Instant oldEnoughTime = currentTime.minus(Duration.ofMinutes(11));
            StateStore stateStore1 = stateStoreWithFixedTime(table1, oldEnoughTime);
            StateStore stateStore2 = stateStoreWithFixedTime(table2, oldEnoughTime);
            String file1 = "file-1.parquet";
            String file2 = "file-2.parquet";
            createFileWithNoReferencesByCompaction(stateStore1, file1, "new-file-1.parquet");
            createFileWithNoReferencesByCompaction(stateStore2, file2, "new-file-2.parquet");

            // When
            RuntimeException failure = new RuntimeException();
            GarbageCollector collector = collectorWithDeleteAction(deleteAllFilesExcept(file1, failure));

            // And / Then
            assertThatThrownBy(() -> collector.runAtTime(currentTime, tables))
                    .isInstanceOfSatisfying(FailedGarbageCollectionException.class,
                            e -> assertThat(e.getTableFailures())
                                    .usingRecursiveFieldByFieldElementComparator()
                                    .containsExactly(fileFailure(table1, file1, failure)));
            assertThat(filesInBucket).isEqualTo(Set.of(file1, "new-file-1.parquet", "new-file-2.parquet"));
            assertThat(stateStore1.getAllFilesWithMaxUnreferenced(10))
                    .isEqualTo(activeAndReadyForGCFilesReport(oldEnoughTime, List.of(activeReference("new-file-1.parquet")), List.of(file1)));
            assertThat(stateStore2.getAllFilesWithMaxUnreferenced(10))
                    .isEqualTo(activeFilesReport(oldEnoughTime, List.of(activeReference("new-file-2.parquet"))));
        }

        @Test
        void shouldCountFilesDeletedOverMultipleTables() throws Exception {
            // Given
            instanceProperties.setNumber(GARBAGE_COLLECTOR_BATCH_SIZE, 2);
            TableProperties table1 = createTableWithGcDelayMinutes(10);
            TableProperties table2 = createTableWithGcDelayMinutes(10);
            Instant currentTime = Instant.parse("2023-06-28T13:46:00Z");
            Instant oldEnoughTime = currentTime.minus(Duration.ofMinutes(11));
            StateStore stateStore1 = stateStoreWithFixedTime(table1, oldEnoughTime);
            StateStore stateStore2 = stateStoreWithFixedTime(table2, oldEnoughTime);
            createFileWithNoReferencesByCompaction(stateStore1, "old-file-1a.parquet", "new-file-1a.parquet");
            createFileWithNoReferencesByCompaction(stateStore1, "old-file-1b.parquet", "new-file-1b.parquet");
            createFileWithNoReferencesByCompaction(stateStore2, "old-file-2.parquet", "new-file-2.parquet");

            // When
            int totalDeleted = collectGarbageAtTime(currentTime);

            // Then
            assertThat(totalDeleted).isEqualTo(3);
        }
    }

    @Nested
    @DisplayName("Asynchronous commits for deleted files")
    class AsynchronousCommits {

        private final TableProperties table = createTable();
        private final StateStore stateStore = stateStore(table);

        @Test
        void shouldSendCommitForTheDeletionOfFilesAsychronously() throws Exception {
            // Given
            Instant currentTime = Instant.parse("2023-06-28T13:46:00Z");
            Instant oldEnoughTime = currentTime.minus(Duration.ofMinutes(11));
            stateStore.fixFileUpdateTime(oldEnoughTime);
            table.setNumber(GARBAGE_COLLECTOR_DELAY_BEFORE_DELETION, 10);
            table.set(GARBAGE_COLLECTOR_ASYNC_COMMIT, "true");
            createFileWithNoReferencesByCompaction(stateStore, "old-file.parquet", "new-file.parquet");

            // When
            collectGarbageAtTime(currentTime);

            // Then
            assertThat(filesInBucket).isEqualTo(Set.of("new-file.parquet"));
            assertThat(stateStore.getAllFilesWithMaxUnreferenced(10))
                    .isEqualTo(activeAndReadyForGCFilesReport(oldEnoughTime, List.of(activeReference("new-file.parquet")), List.of("old-file.parquet")));
            assertThat(sentCommits).containsExactly(
                    StateStoreCommitRequest.create(table.get(TABLE_ID),
                            new DeleteFilesTransaction(List.of("old-file.parquet"))));
        }

        @Test
        void shouldSendOneCommitPerFileBatch() throws Exception {
            // Given
            instanceProperties.setNumber(GARBAGE_COLLECTOR_BATCH_SIZE, 2);
            Instant currentTime = Instant.parse("2023-06-28T13:46:00Z");
            Instant oldEnoughTime = currentTime.minus(Duration.ofMinutes(11));
            stateStore.fixFileUpdateTime(oldEnoughTime);
            table.setNumber(GARBAGE_COLLECTOR_DELAY_BEFORE_DELETION, 10);
            table.set(GARBAGE_COLLECTOR_ASYNC_COMMIT, "true");

            createFileWithNoReferencesByCompaction(stateStore, "old-file-1.parquet", "new-file-1.parquet");
            createFileWithNoReferencesByCompaction(stateStore, "old-file-2.parquet", "new-file-2.parquet");
            createFileWithNoReferencesByCompaction(stateStore, "old-file-3.parquet", "new-file-3.parquet");

            // When
            collectGarbageAtTime(currentTime);

            // Then
            assertThat(filesInBucket).isEqualTo(Set.of("new-file-1.parquet", "new-file-2.parquet", "new-file-3.parquet"));
            assertThat(stateStore.getAllFilesWithMaxUnreferenced(10))
                    .isEqualTo(activeAndReadyForGCFilesReport(oldEnoughTime,
                            List.of(activeReference("new-file-1.parquet"),
                                    activeReference("new-file-2.parquet"),
                                    activeReference("new-file-3.parquet")),
                            List.of("old-file-1.parquet",
                                    "old-file-2.parquet",
                                    "old-file-3.parquet")));
            assertThat(sentCommits).containsExactly(
                    StateStoreCommitRequest.create(table.get(TABLE_ID),
                            new DeleteFilesTransaction(List.of("old-file-1.parquet", "old-file-2.parquet"))),
                    StateStoreCommitRequest.create(table.get(TABLE_ID),
                            new DeleteFilesTransaction(List.of("old-file-3.parquet"))));
        }

        @Test
        void shouldNotSendCommitWhenUpdatingStateStoreSynchronously() throws Exception {
            // Given
            Instant currentTime = Instant.parse("2023-06-28T13:46:00Z");
            Instant oldEnoughTime = currentTime.minus(Duration.ofMinutes(11));
            stateStore.fixFileUpdateTime(oldEnoughTime);
            table.setNumber(GARBAGE_COLLECTOR_DELAY_BEFORE_DELETION, 10);
            table.set(GARBAGE_COLLECTOR_ASYNC_COMMIT, "false");
            createFileWithNoReferencesByCompaction(stateStore, "old-file.parquet", "new-file.parquet");

            // When
            collectGarbageAtTime(currentTime);

            // Then
            assertThat(filesInBucket).isEqualTo(Set.of("new-file.parquet"));
            assertThat(stateStore.getAllFilesWithMaxUnreferenced(10))
                    .isEqualTo(activeAndReadyForGCFilesReport(oldEnoughTime, List.of(activeReference("new-file.parquet")), List.of()));
            assertThat(sentCommits).isEmpty();
        }
    }

    private static TableFailures fileFailure(TableProperties table, String filename, Exception failure) {
        return new TableFailures(table.getStatus(), null,
                List.of(new FileFailure(filename, failure)),
                List.of());
    }

    private FileReference createActiveFile(String filename, StateStore stateStore) throws Exception {
        FileReference fileReference = FileReferenceFactory.from(partitions).rootFile(filename, 100L);
        update(stateStore).addFile(fileReference);
        filesInBucket.add(filename);
        return fileReference;
    }

    private void createFileWithNoReferencesByCompaction(StateStore stateStore,
            String oldFilePath, String newFilePath) throws Exception {
        FileReference oldFile = createActiveFile(oldFilePath, stateStore);
        filesInBucket.add(newFilePath);
        update(stateStore).assignJobIds(List.of(
                assignJobOnPartitionToFiles("job1", "root", List.of(oldFile.getFilename()))));
        update(stateStore).atomicallyReplaceFileReferencesWithNewOnes(List.of(replaceJobFileReferences(
                "job1", List.of(oldFile.getFilename()), FileReferenceFactory.from(partitions).rootFile(newFilePath.toString(), 100))));
    }

    private FileReference activeReference(String filePath) {
        return FileReferenceFactory.from(partitions).rootFile(filePath, 100);
    }

    private TableProperties createTable() {
        TableProperties tableProperties = createTestTableProperties(instanceProperties, TEST_SCHEMA);
        tables.add(tableProperties);
        update(stateStoreProvider.getStateStore(tableProperties)).initialise(partitions.getAllPartitions());
        return tableProperties;
    }

    private TableProperties createTableWithGcDelayMinutes(int delay) {
        TableProperties tableProperties = createTable();
        tableProperties.setNumber(GARBAGE_COLLECTOR_DELAY_BEFORE_DELETION, delay);
        return tableProperties;
    }

    private StateStore stateStore(TableProperties table) {
        return stateStoreProvider.getStateStore(table);
    }

    private StateStore stateStoreWithFixedTime(TableProperties table, Instant fixedTime) {
        StateStore store = stateStore(table);
        store.fixFileUpdateTime(fixedTime);
        return store;
    }

    private int collectGarbageAtTime(Instant time) throws Exception {
        return collectorNew().runAtTime(time, tables);
    }

    private GarbageCollector collectorNew() throws Exception {
        return new GarbageCollector(deleteAllFilesSuccessfully(), instanceProperties, stateStoreProvider, sentCommits::add);
    }

    private GarbageCollector collectorWithDeleteAction(DeleteFiles deleteFiles) throws Exception {
        return new GarbageCollector(deleteFiles, instanceProperties, stateStoreProvider, sentCommits::add);
    }

    private DeleteFiles deleteAllFilesSuccessfully() {
        return (filenames, deleted) -> {
            filesInBucket.removeAll(filenames);
            filenames.forEach(deleted::deleted);
        };
    }

    private DeleteFiles deleteAllFilesExcept(String failFilename, Exception failure) {
        return (filenames, deleted) -> {
            for (String filename : filenames) {
                if (failFilename.equals(filename)) {
                    deleted.failed(filename, failure);
                } else {
                    deleted.deleted(filename);
                    filesInBucket.remove(filename);
                }
            }
        };
    }

    private static Schema getSchema() {
        return Schema.builder()
                .rowKeyFields(new Field("key", new IntType()))
                .valueFields(new Field("value", new StringType()))
                .build();
    }
}
