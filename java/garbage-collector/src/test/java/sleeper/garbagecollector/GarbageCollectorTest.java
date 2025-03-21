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

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import sleeper.core.properties.table.TableProperties;
import sleeper.core.statestore.StateStore;
import sleeper.core.statestore.commit.StateStoreCommitRequest;
import sleeper.core.statestore.transactionlog.transaction.impl.DeleteFilesTransaction;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static sleeper.core.properties.instance.GarbageCollectionProperty.GARBAGE_COLLECTOR_BATCH_SIZE;
import static sleeper.core.properties.table.TableProperty.GARBAGE_COLLECTOR_ASYNC_COMMIT;
import static sleeper.core.properties.table.TableProperty.GARBAGE_COLLECTOR_DELAY_BEFORE_DELETION;
import static sleeper.core.properties.table.TableProperty.TABLE_ID;
import static sleeper.core.statestore.AllReferencesToAFileTestHelper.fileWithNoReferences;
import static sleeper.core.statestore.FilesReportTestHelper.activeAndReadyForGCFilesReport;
import static sleeper.core.statestore.FilesReportTestHelper.activeFilesReport;
import static sleeper.core.statestore.testutils.StateStoreUpdatesWrapper.update;

public class GarbageCollectorTest extends GarbageCollectorTestBase {

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

}
