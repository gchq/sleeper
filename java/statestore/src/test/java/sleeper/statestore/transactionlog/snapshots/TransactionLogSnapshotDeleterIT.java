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
package sleeper.statestore.transactionlog.snapshots;

import org.junit.jupiter.api.Test;

import sleeper.core.partition.PartitionsBuilder;
import sleeper.core.properties.table.TableProperties;
import sleeper.core.statestore.FileReferenceFactory;
import sleeper.core.statestore.StateStore;
import sleeper.statestore.transactionlog.snapshots.TransactionLogSnapshotDeleter.SnapshotFileDeleter;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.time.Instant;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static sleeper.core.properties.table.TableProperty.TRANSACTION_LOG_SNAPSHOT_EXPIRY_IN_DAYS;

public class TransactionLogSnapshotDeleterIT extends TransactionLogSnapshotTestBase {

    @Test
    void shouldDeleteOldSnapshotsForOneTable() throws Exception {
        // Given
        TableProperties table = createTable("test-table-id-1", "test-table-1");
        table.setNumber(TRANSACTION_LOG_SNAPSHOT_EXPIRY_IN_DAYS, 1);
        StateStore stateStore = createStateStoreWithInMemoryTransactionLog(table);
        PartitionsBuilder partitionsBuilder = new PartitionsBuilder(schema).rootFirst("root");
        stateStore.initialise(partitionsBuilder.buildList());
        FileReferenceFactory factory = FileReferenceFactory.from(stateStore);
        stateStore.addFile(factory.rootFile("test1.parquet", 123L));
        createSnapshotsAt(table, Instant.parse("2024-04-25T11:24:00Z"));

        stateStore.clearFileData();
        stateStore.initialise(partitionsBuilder.splitToNewChildren("root", "L", "R", 123L).buildList());
        factory = FileReferenceFactory.from(stateStore);
        stateStore.addFile(factory.rootFile("test3.parquet", 789L));
        createSnapshotsAt(table, Instant.parse("2024-04-27T11:24:00Z"));

        // When
        SnapshotDeletionTracker snapshotDeleteTracker = deleteSnapshotsAt(table, Instant.parse("2024-04-27T11:24:00Z"));

        // Then
        assertThat(snapshotStore(table).getLatestSnapshots())
                .isEqualTo(new LatestSnapshots(
                        filesSnapshot(table, 3),
                        partitionsSnapshot(table, 2)));
        assertThat(snapshotStore(table).getFilesSnapshots())
                .containsExactly(filesSnapshot(table, 3));
        assertThat(snapshotStore(table).getPartitionsSnapshots())
                .containsExactly(partitionsSnapshot(table, 2));
        assertThat(tableFiles(table))
                .containsExactlyInAnyOrder(
                        filesSnapshotPath(table, 3),
                        partitionsSnapshotPath(table, 2));
        assertThat(snapshotDeleteTracker.getLastTransactionNumber()).isEqualTo(1);
        assertThat(snapshotDeleteTracker.getDeletedCount()).isEqualTo(2);
    }

    @Test
    void shouldNotDeleteOldSnapshotsIfTheyAreNotOldEnoughForOneTable() throws Exception {
        // Given
        TableProperties table = createTable("test-table-id-1", "test-table-1");
        table.setNumber(TRANSACTION_LOG_SNAPSHOT_EXPIRY_IN_DAYS, 1);
        StateStore stateStore = createStateStoreWithInMemoryTransactionLog(table);
        PartitionsBuilder partitionsBuilder = new PartitionsBuilder(schema).rootFirst("root");
        stateStore.initialise(partitionsBuilder.buildList());
        FileReferenceFactory factory = FileReferenceFactory.from(stateStore);
        stateStore.addFile(factory.rootFile("test1.parquet", 123L));
        createSnapshotsAt(table, Instant.parse("2024-04-27T11:23:00Z"));

        stateStore.clearFileData();
        stateStore.initialise(partitionsBuilder.splitToNewChildren("root", "L", "R", 123L).buildList());
        factory = FileReferenceFactory.from(stateStore);
        stateStore.addFile(factory.rootFile("test3.parquet", 789L));
        createSnapshotsAt(table, Instant.parse("2024-04-27T11:24:00Z"));

        // When
        SnapshotDeletionTracker snapshotDeleteTracker = deleteSnapshotsAt(table, Instant.parse("2024-04-27T11:25:00Z"));

        // Then
        assertThat(snapshotStore(table).getLatestSnapshots())
                .isEqualTo(new LatestSnapshots(
                        filesSnapshot(table, 3),
                        partitionsSnapshot(table, 2)));
        assertThat(snapshotStore(table).getFilesSnapshots())
                .containsExactlyInAnyOrder(
                        filesSnapshot(table, 3),
                        filesSnapshot(table, 1));
        assertThat(snapshotStore(table).getPartitionsSnapshots())
                .containsExactlyInAnyOrder(
                        partitionsSnapshot(table, 2),
                        partitionsSnapshot(table, 1));
        assertThat(tableFiles(table))
                .containsExactlyInAnyOrder(
                        filesSnapshotPath(table, 3),
                        filesSnapshotPath(table, 1),
                        partitionsSnapshotPath(table, 2),
                        partitionsSnapshotPath(table, 1));
        assertThat(snapshotDeleteTracker.getLastTransactionNumber()).isNull();
        assertThat(snapshotDeleteTracker.getDeletedCount()).isEqualTo(0);
    }

    @Test
    void shouldNotDeleteOldSnapshotsIfTheyAreAlsoLatestSnapshots() throws Exception {
        // Given
        TableProperties table = createTable("test-table-id-1", "test-table-1");
        table.setNumber(TRANSACTION_LOG_SNAPSHOT_EXPIRY_IN_DAYS, 1);
        StateStore stateStore = createStateStoreWithInMemoryTransactionLog(table);
        stateStore.initialise();
        FileReferenceFactory factory = FileReferenceFactory.from(stateStore);
        stateStore.addFile(factory.rootFile("test1.parquet", 123L));
        createSnapshotsAt(table, Instant.parse("2024-04-25T11:24:00Z"));

        // When
        SnapshotDeletionTracker snapshotDeleteTracker = deleteSnapshotsAt(table, Instant.parse("2024-04-27T11:24:00Z"));

        // Then
        assertThat(snapshotStore(table).getLatestSnapshots())
                .isEqualTo(new LatestSnapshots(
                        filesSnapshot(table, 1),
                        partitionsSnapshot(table, 1)));
        assertThat(snapshotStore(table).getFilesSnapshots())
                .containsExactly(filesSnapshot(table, 1));
        assertThat(snapshotStore(table).getPartitionsSnapshots())
                .containsExactly(partitionsSnapshot(table, 1));
        assertThat(tableFiles(table))
                .containsExactlyInAnyOrder(
                        filesSnapshotPath(table, 1),
                        partitionsSnapshotPath(table, 1));

        assertThat(snapshotDeleteTracker.getLastTransactionNumber()).isNull();
        assertThat(snapshotDeleteTracker.getDeletedCount()).isEqualTo(0);
    }

    @Test
    void shouldNotDeleteSnapshotMetadataIfSnapshotFileFailedToDelete() throws Exception {
        // Given
        TableProperties table = createTable("test-table-id-1", "test-table-1");
        table.setNumber(TRANSACTION_LOG_SNAPSHOT_EXPIRY_IN_DAYS, 1);
        StateStore stateStore = createStateStoreWithInMemoryTransactionLog(table);
        PartitionsBuilder partitionsBuilder = new PartitionsBuilder(schema).rootFirst("root");
        stateStore.initialise(partitionsBuilder.buildList());
        FileReferenceFactory factory = FileReferenceFactory.from(stateStore);
        stateStore.addFile(factory.rootFile("test1.parquet", 123L));
        createSnapshotsAt(table, Instant.parse("2024-04-24T11:24:00Z"));
        stateStore.clearFileData();
        stateStore.initialise(partitionsBuilder.splitToNewChildren("root", "L", "R", 123L).buildList());
        factory = FileReferenceFactory.from(stateStore);
        stateStore.addFile(factory.rootFile("test3.parquet", 789L));
        createSnapshotsAt(table, Instant.parse("2024-04-25T11:24:00Z"));
        IOException exception = new IOException("Failed to delete file");

        // When / Then
        assertThatThrownBy(() -> deleteSnapshotsAt(table, Instant.parse("2024-04-27T11:24:00Z"), failedDeletion(exception)))
                .isInstanceOf(UncheckedIOException.class)
                .hasCause(exception);
        assertThat(snapshotStore(table).getLatestSnapshots())
                .isEqualTo(new LatestSnapshots(
                        filesSnapshot(table, 3),
                        partitionsSnapshot(table, 2)));
        assertThat(snapshotStore(table).getFilesSnapshots())
                .containsExactlyInAnyOrder(
                        filesSnapshot(table, 3),
                        filesSnapshot(table, 1));
        assertThat(snapshotStore(table).getPartitionsSnapshots())
                .containsExactlyInAnyOrder(
                        partitionsSnapshot(table, 2),
                        partitionsSnapshot(table, 1));
        assertThat(tableFiles(table))
                .containsExactlyInAnyOrder(
                        filesSnapshotPath(table, 3),
                        filesSnapshotPath(table, 1),
                        partitionsSnapshotPath(table, 2),
                        partitionsSnapshotPath(table, 1));

    }

    @Test
    void shouldDeleteOldSnapshotMetadataIfSnapshotFilesHaveAlreadyBeenDeleted() throws Exception {
        // Given
        TableProperties table = createTable("test-table-id-1", "test-table-1");
        table.setNumber(TRANSACTION_LOG_SNAPSHOT_EXPIRY_IN_DAYS, 1);
        StateStore stateStore = createStateStoreWithInMemoryTransactionLog(table);
        PartitionsBuilder partitionsBuilder = new PartitionsBuilder(schema).rootFirst("root");
        stateStore.initialise(partitionsBuilder.buildList());
        FileReferenceFactory factory = FileReferenceFactory.from(stateStore);
        stateStore.addFile(factory.rootFile("test1.parquet", 123L));
        createSnapshotsAt(table, Instant.parse("2024-04-24T11:24:00Z"));
        stateStore.clearFileData();
        stateStore.initialise(partitionsBuilder.splitToNewChildren("root", "L", "R", 123L).buildList());
        factory = FileReferenceFactory.from(stateStore);
        stateStore.addFile(factory.rootFile("test3.parquet", 789L));
        createSnapshotsAt(table, Instant.parse("2024-04-25T11:24:00Z"));
        // Delete files for snapshots that are eligible for deletion
        deleteFilesSnapshotFile(table, 1);
        deletePartitionsSnapshotFile(table, 1);

        // When
        SnapshotDeletionTracker snapshotDeleteTracker = deleteSnapshotsAt(table, Instant.parse("2024-04-27T11:24:00Z"));

        // Then
        assertThat(snapshotStore(table).getLatestSnapshots())
                .isEqualTo(new LatestSnapshots(
                        filesSnapshot(table, 3),
                        partitionsSnapshot(table, 2)));
        assertThat(snapshotStore(table).getFilesSnapshots())
                .containsExactly(filesSnapshot(table, 3));
        assertThat(snapshotStore(table).getPartitionsSnapshots())
                .containsExactly(partitionsSnapshot(table, 2));
        assertThat(tableFiles(table))
                .containsExactlyInAnyOrder(
                        filesSnapshotPath(table, 3),
                        partitionsSnapshotPath(table, 2));

        assertThat(snapshotDeleteTracker.getLastTransactionNumber()).isEqualTo(1);
        assertThat(snapshotDeleteTracker.getDeletedCount()).isEqualTo(2);
    }

    private SnapshotFileDeleter failedDeletion(IOException e) {
        return file -> {
            throw e;
        };
    }
}
