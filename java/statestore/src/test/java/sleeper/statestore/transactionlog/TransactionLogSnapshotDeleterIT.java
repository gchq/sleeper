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
package sleeper.statestore.transactionlog;

import org.junit.jupiter.api.Test;

import sleeper.configuration.properties.table.TableProperties;
import sleeper.core.partition.PartitionsBuilder;
import sleeper.core.statestore.FileReferenceFactory;
import sleeper.core.statestore.StateStore;
import sleeper.statestore.transactionlog.DynamoDBTransactionLogSnapshotMetadataStore.LatestSnapshots;

import java.time.Instant;

import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.configuration.properties.table.TableProperty.TRANSACTION_LOG_SNAPSHOT_EXPIRY_IN_DAYS;

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
        deleteSnapshotsAt(table, Instant.parse("2024-04-27T11:24:00Z"));

        // Then
        assertThat(snapshotStore(table).getLatestSnapshots())
                .isEqualTo(new LatestSnapshots(
                        filesSnapshot(table, 3),
                        partitionsSnapshot(table, 2)));
        assertThat(snapshotStore(table).getFilesSnapshots())
                .containsExactly(filesSnapshot(table, 3));
        assertThat(snapshotStore(table).getPartitionsSnapshots())
                .containsExactly(partitionsSnapshot(table, 2));
        assertThat(filesSnapshotFileExists(table, 3)).isTrue();
        assertThat(filesSnapshotFileExists(table, 1)).isFalse();
        assertThat(partitionsSnapshotFileExists(table, 2)).isTrue();
        assertThat(partitionsSnapshotFileExists(table, 1)).isFalse();
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
        deleteSnapshotsAt(table, Instant.parse("2024-04-27T11:25:00Z"));

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
        assertThat(filesSnapshotFileExists(table, 3)).isTrue();
        assertThat(filesSnapshotFileExists(table, 1)).isTrue();
        assertThat(partitionsSnapshotFileExists(table, 2)).isTrue();
        assertThat(partitionsSnapshotFileExists(table, 1)).isTrue();
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
        deleteSnapshotsAt(table, Instant.parse("2024-04-27T11:24:00Z"));

        // Then
        assertThat(snapshotStore(table).getLatestSnapshots())
                .isEqualTo(new LatestSnapshots(
                        filesSnapshot(table, 1),
                        partitionsSnapshot(table, 1)));
        assertThat(snapshotStore(table).getFilesSnapshots())
                .containsExactly(filesSnapshot(table, 1));
        assertThat(snapshotStore(table).getPartitionsSnapshots())
                .containsExactly(partitionsSnapshot(table, 1));
        assertThat(filesSnapshotFileExists(table, 1)).isTrue();
        assertThat(partitionsSnapshotFileExists(table, 1)).isTrue();
    }

    @Test
    void shouldDeleteOldSnapshotsIfSnapshotFilesHaveAlreadyBeenDeleted() throws Exception {
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
        deleteSnapshotsAt(table, Instant.parse("2024-04-27T11:24:00Z"));

        // Then
        assertThat(snapshotStore(table).getLatestSnapshots())
                .isEqualTo(new LatestSnapshots(
                        filesSnapshot(table, 3),
                        partitionsSnapshot(table, 2)));
        assertThat(snapshotStore(table).getFilesSnapshots())
                .containsExactly(filesSnapshot(table, 3));
        assertThat(snapshotStore(table).getPartitionsSnapshots())
                .containsExactly(partitionsSnapshot(table, 2));
    }
}
