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

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import sleeper.core.partition.Partition;
import sleeper.core.partition.PartitionsBuilder;
import sleeper.core.schema.Schema;
import sleeper.core.schema.type.StringType;
import sleeper.core.statestore.FileReference;
import sleeper.core.statestore.FileReferenceFactory;
import sleeper.core.statestore.StateStore;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.configuration.properties.table.TableProperty.ADD_TRANSACTION_MAX_ATTEMPTS;
import static sleeper.configuration.properties.table.TableProperty.TIME_BETWEEN_SNAPSHOT_CHECKS_SECS;
import static sleeper.configuration.properties.table.TableProperty.TIME_BETWEEN_TRANSACTION_CHECKS_MS;
import static sleeper.core.schema.SchemaTestHelper.schemaWithKey;
import static sleeper.core.statestore.FileReferenceTestData.DEFAULT_UPDATE_TIME;

public class TransactionLogStateStoreSnapshotsIT extends TransactionLogStateStoreOneTableTestBase {

    private final Schema schema = schemaWithKey("key", new StringType());
    private final PartitionsBuilder partitions = new PartitionsBuilder(schema).singlePartition("root");

    @BeforeEach
    void setUp() {
        tableProperties.setSchema(schema);
        tableProperties.setNumber(ADD_TRANSACTION_MAX_ATTEMPTS, 1);
        tableProperties.setNumber(TIME_BETWEEN_SNAPSHOT_CHECKS_SECS, 0);
        tableProperties.setNumber(TIME_BETWEEN_TRANSACTION_CHECKS_MS, 0);
    }

    @Test
    void shouldLoadFilesFromSnapshotWhenNotInLogOnFirstLoad() throws Exception {
        // Given
        FileReference file = fileFactory().rootFile(123);

        // When
        createSnapshotWithFreshStateAtTransactionNumber(1, stateStore -> {
            stateStore.addFile(file);
        });

        // Then
        assertThat(stateStore().getFileReferences()).containsExactly(file);
    }

    @Test
    void shouldLoadPartitionsFromSnapshotWhenNotInLogOnFirstLoad() throws Exception {
        // Given
        partitions.splitToNewChildren("root", "L", "R", "abc");

        // When
        createSnapshotWithFreshStateAtTransactionNumber(1, stateStore -> {
            stateStore.initialise(partitions.buildList());
        });

        // Then
        assertThat(stateStore().getAllPartitions())
                .containsExactlyInAnyOrderElementsOf(partitions.buildList());
    }

    @Test
    void shouldNotLoadFilesSnapshotWhenOnlyOneTransactionAheadAfterLoadingLog() throws Exception {
        // Given
        StateStore stateStore = stateStore(builder -> builder
                .minTransactionsAheadToLoadSnapshot(2));
        FileReference logFile = fileFactory().rootFile("log-file.parquet", 123);
        FileReference snapshotFile = fileFactory().rootFile("snapshot-file.parquet", 123);
        stateStore.addFile(logFile);

        // When
        createSnapshotWithFreshStateAtTransactionNumber(2, snapshotStateStore -> {
            snapshotStateStore.addFile(snapshotFile);
        });

        // Then
        assertThat(stateStore.getFileReferences()).containsExactly(logFile);
    }

    @Test
    void shouldLoadFilesSnapshotWhenMoreThanConfiguredTransactionsAheadAfterLoadingLog() throws Exception {
        // Given
        StateStore stateStore = stateStore(builder -> builder
                .minTransactionsAheadToLoadSnapshot(2));
        FileReference logFile = fileFactory().rootFile("log-file.parquet", 123);
        FileReference snapshotFile = fileFactory().rootFile("snapshot-file.parquet", 123);
        stateStore.addFile(logFile);

        // When
        createSnapshotWithFreshStateAtTransactionNumber(3, snapshotStateStore -> {
            snapshotStateStore.addFile(snapshotFile);
        });

        // Then
        assertThat(stateStore.getFileReferences()).containsExactly(snapshotFile);
    }

    @Test
    void shouldNotLoadPartitionsSnapshotWhenOnlyOneTransactionAheadAfterLoadingLog() throws Exception {
        // Given
        StateStore stateStore = stateStore(builder -> builder
                .minTransactionsAheadToLoadSnapshot(2));
        List<Partition> logPartitions = new PartitionsBuilder(schema).rootFirst("A").buildList();
        List<Partition> snapshotPartitions = new PartitionsBuilder(schema).rootFirst("B").buildList();
        stateStore.initialise(logPartitions);

        // When
        createSnapshotWithFreshStateAtTransactionNumber(2, snapshotStateStore -> {
            snapshotStateStore.initialise(snapshotPartitions);
        });

        // Then
        assertThat(stateStore.getAllPartitions()).containsExactlyElementsOf(logPartitions);
    }

    @Test
    void shouldLoadPartitionsSnapshotWhenMoreThanConfiguredTransactionsAheadAfterLoadingLog() throws Exception {
        // Given
        StateStore stateStore = stateStore(builder -> builder
                .minTransactionsAheadToLoadSnapshot(2));
        List<Partition> logPartitions = new PartitionsBuilder(schema).rootFirst("A").buildList();
        List<Partition> snapshotPartitions = new PartitionsBuilder(schema).rootFirst("B").buildList();
        stateStore.initialise(logPartitions);

        // When
        createSnapshotWithFreshStateAtTransactionNumber(3, snapshotStateStore -> {
            snapshotStateStore.initialise(snapshotPartitions);
        });

        // Then
        assertThat(stateStore.getAllPartitions()).containsExactlyElementsOf(snapshotPartitions);
    }

    private StateStore stateStore() {
        return stateStore(builder -> {
        });
    }

    private FileReferenceFactory fileFactory() {
        return FileReferenceFactory.fromUpdatedAt(partitions.buildTree(), DEFAULT_UPDATE_TIME);
    }

}
