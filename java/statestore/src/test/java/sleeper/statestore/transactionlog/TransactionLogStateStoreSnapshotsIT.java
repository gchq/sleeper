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

import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import sleeper.core.partition.PartitionsBuilder;
import sleeper.core.schema.Schema;
import sleeper.core.schema.type.StringType;
import sleeper.core.statestore.FileReference;
import sleeper.core.statestore.FileReferenceFactory;
import sleeper.core.statestore.StateStore;
import sleeper.core.statestore.StateStoreException;
import sleeper.core.statestore.transactionlog.InMemoryTransactionLogStore;
import sleeper.core.statestore.transactionlog.TransactionLogStateStore;
import sleeper.core.table.TableStatus;

import java.util.function.Consumer;

import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.core.schema.SchemaTestHelper.schemaWithKey;
import static sleeper.core.statestore.FileReferenceTestData.DEFAULT_UPDATE_TIME;

@Disabled
public class TransactionLogStateStoreSnapshotsIT extends TransactionLogStateStoreOneTableTestBase {

    private final Schema schema = schemaWithKey("key", new StringType());
    private final PartitionsBuilder partitions = new PartitionsBuilder(schema).singlePartition("root");

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
    void shouldNotLoadSnapshotWhenOnlyOneTransactionAheadAfterLoadingLog() throws Exception {
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
    void shouldLoadSnapshotWhenMoreThanConfiguredTransactionsAheadAfterLoadingLog() throws Exception {
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

    private StateStore stateStore() {
        return stateStore(builder -> {
        });
    }

    protected StateStore stateStore(Consumer<TransactionLogStateStore.Builder> config) {
        TransactionLogStateStore.Builder builder = stateStoreBuilder(tableProperties)
                .maxAddTransactionAttempts(1);
        config.accept(builder);
        return stateStore(builder);
    }

    private FileReferenceFactory fileFactory() {
        return FileReferenceFactory.fromUpdatedAt(partitions.buildTree(), DEFAULT_UPDATE_TIME);
    }

    protected void createSnapshotWithFreshStateAtTransactionNumber(
            long transactionNumber, SetupStateStore setupState) throws Exception {
        TableStatus sleeperTable = tableProperties.getStatus();
        InMemoryTransactionLogStore fileTransactions = new InMemoryTransactionLogStore();
        InMemoryTransactionLogStore partitionTransactions = new InMemoryTransactionLogStore();
        StateStore stateStore = TransactionLogStateStore.builder()
                .sleeperTable(sleeperTable)
                .schema(schema)
                .filesLogStore(fileTransactions)
                .partitionsLogStore(partitionTransactions)
                .build();
        stateStore.fixFileUpdateTime(DEFAULT_UPDATE_TIME);
        stateStore.fixPartitionUpdateTime(DEFAULT_UPDATE_TIME);
        setupState.run(stateStore);
        //fileSnapshots.setLatestSnapshot(createFilesSnapshot(sleeperTable, fileTransactions, transactionNumber));
        // partitionSnapshots.setLatestSnapshot(createPartitionsSnapshot(sleeperTable, partitionTransactions, transactionNumber));
    }

    public interface SetupStateStore {
        void run(StateStore stateStore) throws StateStoreException;
    }

}
