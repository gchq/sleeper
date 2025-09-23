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
package sleeper.core.statestore.testutils;

import sleeper.core.properties.table.TableProperties;
import sleeper.core.statestore.StateStore;
import sleeper.core.statestore.StateStoreException;
import sleeper.core.statestore.transactionlog.TransactionLogStateStore;
import sleeper.core.statestore.transactionlog.log.TransactionBodyStore;
import sleeper.core.statestore.transactionlog.log.TransactionLogStore;
import sleeper.core.statestore.transactionlog.snapshot.TransactionLogSnapshot;
import sleeper.core.statestore.transactionlog.snapshot.TransactionLogSnapshotCreator;
import sleeper.core.statestore.transactionlog.state.StateStoreFiles;
import sleeper.core.statestore.transactionlog.state.StateStorePartitions;
import sleeper.core.statestore.transactionlog.transaction.FileReferenceTransaction;
import sleeper.core.statestore.transactionlog.transaction.PartitionTransaction;

import static sleeper.core.statestore.FileReferenceTestData.DEFAULT_UPDATE_TIME;

/**
 * A test helper to setup in-memory snapshots derived from a transaction log.
 */
public class InMemoryTransactionLogSnapshotSetup {

    private final TableProperties tableProperties;
    private final TransactionLogStore filesLog;
    private final TransactionLogStore partitionsLog;
    private final TransactionBodyStore transactionBodyStore;

    /**
     * Sets up in-memory transaction logs with the given state. Sets up state by performing the requested interactions
     * with the state store. The constructed logs can then be used to derive snapshots.
     *
     * @param  tableProperties     the configuration of the Sleeper table the state is for
     * @param  setupState          a function to set up the required state
     * @return                     a wrapper around the transation logs, with the ability to create snapshots
     * @throws StateStoreException if the setup function throws a StateStoreException
     */
    public static InMemoryTransactionLogSnapshotSetup setupSnapshotWithFreshState(
            TableProperties tableProperties, SetupStateStore setupState) throws StateStoreException {
        InMemoryTransactionLogStore fileTransactions = new InMemoryTransactionLogStore();
        InMemoryTransactionLogStore partitionTransactions = new InMemoryTransactionLogStore();
        InMemoryTransactionBodyStore transactionBodyStore = new InMemoryTransactionBodyStore();
        StateStore stateStore = TransactionLogStateStore.builder()
                .tableProperties(tableProperties)
                .filesLogStore(fileTransactions)
                .partitionsLogStore(partitionTransactions)
                .transactionBodyStore(transactionBodyStore)
                .build();
        stateStore.fixFileUpdateTime(DEFAULT_UPDATE_TIME);
        stateStore.fixPartitionUpdateTime(DEFAULT_UPDATE_TIME);
        setupState.run(stateStore);
        return new InMemoryTransactionLogSnapshotSetup(tableProperties, fileTransactions, partitionTransactions, transactionBodyStore);
    }

    /**
     * A setup function to create required state in a state store. Will be used to set up an in-memory transaction log
     * with the given state.
     */
    public interface SetupStateStore {

        /**
         * Set up the required state in a state store.
         *
         * @param  stateStore          the state store
         * @throws StateStoreException if some operation on the state store fails
         */
        void run(StateStore stateStore) throws StateStoreException;
    }

    InMemoryTransactionLogSnapshotSetup(
            TableProperties tableProperties, TransactionLogStore filesLog, TransactionLogStore partitionsLog,
            TransactionBodyStore transactionBodyStore) {
        this.tableProperties = tableProperties;
        this.filesLog = filesLog;
        this.partitionsLog = partitionsLog;
        this.transactionBodyStore = transactionBodyStore;
    }

    /**
     * Create a snapshot of file references based on the state that was set. Fixes the transaction number that the
     * snapshot should be against, to simulate a snapshot being made against some other transaction log.
     *
     * @param  transactionNumber   the simulated transaction number to set
     * @return                     the snapshot
     * @throws StateStoreException if there are any failures updating the state from the log
     */
    public TransactionLogSnapshot createFilesSnapshot(long transactionNumber) throws StateStoreException {
        TransactionLogSnapshot snapshot = TransactionLogSnapshot.filesInitialState();
        snapshot = TransactionLogSnapshotCreator.createSnapshotIfChanged(
                snapshot, filesLog, transactionBodyStore, FileReferenceTransaction.class, tableProperties.getStatus())
                .orElse(snapshot);
        return new TransactionLogSnapshot((StateStoreFiles) snapshot.getState(), transactionNumber);
    }

    /**
     * Create a snapshot of partitions based on the state that was set. Fixes the transaction number that the
     * snapshot should be against, to simulate a snapshot being made against some other transaction log.
     *
     * @param  transactionNumber   the simulated transaction number to set
     * @return                     the snapshot
     * @throws StateStoreException if there are any failures updating the state from the log
     */
    public TransactionLogSnapshot createPartitionsSnapshot(long transactionNumber) throws StateStoreException {
        TransactionLogSnapshot snapshot = TransactionLogSnapshot.partitionsInitialState();
        snapshot = TransactionLogSnapshotCreator.createSnapshotIfChanged(
                snapshot, partitionsLog, transactionBodyStore, PartitionTransaction.class, tableProperties.getStatus())
                .orElse(snapshot);
        return new TransactionLogSnapshot((StateStorePartitions) snapshot.getState(), transactionNumber);
    }

    public TransactionLogStore getFilesLog() {
        return filesLog;
    }

    public TransactionLogStore getPartitionsLog() {
        return partitionsLog;
    }

    public TransactionBodyStore getTransactionBodyStore() {
        return transactionBodyStore;
    }
}
