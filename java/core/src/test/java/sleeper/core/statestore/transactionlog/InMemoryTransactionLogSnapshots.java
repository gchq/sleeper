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
package sleeper.core.statestore.transactionlog;

import sleeper.core.schema.Schema;
import sleeper.core.statestore.StateStore;
import sleeper.core.statestore.StateStoreException;
import sleeper.core.table.TableStatus;

import java.util.Optional;

import static sleeper.core.statestore.FileReferenceTestData.DEFAULT_UPDATE_TIME;

/**
 * An in-memory implementation of snapshots derived from a transaction log.
 */
public class InMemoryTransactionLogSnapshots implements TransactionLogSnapshotLoader {

    private TransactionLogSnapshot latestSnapshot;

    public static InMemoryTransactionLogSnapshotSetup setupSnapshotWithFreshState(
            TableStatus sleeperTable, Schema schema, SetupStateStore setupState) throws StateStoreException {
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
        return new InMemoryTransactionLogSnapshotSetup(sleeperTable, fileTransactions, partitionTransactions);
    }

    public void setLatestSnapshot(TransactionLogSnapshot latestSnapshot) {
        this.latestSnapshot = latestSnapshot;
    }

    @Override
    public Optional<TransactionLogSnapshot> loadLatestSnapshotIfAtMinimumTransaction(long transactionNumber) {
        return Optional.ofNullable(latestSnapshot)
                .filter(snapshot -> snapshot.getTransactionNumber() >= transactionNumber);
    }

    public interface SetupStateStore {
        void run(StateStore stateStore) throws StateStoreException;
    }

}
