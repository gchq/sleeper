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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sleeper.core.table.TableStatus;

import java.util.Optional;

/**
 * Creates snapshots of the state derived from a transaction log.
 */
public class TransactionLogSnapshotCreator {
    public static final Logger LOGGER = LoggerFactory.getLogger(TransactionLogSnapshotCreator.class);

    private TransactionLogSnapshotCreator() {
    }

    /**
     * Creates a snapshot if there are new transactions in the log since the last snapshot. Seeks through the log
     * starting at the transaction the last snapshot was made against, and applies each transaction to the state.
     * <p>
     * The state object held in the previous snapshot will be mutated and reused in the new snapshot object if one is
     * made.
     *
     * @param  <T>             the type of the state derived from the log
     * @param  lastSnapshot    the last snapshot
     * @param  logStore        the transaction log to read from
     * @param  transactionType the type of transactions to read from the log
     * @param  tableStatus     the Sleeper table the log is for, to be used in logging
     * @return                 the new snapshot, if there were updates since the last snapshot
     */
    public static <T> Optional<TransactionLogSnapshot> createSnapshotIfChanged(
            TransactionLogSnapshot lastSnapshot,
            TransactionLogStore logStore,
            Class<? extends StateStoreTransaction<T>> transactionType,
            TableStatus tableStatus) {
        TransactionLogSnapshot newSnapshot = updateState(
                lastSnapshot, transactionType, logStore, tableStatus);
        if (lastSnapshot.getTransactionNumber() >= newSnapshot.getTransactionNumber()) {
            LOGGER.info("No new {}s found after transaction number {}, skipping snapshot creation.",
                    transactionType.getSimpleName(),
                    lastSnapshot.getTransactionNumber());
            return Optional.empty();
        }
        LOGGER.info("Transaction found with number {} is newer than latest {} number {}.",
                newSnapshot.getTransactionNumber(), transactionType.getSimpleName(), lastSnapshot.getTransactionNumber());
        LOGGER.info("Creating a new snapshot from latest {}.", transactionType.getSimpleName());
        return Optional.of(newSnapshot);
    }

    private static <T> TransactionLogSnapshot updateState(
            TransactionLogSnapshot lastSnapshot,
            Class<? extends StateStoreTransaction<T>> transactionType, TransactionLogStore logStore,
            TableStatus table) {
        T state = lastSnapshot.getState();
        TransactionLogHead<T> head = TransactionLogHead.builder()
                .transactionType(transactionType)
                .logStore(logStore)
                .lastTransactionNumber(lastSnapshot.getTransactionNumber())
                .state(state)
                .sleeperTable(table)
                .build();
        head.update();
        return new TransactionLogSnapshot(state, head.lastTransactionNumber());
    }
}
