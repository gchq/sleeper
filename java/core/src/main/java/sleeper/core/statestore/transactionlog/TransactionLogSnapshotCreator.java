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

import sleeper.core.statestore.StateStoreException;
import sleeper.core.table.TableStatus;

import java.util.Optional;

public class TransactionLogSnapshotCreator {
    public static final Logger LOGGER = LoggerFactory.getLogger(TransactionLogSnapshotCreator.class);

    private TransactionLogSnapshotCreator() {
    }

    public static <T> Optional<TransactionLogSnapshot> createSnapshotIfChanged(
            TransactionLogSnapshot lastSnapshot,
            TransactionLogStore logStore,
            Class<? extends StateStoreTransaction<T>> transactionType,
            TableStatus tableStatus) throws StateStoreException {
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

    public static <T> TransactionLogSnapshot updateState(
            TransactionLogSnapshot lastSnapshot,
            Class<? extends StateStoreTransaction<T>> transactionType, TransactionLogStore logStore,
            TableStatus table) throws StateStoreException {
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
