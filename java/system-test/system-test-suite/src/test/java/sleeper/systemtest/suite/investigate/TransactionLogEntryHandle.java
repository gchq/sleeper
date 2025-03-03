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
package sleeper.systemtest.suite.investigate;

import sleeper.core.statestore.transactionlog.log.TransactionBodyStore;
import sleeper.core.statestore.transactionlog.log.TransactionLogEntry;
import sleeper.core.statestore.transactionlog.log.TransactionLogRange;
import sleeper.core.statestore.transactionlog.log.TransactionLogStore;
import sleeper.core.statestore.transactionlog.transaction.StateStoreTransaction;
import sleeper.core.statestore.transactionlog.transaction.TransactionType;

import java.util.LinkedList;
import java.util.List;

import static java.util.stream.Collectors.toCollection;

public record TransactionLogEntryHandle(TransactionLogEntry original, StateStoreTransaction<?> transaction) {

    public static List<TransactionLogEntryHandle> load(TransactionLogStore logStore) {
        return logStore
                .readTransactions(TransactionLogRange.fromMinimum(1))
                .map(entry -> new TransactionLogEntryHandle(entry, entry.getTransaction().orElseThrow()))
                .collect(toCollection(LinkedList::new));
    }

    public static List<TransactionLogEntryHandle> load(String tableId, TransactionLogStore logStore, TransactionBodyStore bodyStore) {
        return logStore
                .readTransactions(TransactionLogRange.fromMinimum(1))
                .map(entry -> {
                    StateStoreTransaction<?> transaction = entry.getTransactionOrLoadFromPointer(tableId, bodyStore);
                    return new TransactionLogEntryHandle(entry, transaction);
                })
                .collect(toCollection(LinkedList::new));
    }

    public <S> void apply(S state) {
        StateStoreTransaction<S> transaction = castTransaction();
        transaction.apply(state, original.getUpdateTime());
    }

    public boolean isType(TransactionType transactionType) {
        return original.getTransactionType() == transactionType;
    }

    public <S, T extends StateStoreTransaction<S>> T castTransaction() {
        return (T) transaction;
    }

    public long transactionNumber() {
        return original.getTransactionNumber();
    }
}
