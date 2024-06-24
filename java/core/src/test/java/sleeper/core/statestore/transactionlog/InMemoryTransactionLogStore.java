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

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * An in-memory implementation of a transaction log store. Holds transactions in a list. Can simulate some other process
 * performing some operation just as a transaction is added or as transactions are read.
 */
public class InMemoryTransactionLogStore implements TransactionLogStore {

    private static final Runnable DO_NOTHING = () -> {
    };

    private List<TransactionLogEntry> transactions = new ArrayList<>();
    private Runnable startOfNextAdd = DO_NOTHING;
    private Runnable startOfNextRead = DO_NOTHING;

    @Override
    public void addTransaction(TransactionLogEntry entry) throws DuplicateTransactionNumberException {
        long transactionNumber = entry.getTransactionNumber();
        doStartOfAddTransaction();
        if (transactionNumber <= transactions.size()) {
            throw new DuplicateTransactionNumberException(transactionNumber);
        }
        if (transactionNumber > transactions.size() + 1) {
            throw new IllegalStateException("Attempted to add transaction " + transactionNumber + " when we only have " + transactions.size());
        }
        transactions.add(entry);
    }

    @Override
    public Stream<TransactionLogEntry> readTransactionsAfter(long lastTransactionNumber) {
        doStartOfReadTransactions();
        return transactions.stream()
                .skip(lastTransactionNumber);
    }

    @Override
    public void deleteTransactionsAtOrBefore(long transactionNumber, Instant updateTime) {
        transactions = transactions.stream()
                .filter(transaction -> transaction.getTransactionNumber() > transactionNumber)
                .collect(Collectors.toList());
    }

    /**
     * Sets some operation that should happen just before the next transaction is added. This will occur after any
     * local state has been brought up to date in the state store. If the given operation adds a transaction to the
     * log, it will conflict with the transaction being added. If an exception is thrown in the operation, that will be
     * thrown out of the transaction log store.
     *
     * @param action the operation to occur before the next transaction
     */
    public void atStartOfNextAddTransaction(ThrowingRunnable action) {
        startOfNextAdd = wrappingCheckedExceptions(action);
    }

    /**
     * Sets some operation that should happen just before the next time transactions are read. This will occur the next
     * time the local state store brings itself up to date. If an exception is thrown in the operation, that will be
     * thrown out of the transaction log store.
     *
     * @param action the operation to occur before the next time transactions are read
     */
    public void atStartOfNextReadTransactions(ThrowingRunnable action) {
        startOfNextRead = wrappingCheckedExceptions(action);
    }

    public long getLastTransactionNumber() {
        return transactions.size();
    }

    private void doStartOfAddTransaction() {
        Runnable action = startOfNextAdd;
        startOfNextAdd = DO_NOTHING;
        action.run();
    }

    private void doStartOfReadTransactions() {
        Runnable action = startOfNextRead;
        startOfNextRead = DO_NOTHING;
        action.run();
    }

    private static Runnable wrappingCheckedExceptions(ThrowingRunnable action) {
        return () -> {
            try {
                action.run();
            } catch (RuntimeException e) {
                throw e;
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        };
    }

    /**
     * Performs some action that can throw checked exceptions.
     */
    public interface ThrowingRunnable {

        /**
         * Perform the action.
         *
         * @throws Exception if any exception was thrown by the action
         */
        void run() throws Exception;
    }
}
