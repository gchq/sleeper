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

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * An in-memory implementation of a transaction log store. Holds transactions in a list. Can simulate some other process
 * performing some operation just as a transaction is added or as transactions are read.
 */
public class InMemoryTransactionLogStore implements TransactionLogStore {

    private List<TransactionLogEntry> transactionEntries = new ArrayList<>();
    private Consumer<TransactionLogEntry> onReadTransactionLogEntry = entry -> {
    };
    private Runnable startOfAdd = () -> {
    };
    private Runnable startOfRead = () -> {
    };
    private boolean runningTrigger = false;

    @Override
    public void addTransaction(TransactionLogEntry entry) throws DuplicateTransactionNumberException {
        long transactionNumber = entry.getTransactionNumber();
        doStartOfAddTransaction();
        if (transactionNumber <= transactionEntries.size()) {
            throw new DuplicateTransactionNumberException(transactionNumber);
        }
        if (transactionNumber > transactionEntries.size() + 1) {
            throw new IllegalStateException("Attempted to add transaction " + transactionNumber + " when we only have " + transactionEntries.size());
        }
        transactionEntries.add(entry);
    }

    @Override
    public Stream<TransactionLogEntry> readTransactionsAfter(long lastTransactionNumber) {
        doStartOfReadTransactions();
        return transactionEntries.stream()
                .skip(lastTransactionNumber)
                .peek(onReadTransactionLogEntry);
    }

    @Override
    public Stream<TransactionLogEntry> readTransactionsBetween(long lastTransactionNumber, long nextTransactionNumber) {
        return readTransactionsAfter(lastTransactionNumber)
                .limit(Math.max(0, nextTransactionNumber - lastTransactionNumber - 1));
    }

    @Override
    public void deleteTransactionsAtOrBefore(long transactionNumber) {
        transactionEntries = transactionEntries.stream()
                .filter(transaction -> transaction.getTransactionNumber() > transactionNumber)
                .collect(Collectors.toList());
    }

    /**
     * Sets a listener for when a log entry is read.
     *
     * @param onReadTransactionLogEntry the listener
     */
    public void onReadTransactionLogEntry(Consumer<TransactionLogEntry> onReadTransactionLogEntry) {
        this.onReadTransactionLogEntry = onReadTransactionLogEntry;
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
        startOfAdd = onNext(action);
    }

    /**
     * Sets operations that should happen just before the next transactions are added. Each time a transaction is added,
     * a single action will be taken from the list and run. This will occur after any local state has been brought up to
     * date in the state store. If the given operation adds a transaction to the log, it will conflict with the
     * transaction being added. If an exception is thrown in the operation, that will be thrown out of the transaction
     * log store.
     *
     * @param actions the operations to occur as each transaction is added
     */
    public void atStartOfNextAddTransactions(List<ThrowingRunnable> actions) {
        startOfAdd = onNext(actions);
    }

    /**
     * Sets some operation that should happen just before any transaction is added. This will occur after any
     * local state has been brought up to date in the state store. If the given operation adds a transaction to the
     * log, it will conflict with the transaction being added. If an exception is thrown in the operation, that will be
     * thrown out of the transaction log store.
     *
     * @param action the operation to occur before each transaction
     */
    public void atStartOfAddTransaction(ThrowingRunnable action) {
        startOfAdd = wrappingCheckedExceptions(action);
    }

    /**
     * Sets some operation that should happen just before the next time transactions are read. This will occur the next
     * time the local state store brings itself up to date. If an exception is thrown in the operation, that will be
     * thrown out of the transaction log store.
     *
     * @param action the operation to occur before the next time transactions are read
     */
    public void atStartOfNextReadTransactions(ThrowingRunnable action) {
        startOfRead = onNext(action);
    }

    /**
     * Sets some operation that should happen just before transactions are read. This will occur every time the local
     * state store brings itself up to date. If an exception is thrown in the operation, that will be thrown out of the
     * transaction log store.
     *
     * @param action the operation to occur before transactions are read
     */
    public void atStartOfReadTransactions(ThrowingRunnable action) {
        startOfRead = wrappingCheckedExceptions(action);
    }

    public long getLastTransactionNumber() {
        return transactionEntries.size();
    }

    public TransactionLogEntry getLastEntry() {
        return transactionEntries.get(transactionEntries.size() - 1);
    }

    private void doStartOfAddTransaction() {
        startOfAdd.run();
    }

    private void doStartOfReadTransactions() {
        startOfRead.run();
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

        ThrowingRunnable DO_NOTHING = () -> {
        };
    }

    private TriggerActions onNext(ThrowingRunnable action) {
        return onNext(List.of(action));
    }

    private TriggerActions onNext(List<ThrowingRunnable> actions) {
        return new TriggerActions(actions);
    }

    /**
     * Tracks actions that will be done when some trigger is met.
     */
    private class TriggerActions implements Runnable {

        private final Queue<Runnable> queue = new LinkedList<>();

        private TriggerActions(Collection<ThrowingRunnable> actions) {
            actions.stream()
                    .map(action -> wrappingCheckedExceptions(action))
                    .forEach(queue::add);
        }

        @Override
        public void run() {
            if (runningTrigger) {
                return;
            }
            Runnable runnable = queue.poll();
            if (runnable != null) {
                runningTrigger = true;
                runnable.run();
                runningTrigger = false;
            }
        }
    }
}
