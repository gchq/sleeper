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
import java.util.List;
import java.util.stream.Stream;

public class InMemoryTransactionLogStore implements TransactionLogStore {

    private static final Runnable DO_NOTHING = () -> {
    };

    private final List<StateStoreTransaction<?>> transactions = new ArrayList<>();
    private Runnable beforeNextAdd = DO_NOTHING;
    private Runnable beforeNextRead = DO_NOTHING;

    @Override
    public void addTransaction(StateStoreTransaction<?> transaction, long transactionNumber) throws DuplicateTransactionNumberException {
        doBeforeNextAdd();
        if (transactionNumber <= transactions.size()) {
            throw new DuplicateTransactionNumberException(transactionNumber);
        }
        if (transactionNumber > transactions.size() + 1) {
            throw new IllegalStateException("Attempted to add transaction " + transactionNumber + " when we only have " + transactions.size());
        }
        transactions.add(transaction);
    }

    @Override
    public Stream<StateStoreTransaction<?>> readTransactionsAfter(long lastTransactionNumber) {
        doBeforeNextRead();
        return transactions.stream()
                .skip(lastTransactionNumber);
    }

    public void beforeNextAddTransaction(ThrowingRunnable action) {
        beforeNextAdd = wrappingCheckedExceptions(action);
    }

    public void beforeNextReadTransactions(ThrowingRunnable action) {
        beforeNextRead = wrappingCheckedExceptions(action);
    }

    private void doBeforeNextAdd() {
        Runnable action = beforeNextAdd;
        beforeNextAdd = DO_NOTHING;
        action.run();
    }

    private void doBeforeNextRead() {
        Runnable action = beforeNextRead;
        beforeNextRead = DO_NOTHING;
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

    public interface ThrowingRunnable {
        void run() throws Exception;
    }
}
