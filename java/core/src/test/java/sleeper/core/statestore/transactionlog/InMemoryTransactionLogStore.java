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

    @Override
    public void addTransaction(StateStoreTransaction<?> transaction, long transactionNumber) throws UnreadTransactionException {
        doBeforeNextAdd();
        if (transactions.size() + 1 != transactionNumber) {
            throw new UnreadTransactionException(transactionNumber, transactions.size());
        }
        transactions.add(transaction);
    }

    @Override
    public Stream<StateStoreTransaction<?>> readTransactionsAfter(long lastTransactionNumber) {
        return transactions.stream()
                .skip(lastTransactionNumber);
    }

    public void beforeNextAddTransaction(ThrowingRunnable action) {
        beforeNextAdd = () -> {
            try {
                action.run();
            } catch (RuntimeException e) {
                throw e;
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        };
    }

    private void doBeforeNextAdd() {
        Runnable action = beforeNextAdd;
        beforeNextAdd = DO_NOTHING;
        action.run();
    }

    public interface ThrowingRunnable {
        void run() throws Exception;
    }
}
