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

import sleeper.core.statestore.transactionlog.transactions.TransactionType;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/**
 * An in memory store of the bodies of transactions that are not held directly in the log.
 */
public class InMemoryTransactionBodyStore implements TransactionBodyStore {

    private final Map<TransactionBodyPointer, StateStoreTransaction<?>> transactionByPointer = new HashMap<>();

    @Override
    public void store(TransactionBodyPointer pointer, StateStoreTransaction<?> transaction) {
        transactionByPointer.put(pointer, transaction);
    }

    @Override
    public <T extends StateStoreTransaction<?>> T getBody(TransactionBodyPointer pointer, TransactionType transactionType) {
        return (T) Objects.requireNonNull(transactionByPointer.get(pointer));
    }

    public Map<TransactionBodyPointer, StateStoreTransaction<?>> getTransactionByPointer() {
        return transactionByPointer;
    }
}
