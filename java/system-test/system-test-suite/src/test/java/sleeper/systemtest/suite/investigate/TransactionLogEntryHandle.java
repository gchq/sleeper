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

import sleeper.core.statestore.transactionlog.log.TransactionLogEntry;
import sleeper.core.statestore.transactionlog.transaction.StateStoreTransaction;
import sleeper.core.statestore.transactionlog.transaction.TransactionType;

public record TransactionLogEntryHandle(TransactionLogEntry original, StateStoreTransaction<?> transaction) {

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
