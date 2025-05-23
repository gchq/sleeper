/*
 * Copyright 2022-2025 Crown Copyright
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
package sleeper.core.statestore.transactionlog.state;

import sleeper.core.statestore.transactionlog.log.TransactionLogEntry;
import sleeper.core.statestore.transactionlog.transaction.StateStoreTransaction;

/**
 * Listens to see the state before a transaction is applied to the local state. This is when the transaction is in the
 * log, but before it is applied locally.
 *
 * @param <S> the type of state to listen on
 * @param <T> the type of transaction to listen for
 */
@FunctionalInterface
public interface StateListenerBeforeApplyByType<S, T extends StateStoreTransaction<S>> {

    /**
     * Informs the listener that the transaction is about to be applied to the local state.
     *
     * @param entry       the log entry
     * @param transaction the transaction
     * @param state       the state
     */
    void beforeApply(TransactionLogEntry entry, T transaction, S state);

}
