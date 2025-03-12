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
package sleeper.core.statestore.transactionlog.transaction;

import sleeper.core.statestore.StateStore;
import sleeper.core.statestore.StateStoreException;

import java.time.Instant;

/**
 * A transaction that updates some state held in a state store.
 *
 * @param <T> the type of the state being updated
 */
public interface StateStoreTransaction<T> {

    /**
     * Validates whether this transaction may be applied to the given state.
     *
     * @param  state               the state before the transaction
     * @throws StateStoreException thrown if the transaction may not be applied
     */
    void validate(T state) throws StateStoreException;

    /**
     * Applies this transaction to the given state.
     *
     * @param state      the state to be updated
     * @param updateTime the time that the update was added to the state store
     */
    void apply(T state, Instant updateTime);

    /**
     * Checks if this transaction is empty and can be ignored.
     *
     * @return true if the transaction is empty
     */
    default boolean isEmpty() {
        return false;
    }

    /**
     * Validates against the state store before attempting to add this transaction. Should not be relied upon
     * but can be used as an extra check against state that this transaction does not operate on. Note that
     * any checks from this are before the state is updated as part of the transaction. This cannot validate
     * against the state the transaction is applied to as it may be updated by another process in the meantime.
     *
     * @param stateStore the state store
     * @see              #validate
     */
    default void checkBefore(StateStore stateStore) throws StateStoreException {
    }
}
