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

import sleeper.core.statestore.transactionlog.state.StateStoreFiles;
import sleeper.core.statestore.transactionlog.state.StateStorePartitions;

/**
 * A snapshot of the state at a given point in a transaction log. Since the state is always derived from all the
 * transactions in the log up to a given point, if we have a snapshot at a given transaction we know we only need to
 * look at transactions after that point.
 */
public class TransactionLogSnapshot {

    private final Object state;
    private final long transactionNumber;

    public TransactionLogSnapshot(StateStoreFiles state, long transactionNumber) {
        this((Object) state, transactionNumber);
    }

    public TransactionLogSnapshot(StateStorePartitions state, long transactionNumber) {
        this((Object) state, transactionNumber);
    }

    /**
     * Creates a snapshot of the initial state of file references when no transactions have occurred.
     *
     * @return the snapshot
     */
    public static TransactionLogSnapshot filesInitialState() {
        return new TransactionLogSnapshot(new StateStoreFiles(), 0);
    }

    /**
     * Creates a snapshot of the initial state of partitions when no transactions have occurred.
     *
     * @return the snapshot
     */
    public static TransactionLogSnapshot partitionsInitialState() {
        return new TransactionLogSnapshot(new StateStorePartitions(), 0);
    }

    TransactionLogSnapshot(Object state, long transactionNumber) {
        this.state = state;
        this.transactionNumber = transactionNumber;
    }

    /**
     * Retrieves the state. This relies on keeping track of the correct type external to this class.
     *
     * @param  <T> the type of the state
     * @return     the state
     */
    public <T> T getState() {
        return (T) state;
    }

    public long getTransactionNumber() {
        return transactionNumber;
    }
}
