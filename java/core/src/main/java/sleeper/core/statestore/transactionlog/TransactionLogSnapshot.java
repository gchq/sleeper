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

public class TransactionLogSnapshot {

    private final Object state;
    private final long transactionNumber;

    public TransactionLogSnapshot(StateStoreFiles state, long transactionNumber) {
        this((Object) state, transactionNumber);
    }

    public TransactionLogSnapshot(StateStorePartitions state, long transactionNumber) {
        this((Object) state, transactionNumber);
    }

    public static TransactionLogSnapshot filesInitialState() {
        return new TransactionLogSnapshot(new StateStoreFiles(), 0);
    }

    public static TransactionLogSnapshot partitionsInitialState() {
        return new TransactionLogSnapshot(new StateStorePartitions(), 0);
    }

    TransactionLogSnapshot(Object state, long transactionNumber) {
        this.state = state;
        this.transactionNumber = transactionNumber;
    }

    public <T> T getState() {
        return (T) state;
    }

    public long getTransactionNumber() {
        return transactionNumber;
    }
}
