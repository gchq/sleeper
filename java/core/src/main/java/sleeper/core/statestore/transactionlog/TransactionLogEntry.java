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

import java.util.Objects;

/**
 * An entry in a state store transaction log.
 */
public class TransactionLogEntry {

    private final long transactionNumber;
    private final StateStoreTransaction<?> transaction;

    public TransactionLogEntry(long transactionNumber, StateStoreTransaction<?> transaction) {
        this.transactionNumber = transactionNumber;
        this.transaction = transaction;
    }

    public long getTransactionNumber() {
        return transactionNumber;
    }

    public StateStoreTransaction<?> getTransaction() {
        return transaction;
    }

    @Override
    public int hashCode() {
        return Objects.hash(transactionNumber, transaction);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (!(obj instanceof TransactionLogEntry)) {
            return false;
        }
        TransactionLogEntry other = (TransactionLogEntry) obj;
        return transactionNumber == other.transactionNumber && Objects.equals(transaction, other.transaction);
    }

    @Override
    public String toString() {
        return "TransactionLogEntry{transactionNumber=" + transactionNumber + ", transaction=" + transaction + "}";
    }

}
