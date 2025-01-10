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

import java.time.Instant;
import java.util.Objects;
import java.util.Optional;

/**
 * An entry in a state store transaction log.
 */
public class TransactionLogEntry {

    private final long transactionNumber;
    private final Instant updateTime;
    private final TransactionType transactionType;
    private final TransactionBodyPointer bodyPointer;
    private final StateStoreTransaction<?> transaction;

    public TransactionLogEntry(long transactionNumber, Instant updateTime, StateStoreTransaction<?> transaction) {
        this(transactionNumber, updateTime, AddTransactionRequest.transaction(transaction));
    }

    public TransactionLogEntry(long transactionNumber, Instant updateTime, AddTransactionRequest request) {
        this.transactionNumber = transactionNumber;
        this.updateTime = updateTime;
        this.transactionType = TransactionType.getType(request.getTransaction());
        Optional<TransactionBodyPointer> bodyPointer = request.getBodyPointer();
        if (bodyPointer.isPresent()) {
            this.bodyPointer = bodyPointer.get();
            this.transaction = null;
        } else {
            this.bodyPointer = null;
            this.transaction = request.getTransaction();
        }
    }

    public long getTransactionNumber() {
        return transactionNumber;
    }

    public Instant getUpdateTime() {
        return updateTime;
    }

    public TransactionType getTransactionType() {
        return transactionType;
    }

    public Optional<TransactionBodyPointer> getBodyPointer() {
        return Optional.ofNullable(bodyPointer);
    }

    public Optional<StateStoreTransaction<?>> getTransaction() {
        return Optional.ofNullable(transaction);
    }

    public StateStoreTransaction<?> getTransactionOrLoadFromPointer(TransactionBodyStore bodyStore) {
        if (transaction != null) {
            return transaction;
        } else {
            return bodyStore.getBody(bodyPointer);
        }
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
        return transactionNumber == other.transactionNumber
                && Objects.equals(updateTime, other.updateTime)
                && Objects.equals(transaction, other.transaction);
    }

    @Override
    public String toString() {
        return "TransactionLogEntry{transactionNumber=" + transactionNumber + ", updateTime=" + updateTime + ", transaction=" + transaction + "}";
    }

}
