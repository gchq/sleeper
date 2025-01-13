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
import java.util.function.Consumer;

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
        this(transactionNumber, updateTime, TransactionType.getType(transaction), null, transaction);
    }

    public TransactionLogEntry(long transactionNumber, Instant updateTime, TransactionType transactionType, TransactionBodyPointer bodyPointer) {
        this(transactionNumber, updateTime, transactionType, bodyPointer, null);
    }

    private TransactionLogEntry(long transactionNumber, Instant updateTime, TransactionType transactionType, TransactionBodyPointer bodyPointer, StateStoreTransaction<?> transaction) {
        this.transactionNumber = transactionNumber;
        this.updateTime = updateTime;
        this.transactionType = transactionType;
        this.bodyPointer = bodyPointer;
        this.transaction = transaction;
    }

    /**
     * Creates a transaction log entry from a request to add a transaction.
     *
     * @param  transactionNumber the transaction number
     * @param  updateTime        the update time
     * @param  request           the request
     * @return                   the log entry
     */
    public static TransactionLogEntry fromRequest(long transactionNumber, Instant updateTime, AddTransactionRequest request) {
        Optional<String> bodyPointer = request.getBodyKey();
        if (bodyPointer.isPresent()) {
            return new TransactionLogEntry(transactionNumber, updateTime, request.getTransactionType(), new TransactionBodyPointer("bucket", bodyPointer.get()));
        } else {
            return new TransactionLogEntry(transactionNumber, updateTime, request.getTransaction());
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

    /**
     * Applies some operation on the transaction or the pointer, whichever is held in the entry.
     *
     * @param withTransaction the operation on a transaction
     * @param withPointer     the operation on a pointer
     */
    public void withTransactionOrPointer(Consumer<StateStoreTransaction<?>> withTransaction, Consumer<TransactionBodyPointer> withPointer) {
        if (transaction != null) {
            withTransaction.accept(transaction);
        } else {
            withPointer.accept(bodyPointer);
        }
    }

    /**
     * Returns the transaction object held in this entry. Loads it from a transaction body store if the entry is a
     * pointer.
     *
     * @param  bodyStore the transaction body store
     * @return           the transaction
     */
    public StateStoreTransaction<?> getTransactionOrLoadFromPointer(TransactionBodyStore bodyStore) {
        if (transaction != null) {
            return transaction;
        } else {
            return bodyStore.getBody(bodyPointer, transactionType);
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
