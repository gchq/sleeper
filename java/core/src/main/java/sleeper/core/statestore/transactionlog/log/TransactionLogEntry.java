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
package sleeper.core.statestore.transactionlog.log;

import sleeper.core.statestore.transactionlog.AddTransactionRequest;
import sleeper.core.statestore.transactionlog.transaction.StateStoreTransaction;
import sleeper.core.statestore.transactionlog.transaction.TransactionSerDe;
import sleeper.core.statestore.transactionlog.transaction.TransactionType;

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
    private final String bodyKey;
    private final StateStoreTransaction<?> transaction;
    private final String serialisedTransaction;

    public TransactionLogEntry(long transactionNumber, Instant updateTime, StateStoreTransaction<?> transaction) {
        this(transactionNumber, updateTime, transaction, null);
    }

    public TransactionLogEntry(long transactionNumber, Instant updateTime, StateStoreTransaction<?> transaction, String serialisedTransaction) {
        this(transactionNumber, updateTime, TransactionType.getType(transaction), null, transaction, serialisedTransaction);
    }

    public TransactionLogEntry(long transactionNumber, Instant updateTime, TransactionType transactionType, String bodyKey) {
        this(transactionNumber, updateTime, transactionType, bodyKey, null, null);
    }

    private TransactionLogEntry(long transactionNumber, Instant updateTime, TransactionType transactionType, String bodyKey, StateStoreTransaction<?> transaction, String serialisedTransaction) {
        this.transactionNumber = transactionNumber;
        this.updateTime = updateTime;
        this.transactionType = transactionType;
        this.bodyKey = bodyKey;
        this.transaction = transaction;
        this.serialisedTransaction = serialisedTransaction;
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
        Optional<String> bodyKey = request.getBodyKey();
        if (bodyKey.isPresent()) {
            return new TransactionLogEntry(transactionNumber, updateTime, request.getTransactionType(), bodyKey.get());
        } else {
            StateStoreTransaction<?> transaction = request.getTransaction();
            String serialisedTransaction = request.getSerialisedTransaction().orElse(null);
            return new TransactionLogEntry(transactionNumber, updateTime, transaction, serialisedTransaction);
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

    public Optional<String> getBodyKey() {
        return Optional.ofNullable(bodyKey);
    }

    public Optional<StateStoreTransaction<?>> getTransaction() {
        return Optional.ofNullable(transaction);
    }

    /**
     * Applies some operation on the transaction or the object key in the data bucket, whichever is held in the entry.
     *
     * @param withTransaction the operation on a transaction
     * @param withObjectKey   the operation on an object key
     */
    public void withSerialisedTransactionOrObjectKey(TransactionSerDe serDe, Consumer<String> withTransaction, Consumer<String> withObjectKey) {
        if (bodyKey != null) {
            withObjectKey.accept(bodyKey);
        } else if (serialisedTransaction != null) {
            withTransaction.accept(serialisedTransaction);
        } else {
            withTransaction.accept(serDe.toJson(transaction));
        }
    }

    /**
     * Returns the transaction object held in this entry. Loads it from a transaction body store if the entry is a
     * pointer.
     *
     * @param  bodyStore the transaction body store
     * @return           the transaction
     */
    public StateStoreTransaction<?> getTransactionOrLoadFromPointer(String tableId, TransactionBodyStore bodyStore) {
        if (transaction != null) {
            return transaction;
        } else {
            return bodyStore.getBody(bodyKey, tableId, transactionType);
        }
    }

    @Override
    public int hashCode() {
        return Objects.hash(transactionNumber, updateTime, transactionType, bodyKey, transaction);
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
        return transactionNumber == other.transactionNumber && Objects.equals(updateTime, other.updateTime) && transactionType == other.transactionType && Objects.equals(bodyKey, other.bodyKey)
                && Objects.equals(transaction, other.transaction);
    }

    @Override
    public String toString() {
        return "TransactionLogEntry{transactionNumber=" + transactionNumber + ", updateTime=" + updateTime +
                ", transactionType=" + transactionType + ", bodyKey=" + bodyKey + ", transaction=" + transaction + "}";
    }

}
