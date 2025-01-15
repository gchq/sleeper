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
package sleeper.core.statestore.commit;

import sleeper.core.statestore.transactionlog.AddTransactionRequest;
import sleeper.core.statestore.transactionlog.StateStoreTransaction;
import sleeper.core.statestore.transactionlog.TransactionBodyStore;
import sleeper.core.statestore.transactionlog.transactions.TransactionType;

import java.util.Objects;
import java.util.Optional;

/**
 * A commit request for a transaction to be added to the state store.
 */
public class StateStoreCommitRequestByTransaction {

    private final String tableId;
    private final TransactionType transactionType;
    private final String bodyKey;
    private final StateStoreTransaction<?> transaction;

    private StateStoreCommitRequestByTransaction(String tableId, TransactionType transactionType, String bodyKey, StateStoreTransaction<?> transaction) {
        this.tableId = tableId;
        this.transactionType = transactionType;
        this.bodyKey = bodyKey;
        this.transaction = transaction;
    }

    /**
     * Creates a commit request for a transaction in S3.
     *
     * @param  tableId     the Sleeper table ID
     * @param  bodyKey     the object key in the data bucket where the transaction body is stored
     * @param  transaction the transaction
     * @return             the commit request
     */
    public static StateStoreCommitRequestByTransaction create(String tableId, String bodyKey, StateStoreTransaction<?> transaction) {
        return create(tableId, bodyKey, TransactionType.getType(transaction));
    }

    /**
     * Creates a commit request for a transaction in S3.
     *
     * @param  tableId         the Sleeper table ID
     * @param  bodyKey         the object key in the data bucket where the transaction body is stored
     * @param  transactionType the transaction type
     * @return                 the commit request
     */
    public static StateStoreCommitRequestByTransaction create(String tableId, String bodyKey, TransactionType transactionType) {
        return new StateStoreCommitRequestByTransaction(tableId, transactionType, bodyKey, null);
    }

    /**
     * Creates a commit request for a transaction held directly.
     *
     * @param  tableId     the Sleeper table ID
     * @param  transaction the transaction
     * @return             the commit request
     */
    public static StateStoreCommitRequestByTransaction create(String tableId, StateStoreTransaction<?> transaction) {
        return new StateStoreCommitRequestByTransaction(tableId, TransactionType.getType(transaction), null, transaction);
    }

    public String getTableId() {
        return tableId;
    }

    public TransactionType getTransactionType() {
        return transactionType;
    }

    public String getBodyKey() {
        return bodyKey;
    }

    /**
     * Reads the transaction from S3.
     *
     * @param  transactionBodyStore the transaction body store
     * @return                      the transaction
     */
    public <T extends StateStoreTransaction<?>> T getTransaction(TransactionBodyStore transactionBodyStore) {
        return transactionBodyStore.getBody(bodyKey, transactionType);
    }

    /**
     * Retrieves the transaction if it is held directly. If it is held in S3, this returns an empty optional.
     *
     * @return the transaction
     */
    public <T extends StateStoreTransaction<?>> Optional<T> getTransactionIfHeld() {
        return (Optional<T>) Optional.ofNullable(transaction);
    }

    /**
     * Reads the transaction from S3 and converts to a request for the state store.
     *
     * @param  transactionBodyStore the transaction body store
     * @return                      the request
     */
    public AddTransactionRequest toAddTransactionRequest(TransactionBodyStore transactionBodyStore) {
        StateStoreTransaction<?> transaction = transactionBodyStore.getBody(bodyKey, transactionType);
        return AddTransactionRequest.transactionInBucket(bodyKey, transaction);
    }

    @Override
    public int hashCode() {
        return Objects.hash(tableId, transactionType, bodyKey, transaction);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (!(obj instanceof StateStoreCommitRequestByTransaction)) {
            return false;
        }
        StateStoreCommitRequestByTransaction other = (StateStoreCommitRequestByTransaction) obj;
        return Objects.equals(tableId, other.tableId) && transactionType == other.transactionType && Objects.equals(bodyKey, other.bodyKey) && Objects.equals(transaction, other.transaction);
    }

    @Override
    public String toString() {
        return "StateStoreCommitRequestByTransaction{tableId=" + tableId + ", transactionType=" + transactionType + ", bodyKey=" + bodyKey + ", transaction=" + transaction + "}";
    }

}
