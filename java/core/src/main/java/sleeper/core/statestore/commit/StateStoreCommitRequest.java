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

import sleeper.core.statestore.transactionlog.transactions.StateStoreTransaction;
import sleeper.core.statestore.transactionlog.transactions.TransactionType;

import java.util.Objects;
import java.util.Optional;

/**
 * A commit request for a transaction to be added to the state store.
 */
public class StateStoreCommitRequest {

    private final String tableId;
    private final TransactionType transactionType;
    private final String bodyKey;
    private final StateStoreTransaction<?> transaction;

    private StateStoreCommitRequest(String tableId, TransactionType transactionType, String bodyKey, StateStoreTransaction<?> transaction) {
        this.tableId = tableId;
        this.transactionType = transactionType;
        this.bodyKey = bodyKey;
        this.transaction = transaction;
    }

    /**
     * Creates a commit request for a transaction in S3.
     *
     * @param  tableId         the Sleeper table ID
     * @param  bodyKey         the object key in the data bucket where the transaction body is stored
     * @param  transactionType the transaction type
     * @return                 the commit request
     */
    public static StateStoreCommitRequest create(String tableId, String bodyKey, TransactionType transactionType) {
        return new StateStoreCommitRequest(tableId, transactionType, bodyKey, null);
    }

    /**
     * Creates a commit request for a transaction held directly.
     *
     * @param  tableId     the Sleeper table ID
     * @param  transaction the transaction
     * @return             the commit request
     */
    public static StateStoreCommitRequest create(String tableId, StateStoreTransaction<?> transaction) {
        return new StateStoreCommitRequest(tableId, TransactionType.getType(transaction), null, transaction);
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
     * Retrieves the transaction if it is held directly. If it is held in S3, this returns an empty optional.
     *
     * @return the transaction
     */
    public <T extends StateStoreTransaction<?>> Optional<T> getTransactionIfHeld() {
        return (Optional<T>) Optional.ofNullable(transaction);
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
        if (!(obj instanceof StateStoreCommitRequest)) {
            return false;
        }
        StateStoreCommitRequest other = (StateStoreCommitRequest) obj;
        return Objects.equals(tableId, other.tableId) && transactionType == other.transactionType && Objects.equals(bodyKey, other.bodyKey) && Objects.equals(transaction, other.transaction);
    }

    @Override
    public String toString() {
        return "StateStoreCommitRequest{tableId=" + tableId + ", transactionType=" + transactionType + ", bodyKey=" + bodyKey + ", transaction=" + transaction + "}";
    }

}
