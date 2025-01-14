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

import sleeper.core.statestore.transactionlog.StateStoreTransaction;
import sleeper.core.statestore.transactionlog.transactions.TransactionType;

import java.util.Objects;

/**
 * A commit request for a transaction that was uploaded to S3 before being submitted to the committer.
 */
public class StateStoreCommitRequestByTransactionInS3 {

    private final String tableId;
    private final TransactionType type;
    private final String bodyKey;

    private StateStoreCommitRequestByTransactionInS3(String tableId, TransactionType type, String bodyKey) {
        this.tableId = tableId;
        this.type = type;
        this.bodyKey = bodyKey;
    }

    /**
     * Creates a commit request for a transaction in S3.
     *
     * @param  tableId     the Sleeper table ID
     * @param  bodyKey     the object key in the data bucket where the transaction body is stored
     * @param  transaction the transaction
     * @return             the commit request
     */
    public static StateStoreCommitRequestByTransactionInS3 create(String tableId, String bodyKey, StateStoreTransaction<?> transaction) {
        return new StateStoreCommitRequestByTransactionInS3(tableId, TransactionType.getType(transaction), bodyKey);
    }

    @Override
    public int hashCode() {
        return Objects.hash(tableId, type, bodyKey);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (!(obj instanceof StateStoreCommitRequestByTransactionInS3)) {
            return false;
        }
        StateStoreCommitRequestByTransactionInS3 other = (StateStoreCommitRequestByTransactionInS3) obj;
        return Objects.equals(tableId, other.tableId) && type == other.type && Objects.equals(bodyKey, other.bodyKey);
    }

    @Override
    public String toString() {
        return "StateStoreCommitRequestByTransactionInS3{tableId=" + tableId + ", type=" + type + ", bodyKey=" + bodyKey + "}";
    }

}
