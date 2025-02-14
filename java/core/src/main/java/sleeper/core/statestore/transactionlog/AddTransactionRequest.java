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

import sleeper.core.statestore.transactionlog.state.StateListenerBeforeApply;
import sleeper.core.statestore.transactionlog.transaction.FileReferenceTransaction;
import sleeper.core.statestore.transactionlog.transaction.PartitionTransaction;
import sleeper.core.statestore.transactionlog.transaction.StateStoreTransaction;
import sleeper.core.statestore.transactionlog.transaction.TransactionType;

import java.util.Optional;

/**
 * Holds a transaction that should be added to the log. The transaction may or may not already be held in S3.
 */
public class AddTransactionRequest {

    private final String bodyKey;
    private final StateStoreTransaction<?> transaction;
    private final boolean commitIfInvalid;
    private final StateListenerBeforeApply beforeApplyListener;

    private AddTransactionRequest(Builder builder) {
        bodyKey = builder.bodyKey;
        transaction = builder.transaction;
        commitIfInvalid = builder.commitIfInvalid;
        beforeApplyListener = builder.beforeApplyListener;
    }

    /**
     * Creates a builder to add a given transaction.
     *
     * @param  transaction the transaction object
     * @return             the request
     */
    public static Builder withTransaction(StateStoreTransaction<?> transaction) {
        return new Builder(transaction);
    }

    /**
     * Retrieves the transaction.
     *
     * @param  <T> the expected type of the transaction (e.g. {@link FileReferenceTransaction} or
     *             {@link PartitionTransaction})
     * @return     the transaction
     */
    public <T extends StateStoreTransaction<?>> T getTransaction() {
        return (T) transaction;
    }

    public TransactionType getTransactionType() {
        return TransactionType.getType(transaction);
    }

    public Optional<String> getBodyKey() {
        return Optional.ofNullable(bodyKey);
    }

    public boolean isCommitIfInvalid() {
        return commitIfInvalid;
    }

    /**
     * Retrieves the listener for after the transaction is added.
     *
     * @return the listener
     */
    public StateListenerBeforeApply getBeforeApplyListener() {
        return beforeApplyListener;
    }

    /**
     * A builder for this class.
     */
    public static class Builder {

        private final StateStoreTransaction<?> transaction;
        private String bodyKey;
        private StateListenerBeforeApply beforeApplyListener = StateListenerBeforeApply.none();
        private boolean commitIfInvalid = false;

        private Builder(StateStoreTransaction<?> transaction) {
            this.transaction = transaction;
        }

        public AddTransactionRequest build() {
            return new AddTransactionRequest(this);
        }

        /**
         * Sets the location the transaction is held in S3. The log will point to this file instead of holding the
         * transaction in the log.
         *
         * @param  bodyKey the object key in the data bucket
         * @return         this builder
         */
        public Builder bodyKey(String bodyKey) {
            this.bodyKey = bodyKey;
            return this;
        }

        /**
         * Sets a listener to be notified after the transaction has been added to the log but before it is applied to
         * the local state.
         *
         * @param  beforeApplyListener the listener
         * @return                     this builder
         */
        public Builder beforeApplyListener(StateListenerBeforeApply beforeApplyListener) {
            this.beforeApplyListener = beforeApplyListener;
            return this;
        }

        /**
         * Sets whether the transaction should be committed to the log even if it was invalid. If false, an attempt to
         * commit the transaction will throw an exception if it does not validate.
         *
         * @param  commitIfInvalid true if an invalid transaction should be committed, false otherwise
         * @return                 this builder
         */
        public Builder commitIfInvalid(boolean commitIfInvalid) {
            this.commitIfInvalid = commitIfInvalid;
            return this;
        }
    }
}
