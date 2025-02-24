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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sleeper.core.statestore.StateStore;
import sleeper.core.statestore.StateStoreException;
import sleeper.core.statestore.transactionlog.state.StateListenerBeforeApply;
import sleeper.core.statestore.transactionlog.transaction.FileReferenceTransaction;
import sleeper.core.statestore.transactionlog.transaction.PartitionTransaction;
import sleeper.core.statestore.transactionlog.transaction.StateStoreTransaction;
import sleeper.core.statestore.transactionlog.transaction.TransactionType;

import java.util.Optional;

/**
 * Holds a transaction that should be added to the log. The transaction may or may not be held in S3. When writing to a
 * log we could point to the existing file in S3 rather than writing the transaction directly, particularly if it's too
 * big.
 */
public class AddTransactionRequest {
    public static final Logger LOGGER = LoggerFactory.getLogger(AddTransactionRequest.class);

    private final StateStoreTransaction<?> transaction;
    private final String bodyKey;
    private final String serialisedTransaction;
    private final StateListenerBeforeApply beforeApplyListener;

    private AddTransactionRequest(Builder builder) {
        transaction = builder.transaction;
        bodyKey = builder.bodyKey;
        serialisedTransaction = builder.serialisedTransaction;
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

    /**
     * Retrieves the serialised string representation of the transaction, if it has been serialised and not uploaded to
     * S3.
     *
     * @return a string representation of the transaction, unless it has not yet been serialised or it has been uploaded
     *         to S3
     */
    public Optional<String> getSerialisedTransaction() {
        return Optional.ofNullable(serialisedTransaction);
    }

    /**
     * Checks whether the transaction should be added. This is not the main validation check, as that waits until we
     * have an up to date view of the state that will be updated.
     *
     * @param  stateStore          the state store
     * @return                     true if it should be added
     * @throws StateStoreException if the transaction is invalid without comparing against the state
     */
    public boolean checkBeforeAdd(StateStore stateStore) throws StateStoreException {
        if (transaction.isEmpty()) {
            LOGGER.warn("Ignoring empty transaction of type {}", transaction.getClass());
            return false;
        }
        transaction.checkBefore(stateStore);
        return true;
    }

    public TransactionType getTransactionType() {
        return TransactionType.getType(transaction);
    }

    public Optional<String> getBodyKey() {
        return Optional.ofNullable(bodyKey);
    }

    /**
     * Retrieves the listener for after the transaction is added.
     *
     * @return the listener
     */
    public StateListenerBeforeApply getBeforeApplyListener() {
        return beforeApplyListener;
    }

    public Builder toBuilder() {
        return withTransaction(transaction).bodyKey(bodyKey).beforeApplyListener(beforeApplyListener);
    }

    /**
     * A builder for this class.
     */
    public static class Builder {

        private final StateStoreTransaction<?> transaction;
        private String bodyKey;
        private String serialisedTransaction;
        private StateListenerBeforeApply beforeApplyListener = StateListenerBeforeApply.none();

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
         * Sets the string representation of the transaction. To decide whether to upload the transaction to S3, we have
         * to serialise the transaction and check if it's too big. If it's not too big, we want to hold the transaction
         * directly in the log, but we want to avoid reserialising it again, so we can hold it here. This should only
         * be set at that point, and will not be set at all if the transaction is uploaded to S3.
         *
         * @param  serialisedTransaction the object key in the data bucket
         * @return                       this builder
         */
        public Builder serialisedTransaction(String serialisedTransaction) {
            this.serialisedTransaction = serialisedTransaction;
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
    }
}
