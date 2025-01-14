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

import java.util.Optional;

/**
 * Holds a transaction that should be added to the log. The transaction may or may not already be held in S3.
 */
public class AddTransactionRequest {

    private final String bodyKey;
    private final StateStoreTransaction<?> transaction;
    private final StateListenerBeforeApply<?> beforeApplyListener;

    private AddTransactionRequest(Builder<?> builder) {
        bodyKey = builder.bodyKey;
        transaction = builder.transaction;
        beforeApplyListener = builder.beforeApplyListener;
    }

    /**
     * Creates a request to add a transaction that's held in S3.
     *
     * @param  bodyKey     the object key in the data bucket
     * @param  transaction the transaction object
     * @return             the request
     */
    public static AddTransactionRequest transactionInBucket(String bodyKey, StateStoreTransaction<?> transaction) {
        return withTransaction(transaction).bodyKey(bodyKey).build();
    }

    /**
     * Creates a request to add a transaction that's not held in S3.
     *
     * @param  transaction the transaction object
     * @return             the request
     */
    public static AddTransactionRequest transaction(StateStoreTransaction<?> transaction) {
        return withTransaction(transaction).build();
    }

    /**
     * Creates a builder to add a given transaction.
     *
     * @param  transaction the transaction object
     * @return             the request
     */
    public static <S> Builder<S> withTransaction(StateStoreTransaction<S> transaction) {
        return new Builder<>(transaction);
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

    /**
     * Retrieves the listener for after the transaction is added.
     *
     * @param  <S> the type of the local state
     * @param  <T> the type of the transaction being added
     * @return     the listener
     */
    public <S, T extends StateStoreTransaction<S>> StateListenerBeforeApply<S> getBeforeApplyListener() {
        return (StateListenerBeforeApply<S>) beforeApplyListener;
    }

    /**
     * A builder for this class.
     *
     * @param <S> the type of state the transaction operates on
     */
    public static class Builder<S> {

        private final StateStoreTransaction<S> transaction;
        private String bodyKey;
        private StateListenerBeforeApply<S> beforeApplyListener = StateListenerBeforeApply.none();

        private Builder(StateStoreTransaction<S> transaction) {
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
        public Builder<S> bodyKey(String bodyKey) {
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
        public Builder<S> beforeApplyListener(StateListenerBeforeApply<S> beforeApplyListener) {
            this.beforeApplyListener = beforeApplyListener;
            return this;
        }
    }
}
