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

import sleeper.core.schema.Schema;
import sleeper.core.statestore.DelegatingStateStore;
import sleeper.core.table.TableStatus;
import sleeper.core.util.ExponentialBackoffWithJitter;
import sleeper.core.util.ExponentialBackoffWithJitter.WaitRange;

public class TransactionLogStateStore extends DelegatingStateStore {

    public static final int MAX_ADD_TRANSACTION_ATTEMPTS = 10;
    public static final WaitRange RETRY_WAIT_RANGE = WaitRange.firstAndMaxWaitCeilingSecs(0.2, 30);

    public TransactionLogStateStore(Builder builder) {
        this(builder, TransactionLogHead.builder()
                .sleeperTable(builder.sleeperTable)
                .maxAddTransactionAttempts(builder.maxAddTransactionAttempts)
                .retryBackoff(builder.retryBackoff));
    }

    private TransactionLogStateStore(Builder builder, TransactionLogHead.Builder<?> headBuilder) {
        super(
                new TransactionLogFileReferenceStore(headBuilder.forFiles()
                        .state(builder.filesState).logStore(builder.filesLogStore).build()),
                new TransactionLogPartitionStore(builder.schema, headBuilder.forPartitions()
                        .state(builder.partitionsState).logStore(builder.partitionsLogStore).build()));
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {
        private TableStatus sleeperTable;
        private Schema schema;
        private TransactionLogStore filesLogStore;
        private TransactionLogStore partitionsLogStore;
        private StateStoreFiles filesState = new StateStoreFiles();
        private StateStorePartitions partitionsState = new StateStorePartitions();
        private int maxAddTransactionAttempts = MAX_ADD_TRANSACTION_ATTEMPTS;
        private ExponentialBackoffWithJitter retryBackoff = new ExponentialBackoffWithJitter(RETRY_WAIT_RANGE);

        private Builder() {
        }

        public Builder sleeperTable(TableStatus sleeperTable) {
            this.sleeperTable = sleeperTable;
            return this;
        }

        public Builder schema(Schema schema) {
            this.schema = schema;
            return this;
        }

        public Builder filesLogStore(TransactionLogStore filesLogStore) {
            this.filesLogStore = filesLogStore;
            return this;
        }

        public Builder partitionsLogStore(TransactionLogStore partitionsLogStore) {
            this.partitionsLogStore = partitionsLogStore;
            return this;
        }

        public Builder filesState(StateStoreFiles filesState) {
            this.filesState = filesState;
            return this;
        }

        public Builder partitionsState(StateStorePartitions partitionsState) {
            this.partitionsState = partitionsState;
            return this;
        }

        public Builder maxAddTransactionAttempts(int maxAddTransactionAttempts) {
            this.maxAddTransactionAttempts = maxAddTransactionAttempts;
            return this;
        }

        public Builder retryBackoff(ExponentialBackoffWithJitter retryBackoff) {
            this.retryBackoff = retryBackoff;
            return this;
        }

        public TransactionLogStateStore build() {
            return new TransactionLogStateStore(this);
        }
    }

}
