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

import java.time.Duration;
import java.time.Instant;
import java.util.function.Supplier;

public class TransactionLogStateStore extends DelegatingStateStore {

    public static final int DEFAULT_MAX_ADD_TRANSACTION_ATTEMPTS = 10;
    public static final WaitRange DEFAULT_RETRY_WAIT_RANGE = WaitRange.firstAndMaxWaitCeilingSecs(0.2, 30);
    public static final Duration DEFAULT_TIME_BETWEEN_SNAPSHOT_CHECKS = Duration.ofMinutes(1);
    public static final Duration DEFAULT_TIME_BETWEEN_TRANSACTION_CHECKS = Duration.ofSeconds(1);

    public TransactionLogStateStore(Builder builder) {
        this(builder, TransactionLogHead.builder()
                .sleeperTable(builder.sleeperTable)
                .maxAddTransactionAttempts(builder.maxAddTransactionAttempts)
                .timeBetweenSnapshotChecks(builder.timeBetweenSnapshotChecks)
                .timeBetweenTransactionChecks(builder.timeBetweenTransactionChecks)
                .minTransactionsAheadToLoadSnapshot(builder.minTransactionsAheadToLoadSnapshot)
                .retryBackoff(builder.retryBackoff));
    }

    private TransactionLogStateStore(Builder builder, TransactionLogHead.Builder<?> headBuilder) {
        this(builder.schema,
                headBuilder.forFiles()
                        .logStore(builder.filesLogStore)
                        .snapshotLoader(builder.filesSnapshotLoader)
                        .stateUpdateClock(builder.filesStateUpdateClock)
                        .build(),
                headBuilder.forPartitions()
                        .logStore(builder.partitionsLogStore)
                        .snapshotLoader(builder.partitionsSnapshotLoader)
                        .stateUpdateClock(builder.partitionsStateUpdateClock)
                        .build());
    }

    private TransactionLogStateStore(Schema schema, TransactionLogHead<StateStoreFiles> filesHead, TransactionLogHead<StateStorePartitions> partitionsHead) {
        super(new TransactionLogFileReferenceStore(filesHead),
                new TransactionLogPartitionStore(schema, partitionsHead));
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {
        private TableStatus sleeperTable;
        private Schema schema;
        private TransactionLogStore filesLogStore;
        private TransactionLogStore partitionsLogStore;
        private long minTransactionsAheadToLoadSnapshot = 1;
        private int maxAddTransactionAttempts = DEFAULT_MAX_ADD_TRANSACTION_ATTEMPTS;
        private ExponentialBackoffWithJitter retryBackoff = new ExponentialBackoffWithJitter(DEFAULT_RETRY_WAIT_RANGE);
        private TransactionLogSnapshotLoader filesSnapshotLoader = TransactionLogSnapshotLoader.neverLoad();
        private TransactionLogSnapshotLoader partitionsSnapshotLoader = TransactionLogSnapshotLoader.neverLoad();
        private Duration timeBetweenSnapshotChecks = DEFAULT_TIME_BETWEEN_SNAPSHOT_CHECKS;
        private Duration timeBetweenTransactionChecks = DEFAULT_TIME_BETWEEN_TRANSACTION_CHECKS;
        private Supplier<Instant> filesStateUpdateClock = Instant::now;
        private Supplier<Instant> partitionsStateUpdateClock = Instant::now;

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

        public Builder maxAddTransactionAttempts(int maxAddTransactionAttempts) {
            this.maxAddTransactionAttempts = maxAddTransactionAttempts;
            return this;
        }

        public Builder retryBackoff(ExponentialBackoffWithJitter retryBackoff) {
            this.retryBackoff = retryBackoff;
            return this;
        }

        public Builder filesSnapshotLoader(TransactionLogSnapshotLoader filesSnapshotLoader) {
            this.filesSnapshotLoader = filesSnapshotLoader;
            return this;
        }

        public Builder partitionsSnapshotLoader(TransactionLogSnapshotLoader partitionsSnapshotLoader) {
            this.partitionsSnapshotLoader = partitionsSnapshotLoader;
            return this;
        }

        public Builder timeBetweenSnapshotChecks(Duration timeBetweenSnapshotChecks) {
            this.timeBetweenSnapshotChecks = timeBetweenSnapshotChecks;
            return this;
        }

        public Builder timeBetweenTransactionChecks(Duration timeBetweenTransactionChecks) {
            this.timeBetweenTransactionChecks = timeBetweenTransactionChecks;
            return this;
        }

        public Builder filesStateUpdateClock(Supplier<Instant> filesStateUpdateClock) {
            this.filesStateUpdateClock = filesStateUpdateClock;
            return this;
        }

        public Builder partitionsStateUpdateClock(Supplier<Instant> partitionsStateUpdateClock) {
            this.partitionsStateUpdateClock = partitionsStateUpdateClock;
            return this;
        }

        public Builder minTransactionsAheadToLoadSnapshot(long minTransactionsAheadToLoadSnapshot) {
            this.minTransactionsAheadToLoadSnapshot = minTransactionsAheadToLoadSnapshot;
            return this;
        }

        public TransactionLogStateStore build() {
            return new TransactionLogStateStore(this);
        }
    }

}
