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
package sleeper.core.statestore.transactionlog.transaction.impl;

import sleeper.core.partition.Partition;
import sleeper.core.partition.PartitionsFromSplitPoints;
import sleeper.core.schema.Schema;
import sleeper.core.statestore.StateStore;
import sleeper.core.statestore.StateStoreException;
import sleeper.core.statestore.transactionlog.AddTransactionRequest;
import sleeper.core.statestore.transactionlog.state.StateStorePartitions;
import sleeper.core.statestore.transactionlog.transaction.PartitionTransaction;

import java.time.Instant;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * Sets all partitions in a Sleeper table. These should build into a complete partition tree, where there is a single
 * root and all partitions are connected to that either directly or indirectly via other partitions. Any partitions that
 * were present before will be deleted.
 */
public class InitialisePartitionsTransaction implements PartitionTransaction {

    private final List<Partition> partitions;

    public InitialisePartitionsTransaction(List<Partition> partitions) {
        this.partitions = partitions;
    }

    /**
     * Creates a transaction to initialise the table with a single partition covering all keys. This is the root
     * partition which may be split in the future.
     *
     * @param  schema the table schema
     * @return        the transaction
     */
    public static InitialisePartitionsTransaction singlePartition(Schema schema) {
        return new InitialisePartitionsTransaction(new PartitionsFromSplitPoints(schema, Collections.emptyList()).construct());
    }

    /**
     * Commits this transaction directly to the state store without going to the commit queue. This will throw any
     * validation exceptions immediately, even if they wouldn't be as part of an asynchronous commit.
     *
     * @param  stateStore          the state store
     * @throws StateStoreException if the update fails
     */
    public void synchronousCommit(StateStore stateStore) throws StateStoreException {
        stateStore.addPartitionsTransaction(AddTransactionRequest.withTransaction(this).build());
    }

    @Override
    public void validate(StateStorePartitions stateStorePartitions) {
    }

    @Override
    public void apply(StateStorePartitions stateStorePartitions, Instant updateTime) {
        stateStorePartitions.clear();
        partitions.forEach(stateStorePartitions::put);
    }

    @Override
    public void checkBefore(StateStore stateStore) throws StateStoreException {
        if (!stateStore.hasNoFiles()) {
            throw new StateStoreException("Cannot initialise state store when files are present");
        }
    }

    @Override
    public int hashCode() {
        return Objects.hash(partitions);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (!(obj instanceof InitialisePartitionsTransaction)) {
            return false;
        }
        InitialisePartitionsTransaction other = (InitialisePartitionsTransaction) obj;
        return Objects.equals(partitions, other.partitions);
    }

    @Override
    public String toString() {
        return "InitialisePartitionsTransaction{partitions=" + partitions + "}";
    }

}
