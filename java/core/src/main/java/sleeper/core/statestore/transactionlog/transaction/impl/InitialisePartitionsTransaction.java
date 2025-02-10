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
import sleeper.core.statestore.transactionlog.state.StateStorePartitions;
import sleeper.core.statestore.transactionlog.transaction.PartitionTransaction;

import java.time.Instant;
import java.util.List;
import java.util.Objects;

/**
 * A transaction to set all partitions in a Sleeper table. This should specify the whole partition tree. Any partitions
 * that were present before will be deleted.
 */
public class InitialisePartitionsTransaction implements PartitionTransaction {

    private final List<Partition> partitions;

    public InitialisePartitionsTransaction(List<Partition> partitions) {
        this.partitions = partitions;
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
