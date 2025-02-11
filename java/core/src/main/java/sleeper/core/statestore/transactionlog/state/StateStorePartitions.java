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
package sleeper.core.statestore.transactionlog.state;

import sleeper.core.partition.Partition;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/**
 * Holds the state of partitions, for a state store backed by a transaction log. This object is mutable, is cached
 * in memory in the state store, and is updated by applying each transaction in the log in sequence. This is not thread
 * safe.
 * <p>
 * The methods to update this object should only ever be called by the transactions.
 */
public class StateStorePartitions {

    private final Map<String, Partition> partitionById = new HashMap<>();

    /**
     * Retrieves all partitions in the state store.
     *
     * @return the partitions
     */
    public Collection<Partition> all() {
        return partitionById.values();
    }

    /**
     * Retreives all information held about a specific partition.
     *
     * @param  id the partition ID
     * @return    the partition if it exists in the state store
     */
    public Optional<Partition> byId(String id) {
        return Optional.ofNullable(partitionById.get(id));
    }

    /**
     * Deletes all partitions and empties the state. Should only be called by a transaction.
     */
    public void clear() {
        partitionById.clear();
    }

    /**
     * Adds or updates a partition in the state, by its ID. Should only be called by a transaction.
     *
     * @param partition the partition
     */
    public void put(Partition partition) {
        partitionById.put(partition.getId(), partition);
    }

    @Override
    public int hashCode() {
        return Objects.hash(partitionById);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (!(obj instanceof StateStorePartitions)) {
            return false;
        }
        StateStorePartitions other = (StateStorePartitions) obj;
        return Objects.equals(partitionById, other.partitionById);
    }

    @Override
    public String toString() {
        return "StateStorePartitions{partitionById=" + partitionById + "}";
    }

}
