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
package sleeper.core.statestore;

import sleeper.core.partition.Partition;
import sleeper.core.partition.PartitionTree;

import java.time.Instant;
import java.util.List;
import java.util.Objects;

/**
 * Stores information about the partitions holding records in a Sleeper table.
 */
public interface PartitionStore {

    /**
     * Atomically splits a partition to create child partitions. Updates the existing partition to record it as split,
     * and creates new leaf partitions.
     *
     * @param  splitPartition      The {@link Partition} to be updated (must refer to the new leaves as children).
     * @param  newPartition1       The first new {@link Partition} (must be a leaf partition).
     * @param  newPartition2       The second new {@link Partition} (must be a leaf partition).
     * @throws StateStoreException if split is not valid or update fails
     */
    void atomicallyUpdatePartitionAndCreateNewOnes(Partition splitPartition,
            Partition newPartition1,
            Partition newPartition2) throws StateStoreException;

    /**
     * Returns all partitions.
     *
     * @return                     all the {@link Partition}s
     * @throws StateStoreException if query fails
     */
    List<Partition> getAllPartitions() throws StateStoreException;

    /**
     * Returns all partitions which are leaf partitions.
     *
     * @return                     all the {@link Partition}s which are leaf partitions
     * @throws StateStoreException if query fails
     */
    List<Partition> getLeafPartitions() throws StateStoreException;

    /**
     * Returns a partition by its ID.
     *
     * @param  partitionId         the ID of the partition to be retrieved
     * @return                     the {@link Partition}
     * @throws StateStoreException if query fails
     */
    default Partition getPartition(String partitionId) throws StateStoreException {
        return getAllPartitions().stream()
                .filter(p -> Objects.equals(partitionId, p.getId()))
                .findFirst().orElseThrow(() -> new StateStoreException("Partition not found: " + partitionId));
    }

    /**
     * Initialises the store with a single partition covering all keys. This is the root partition which may be split
     * in the future.
     *
     * @throws StateStoreException if update fails
     */
    void initialise() throws StateStoreException;

    /**
     * Initialises the store with a list of all partitions. This must be a complete {@link PartitionTree}.
     *
     * @param  partitions          The initial list of {@link Partition}s
     * @throws StateStoreException if partitions not provided or update fails
     */
    void initialise(List<Partition> partitions) throws StateStoreException;

    /**
     * Clears all partition data from the store. Note that this will invalidate any file references held in the store,
     * so this should only be used when no files are present. The store must be initialised before the Sleeper table can
     * be used again. Any file references will need to be added again.
     */
    void clearPartitionData() throws StateStoreException;

    /**
     * Used to fix the time of partition updates. Should only be called during tests.
     *
     * @param time the time that any future partition updates will be considered to occur
     */
    default void fixPartitionUpdateTime(Instant time) {
    }
}
