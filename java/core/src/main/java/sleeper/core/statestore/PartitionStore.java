/*
 * Copyright 2022-2023 Crown Copyright
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

import java.util.List;

/**
 * Stores information about the partitions where data files are held (ie. {@link Partition}s).
 */
public interface PartitionStore {

    /**
     * Atomically updates a {@link Partition} and adds two new ones, conditional
     * on the splitPartition being marked as a leaf partition.
     *
     * @param splitPartition The {@link Partition} to be updated
     * @param newPartition1  The first new {@link Partition}
     * @param newPartition2  The second new {@link Partition}
     * @throws StateStoreException if split is not valid or update fails
     */
    void atomicallyUpdatePartitionAndCreateNewOnes(Partition splitPartition,
                                                   Partition newPartition1,
                                                   Partition newPartition2) throws StateStoreException;

    /**
     * Returns all the {@link Partition}s.
     *
     * @return All the {@link Partition}s
     * @throws StateStoreException if query fails
     */
    List<Partition> getAllPartitions() throws StateStoreException;

    /**
     * Returns all the {@link Partition}s which are leaf partitions.
     *
     * @return All the {@link Partition}s which are leaf partitions.
     * @throws StateStoreException if query fails
     */
    List<Partition> getLeafPartitions() throws StateStoreException;

    /**
     * Initialises the store with a single root {@link Partition} covering all
     * keys.
     *
     * @throws StateStoreException if update fails
     */
    void initialise() throws StateStoreException;

    /**
     * Initialises store with the list of {@link Partition}s.
     *
     * @param partitions The initial list of {@link Partition}s
     * @throws StateStoreException if partitions not provided or update fails
     */
    void initialise(List<Partition> partitions) throws StateStoreException;

    void clearTable();
}
