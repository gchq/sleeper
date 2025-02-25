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
import sleeper.core.statestore.transactionlog.AddTransactionRequest;

import java.time.Instant;
import java.util.List;

/**
 * Stores information about the partitions holding records in a Sleeper table.
 */
public interface PartitionStore {

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
    Partition getPartition(String partitionId) throws StateStoreException;

    /**
     * Initialises the store with a single partition covering all keys. This is the root partition which may be split
     * in the future.
     *
     * @throws StateStoreException if update fails
     */
    void initialise() throws StateStoreException;

    /**
     * Used to fix the time of partition updates. Should only be called during tests.
     *
     * @param time the time that any future partition updates will be considered to occur
     */
    void fixPartitionUpdateTime(Instant time);

    /**
     * Adds a partions transaction to the transaction log.
     *
     * @param request the request
     */
    void addPartitionsTransaction(AddTransactionRequest request);
}
