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
package sleeper.core.statestore.testutils;

import sleeper.core.partition.Partition;
import sleeper.core.schema.Schema;
import sleeper.core.statestore.DelegatingStateStore;
import sleeper.core.statestore.StateStore;

import java.util.Arrays;
import java.util.List;

/**
 * A test helper for creating in-memory implementations of the state store.
 */
public class StateStoreTestHelper {

    private StateStoreTestHelper() {
    }

    /**
     * Creates an in-memory state store with the given partitions, where the partitions cannot be changed.
     *
     * @param  partitions the partitions
     * @return            the state store
     */
    public static StateStore inMemoryStateStoreWithFixedPartitions(Partition... partitions) {
        return inMemoryStateStoreWithFixedPartitions(Arrays.asList(partitions));
    }

    /**
     * Creates an in-memory state store with the given partitions, where the partitions cannot be changed.
     *
     * @param  partitions the partitions
     * @return            the state store
     */
    public static StateStore inMemoryStateStoreWithFixedPartitions(List<Partition> partitions) {
        return new DelegatingStateStore(new InMemoryFileReferenceStore(), new FixedPartitionStore(partitions));
    }

    /**
     * Creates an in-memory state store with a single root partition derived from a schema, where the partitions cannot
     * be changed.
     *
     * @param  schema the schema
     * @return        the state store
     */
    public static StateStore inMemoryStateStoreWithFixedSinglePartition(Schema schema) {
        return new DelegatingStateStore(new InMemoryFileReferenceStore(), new FixedPartitionStore(schema));
    }

    /**
     * Creates an in-memory state store initialised with a single root partition derived from a schema.
     *
     * @param  schema the schema
     * @return        the state store
     */
    public static StateStore inMemoryStateStoreWithSinglePartition(Schema schema) {
        return new DelegatingStateStore(new InMemoryFileReferenceStore(), InMemoryPartitionStore.withSinglePartition(schema));
    }

    /**
     * Creates an in-memory state store that has not been initialised. No partitions will be present.
     *
     * @param  schema the schema
     * @return        the state store
     */
    public static StateStore inMemoryStateStoreUninitialised(Schema schema) {
        return new DelegatingStateStore(new InMemoryFileReferenceStore(), new InMemoryPartitionStore(schema));
    }

    /**
     * Creates an in-memory state store initialised with the given partitions.
     *
     * @param  partitions the partitions
     * @return            the state store
     */
    public static StateStore inMemoryStateStoreWithPartitions(List<Partition> partitions) {
        return new DelegatingStateStore(new InMemoryFileReferenceStore(), new InMemoryPartitionStore(partitions));
    }

    /**
     * Creates an in-memory state store that has not been initialised. No partitions will be present. This must be
     * initialised with partitions explicitly, as the schema will not be available to derive a root partition.
     *
     * @return the state store
     */
    public static StateStore inMemoryStateStoreWithNoPartitions() {
        return new DelegatingStateStore(new InMemoryFileReferenceStore(), new InMemoryPartitionStore(List.of()));
    }
}
