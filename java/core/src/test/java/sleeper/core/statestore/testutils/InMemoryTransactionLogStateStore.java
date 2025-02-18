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
import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.table.TableProperties;
import sleeper.core.statestore.StateStoreProvider;
import sleeper.core.statestore.transactionlog.TransactionLogStateStore;

import java.util.List;

/**
 * An implementation of the state store backed by a transaction log held in memory.
 */
public class InMemoryTransactionLogStateStore {

    private InMemoryTransactionLogStateStore() {
    }

    /**
     * Creates a state store for the given Sleeper table.
     *
     * @param  tableProperties the Sleeper table properties
     * @param  transactionLogs the transaction logs
     * @return                 the state store
     */
    public static TransactionLogStateStore create(TableProperties tableProperties, InMemoryTransactionLogs transactionLogs) {
        return transactionLogs.stateStoreBuilder(tableProperties.getStatus(), tableProperties.getSchema())
                .build();
    }

    /**
     * Creates and initialises a state store for the given Sleeper table. Sets a single root partition.
     *
     * @param  tableProperties the Sleeper table properties
     * @param  transactionLogs the transaction logs
     * @return                 the state store
     */
    public static TransactionLogStateStore createAndInitialise(TableProperties tableProperties, InMemoryTransactionLogs transactionLogs) {
        TransactionLogStateStore stateStore = create(tableProperties, transactionLogs);
        stateStore.initialise();
        return stateStore;
    }

    /**
     * Creates and initialises a state store for the given Sleeper table using the provided list of partitions.
     *
     * @param  partitions      the list of partitions
     * @param  tableProperties the Sleeper table properties
     * @param  transactionLogs the transaction logs
     * @return                 the state store
     */
    public static TransactionLogStateStore createAndInitialiseWithPartitions(List<Partition> partitions, TableProperties tableProperties, InMemoryTransactionLogs transactionLogs) {
        TransactionLogStateStore stateStore = create(tableProperties, transactionLogs);
        stateStore.initialise(partitions);
        return stateStore;
    }

    /**
     * Creates a state store provider.
     *
     * @param  instanceProperties the instance properties
     * @param  transactionLogs    the transaction logs
     * @return                    the provider
     */
    public static StateStoreProvider createProvider(InstanceProperties instanceProperties, InMemoryTransactionLogsPerTable transactionLogs) {
        return new StateStoreProvider(
                instanceProperties,
                tableProperties -> create(tableProperties, transactionLogs.forTable(tableProperties)));
    }
}
