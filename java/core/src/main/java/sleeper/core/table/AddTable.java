/*
 * Copyright 2022-2026 Crown Copyright
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
package sleeper.core.table;

import sleeper.core.properties.SleeperPropertiesInvalidException;
import sleeper.core.properties.table.TableProperties;
import sleeper.core.properties.table.TablePropertiesStore;
import sleeper.core.statestore.StateStore;
import sleeper.core.statestore.StateStoreProvider;
import sleeper.core.statestore.transactionlog.transaction.impl.InitialisePartitionsTransaction;

import java.util.List;

/**
 * Adds a table to a Sleeper instance by storing table properties and initialising the state store.
 */
public class AddTable {

    private final TablePropertiesStore tablePropertiesStore;
    private final StateStoreProvider stateStoreProvider;

    public AddTable(TablePropertiesStore tablePropertiesStore, StateStoreProvider stateStoreProvider) {
        this.tablePropertiesStore = tablePropertiesStore;
        this.stateStoreProvider = stateStoreProvider;
    }

    /**
     * Adds a table to the Sleeper instance.
     *
     * @param  tableProperties                   the table properties
     * @param  splitPoints                       the split points to initialise the partition tree
     * @throws SleeperPropertiesInvalidException if table properties provided don't validate
     * @throws TableAlreadyExistsException       if the table already exists
     * @throws IllegalArgumentException          if table properties failed validation
     */
    public void addTable(TableProperties tableProperties, List<Object> splitPoints) {
        tableProperties.validate();
        tablePropertiesStore.createTable(tableProperties);
        StateStore stateStore = stateStoreProvider.getStateStore(tableProperties);
        InitialisePartitionsTransaction.fromSplitPoints(tableProperties, splitPoints).synchronousCommit(stateStore);
    }
}
