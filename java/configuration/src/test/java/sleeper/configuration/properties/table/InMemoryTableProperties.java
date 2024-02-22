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

package sleeper.configuration.properties.table;

import sleeper.core.table.InMemoryTableIndex;
import sleeper.core.table.TableIndex;
import sleeper.core.table.TableNotFoundException;
import sleeper.core.table.TableStatus;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import static sleeper.configuration.properties.table.TableProperty.TABLE_ID;

public class InMemoryTableProperties implements TablePropertiesStore.Client {

    private final Map<String, TableProperties> propertiesByTableId = new HashMap<>();
    private final boolean defensiveCopy;

    private InMemoryTableProperties(boolean defensiveCopy) {
        this.defensiveCopy = defensiveCopy;
    }

    public static TablePropertiesStore getStore() {
        return getStore(new InMemoryTableIndex());
    }

    public static TablePropertiesStore getStore(TableIndex tableIndex) {
        return new TablePropertiesStore(tableIndex, new InMemoryTableProperties(true));
    }

    public static TablePropertiesStore getStoreReturningExactInstance() {
        return getStoreReturningExactInstance(new InMemoryTableIndex());
    }

    public static TablePropertiesStore getStoreReturningExactInstance(TableIndex tableIndex) {
        return new TablePropertiesStore(tableIndex, new InMemoryTableProperties(false));
    }

    public static TablePropertiesStore getStoreReturningExactInstances(Collection<TableProperties> properties) {
        TablePropertiesStore store = getStoreReturningExactInstance();
        properties.forEach(store::save);
        return store;
    }

    @Override
    public TableProperties loadProperties(TableStatus table) {
        return Optional.ofNullable(propertiesByTableId.get(table.getTableUniqueId()))
                .map(this::copyIfSet)
                .orElseThrow(() -> TableNotFoundException.withTable(table));
    }

    @Override
    public void saveProperties(TableProperties tableProperties) {
        propertiesByTableId.put(tableProperties.get(TABLE_ID), copyIfSet(tableProperties));
    }

    @Override
    public void deleteProperties(TableStatus table) {
        propertiesByTableId.remove(table.getTableUniqueId());
    }

    private TableProperties copyIfSet(TableProperties properties) {
        if (defensiveCopy) {
            return TableProperties.copyOf(properties);
        } else {
            return properties;
        }
    }
}
