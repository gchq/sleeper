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

package sleeper.configuration.properties.table;

import sleeper.core.table.InMemoryTableIndex;
import sleeper.core.table.TableId;
import sleeper.core.table.TableIndex;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;

import static sleeper.configuration.properties.table.TableProperty.TABLE_ID;
import static sleeper.configuration.properties.table.TableProperty.TABLE_NAME;

public class InMemoryTablePropertiesStore implements TablePropertiesStore {

    private final TableIndex tableIndex = new InMemoryTableIndex();
    private final Map<String, TableProperties> propertiesByTableId = new HashMap<>();

    @Override
    public TableProperties loadProperties(TableId tableId) {
        return Optional.ofNullable(propertiesByTableId.get(tableId.getTableUniqueId()))
                .orElseThrow();
    }

    @Override
    public Optional<TableProperties> loadByName(String tableName) {
        return loadByNameNoValidation(tableName);
    }

    @Override
    public Optional<TableProperties> loadByNameNoValidation(String tableName) {
        return tableIndex.getTableByName(tableName)
                .map(this::loadProperties);
    }

    @Override
    public Stream<TableProperties> streamAllTables() {
        return tableIndex.streamAllTables().map(this::loadProperties);
    }

    @Override
    public Stream<TableId> streamAllTableIds() {
        return tableIndex.streamAllTables();
    }

    @Override
    public void save(TableProperties tableProperties) {
        String tableId = tableIndex.getOrCreateTableByName(tableProperties.get(TABLE_NAME))
                .getTableUniqueId();
        tableProperties.set(TABLE_ID, tableId);
        propertiesByTableId.put(tableId, tableProperties);
    }

    @Override
    public void deleteByName(String tableName) {
        tableIndex.getTableByName(tableName)
                .ifPresent(tableId -> {
                    tableIndex.delete(tableId);
                    propertiesByTableId.remove(tableId.getTableUniqueId());
                });
    }
}
