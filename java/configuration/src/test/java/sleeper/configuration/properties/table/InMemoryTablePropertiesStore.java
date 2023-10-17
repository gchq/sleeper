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

import sleeper.core.table.TableId;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;

import static sleeper.configuration.properties.table.TableProperty.TABLE_NAME;

public class InMemoryTablePropertiesStore implements TablePropertiesStore {

    private final Map<String, TableProperties> propertiesByTableName = new HashMap<>();

    @Override
    public TableProperties loadProperties(TableId tableId) {
        return loadByName(tableId.getTableName()).orElseThrow();
    }

    @Override
    public Optional<TableProperties> loadByName(String tableName) {
        return loadByNameNoValidation(tableName);
    }

    @Override
    public Optional<TableProperties> loadByNameNoValidation(String tableName) {
        return Optional.ofNullable(propertiesByTableName.get(tableName));
    }

    @Override
    public Stream<TableProperties> streamAllTables() {
        return propertiesByTableName.values().stream();
    }

    @Override
    public Stream<TableId> streamAllTableIds() {
        return streamAllTables().map(TableProperties::getId);
    }

    @Override
    public void save(TableProperties tableProperties) {
        propertiesByTableName.put(tableProperties.get(TABLE_NAME), tableProperties);
    }

    @Override
    public void deleteByName(String tableName) {
        propertiesByTableName.remove(tableName);
    }
}
