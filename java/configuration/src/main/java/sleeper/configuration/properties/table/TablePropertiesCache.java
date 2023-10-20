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

import static sleeper.configuration.properties.table.TableProperty.TABLE_ID;
import static sleeper.configuration.properties.table.TableProperty.TABLE_NAME;

public class TablePropertiesCache {
    private final Map<String, TableProperties> propertiesCacheByTableName = new HashMap<>();
    private final Map<String, TableProperties> propertiesCacheByTableId = new HashMap<>();

    public Optional<TableProperties> getByName(String tableName) {
        return Optional.ofNullable(propertiesCacheByTableName.get(tableName));
    }

    public Optional<TableProperties> getById(String tableId) {
        return Optional.ofNullable(propertiesCacheByTableId.get(tableId));
    }

    public Optional<TableProperties> get(TableId tableId) {
        return getByName(tableId.getTableUniqueId());
    }

    public void add(TableProperties properties) {
        propertiesCacheByTableName.put(properties.get(TABLE_NAME), properties);
        propertiesCacheByTableId.put(properties.get(TABLE_ID), properties);
    }

    public void removeByName(String tableName) {
        getByName(tableName).ifPresent(this::remove);
    }

    public void removeById(String tableId) {
        getById(tableId).ifPresent(this::remove);
    }

    private void remove(TableProperties properties) {
        propertiesCacheByTableName.remove(properties.get(TABLE_NAME));
        propertiesCacheByTableId.remove(properties.get(TABLE_ID));
    }

    public void clear() {
        propertiesCacheByTableName.clear();
    }
}
