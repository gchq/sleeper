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

package sleeper.systemtest.dsl.instance;

import sleeper.configuration.properties.table.TableProperties;
import sleeper.configuration.properties.table.TableProperty;
import sleeper.core.schema.Schema;
import sleeper.core.table.TableIdentity;
import sleeper.core.table.TableIndex;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Stream;

import static java.util.stream.Collectors.toUnmodifiableList;

public class SystemTestTablesContext {
    private final SleeperInstanceContext instance;
    private final Map<String, String> nameToRealName = new HashMap<>();

    public SystemTestTablesContext(SleeperInstanceContext instance) {
        this.instance = instance;
    }

    public void createTable(String name, Schema schema) {
        instance.createTablesGetNames(1, schema, Map.of());
        nameToRealName.put(name, instance.getTableName());
    }

    public void setCurrentTable(String name) {
        String realName = Optional.ofNullable(nameToRealName.get(name)).orElseThrow();
        instance.setCurrentTable(realName);
    }

    public void createTables(int numberOfTables, Schema schema, Map<TableProperty, String> setProperties) {
        instance.createTablesGetNames(numberOfTables, schema, setProperties)
                .forEach(name -> nameToRealName.put(UUID.randomUUID().toString(), name));
    }

    public TableProperties getTableProperties(String tableName) {
        return instance.getTablePropertiesByName(realName(tableName)).orElseThrow();
    }

    public Stream<String> streamTableNames() {
        return nameToRealName.keySet().stream();
    }

    public List<TableIdentity> loadTableIdentities() {
        TableIndex tableIndex = instance.getTableIndex();
        return nameToRealName.values().stream()
                .map(realName -> tableIndex.getTableByName(realName).orElseThrow())
                .collect(toUnmodifiableList());
    }

    public void reset() {
        nameToRealName.clear();
    }

    private String realName(String name) {
        return Optional.ofNullable(nameToRealName.get(name)).orElseThrow();
    }
}
