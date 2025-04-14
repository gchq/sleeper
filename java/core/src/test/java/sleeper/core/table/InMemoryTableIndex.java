/*
 * Copyright 2022-2025 Crown Copyright
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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.TreeMap;
import java.util.stream.Stream;

/**
 * An in-memory implementation of the table index interface.
 */
public class InMemoryTableIndex implements TableIndex {

    private final Map<String, TableStatus> indexByName = new TreeMap<>();
    private final Map<String, TableStatus> indexById = new HashMap<>();

    @Override
    public void create(TableStatus table) throws TableAlreadyExistsException {
        if (indexByName.containsKey(table.getTableName())) {
            throw new TableAlreadyExistsException(table);
        }
        save(table);
    }

    /**
     * Saves a table to the table index.
     *
     * @param table the table status
     */
    public void save(TableStatus table) {
        indexByName.put(table.getTableName(), table);
        indexById.put(table.getTableUniqueId(), table);
    }

    @Override
    public Stream<TableStatus> streamAllTables() {
        return new ArrayList<>(indexByName.values()).stream();
    }

    @Override
    public Stream<TableStatus> streamOnlineTables() {
        return streamAllTables().filter(TableStatus::isOnline);
    }

    @Override
    public Optional<TableStatus> getTableByName(String tableName) {
        return Optional.ofNullable(indexByName.get(tableName));
    }

    @Override
    public Optional<TableStatus> getTableByUniqueId(String tableUniqueId) {
        return Optional.ofNullable(indexById.get(tableUniqueId));
    }

    @Override
    public void delete(TableStatus table) {
        if (!indexById.containsKey(table.getTableUniqueId())) {
            throw TableNotFoundException.withTableId(table.getTableUniqueId());
        }
        TableStatus latest = indexById.get(table.getTableUniqueId());
        if (!Objects.equals(latest.getTableName(), table.getTableName())) {
            throw TableNotFoundException.withTableName(table.getTableName());
        }
        indexByName.remove(latest.getTableName());
        indexById.remove(latest.getTableUniqueId());
    }

    @Override
    public void update(TableStatus table) {
        TableStatus existingTableWithNewName = indexByName.get(table.getTableName());
        if (existingTableWithNewName != null && !existingTableWithNewName.getTableUniqueId().equals(table.getTableUniqueId())) {
            throw new TableAlreadyExistsException(existingTableWithNewName);
        }
        if (!indexById.containsKey(table.getTableUniqueId())) {
            throw TableNotFoundException.withTableId(table.getTableUniqueId());
        }
        TableStatus old = indexById.get(table.getTableUniqueId());
        indexByName.remove(old.getTableName());
        indexByName.put(table.getTableName(), table);
        indexById.put(table.getTableUniqueId(), table);
    }
}
