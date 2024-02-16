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

package sleeper.core.table;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.stream.Stream;

public class InMemoryTableIndex implements TableIndex {

    private final Map<String, TableStatus> indexByName = new TreeMap<>();
    private final Map<String, TableStatus> indexById = new HashMap<>();
    private final Set<String> onlineTableIds = new TreeSet<>();

    @Override
    public void create(TableStatus tableId) throws TableAlreadyExistsException {
        if (indexByName.containsKey(tableId.getTableName())) {
            throw new TableAlreadyExistsException(tableId);
        }
        save(tableId);
    }

    public void save(TableStatus id) {
        indexByName.put(id.getTableName(), id);
        indexById.put(id.getTableUniqueId(), id);
        onlineTableIds.add(id.getTableUniqueId());
    }

    @Override
    public Stream<TableStatus> streamAllTables() {
        return new ArrayList<>(indexByName.values()).stream();
    }

    @Override
    public Stream<TableStatus> streamOnlineTables() {
        return onlineTableIds.stream()
                .flatMap(tableId -> getTableByUniqueId(tableId).stream());
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
    public void delete(TableStatus tableId) {
        if (!indexById.containsKey(tableId.getTableUniqueId())) {
            throw TableNotFoundException.withTableId(tableId.getTableUniqueId());
        }
        TableStatus latestId = indexById.get(tableId.getTableUniqueId());
        if (!Objects.equals(latestId.getTableName(), tableId.getTableName())) {
            throw TableNotFoundException.withTableName(tableId.getTableName());
        }
        indexByName.remove(latestId.getTableName());
        indexById.remove(latestId.getTableUniqueId());
        onlineTableIds.remove(tableId.getTableUniqueId());
    }

    @Override
    public void update(TableStatus tableId) {
        TableStatus existingTableWithNewName = indexByName.get(tableId.getTableName());
        if (existingTableWithNewName != null && !existingTableWithNewName.getTableUniqueId().equals(tableId.getTableUniqueId())) {
            throw new TableAlreadyExistsException(existingTableWithNewName);
        }
        if (!indexById.containsKey(tableId.getTableUniqueId())) {
            throw TableNotFoundException.withTableId(tableId.getTableUniqueId());
        }
        TableStatus oldId = indexById.get(tableId.getTableUniqueId());
        indexByName.remove(oldId.getTableName());
        indexByName.put(tableId.getTableName(), tableId);
        indexById.put(tableId.getTableUniqueId(), tableId);
    }

    @Override
    public void takeOffline(TableStatus tableId) {
        if (!onlineTableIds.remove(tableId.getTableUniqueId())) {
            if (indexById.containsKey(tableId.getTableUniqueId())) {
                throw new TableAlreadyOfflineException(tableId);
            } else {
                throw TableNotFoundException.withTableIdentity(tableId);
            }
        }
    }

    @Override
    public void putOnline(TableStatus tableId) {
        if (!indexById.containsKey(tableId.getTableUniqueId())) {
            throw TableNotFoundException.withTableIdentity(tableId);
        }
        if (onlineTableIds.contains(tableId.getTableUniqueId())) {
            throw new TableAlreadyOnlineException(tableId);
        }
        onlineTableIds.add(tableId.getTableUniqueId());
    }
}
