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

package sleeper.core.table;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.TreeMap;
import java.util.stream.Stream;

public class InMemoryTableIndex implements TableIndex {

    private final Map<String, TableId> indexByName = new TreeMap<>();
    private final Map<String, TableId> indexById = new HashMap<>();

    @Override
    public void create(TableId tableId) throws TableAlreadyExistsException {
        if (indexByName.containsKey(tableId.getTableName())) {
            throw new TableAlreadyExistsException(tableId);
        }
        save(tableId);
    }

    public void save(TableId id) {
        indexByName.put(id.getTableName(), id);
        indexById.put(id.getTableUniqueId(), id);
    }

    @Override
    public Stream<TableId> streamAllTables() {
        return indexByName.values().stream();
    }

    @Override
    public Optional<TableId> getTableByName(String tableName) {
        return Optional.ofNullable(indexByName.get(tableName));
    }

    @Override
    public Optional<TableId> getTableByUniqueId(String tableUniqueId) {
        return Optional.ofNullable(indexById.get(tableUniqueId));
    }

    @Override
    public void delete(TableId tableId) {
        if (!indexById.containsKey(tableId.getTableUniqueId())) {
            throw TableNotFoundException.withTableId(tableId.getTableUniqueId());
        }
        TableId latestId = indexById.get(tableId.getTableUniqueId());
        if (!Objects.equals(latestId.getTableName(), tableId.getTableName())) {
            throw TableNotFoundException.withTableName(tableId.getTableName());
        }
        indexByName.remove(latestId.getTableName());
        indexById.remove(latestId.getTableUniqueId());
    }

    @Override
    public void update(TableId tableId) {
        if (!indexById.containsKey(tableId.getTableUniqueId())) {
            throw TableNotFoundException.withTableId(tableId.getTableUniqueId());
        }
        TableId oldId = indexById.get(tableId.getTableUniqueId());
        indexByName.remove(oldId.getTableName());
        indexByName.put(tableId.getTableName(), tableId);
        indexById.put(tableId.getTableUniqueId(), tableId);
    }
}
