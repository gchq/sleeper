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
import java.util.Optional;
import java.util.TreeMap;
import java.util.stream.Stream;

public class InMemoryTableIndex implements TableIndex {

    private final Map<String, TableId> idByName = new TreeMap<>();
    private final Map<String, TableId> nameById = new HashMap<>();

    @Override
    public void create(TableId tableId) throws TableAlreadyExistsException {
        if (idByName.containsKey(tableId.getTableName())) {
            throw new TableAlreadyExistsException(tableId);
        }
        save(tableId);
    }

    public void save(TableId id) {
        idByName.put(id.getTableName(), id);
        nameById.put(id.getTableUniqueId(), id);
    }

    @Override
    public Stream<TableId> streamAllTables() {
        return idByName.values().stream();
    }

    @Override
    public Optional<TableId> getTableByName(String tableName) {
        return Optional.ofNullable(idByName.get(tableName));
    }

    @Override
    public Optional<TableId> getTableByUniqueId(String tableUniqueId) {
        return Optional.ofNullable(nameById.get(tableUniqueId));
    }

    @Override
    public void delete(TableId tableId) {
        idByName.remove(tableId.getTableName());
        nameById.remove(tableId.getTableUniqueId());
    }
}
