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

import java.util.Map;
import java.util.TreeMap;
import java.util.stream.Stream;

public class InMemoryTableIdStore implements TableIdStore {

    private final Map<String, TableId> idByName = new TreeMap<>();
    private final Map<String, TableId> nameById = new TreeMap<>();
    private int nextIdNumber = 1;

    @Override
    public TableId createTable(String tableName) {
        if (idByName.containsKey(tableName)) {
            throw new TableAlreadyExistsException(tableName);
        }
        TableId id = TableId.idAndName("table-" + nextIdNumber, tableName);
        nextIdNumber++;
        idByName.put(tableName, id);
        nameById.put(id.getTableId(), id);
        return id;
    }

    @Override
    public Stream<TableId> streamAllTables() {
        return idByName.values().stream();
    }

    @Override
    public TableId getTableByName(String tableName) {
        return idByName.get(tableName);
    }

    @Override
    public TableId getTableById(String tableId) {
        return nameById.get(tableId);
    }
}
