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

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

/**
 * This class can be used to cache tables to avoid looking them up repeatedly in the table index database.
 */
public class TableStatusProvider {
    private final TableIndex tableIndex;
    private final Map<String, TableStatus> tableStatusById = new HashMap<>();

    public TableStatusProvider(TableIndex tableIndex) {
        this.tableIndex = tableIndex;
    }

    public Optional<TableStatus> getById(String tableId) {
        if (tableStatusById.containsKey(tableId)) {
            return Optional.ofNullable(tableStatusById.get(tableId));
        } else {
            TableStatus status = tableIndex.getTableByUniqueId(tableId).orElse(null);
            tableStatusById.put(tableId, status);
            return Optional.ofNullable(status);
        }
    }
}
