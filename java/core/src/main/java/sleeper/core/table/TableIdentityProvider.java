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

/**
 * This class can be used to cache table identities to avoid looking them up repeatedly in the table index database.
 */
public class TableIdentityProvider {
    private final TableIndex tableIndex;
    private final Map<String, TableIdentity> tableIdentityById = new HashMap<>();

    public TableIdentityProvider(TableIndex tableIndex) {
        this.tableIndex = tableIndex;
    }

    public Optional<TableIdentity> getById(String tableId) {
        if (tableIdentityById.containsKey(tableId)) {
            return Optional.ofNullable(tableIdentityById.get(tableId));
        } else {
            TableIdentity identity = tableIndex.getTableByUniqueId(tableId).orElse(null);
            tableIdentityById.put(tableId, identity);
            return Optional.ofNullable(identity);
        }
    }
}
