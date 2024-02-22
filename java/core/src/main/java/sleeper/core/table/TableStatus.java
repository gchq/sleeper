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

import java.util.Objects;

public class TableStatus {

    private final String tableUniqueId;
    private final String tableName;

    private TableStatus(String tableUniqueId, String tableName) {
        this.tableUniqueId = tableUniqueId;
        this.tableName = tableName;
    }

    public static TableStatus uniqueIdAndName(String tableUniqueId, String tableName) {
        return new TableStatus(tableUniqueId, tableName);
    }

    public String getTableName() {
        return tableName;
    }

    public String getTableUniqueId() {
        return tableUniqueId;
    }

    @Override
    public boolean equals(Object object) {
        if (this == object) {
            return true;
        }
        if (object == null || getClass() != object.getClass()) {
            return false;
        }
        TableStatus table1 = (TableStatus) object;
        return Objects.equals(tableUniqueId, table1.tableUniqueId) && Objects.equals(tableName, table1.tableName);
    }

    @Override
    public int hashCode() {
        return Objects.hash(tableUniqueId, tableName);
    }

    @Override
    public String toString() {
        return tableName + (tableUniqueId == null ? "" : " (" + tableUniqueId + ")");
    }
}
