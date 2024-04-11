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

/**
 * A data structure for storing metadata about a Sleeper table.
 * Stores the table name, the internal unique ID, and whether the table is online or not.
 */
public class TableStatus {

    private final String tableUniqueId;
    private final String tableName;
    private final boolean online;

    private TableStatus(String tableUniqueId, String tableName, boolean online) {
        this.tableUniqueId = tableUniqueId;
        this.tableName = tableName;
        this.online = online;
    }

    /**
     * Creates an instance of this class.
     *
     * @param  tableUniqueId the table ID
     * @param  tableName     the table name
     * @param  online        whether the table is online or not
     * @return               an instance of this class
     */
    public static TableStatus uniqueIdAndName(String tableUniqueId, String tableName, boolean online) {
        return new TableStatus(tableUniqueId, tableName, online);
    }

    public String getTableName() {
        return tableName;
    }

    public String getTableUniqueId() {
        return tableUniqueId;
    }

    public boolean isOnline() {
        return online;
    }

    /**
     * Takes this table offline.
     *
     * @return a copy of this table status with the online flag set to false
     */
    public TableStatus takeOffline() {
        return new TableStatus(this.tableUniqueId, this.tableName, false);
    }

    /**
     * Puts this table online.
     *
     * @return a copy of this table status with the online flag set to true
     */
    public TableStatus putOnline() {
        return new TableStatus(this.tableUniqueId, this.tableName, true);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        TableStatus that = (TableStatus) o;
        return online == that.online && Objects.equals(tableUniqueId, that.tableUniqueId) && Objects.equals(tableName, that.tableName);
    }

    @Override
    public int hashCode() {
        return Objects.hash(tableUniqueId, tableName, online);
    }

    @Override
    public String toString() {
        return tableName +
                (tableUniqueId == null ? "" : " (" + tableUniqueId + ")") +
                (!online ? " [offline]" : "");
    }
}
