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

/**
 * An exception for when a table could not be found.
 */
public class TableNotFoundException extends RuntimeException {
    private TableNotFoundException(String message, Exception cause) {
        super(message, cause);
    }

    /**
     * Creates an instance of this class with a table ID.
     *
     * @param  tableId the table ID
     * @return         an instance of this class
     */
    public static TableNotFoundException withTableId(String tableId) {
        return withTableId(tableId, null);
    }

    /**
     * Creates an instance of this class with a table ID and a cause exception.
     *
     * @param  tableId the table ID
     * @param  cause   a cause exception
     * @return         an instance of this class
     */
    public static TableNotFoundException withTableId(String tableId, Exception cause) {
        return new TableNotFoundException("Table not found with ID \"" + tableId + "\"", cause);
    }

    /**
     * Creates an instance of this class with a table name.
     *
     * @param  tableName the table name
     * @return           an instance of this class
     */
    public static TableNotFoundException withTableName(String tableName) {
        return withTableName(tableName, null);
    }

    /**
     * Creates an instance of this class with a table name and a cause exception.
     *
     * @param  tableName the table name
     * @param  cause     a cause exception
     * @return           an instance of this class
     */
    public static TableNotFoundException withTableName(String tableName, Exception cause) {
        return new TableNotFoundException("Table not found with name \"" + tableName + "\"", cause);
    }

    /**
     * Creates an instance of this class with a table status.
     *
     * @param  table the table status
     * @return       an instance of this class
     */
    public static TableNotFoundException withTable(TableStatus table) {
        return withTable(table, null);
    }

    /**
     * Creates an instance of this class with a table status and a cause exception.
     *
     * @param  table the table status
     * @param  cause a cause exception
     * @return       an instance of this class
     */
    public static TableNotFoundException withTable(TableStatus table, Exception cause) {
        return new TableNotFoundException("Table not found: " + table, cause);
    }
}
