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

public class TableNotFoundException extends RuntimeException {
    private TableNotFoundException(String message) {
        super(message);
    }

    public static TableNotFoundException withTableId(String tableId) {
        return new TableNotFoundException("Table not found with ID \"" + tableId + "\"");
    }

    public static TableNotFoundException withTableName(String tableName) {
        return new TableNotFoundException("Table not found with name \"" + tableName + "\"");
    }
}
