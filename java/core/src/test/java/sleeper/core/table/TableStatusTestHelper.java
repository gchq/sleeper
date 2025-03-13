/*
 * Copyright 2022-2025 Crown Copyright
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
 * A helper class for creating a table status.
 */
public class TableStatusTestHelper {
    private TableStatusTestHelper() {
    }

    /**
     * Creates a table status object.
     *
     * @param  tableUniqueId the table ID
     * @param  tableName     the table name
     * @return               a {@link TableStatus} object
     */
    public static TableStatus uniqueIdAndName(String tableUniqueId, String tableName) {
        return TableStatus.uniqueIdAndName(tableUniqueId, tableName, true);
    }

}
