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

import java.util.Optional;
import java.util.stream.Stream;

/**
 * An interface for a class that stores table statuses.
 */
public interface TableIndex {
    /**
     * Stores a table status.
     *
     * @param  table                       the table status to add
     * @throws TableAlreadyExistsException if the table already exists
     */
    void create(TableStatus table) throws TableAlreadyExistsException;

    /**
     * Streams all table statuses.
     *
     * @return all table statuses in the store
     */
    Stream<TableStatus> streamAllTables();

    /**
     * Streams table statuses that are marked as online.
     *
     * @return all online table statuses in the store
     */
    Stream<TableStatus> streamOnlineTables();

    /**
     * Gets a table by the table name.
     *
     * @param  tableName the table name
     * @return           the table status with the provided table name, or an empty optional if no table exists with
     *                   that name
     */
    Optional<TableStatus> getTableByName(String tableName);

    /**
     * Gets a table by the table ID.
     *
     * @param  tableUniqueId the table ID
     * @return               the table status with the provided table ID, or an empty optional if no table exists with
     *                       that ID
     */
    Optional<TableStatus> getTableByUniqueId(String tableUniqueId);

    /**
     * Removes a table status from the store.
     *
     * @param table the table status to remove
     */
    void delete(TableStatus table);

    /**
     * Updates a table status in the store.
     *
     * @param table the updated table status
     */
    void update(TableStatus table);
}
