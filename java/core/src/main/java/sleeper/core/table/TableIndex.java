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

import java.util.Optional;
import java.util.stream.Stream;

/**
 * Stores the status of Sleeper tables in an instance. This is used to define and look up which tables exist in the
 * instance. Note that the configuration of the table is stored separately, and must be kept in sync with the index.
 */
public interface TableIndex {
    /**
     * Creates a new Sleeper table.
     *
     * @param  table                       the table status to add
     * @throws TableAlreadyExistsException if the table already exists
     */
    void create(TableStatus table) throws TableAlreadyExistsException;

    /**
     * Streams statuses of all Sleeper tables in the instance.
     *
     * @return statuses of all tables
     */
    Stream<TableStatus> streamAllTables();

    /**
     * Streams statuses of all Sleeper tables that are marked as online.
     *
     * @return statuses of all online tables
     */
    Stream<TableStatus> streamOnlineTables();

    /**
     * Gets the status of a Sleeper table by the table name.
     *
     * @param  tableName the table name
     * @return           the table status with the provided table name, or an empty optional if no table exists with
     *                   that name
     */
    Optional<TableStatus> getTableByName(String tableName);

    /**
     * Gets the status of a Sleeper table by the internal table ID.
     *
     * @param  tableUniqueId the table ID
     * @return               the table status with the provided table ID, or an empty optional if no table exists with
     *                       that ID
     */
    Optional<TableStatus> getTableByUniqueId(String tableUniqueId);

    /**
     * Deletes a Sleeper table.
     *
     * @param table the status of the table to remove
     */
    void delete(TableStatus table);

    /**
     * Update the status of a Sleeper table. This can be used to rename a table, put it online, or take it offline.
     *
     * @param table the updated table status
     */
    void update(TableStatus table);
}
