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

package sleeper.core.properties.table;

import sleeper.core.table.TableAlreadyExistsException;
import sleeper.core.table.TableIdGenerator;
import sleeper.core.table.TableIndex;
import sleeper.core.table.TableNotFoundException;
import sleeper.core.table.TableStatus;

import java.util.Objects;
import java.util.Optional;
import java.util.stream.Stream;

import static sleeper.core.properties.table.TableProperty.TABLE_ID;
import static sleeper.core.properties.table.TableProperty.TABLE_NAME;
import static sleeper.core.properties.table.TableProperty.TABLE_ONLINE;

/**
 * A store to load and save table properties via the table index of the Sleeper instance.
 */
public class TablePropertiesStore {

    private static final TableIdGenerator ID_GENERATOR = new TableIdGenerator();

    private final TableIndex tableIndex;
    private final Client client;

    public TablePropertiesStore(TableIndex tableIndex, Client client) {
        this.tableIndex = tableIndex;
        this.client = client;
    }

    /**
     * Loads properties for the given Sleeper table.
     *
     * @param  table                  the table status
     * @return                        the table properties
     * @throws TableNotFoundException if the table with the given name is not found
     */
    TableProperties loadProperties(TableStatus table) {
        TableProperties tableProperties = client.loadProperties(table);
        tableProperties.validate();
        return tableProperties;
    }

    /**
     * Loads and validates properties for the Sleeper table with the given name.
     *
     * @param  tableName              the table name
     * @return                        the table properties
     * @throws TableNotFoundException if the table with the given name is not found
     */
    public TableProperties loadByName(String tableName) {
        return tableIndex.getTableByName(tableName)
                .map(this::loadProperties)
                .orElseThrow(() -> TableNotFoundException.withTableName(tableName));
    }

    /**
     * Loads and validates properties for the Sleeper table with the given unique ID.
     *
     * @param  tableId                the unique table ID
     * @return                        the table properties
     * @throws TableNotFoundException if the table with the given ID is not found
     */
    public TableProperties loadById(String tableId) {
        return tableIndex.getTableByUniqueId(tableId)
                .map(this::loadProperties)
                .orElseThrow(() -> TableNotFoundException.withTableId(tableId));
    }

    /**
     * Loads properties for the Sleeper table with the given name, with no validation.
     *
     * @param  tableName              the table name
     * @return                        the table properties
     * @throws TableNotFoundException if the table with the given name is not found
     */
    public TableProperties loadByNameNoValidation(String tableName) {
        return tableIndex.getTableByName(tableName)
                .map(client::loadProperties)
                .orElseThrow(() -> TableNotFoundException.withTableName(tableName));
    }

    /**
     * Loads properties for all tables in the Sleeper instance.
     *
     * @return the table properties
     */
    public Stream<TableProperties> streamAllTables() {
        return streamAllTableStatuses().map(this::loadProperties);
    }

    /**
     * Loads properties for online tables in the Sleeper instance.
     *
     * @return the table properties for online tables
     */
    public Stream<TableProperties> streamOnlineTables() {
        return tableIndex.streamOnlineTables().map(this::loadProperties);
    }

    /**
     * Loads the table index entry for all tables in the Sleeper instance.
     *
     * @return the table statuses
     */
    public Stream<TableStatus> streamAllTableStatuses() {
        return tableIndex.streamAllTables();
    }

    /**
     * Loads the table index entry for all online tables in the Sleeper instance.
     *
     * @return the statuses of online tables
     */
    public Stream<TableStatus> streamOnlineTableIds() {
        return tableIndex.streamOnlineTables();
    }

    /**
     * Creates a new Sleeper table in the table index, and saves the table properties.
     *
     * @param tableProperties the table properties
     */
    public void createTable(TableProperties tableProperties) {
        String tableName = tableProperties.get(TableProperty.TABLE_NAME);
        tableIndex.getTableByName(tableName).ifPresent(tableId -> {
            throw new TableAlreadyExistsException(tableId);
        });
        createWhenNotInIndex(tableProperties);
    }

    /**
     * Updates a Sleeper table in the table index, and saves its table properties.
     *
     * @param  tableProperties        the table properties
     * @throws TableNotFoundException if the table with the given name is not found
     */
    public void update(TableProperties tableProperties) {
        Optional<TableStatus> existingOpt = getExistingStatus(tableProperties);
        if (existingOpt.isPresent()) {
            updateTable(existingOpt.get(), tableProperties);
        } else {
            throw TableNotFoundException.withTableName(tableProperties.get(TABLE_NAME));
        }
    }

    /**
     * Creates or updates a Sleeper table in the table index, and saves its table properties.
     *
     * @param tableProperties the table properties
     */
    public void save(TableProperties tableProperties) {
        Optional<TableStatus> existingOpt = getExistingStatus(tableProperties);
        if (existingOpt.isPresent()) {
            updateTable(existingOpt.get(), tableProperties);
        } else {
            createWhenNotInIndex(tableProperties);
        }
    }

    private Optional<TableStatus> getExistingStatus(TableProperties tableProperties) {
        if (tableProperties.isSet(TABLE_ID)) {
            return tableIndex.getTableByUniqueId(tableProperties.get(TABLE_ID));
        } else {
            return tableIndex.getTableByName(tableProperties.get(TABLE_NAME));
        }
    }

    private void updateTable(TableStatus existing, TableProperties tableProperties) {
        String tableName = tableProperties.get(TABLE_NAME);
        boolean isOnline = tableProperties.getBoolean(TABLE_ONLINE);
        if (!Objects.equals(existing.getTableName(), tableName) || !(existing.isOnline() == isOnline)) {
            tableIndex.update(TableStatus.uniqueIdAndName(existing.getTableUniqueId(),
                    tableName, isOnline));
        }
        tableProperties.set(TABLE_ID, existing.getTableUniqueId());
        client.saveProperties(tableProperties);
    }

    private void createWhenNotInIndex(TableProperties tableProperties) {
        if (!tableProperties.isSet(TABLE_ID)) {
            tableProperties.set(TABLE_ID, ID_GENERATOR.generateString());
        }
        client.saveProperties(tableProperties);
        tableIndex.create(tableProperties.getStatus());
    }

    /**
     * Deletes a Sleeper table by its name.
     *
     * @param tableName the table name
     */
    public void deleteByName(String tableName) {
        tableIndex.getTableByName(tableName)
                .ifPresent(this::delete);
    }

    /**
     * Deletes a Sleeper table.
     *
     * @param table the table status
     */
    public void delete(TableStatus table) {
        tableIndex.delete(table);
        client.deleteProperties(table);
    }

    /**
     * Loads, saves and deletes table properties in the underlying store.
     */
    public interface Client {

        /**
         * Loads properties of the given Sleeper table.
         *
         * @param  table the table status
         * @return       the table properties
         */
        TableProperties loadProperties(TableStatus table);

        /**
         * Saves table properties.
         *
         * @param tableProperties the table properties
         */
        void saveProperties(TableProperties tableProperties);

        /**
         * Deletes table properties.
         *
         * @param table the table status
         */
        void deleteProperties(TableStatus table);
    }
}
