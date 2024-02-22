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

package sleeper.configuration.properties.table;

import sleeper.core.table.TableAlreadyExistsException;
import sleeper.core.table.TableIdGenerator;
import sleeper.core.table.TableStatus;
import sleeper.core.table.TableIndex;
import sleeper.core.table.TableNotFoundException;

import java.util.Objects;
import java.util.Optional;
import java.util.stream.Stream;

import static sleeper.configuration.properties.table.TableProperty.TABLE_ID;
import static sleeper.configuration.properties.table.TableProperty.TABLE_NAME;

public class TablePropertiesStore {

    private static final TableIdGenerator ID_GENERATOR = new TableIdGenerator();

    private final TableIndex tableIndex;
    private final Client client;

    public TablePropertiesStore(TableIndex tableIndex, Client client) {
        this.tableIndex = tableIndex;
        this.client = client;
    }

    TableProperties loadProperties(TableStatus table) {
        TableProperties tableProperties = client.loadProperties(table);
        tableProperties.validate();
        return tableProperties;
    }

    public TableProperties loadByName(String tableName) {
        return tableIndex.getTableByName(tableName)
                .map(this::loadProperties)
                .orElseThrow(() -> TableNotFoundException.withTableName(tableName));
    }

    public TableProperties loadById(String tableId) {
        return tableIndex.getTableByUniqueId(tableId)
                .map(this::loadProperties)
                .orElseThrow(() -> TableNotFoundException.withTableId(tableId));
    }

    public TableProperties loadByNameNoValidation(String tableName) {
        return tableIndex.getTableByName(tableName)
                .map(client::loadProperties)
                .orElseThrow(() -> TableNotFoundException.withTableName(tableName));
    }

    public Stream<TableProperties> streamAllTables() {
        return streamAllTableStatuses().map(this::loadProperties);
    }

    public Stream<TableStatus> streamAllTableStatuses() {
        return tableIndex.streamAllTables();
    }

    public void createTable(TableProperties tableProperties) {
        String tableName = tableProperties.get(TableProperty.TABLE_NAME);
        tableIndex.getTableByName(tableName).ifPresent(tableId -> {
            throw new TableAlreadyExistsException(tableId);
        });
        createWhenNotInIndex(tableProperties);
    }

    public void save(TableProperties tableProperties) {
        Optional<TableStatus> existingStatus = getExistingStatus(tableProperties);
        if (existingStatus.isPresent()) {
            TableStatus status = existingStatus.get();
            String tableName = tableProperties.get(TABLE_NAME);
            if (!Objects.equals(status.getTableName(), tableName)) {
                tableIndex.update(TableStatus.uniqueIdAndName(status.getTableUniqueId(), tableName));
            }
            tableProperties.set(TABLE_ID, status.getTableUniqueId());
            client.saveProperties(tableProperties);
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

    private void createWhenNotInIndex(TableProperties tableProperties) {
        if (!tableProperties.isSet(TABLE_ID)) {
            tableProperties.set(TABLE_ID, ID_GENERATOR.generateString());
        }
        client.saveProperties(tableProperties);
        tableIndex.create(tableProperties.getStatus());
    }

    public void deleteByName(String tableName) {
        tableIndex.getTableByName(tableName)
                .ifPresent(this::delete);
    }

    public void delete(TableStatus table) {
        tableIndex.delete(table);
        client.deleteProperties(table);
    }

    public interface Client {
        TableProperties loadProperties(TableStatus table);

        void saveProperties(TableProperties tableProperties);

        void deleteProperties(TableStatus table);
    }
}
