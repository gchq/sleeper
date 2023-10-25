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

package sleeper.configuration.properties.table;

import sleeper.core.table.TableAlreadyExistsException;
import sleeper.core.table.TableId;
import sleeper.core.table.TableIdGenerator;
import sleeper.core.table.TableIndex;

import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
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

    public TableProperties loadProperties(TableId tableId) {
        TableProperties tableProperties = client.loadProperties(tableId);
        tableProperties.validate();
        return tableProperties;
    }

    public Optional<TableProperties> loadByName(String tableName) {
        return tableIndex.getTableByName(tableName)
                .map(this::loadProperties);
    }

    public Optional<TableProperties> loadById(String tableId) {
        return tableIndex.getTableByUniqueId(tableId)
                .map(this::loadProperties);
    }

    public Optional<TableProperties> loadByNameNoValidation(String tableName) {
        return tableIndex.getTableByName(tableName)
                .map(client::loadProperties);
    }

    public Stream<TableProperties> streamAllTables() {
        return streamAllTableIds().map(this::loadProperties);
    }

    public Stream<TableId> streamAllTableIds() {
        return tableIndex.streamAllTables();
    }

    public List<String> listTableNames() {
        return streamAllTableIds().map(TableId::getTableName).collect(Collectors.toUnmodifiableList());
    }

    public List<TableId> listTableIds() {
        return streamAllTableIds().collect(Collectors.toUnmodifiableList());
    }

    public Optional<TableId> lookupByName(String tableName) {
        return tableIndex.getTableByName(tableName);
    }

    public void createTable(TableProperties tableProperties) {
        String tableName = tableProperties.get(TableProperty.TABLE_NAME);
        tableIndex.getTableByName(tableName).ifPresent(tableId -> {
            throw new TableAlreadyExistsException(tableId);
        });
        createWhenNotInIndex(tableProperties);
    }

    public void save(TableProperties tableProperties) {
        String tableName = tableProperties.get(TABLE_NAME);
        Optional<TableId> existingId = tableIndex.getTableByName(tableName);
        if (existingId.isPresent()) {
            tableProperties.set(TABLE_ID, existingId.get().getTableUniqueId());
            client.saveProperties(tableProperties);
        } else {
            createWhenNotInIndex(tableProperties);
        }
    }

    private void createWhenNotInIndex(TableProperties tableProperties) {
        if (!tableProperties.isSet(TABLE_ID)) {
            tableProperties.set(TABLE_ID, ID_GENERATOR.generateString());
        }
        client.saveProperties(tableProperties);
        tableIndex.create(tableProperties.getId());
    }

    public void deleteByName(String tableName) {
        tableIndex.getTableByName(tableName)
                .ifPresent(tableId -> {
                    tableIndex.delete(tableId);
                    client.deleteProperties(tableId);
                });
    }

    public interface Client {
        TableProperties loadProperties(TableId tableId);

        void saveProperties(TableProperties tableProperties);

        void deleteProperties(TableId tableId);
    }
}
