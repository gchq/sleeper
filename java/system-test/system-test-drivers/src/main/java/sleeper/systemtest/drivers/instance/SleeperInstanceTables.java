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

package sleeper.systemtest.drivers.instance;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.s3.AmazonS3;

import sleeper.configuration.properties.instance.InstanceProperties;
import sleeper.configuration.properties.table.TableProperties;
import sleeper.configuration.properties.table.TablePropertiesProvider;
import sleeper.core.schema.Schema;
import sleeper.core.table.TableIdentity;

import java.util.Map;

public class SleeperInstanceTables {

    private final Map<String, Table> tablesById;
    private final Table currentTable;

    private SleeperInstanceTables(Table table) {
        this.tablesById = Map.of(table.getId().getTableUniqueId(), table);
        this.currentTable = table;
    }

    public static SleeperInstanceTables loadOneExistingTable(
            AmazonS3 s3, AmazonDynamoDB dynamoDB, InstanceProperties instanceProperties, String tableName) {
        TablePropertiesProvider tablePropertiesProvider = new TablePropertiesProvider(instanceProperties, s3, dynamoDB);
        TableProperties tableProperties = tablePropertiesProvider.getByName(tableName);
        return new SleeperInstanceTables(new Table(tableProperties, tablePropertiesProvider));
    }

    public TableProperties getTableProperties() {
        return currentTable.tableProperties;
    }

    public TablePropertiesProvider getTablePropertiesProvider() {
        return currentTable.tablePropertiesProvider;
    }

    public Schema getSchema() {
        return currentTable.tableProperties.getSchema();
    }

    private static class Table {
        private final TableProperties tableProperties;
        private final TablePropertiesProvider tablePropertiesProvider;

        public Table(TableProperties tableProperties, TablePropertiesProvider tablePropertiesProvider) {
            this.tableProperties = tableProperties;
            this.tablePropertiesProvider = tablePropertiesProvider;
        }

        public TableIdentity getId() {
            return tableProperties.getId();
        }
    }

}
