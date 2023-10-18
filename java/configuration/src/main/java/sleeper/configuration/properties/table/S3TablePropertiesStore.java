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

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.s3.AmazonS3;

import sleeper.configuration.properties.instance.InstanceProperties;
import sleeper.configuration.table.index.DynamoDBTableIndex;
import sleeper.core.table.TableId;
import sleeper.core.table.TableIndex;

import java.util.Optional;
import java.util.stream.Stream;

import static sleeper.configuration.properties.instance.CdkDefinedInstanceProperty.CONFIG_BUCKET;
import static sleeper.configuration.properties.table.TableProperties.TABLES_PREFIX;
import static sleeper.configuration.properties.table.TableProperty.TABLE_ID;
import static sleeper.configuration.properties.table.TableProperty.TABLE_NAME;

public class S3TablePropertiesStore implements TablePropertiesStore {

    private final InstanceProperties instanceProperties;
    private final AmazonS3 s3Client;
    private final TableIndex tableIndex;

    public S3TablePropertiesStore(InstanceProperties instanceProperties, AmazonS3 s3Client, AmazonDynamoDB dynamoClient) {
        this.instanceProperties = instanceProperties;
        this.s3Client = s3Client;
        this.tableIndex = new DynamoDBTableIndex(dynamoClient, instanceProperties);
    }

    @Override
    public TableProperties loadProperties(TableId tableId) {
        TableProperties properties = new TableProperties(instanceProperties);
        properties.loadFromS3(s3Client, tableId.getTableName());
        return properties;
    }

    @Override
    public Optional<TableProperties> loadByName(String tableName) {
        return tableIndex.getTableByName(tableName)
                .map(this::loadProperties);
    }

    @Override
    public Optional<TableProperties> loadByNameNoValidation(String tableName) {
        return tableIndex.getTableByName(tableName)
                .map(tableId -> new TableProperties(instanceProperties,
                        TableProperties.loadPropertiesFromS3(s3Client, instanceProperties, tableName)));
    }

    @Override
    public Stream<TableProperties> streamAllTables() {
        return streamAllTableIds().map(this::loadProperties);
    }

    @Override
    public Stream<TableId> streamAllTableIds() {
        return tableIndex.streamAllTables();
    }

    @Override
    public void save(TableProperties tableProperties) {
        String tableId = tableIndex.getOrCreateTableByName(tableProperties.get(TABLE_NAME))
                .getTableUniqueId();
        tableProperties.set(TABLE_ID, tableId);
        tableProperties.saveToS3(s3Client);
    }

    @Override
    public void deleteByName(String tableName) {
        tableIndex.getTableByName(tableName)
                .ifPresent(tableId -> {
                    tableIndex.delete(tableId);
                    s3Client.deleteObject(instanceProperties.get(CONFIG_BUCKET), TABLES_PREFIX + "/" + tableName);
                });
    }
}
