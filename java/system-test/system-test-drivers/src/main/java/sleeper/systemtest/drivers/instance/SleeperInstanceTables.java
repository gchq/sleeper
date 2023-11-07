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
import org.apache.hadoop.conf.Configuration;

import sleeper.clients.deploy.DeployInstanceConfiguration;
import sleeper.clients.status.update.AddTable;
import sleeper.clients.status.update.ReinitialiseTable;
import sleeper.configuration.properties.instance.InstanceProperties;
import sleeper.configuration.properties.table.S3TableProperties;
import sleeper.configuration.properties.table.TableProperties;
import sleeper.configuration.properties.table.TablePropertiesProvider;
import sleeper.configuration.properties.table.TablePropertiesStore;
import sleeper.core.schema.Schema;
import sleeper.core.statestore.StateStoreException;
import sleeper.statestore.StateStoreFactory;
import sleeper.statestore.StateStoreProvider;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static java.util.function.UnaryOperator.identity;
import static sleeper.configuration.properties.instance.CommonProperty.ID;
import static sleeper.configuration.properties.table.TableProperty.TABLE_NAME;

public class SleeperInstanceTables {

    private final DeployInstanceConfiguration deployConfiguration;
    private final TablePropertiesProvider tablePropertiesProvider;
    private final StateStoreProvider stateStoreProvider;
    private final List<TableProperties> tableProperties;
    private final Map<String, TableProperties> tableByName;
    private TableProperties currentTable;

    private SleeperInstanceTables(
            DeployInstanceConfiguration deployConfiguration,
            TablePropertiesProvider tablePropertiesProvider,
            StateStoreProvider stateStoreProvider,
            List<TableProperties> tableProperties) {
        this.deployConfiguration = deployConfiguration;
        this.tablePropertiesProvider = tablePropertiesProvider;
        this.stateStoreProvider = stateStoreProvider;
        this.tableProperties = tableProperties;
        this.tableByName = tableProperties.stream()
                .collect(Collectors.toMap(p -> p.get(TABLE_NAME), identity()));
        if (tableProperties.size() == 1) {
            currentTable = tableProperties.get(0);
        }
    }

    public static SleeperInstanceTables load(
            DeployInstanceConfiguration deployConfig,
            InstanceProperties instanceProperties,
            AmazonS3 s3,
            AmazonDynamoDB dynamoDB,
            Configuration hadoopConfiguration) {
        TablePropertiesProvider tablePropertiesProvider = new TablePropertiesProvider(instanceProperties, s3, dynamoDB);
        StateStoreProvider stateStoreProvider = new StateStoreProvider(dynamoDB, instanceProperties, hadoopConfiguration);
        List<TableProperties> tableProperties = tablePropertiesProvider.streamAllTables()
                .collect(Collectors.toUnmodifiableList());
        return new SleeperInstanceTables(deployConfig, tablePropertiesProvider, stateStoreProvider, tableProperties);
    }

    public SleeperInstanceTables reset(InstanceProperties instanceProperties,
                                       AmazonS3 s3, AmazonDynamoDB dynamoDB, Configuration hadoopConfiguration) {
        String instanceId = instanceProperties.get(ID);
        TablePropertiesStore tablePropertiesStore = S3TableProperties.getStore(instanceProperties, s3, dynamoDB);
        Map<String, TableProperties> configTableByName = deployConfiguration.getTableProperties().stream()
                .collect(Collectors.toMap(properties -> properties.get(TABLE_NAME), properties -> properties));
        Set<String> tableNames = new HashSet<>(configTableByName.keySet());
        tableProperties.forEach(properties -> tableNames.add(properties.get(TABLE_NAME)));
        for (String tableName : tableNames) {
            TableProperties deployedProperties = tableByName.get(tableName);
            TableProperties configuredProperties = configTableByName.get(tableName);
            if (deployedProperties != null) {
                if (configuredProperties != null) {
                    ResetProperties.reset(deployedProperties, configuredProperties);
                    tablePropertiesStore.save(deployedProperties);
                }
                try {
                    new ReinitialiseTable(s3, dynamoDB,
                            instanceId, tableName,
                            true).run();
                } catch (IOException e) {
                    throw new UncheckedIOException(e);
                } catch (StateStoreException e) {
                    throw new RuntimeException(e);
                }
                if (configuredProperties == null) {
                    new StateStoreFactory(dynamoDB, instanceProperties, hadoopConfiguration)
                            .getStateStore(deployedProperties).clearTable();
                    tablePropertiesStore.delete(deployedProperties.getId());
                }
            } else {
                try {
                    new AddTable(s3, dynamoDB, instanceProperties, configuredProperties, hadoopConfiguration).run();
                } catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
            }
        }
        return load(deployConfiguration, instanceProperties, s3, dynamoDB, hadoopConfiguration);
    }

    public Optional<TableProperties> getTableProperties(String tableName) {
        return Optional.ofNullable(tableByName.get(tableName));
    }

    public TableProperties getTableProperties() {
        return currentTable;
    }

    public Schema getSchema() {
        return currentTable.getSchema();
    }

    public TablePropertiesProvider getTablePropertiesProvider() {
        return tablePropertiesProvider;
    }

    public StateStoreProvider getStateStoreProvider() {
        return stateStoreProvider;
    }
}
