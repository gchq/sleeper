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

import sleeper.clients.status.update.AddTable;
import sleeper.clients.status.update.ReinitialiseTable;
import sleeper.configuration.properties.instance.InstanceProperties;
import sleeper.configuration.properties.table.S3TableProperties;
import sleeper.configuration.properties.table.TableProperties;
import sleeper.configuration.properties.table.TablePropertiesProvider;
import sleeper.configuration.properties.table.TablePropertiesStore;
import sleeper.configuration.table.index.DynamoDBTableIndex;
import sleeper.core.statestore.StateStore;
import sleeper.core.statestore.StateStoreException;
import sleeper.core.table.TableIndex;
import sleeper.statestore.StateStoreFactory;
import sleeper.statestore.StateStoreProvider;

import java.io.IOException;
import java.io.UncheckedIOException;

public class SleeperInstanceTablesDriver {

    private final AmazonS3 s3;
    private final AmazonDynamoDB dynamoDB;
    private final Configuration hadoopConfiguration;

    public SleeperInstanceTablesDriver(AmazonS3 s3, AmazonDynamoDB dynamoDB, Configuration hadoopConfiguration) {
        this.s3 = s3;
        this.dynamoDB = dynamoDB;
        this.hadoopConfiguration = hadoopConfiguration;
    }

    public void save(InstanceProperties instanceProperties, TableProperties deployedProperties) {
        tablePropertiesStore(instanceProperties).save(deployedProperties);
    }

    public void reinitialise(String instanceId, String tableName) {
        try {
            new ReinitialiseTable(s3, dynamoDB,
                    instanceId, tableName,
                    true).run();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        } catch (StateStoreException e) {
            throw new RuntimeException(e);
        }
    }

    public void delete(InstanceProperties instanceProperties, TableProperties tableProperties) {
        stateStore(instanceProperties, tableProperties).clearTable();
        tablePropertiesStore(instanceProperties).delete(tableProperties.getId());
    }

    public void add(InstanceProperties instanceProperties, TableProperties properties) {
        try {
            new AddTable(s3, dynamoDB, instanceProperties, properties, hadoopConfiguration).run();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    public TablePropertiesProvider createTablePropertiesProvider(InstanceProperties instanceProperties) {
        return new TablePropertiesProvider(instanceProperties, s3, dynamoDB);
    }

    public StateStoreProvider createStateStoreProvider(InstanceProperties instanceProperties) {
        return new StateStoreProvider(dynamoDB, instanceProperties, hadoopConfiguration);
    }

    public TableIndex tableIndex(InstanceProperties instanceProperties) {
        return new DynamoDBTableIndex(instanceProperties, dynamoDB);
    }

    private TablePropertiesStore tablePropertiesStore(InstanceProperties instanceProperties) {
        return S3TableProperties.getStore(instanceProperties, s3, dynamoDB);
    }

    private StateStore stateStore(InstanceProperties instanceProperties, TableProperties properties) {
        return new StateStoreFactory(dynamoDB, instanceProperties, hadoopConfiguration)
                .getStateStore(properties);
    }
}
