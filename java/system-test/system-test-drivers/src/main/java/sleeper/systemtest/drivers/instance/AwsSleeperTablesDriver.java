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

package sleeper.systemtest.drivers.instance;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.s3.AmazonS3;
import org.apache.hadoop.conf.Configuration;

import sleeper.clients.status.update.AddTable;
import sleeper.configuration.properties.S3TableProperties;
import sleeper.configuration.table.index.DynamoDBTableIndex;
import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.table.TableProperties;
import sleeper.core.properties.table.TablePropertiesProvider;
import sleeper.core.properties.table.TablePropertiesStore;
import sleeper.core.statestore.StateStoreProvider;
import sleeper.core.table.TableIndex;
import sleeper.statestore.StateStoreFactory;
import sleeper.systemtest.drivers.util.SystemTestClients;
import sleeper.systemtest.dsl.instance.SleeperTablesDriver;

import java.io.IOException;
import java.io.UncheckedIOException;

public class AwsSleeperTablesDriver implements SleeperTablesDriver {

    private final AmazonS3 s3;
    private final AmazonDynamoDB dynamoDB;
    private final Configuration hadoopConfiguration;

    public AwsSleeperTablesDriver(SystemTestClients clients) {
        this.s3 = clients.getS3();
        this.dynamoDB = clients.getDynamoDB();
        this.hadoopConfiguration = clients.createHadoopConf();
    }

    public void saveTableProperties(InstanceProperties instanceProperties, TableProperties tableProperties) {
        tablePropertiesStore(instanceProperties).save(tableProperties);
    }

    public void addTable(InstanceProperties instanceProperties, TableProperties properties) {
        try {
            new AddTable(s3, dynamoDB, instanceProperties, properties, hadoopConfiguration).run();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    public TablePropertiesProvider createTablePropertiesProvider(InstanceProperties instanceProperties) {
        return S3TableProperties.createProvider(instanceProperties, s3, dynamoDB);
    }

    public StateStoreProvider createStateStoreProvider(InstanceProperties instanceProperties) {
        return StateStoreFactory.createProvider(instanceProperties, s3, dynamoDB, hadoopConfiguration);
    }

    public TableIndex tableIndex(InstanceProperties instanceProperties) {
        return new DynamoDBTableIndex(instanceProperties, dynamoDB);
    }

    private TablePropertiesStore tablePropertiesStore(InstanceProperties instanceProperties) {
        return S3TableProperties.createStore(instanceProperties, s3, dynamoDB);
    }
}
