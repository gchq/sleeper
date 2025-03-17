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
package sleeper.clients;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.s3.AmazonS3;
import org.apache.hadoop.conf.Configuration;

import sleeper.configuration.jars.S3UserJarsLoader;
import sleeper.configuration.properties.S3InstanceProperties;
import sleeper.configuration.properties.S3TableProperties;
import sleeper.configuration.table.index.DynamoDBTableIndex;
import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.table.TableProperties;
import sleeper.core.properties.table.TablePropertiesProvider;
import sleeper.core.statestore.StateStore;
import sleeper.core.statestore.StateStoreProvider;
import sleeper.core.table.TableIndex;
import sleeper.core.table.TableStatus;
import sleeper.core.util.ObjectFactory;
import sleeper.core.util.ObjectFactoryException;
import sleeper.query.core.recordretrieval.LeafPartitionRecordRetriever;
import sleeper.query.core.recordretrieval.LeafPartitionRecordRetrieverProvider;
import sleeper.query.core.recordretrieval.QueryExecutor;
import sleeper.query.runner.recordretrieval.LeafPartitionRecordRetrieverImpl;
import sleeper.statestore.StateStoreFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Stream;

public class AwsSleeperClient {

    private final InstanceProperties instanceProperties;
    private final TableIndex tableIndex;
    private final TablePropertiesProvider tablePropertiesProvider;
    private final StateStoreProvider stateStoreProvider;
    private final ObjectFactory objectFactory;
    private final LeafPartitionRecordRetrieverProvider recordRetrieverProvider;

    private AwsSleeperClient(Builder builder) {
        instanceProperties = builder.instanceProperties;
        tableIndex = builder.tableIndex;
        tablePropertiesProvider = builder.tablePropertiesProvider;
        stateStoreProvider = builder.stateStoreProvider;
        objectFactory = builder.objectFactory;
        recordRetrieverProvider = builder.recordRetrieverProvider;
    }

    public static AwsSleeperClient byInstanceId(AmazonS3 s3Client, AmazonDynamoDB dynamoClient, Configuration hadoopConf, String instanceId) throws IOException, ObjectFactoryException {
        InstanceProperties instanceProperties = S3InstanceProperties.loadGivenInstanceId(s3Client, instanceId);
        TableIndex tableIndex = new DynamoDBTableIndex(instanceProperties, dynamoClient);
        String userJarsDir = Files.createTempDirectory("sleeper-user-jars").toString();
        ExecutorService queryExecutorService = Executors.newFixedThreadPool(10);
        return builder()
                .instanceProperties(instanceProperties)
                .tableIndex(tableIndex)
                .tablePropertiesProvider(S3TableProperties.createProvider(instanceProperties, tableIndex, s3Client))
                .stateStoreProvider(StateStoreFactory.createProvider(instanceProperties, s3Client, dynamoClient, hadoopConf))
                .objectFactory(new S3UserJarsLoader(instanceProperties, s3Client, userJarsDir).buildObjectFactory())
                .recordRetrieverProvider(LeafPartitionRecordRetrieverImpl.createProvider(queryExecutorService, hadoopConf))
                .build();
    }

    public static Builder builder() {
        return new Builder();
    }

    public InstanceProperties getInstanceProperties() {
        return instanceProperties;
    }

    public Stream<TableStatus> streamAllTables() {
        return tableIndex.streamAllTables();
    }

    public TableProperties getTableProperties(TableStatus tableStatus) {
        return tablePropertiesProvider.get(tableStatus);
    }

    public TableProperties getTableProperties(String tableName) {
        return tablePropertiesProvider.getByName(tableName);
    }

    public StateStore getStateStore(String tableName) {
        return stateStoreProvider.getStateStore(getTableProperties(tableName));
    }

    public QueryExecutor getQueryExecutor(String tableName) {
        TableProperties tableProperties = getTableProperties(tableName);
        StateStore stateStore = stateStoreProvider.getStateStore(tableProperties);
        LeafPartitionRecordRetriever recordRetriever = recordRetrieverProvider.getRecordRetriever(tableProperties);
        QueryExecutor executor = new QueryExecutor(objectFactory, tableProperties, stateStore, recordRetriever);
        executor.init();
        return executor;
    }

    public static class Builder {
        private InstanceProperties instanceProperties;
        private TableIndex tableIndex;
        private TablePropertiesProvider tablePropertiesProvider;
        private StateStoreProvider stateStoreProvider;
        private ObjectFactory objectFactory = ObjectFactory.noUserJars();
        private LeafPartitionRecordRetrieverProvider recordRetrieverProvider;

        public Builder instanceProperties(InstanceProperties instanceProperties) {
            this.instanceProperties = instanceProperties;
            return this;
        }

        public Builder tableIndex(TableIndex tableIndex) {
            this.tableIndex = tableIndex;
            return this;
        }

        public Builder tablePropertiesProvider(TablePropertiesProvider tablePropertiesProvider) {
            this.tablePropertiesProvider = tablePropertiesProvider;
            return this;
        }

        public Builder stateStoreProvider(StateStoreProvider stateStoreProvider) {
            this.stateStoreProvider = stateStoreProvider;
            return this;
        }

        public Builder objectFactory(ObjectFactory objectFactory) {
            this.objectFactory = objectFactory;
            return this;
        }

        public Builder recordRetrieverProvider(LeafPartitionRecordRetrieverProvider recordRetrieverProvider) {
            this.recordRetrieverProvider = recordRetrieverProvider;
            return this;
        }

        public AwsSleeperClient build() {
            return new AwsSleeperClient(this);
        }
    }

}
