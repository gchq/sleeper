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
package sleeper.clients.api.aws;

import org.apache.hadoop.conf.Configuration;

import sleeper.clients.api.BulkImportJobSender;
import sleeper.clients.api.IngestBatcherSender;
import sleeper.clients.api.IngestJobSender;
import sleeper.clients.api.SleeperClient;
import sleeper.clients.api.SleeperClientFactory;
import sleeper.configuration.properties.S3InstanceProperties;
import sleeper.configuration.properties.S3TableProperties;
import sleeper.configuration.table.index.DynamoDBTableIndex;
import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.table.TableIndex;
import sleeper.core.util.ObjectFactory;
import sleeper.query.runner.recordretrieval.LeafPartitionRecordRetrieverImpl;
import sleeper.statestore.StateStoreFactory;

import java.util.concurrent.ExecutorService;

/**
 * Creates Sleeper clients to interact with AWS.
 */
public class AwsSleeperClientFactory implements SleeperClientFactory {

    private final ShutdownWrapper<ExecutorService> queryExecutorService;
    private final SleeperClientAwsClients awsClients;
    private final Configuration hadoopConf;

    private AwsSleeperClientFactory(
            ShutdownWrapper<ExecutorService> queryExecutorService,
            SleeperClientAwsClients awsClients,
            Configuration hadoopConf) {
        this.queryExecutorService = queryExecutorService;
        this.awsClients = awsClients;
        this.hadoopConf = hadoopConf;
    }

    @Override
    public SleeperClient createClientForInstance(String instanceId) {
        return createClientForInstance(S3InstanceProperties.loadGivenInstanceId(awsClients.s3(), instanceId));
    }

    @Override
    public SleeperClient createClientForInstance(InstanceProperties instanceProperties) {
        TableIndex tableIndex = new DynamoDBTableIndex(instanceProperties, awsClients.dynamo());

        return new SleeperClient.Builder()
                .instanceProperties(instanceProperties)
                .tableIndex(tableIndex)
                .tablePropertiesProvider(S3TableProperties.createProvider(instanceProperties, tableIndex, awsClients.s3()))
                .tablePropertiesStore(S3TableProperties.createStore(instanceProperties, awsClients.s3(), awsClients.dynamo()))
                .stateStoreProvider(StateStoreFactory.createProvider(instanceProperties, awsClients.s3(), awsClients.dynamo(), hadoopConf))
                .objectFactory(ObjectFactory.noUserJars())
                .recordRetrieverProvider(LeafPartitionRecordRetrieverImpl.createProvider(queryExecutorService.get(), hadoopConf))
                .ingestJobSender(IngestJobSender.toSqs(instanceProperties, awsClients.sqs()))
                .bulkImportJobSender(BulkImportJobSender.toSqs(instanceProperties, awsClients.sqs()))
                .ingestBatcherSender(IngestBatcherSender.toSqs(instanceProperties, awsClients.sqs()))
                .build();
    }

}
