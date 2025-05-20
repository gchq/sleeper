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

import com.amazonaws.services.s3.AmazonS3;
import org.apache.hadoop.conf.Configuration;

import sleeper.clients.api.BulkImportJobSender;
import sleeper.clients.api.IngestBatcherSender;
import sleeper.clients.api.IngestJobSender;
import sleeper.clients.api.SleeperClient;
import sleeper.clients.util.UncheckedAutoCloseables;
import sleeper.configuration.properties.S3InstanceProperties;
import sleeper.configuration.properties.S3TableProperties;
import sleeper.configuration.table.index.DynamoDBTableIndex;
import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.table.TableIndex;
import sleeper.core.util.ObjectFactory;
import sleeper.query.core.recordretrieval.LeafPartitionRecordRetrieverProvider;
import sleeper.statestore.StateStoreFactory;

import java.util.List;
import java.util.Objects;
import java.util.function.Consumer;

/**
 * Builds Sleeper clients to interact with AWS. This will usually be created from {@link SleeperClient#builder}.
 */
public class AwsSleeperClientBuilder {

    private String instanceId;
    private InstanceProperties instanceProperties;
    private SleeperClientAwsClientsProvider awsProvider = SleeperClientAwsClientsProvider.getDefault();
    private SleeperClientHadoopProvider hadoopProvider = SleeperClientHadoopProvider.getDefault();
    private SleeperClientHadoopQueryProvider queryProvider = SleeperClientHadoopQueryProvider.getDefault();

    /**
     * Creates a Sleeper client.
     *
     * @return the client
     */
    public SleeperClient build() {
        SleeperClientAwsClients awsClients = awsProvider.getAwsClients();
        InstanceProperties instanceProperties = loadInstanceProperties(awsClients.s3());
        Configuration hadoopConf = hadoopProvider.getConfiguration(instanceProperties);
        ShutdownWrapper<LeafPartitionRecordRetrieverProvider> recordRetrieverProvider = queryProvider.getRecordRetrieverProvider(hadoopConf);
        TableIndex tableIndex = new DynamoDBTableIndex(instanceProperties, awsClients.dynamo());

        return new SleeperClient.Builder()
                .instanceProperties(instanceProperties)
                .tableIndex(tableIndex)
                .tablePropertiesProvider(S3TableProperties.createProvider(instanceProperties, tableIndex, awsClients.s3()))
                .tablePropertiesStore(S3TableProperties.createStore(instanceProperties, awsClients.s3(), awsClients.dynamo()))
                .stateStoreProvider(StateStoreFactory.createProvider(instanceProperties, awsClients.s3(), awsClients.dynamo(), hadoopConf))
                .objectFactory(ObjectFactory.noUserJars())
                .recordRetrieverProvider(recordRetrieverProvider.get())
                .ingestJobSender(IngestJobSender.toSqs(instanceProperties, awsClients.sqs()))
                .bulkImportJobSender(BulkImportJobSender.toSqs(instanceProperties, awsClients.sqs()))
                .ingestBatcherSender(IngestBatcherSender.toSqs(instanceProperties, awsClients.sqs()))
                .shutdown(new UncheckedAutoCloseables(List.of(awsClients, recordRetrieverProvider)))
                .build();
    }

    private InstanceProperties loadInstanceProperties(AmazonS3 s3Client) {
        if (instanceProperties != null) {
            return instanceProperties;
        }
        Objects.requireNonNull(instanceId, "instanceId must not be null");
        return S3InstanceProperties.loadGivenInstanceId(s3Client, instanceId);
    }

    /**
     * Sets the ID of the Sleeper instance to interact with.
     *
     * @param  instanceId the instance ID
     * @return            this builder
     */
    public AwsSleeperClientBuilder instanceId(String instanceId) {
        this.instanceId = instanceId;
        return this;
    }

    /**
     * Sets the properties of the Sleeper instance to interact with. This may be set instead of the instance ID if the
     * properties have already been loaded. Usually this is not necessary.
     *
     * @param  instanceProperties the instance properties
     * @return                    this builder
     */
    public AwsSleeperClientBuilder instanceProperties(InstanceProperties instanceProperties) {
        this.instanceProperties = instanceProperties;
        return this;
    }

    /**
     * Sets the number of threads in the thread pool used to read data files in parallel during queries.
     *
     * @param  queryThreadPoolSize the number of threads
     * @return                     this builder
     */
    public AwsSleeperClientBuilder queryThreadPoolSize(int queryThreadPoolSize) {
        this.queryProvider = SleeperClientHadoopQueryProvider.withThreadPoolSize(queryThreadPoolSize);
        return this;
    }

    /**
     * Sets the clients to interact with AWS.
     *
     * @param  clientsConfig configuration to set the clients
     * @return               this builder
     */
    public AwsSleeperClientBuilder awsClients(Consumer<SleeperClientAwsClients.Builder> clientsConfig) {
        return awsProvider(SleeperClientAwsClientsProvider.withConfig(clientsConfig));
    }

    /**
     * Sets the provider of clients to interact with AWS.
     *
     * @param  awsProvider the provider
     * @return             this builder
     */
    public AwsSleeperClientBuilder awsProvider(SleeperClientAwsClientsProvider awsProvider) {
        this.awsProvider = awsProvider;
        return this;
    }

    /**
     * Sets the Hadoop configuration.
     *
     * @param  hadoopConf the configuration
     * @return            this builder
     */
    public AwsSleeperClientBuilder hadoopConf(Configuration hadoopConf) {
        return hadoopProvider(SleeperClientHadoopProvider.withConfig(hadoopConf));
    }

    /**
     * Sets a provider of the Hadoop configuration.
     *
     * @param  hadoopProvider the provider
     * @return                this builder
     */
    public AwsSleeperClientBuilder hadoopProvider(SleeperClientHadoopProvider hadoopProvider) {
        this.hadoopProvider = hadoopProvider;
        return this;
    }

}
