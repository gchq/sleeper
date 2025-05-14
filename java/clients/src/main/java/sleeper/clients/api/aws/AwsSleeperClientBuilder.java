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

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClientBuilder;
import org.apache.hadoop.conf.Configuration;

import sleeper.clients.api.BulkImportJobSender;
import sleeper.clients.api.IngestBatcherSender;
import sleeper.clients.api.IngestJobSender;
import sleeper.clients.api.SleeperClient;
import sleeper.configuration.properties.S3InstanceProperties;
import sleeper.configuration.properties.S3TableProperties;
import sleeper.configuration.table.index.DynamoDBTableIndex;
import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.table.TableIndex;
import sleeper.core.util.ObjectFactory;
import sleeper.parquet.utils.HadoopConfigurationProvider;
import sleeper.query.runner.recordretrieval.LeafPartitionRecordRetrieverImpl;
import sleeper.statestore.StateStoreFactory;

import java.util.List;
import java.util.Objects;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static sleeper.configuration.utils.AwsV1ClientHelper.buildAwsV1Client;

/**
 * Builds Sleeper clients to interact with AWS. This will usually be created from {@link SleeperClient#builder}.
 */
public class AwsSleeperClientBuilder {

    private String instanceId;
    private InstanceProperties instanceProperties;
    private int queryThreadPoolSize = 10;
    private AwsClientShutdown<AmazonS3> s3ClientWrapper;
    private AwsClientShutdown<AmazonDynamoDB> dynamoClientWrapper;
    private AwsClientShutdown<AmazonSQS> sqsClientWrapper;
    private Configuration hadoopConf;

    /**
     * Creates default clients to interact with AWS. This is done by default in {@link SleeperClient#builder}.
     *
     * @return this builder
     */
    public AwsSleeperClientBuilder defaultClients() {
        s3ClientWrapper = AwsClientShutdown.shutdown(buildAwsV1Client(AmazonS3ClientBuilder.standard()), AmazonS3::shutdown);
        dynamoClientWrapper = AwsClientShutdown.shutdown(buildAwsV1Client(AmazonDynamoDBClientBuilder.standard()), AmazonDynamoDB::shutdown);
        sqsClientWrapper = AwsClientShutdown.shutdown(buildAwsV1Client(AmazonSQSClientBuilder.standard()), AmazonSQS::shutdown);
        hadoopConf = HadoopConfigurationProvider.getConfigurationForClient();
        return this;
    }

    /**
     * Creates a Sleeper client.
     *
     * @return the client
     */
    public SleeperClient build() {
        AwsClientShutdown<AmazonS3> s3ClientWrapper = Objects.requireNonNull(this.s3ClientWrapper, "s3Client must not be null");
        AwsClientShutdown<AmazonDynamoDB> dynamoClientWrapper = Objects.requireNonNull(this.dynamoClientWrapper, "dynamoClient must not be null");
        AwsClientShutdown<AmazonSQS> sqsClientWrapper = Objects.requireNonNull(this.sqsClientWrapper, "sqsClient must not be null");
        AmazonS3 s3Client = Objects.requireNonNull(s3ClientWrapper.getClient(), "s3Client must not be null");
        AmazonDynamoDB dynamoClient = Objects.requireNonNull(dynamoClientWrapper.getClient(), "dynamoClient must not be null");
        AmazonSQS sqsClient = Objects.requireNonNull(sqsClientWrapper.getClient(), "sqsClient must not be null");
        Objects.requireNonNull(hadoopConf, "hadoopConf must not be null");

        ExecutorService executorService = Executors.newFixedThreadPool(queryThreadPoolSize);
        InstanceProperties instanceProperties = loadInstanceProperties(s3Client);
        TableIndex tableIndex = new DynamoDBTableIndex(instanceProperties, dynamoClient);

        return new SleeperClient.Builder()
                .instanceProperties(instanceProperties)
                .tableIndex(tableIndex)
                .tablePropertiesProvider(S3TableProperties.createProvider(instanceProperties, tableIndex, s3Client))
                .tablePropertiesStore(S3TableProperties.createStore(instanceProperties, s3Client, dynamoClient))
                .stateStoreProvider(StateStoreFactory.createProvider(instanceProperties, s3Client, dynamoClient, hadoopConf))
                .objectFactory(ObjectFactory.noUserJars())
                .recordRetrieverProvider(LeafPartitionRecordRetrieverImpl.createProvider(executorService, hadoopConf))
                .ingestJobSender(IngestJobSender.toSqs(instanceProperties, sqsClient))
                .bulkImportJobSender(BulkImportJobSender.toSqs(instanceProperties, sqsClient))
                .ingestBatcherSender(IngestBatcherSender.toSqs(instanceProperties, sqsClient))
                .shutdown(new AwsSleeperClientShutdown(executorService, List.of(s3ClientWrapper, dynamoClientWrapper, sqsClientWrapper)))
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
        this.queryThreadPoolSize = queryThreadPoolSize;
        return this;
    }

    /**
     * Sets the AWS client to interact with S3.
     *
     * @param  s3Client the client
     * @return          this builder
     */
    public AwsSleeperClientBuilder s3Client(AmazonS3 s3Client) {
        this.s3ClientWrapper = AwsClientShutdown.noShutdown(s3Client);
        return this;
    }

    /**
     * Sets the AWS client to interact with DynamoDB.
     *
     * @param  dynamoClient the client
     * @return              this builder
     */
    public AwsSleeperClientBuilder dynamoClient(AmazonDynamoDB dynamoClient) {
        this.dynamoClientWrapper = AwsClientShutdown.noShutdown(dynamoClient);
        return this;
    }

    /**
     * Sets the AWS client to interact with SQS.
     *
     * @param  sqsClient the client
     * @return           this builder
     */
    public AwsSleeperClientBuilder sqsClient(AmazonSQS sqsClient) {
        this.sqsClientWrapper = AwsClientShutdown.noShutdown(sqsClient);
        return this;
    }

    /**
     * Sets the Hadoop configuration.
     *
     * @param  hadoopConf the configuration
     * @return            this builder
     */
    public AwsSleeperClientBuilder hadoopConf(Configuration hadoopConf) {
        this.hadoopConf = hadoopConf;
        return this;
    }

}
