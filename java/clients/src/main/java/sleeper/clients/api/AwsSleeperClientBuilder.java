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
package sleeper.clients.api;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClientBuilder;
import org.apache.hadoop.conf.Configuration;

import sleeper.configuration.properties.S3InstanceProperties;
import sleeper.configuration.properties.S3TableProperties;
import sleeper.configuration.table.index.DynamoDBTableIndex;
import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.table.TableIndex;
import sleeper.core.util.ObjectFactory;
import sleeper.parquet.utils.HadoopConfigurationProvider;
import sleeper.query.runner.recordretrieval.LeafPartitionRecordRetrieverImpl;
import sleeper.statestore.StateStoreFactory;

import java.util.Objects;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static sleeper.configuration.utils.AwsV1ClientHelper.buildAwsV1Client;

public class AwsSleeperClientBuilder {

    private String instanceId;
    private int queryThreadPoolSize = 10;
    private AmazonS3 s3Client;
    private AmazonDynamoDB dynamoClient;
    private AmazonSQS sqsClient;
    private Configuration hadoopConf;

    public AwsSleeperClientBuilder defaultClients() {
        return s3Client(buildAwsV1Client(AmazonS3ClientBuilder.standard()))
                .dynamoClient(buildAwsV1Client(AmazonDynamoDBClientBuilder.standard()))
                .sqsClient(buildAwsV1Client(AmazonSQSClientBuilder.standard()))
                .hadoopConf(HadoopConfigurationProvider.getConfigurationForClient());
    }

    public SleeperClient build() {
        Objects.requireNonNull(instanceId, "instanceId must not be null");
        Objects.requireNonNull(s3Client, "s3Client must not be null");
        Objects.requireNonNull(dynamoClient, "dynamoClient must not be null");
        Objects.requireNonNull(sqsClient, "sqsClient must not be null");
        Objects.requireNonNull(hadoopConf, "hadoopConf must not be null");

        ExecutorService executorService = Executors.newFixedThreadPool(queryThreadPoolSize);
        InstanceProperties instanceProperties = S3InstanceProperties.loadGivenInstanceId(s3Client, instanceId);
        TableIndex tableIndex = new DynamoDBTableIndex(instanceProperties, dynamoClient);

        return new SleeperClient.Builder()
                .instanceProperties(instanceProperties)
                .tableIndex(tableIndex)
                .tablePropertiesProvider(S3TableProperties.createProvider(instanceProperties, tableIndex, s3Client))
                .tablePropertiesStore(S3TableProperties.createStore(instanceProperties, s3Client, dynamoClient))
                .stateStoreProvider(StateStoreFactory.createProvider(instanceProperties, s3Client, dynamoClient, hadoopConf))
                .objectFactory(ObjectFactory.noUserJars())
                .recordRetrieverProvider(LeafPartitionRecordRetrieverImpl.createProvider(executorService, hadoopConf))
                .ingestJobSender(SleeperClientIngest.ingestParquetFilesFromS3(instanceProperties, sqsClient))
                .bulkImportJobSender(SleeperClientBulkImport.bulkImportParquetFilesFromS3(instanceProperties, sqsClient))
                .build();
    }

    public AwsSleeperClientBuilder instanceId(String instanceId) {
        this.instanceId = instanceId;
        return this;
    }

    public AwsSleeperClientBuilder queryThreadPoolSize(int queryThreadPoolSize) {
        this.queryThreadPoolSize = queryThreadPoolSize;
        return this;
    }

    public AwsSleeperClientBuilder s3Client(AmazonS3 s3Client) {
        this.s3Client = s3Client;
        return this;
    }

    public AwsSleeperClientBuilder dynamoClient(AmazonDynamoDB dynamoClient) {
        this.dynamoClient = dynamoClient;
        return this;
    }

    public AwsSleeperClientBuilder sqsClient(AmazonSQS sqsClient) {
        this.sqsClient = sqsClient;
        return this;
    }

    public AwsSleeperClientBuilder hadoopConf(Configuration hadoopConf) {
        this.hadoopConf = hadoopConf;
        return this;
    }

}
