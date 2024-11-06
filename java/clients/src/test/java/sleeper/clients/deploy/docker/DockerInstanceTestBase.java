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

package sleeper.clients.deploy.docker;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClientBuilder;
import org.apache.hadoop.conf.Configuration;
import org.testcontainers.containers.localstack.LocalStackContainer;
import org.testcontainers.utility.DockerImageName;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.awscore.client.builder.AwsClientBuilder;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.Message;

import sleeper.clients.docker.DeployDockerInstance;
import sleeper.configuration.jars.ObjectFactory;
import sleeper.core.CommonTestConstants;
import sleeper.core.iterator.CloseableIterator;
import sleeper.core.partition.PartitionTree;
import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.table.TableProperties;
import sleeper.core.record.Record;
import sleeper.core.statestore.StateStore;
import sleeper.ingest.core.job.IngestJob;
import sleeper.ingest.core.job.IngestJobSerDe;
import sleeper.query.model.Query;
import sleeper.query.runner.recordretrieval.QueryExecutor;
import sleeper.statestore.StateStoreFactory;

import java.util.List;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.function.Consumer;

import static sleeper.configuration.testutils.LocalStackAwsV1ClientHelper.buildAwsV1Client;
import static sleeper.core.properties.table.TableProperty.TABLE_NAME;

public class DockerInstanceTestBase {
    public static LocalStackContainer localStackContainer = new LocalStackContainer(DockerImageName.parse(CommonTestConstants.LOCALSTACK_DOCKER_IMAGE_V2))
            .withServices(LocalStackContainer.Service.S3, LocalStackContainer.Service.DYNAMODB, LocalStackContainer.Service.SQS);
    protected final AmazonS3 s3Client = buildAwsV1Client(
            localStackContainer, LocalStackContainer.Service.S3, AmazonS3ClientBuilder.standard());
    protected final AmazonDynamoDB dynamoDB = buildAwsV1Client(
            localStackContainer, LocalStackContainer.Service.DYNAMODB, AmazonDynamoDBClientBuilder.standard());
    protected final SqsClient sqsClient = buildAwsV2Client(localStackContainer, LocalStackContainer.Service.SQS, SqsClient.builder());
    protected final AmazonSQS sqsClientV1 = buildAwsV1Client(
            localStackContainer, LocalStackContainer.Service.SQS, AmazonSQSClientBuilder.standard());

    static {
        localStackContainer.start();
    }

    public void deployInstance(String instanceId) {
        deployInstance(instanceId, tableProperties -> {
        });
    }

    public void deployInstance(String instanceId, Consumer<TableProperties> extraProperties) {
        DeployDockerInstance.builder().s3Client(s3Client).dynamoDB(dynamoDB).sqsClient(sqsClient)
                .configuration(getHadoopConfiguration()).extraTableProperties(extraProperties)
                .build().deploy(instanceId);
    }

    public CloseableIterator<Record> queryAllRecords(
            InstanceProperties instanceProperties, TableProperties tableProperties) throws Exception {
        StateStore stateStore = new StateStoreFactory(instanceProperties, s3Client, dynamoDB, getHadoopConfiguration())
                .getStateStore(tableProperties);
        PartitionTree tree = new PartitionTree(stateStore.getAllPartitions());
        QueryExecutor executor = new QueryExecutor(ObjectFactory.noUserJars(), tableProperties,
                stateStore, getHadoopConfiguration(), Executors.newSingleThreadExecutor());
        executor.init(tree.getAllPartitions(), stateStore.getPartitionToReferencedFilesMap());
        return executor.execute(createQueryAllRecords(tree, tableProperties.get(TABLE_NAME)));
    }

    protected IngestJob receiveIngestJob(String queueUrl) {
        List<Message> messages = sqsClient.receiveMessage(request -> request.queueUrl(queueUrl)).messages();
        if (messages.size() != 1) {
            throw new IllegalStateException("Expected to receive one message, found: " + messages);
        }
        String json = messages.get(0).body();
        return new IngestJobSerDe().fromJson(json);
    }

    protected IngestJob receiveIngestJobV1(String queueUrl) {
        List<com.amazonaws.services.sqs.model.Message> messages = sqsClientV1.receiveMessage(queueUrl).getMessages();
        if (messages.size() != 1) {
            throw new IllegalStateException("Expected to receive one message, found: " + messages);
        }
        String json = messages.get(0).getBody();
        return new IngestJobSerDe().fromJson(json);
    }

    private static Query createQueryAllRecords(PartitionTree tree, String tableName) {
        return Query.builder()
                .tableName(tableName)
                .queryId(UUID.randomUUID().toString())
                .regions(List.of(tree.getRootPartition().getRegion()))
                .build();
    }

    public Configuration getHadoopConfiguration() {
        Configuration configuration = new Configuration();
        configuration.setClassLoader(this.getClass().getClassLoader());
        configuration.set("fs.s3a.endpoint", localStackContainer.getEndpointOverride(LocalStackContainer.Service.S3).toString());
        configuration.set("fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider");
        configuration.set("fs.s3a.access.key", localStackContainer.getAccessKey());
        configuration.set("fs.s3a.secret.key", localStackContainer.getSecretKey());
        configuration.setBoolean("fs.s3a.connection.ssl.enabled", false);
        return configuration;
    }

    private static <B extends AwsClientBuilder<B, T>, T> T buildAwsV2Client(LocalStackContainer localStackContainer, LocalStackContainer.Service service, B builder) {
        return builder
                .endpointOverride(localStackContainer.getEndpointOverride(service))
                .credentialsProvider(StaticCredentialsProvider.create(AwsBasicCredentials.create(
                        localStackContainer.getAccessKey(), localStackContainer.getSecretKey())))
                .region(Region.of(localStackContainer.getRegion()))
                .build();
    }
}
