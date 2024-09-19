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
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import sleeper.clients.docker.DeployDockerInstance;
import sleeper.configuration.jars.ObjectFactory;
import sleeper.configuration.properties.instance.InstanceProperties;
import sleeper.configuration.properties.table.TableProperties;
import sleeper.core.CommonTestConstants;
import sleeper.core.iterator.CloseableIterator;
import sleeper.core.partition.PartitionTree;
import sleeper.core.record.Record;
import sleeper.core.statestore.StateStore;
import sleeper.query.model.Query;
import sleeper.query.runner.recordretrieval.QueryExecutor;
import sleeper.statestore.StateStoreFactory;

import java.util.List;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.function.Consumer;

import static sleeper.configuration.properties.table.TableProperty.TABLE_NAME;
import static sleeper.configuration.testutils.LocalStackAwsV1ClientHelper.buildAwsV1Client;

@Testcontainers
public class DockerInstanceTestBase {
    @Container
    public static LocalStackContainer localStackContainer = new LocalStackContainer(DockerImageName.parse(CommonTestConstants.LOCALSTACK_DOCKER_IMAGE))
            .withServices(LocalStackContainer.Service.S3, LocalStackContainer.Service.DYNAMODB, LocalStackContainer.Service.SQS);
    protected final AmazonS3 s3Client = buildAwsV1Client(
            localStackContainer, LocalStackContainer.Service.S3, AmazonS3ClientBuilder.standard());
    protected final AmazonDynamoDB dynamoDB = buildAwsV1Client(
            localStackContainer, LocalStackContainer.Service.DYNAMODB, AmazonDynamoDBClientBuilder.standard());
    protected final AmazonSQS sqsClient = buildAwsV1Client(
            localStackContainer, LocalStackContainer.Service.SQS, AmazonSQSClientBuilder.standard());

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
}
