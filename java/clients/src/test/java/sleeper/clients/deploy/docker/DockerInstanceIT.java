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

package sleeper.clients.deploy.docker;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.HeadBucketRequest;
import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.localstack.LocalStackContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import sleeper.clients.docker.DeployDockerInstance;
import sleeper.clients.docker.TearDownDockerInstance;
import sleeper.configuration.jars.ObjectFactory;
import sleeper.configuration.properties.instance.InstanceProperties;
import sleeper.configuration.properties.table.TableProperties;
import sleeper.core.CommonTestConstants;
import sleeper.core.iterator.CloseableIterator;
import sleeper.core.partition.PartitionTree;
import sleeper.core.record.Record;
import sleeper.core.statestore.StateStore;
import sleeper.query.executor.QueryExecutor;
import sleeper.query.model.Query;
import sleeper.statestore.StateStoreProvider;

import java.util.List;
import java.util.UUID;
import java.util.concurrent.Executors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static sleeper.configuration.properties.instance.SystemDefinedInstanceProperty.CONFIG_BUCKET;
import static sleeper.configuration.properties.table.TableProperty.TABLE_NAME;
import static sleeper.configuration.testutils.LocalStackAwsV1ClientHelper.buildAwsV1Client;

@Testcontainers
public class DockerInstanceIT {
    @Container
    public static LocalStackContainer localStackContainer = new LocalStackContainer(DockerImageName.parse(CommonTestConstants.LOCALSTACK_DOCKER_IMAGE))
            .withServices(LocalStackContainer.Service.S3, LocalStackContainer.Service.DYNAMODB);
    private final AmazonS3 s3Client = buildAwsV1Client(
            localStackContainer, LocalStackContainer.Service.S3, AmazonS3ClientBuilder.standard());
    private final AmazonDynamoDB dynamoDB = buildAwsV1Client(
            localStackContainer, LocalStackContainer.Service.DYNAMODB, AmazonDynamoDBClientBuilder.standard());

    @Test
    void shouldDeployInstance() throws Exception {
        // Given
        String instanceId = "test-instance";

        // When
        DeployDockerInstance.deploy(instanceId, s3Client, dynamoDB);

        // Then
        InstanceProperties instanceProperties = new InstanceProperties();
        instanceProperties.loadFromS3GivenInstanceId(s3Client, "test-instance");
        TableProperties tableProperties = new TableProperties(instanceProperties);
        tableProperties.loadFromS3(s3Client, "system-test");
        assertThat(queryAllRecords(instanceProperties, tableProperties)).isExhausted();
    }

    @Test
    void shouldTearDownInstance() throws Exception {
        // Given
        DeployDockerInstance.deploy("test-instance-2", s3Client, dynamoDB);
        InstanceProperties instanceProperties = new InstanceProperties();
        instanceProperties.loadFromS3GivenInstanceId(s3Client, "test-instance-2");
        TableProperties tableProperties = new TableProperties(instanceProperties);
        tableProperties.loadFromS3(s3Client, "system-test");

        // When
        TearDownDockerInstance.tearDown("test-instance-2", s3Client, dynamoDB);

        // Then
        assertThatThrownBy(() -> s3Client.headBucket(new HeadBucketRequest(instanceProperties.get(CONFIG_BUCKET))))
                .isInstanceOf(AmazonServiceException.class);
    }

    private CloseableIterator<Record> queryAllRecords(
            InstanceProperties instanceProperties, TableProperties tableProperties) throws Exception {
        StateStore stateStore = new StateStoreProvider(dynamoDB, instanceProperties).getStateStore(tableProperties);
        PartitionTree tree = new PartitionTree(tableProperties.getSchema(), stateStore.getAllPartitions());
        QueryExecutor executor = new QueryExecutor(ObjectFactory.noUserJars(), tableProperties,
                stateStore, new Configuration(), Executors.newSingleThreadExecutor());
        executor.init(tree.getAllPartitions(), stateStore.getPartitionToActiveFilesMap());
        return executor.execute(createQueryAllRecords(tree, tableProperties.get(TABLE_NAME)));
    }

    private static Query createQueryAllRecords(PartitionTree tree, String tableName) {
        return new Query.Builder(tableName,
                UUID.randomUUID().toString(),
                List.of(tree.getRootPartition().getRegion())).build();
    }
}
