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
import com.amazonaws.services.dynamodbv2.model.ResourceNotFoundException;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.HeadBucketRequest;
import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.testcontainers.containers.localstack.LocalStackContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;
import software.amazon.awssdk.services.s3.S3AsyncClient;

import sleeper.clients.docker.DeployDockerInstance;
import sleeper.clients.docker.TearDownDockerInstance;
import sleeper.configuration.jars.ObjectFactory;
import sleeper.configuration.properties.instance.InstanceProperties;
import sleeper.configuration.properties.table.TableProperties;
import sleeper.core.CommonTestConstants;
import sleeper.core.iterator.CloseableIterator;
import sleeper.core.iterator.WrappedIterator;
import sleeper.core.partition.PartitionTree;
import sleeper.core.record.Record;
import sleeper.core.statestore.StateStore;
import sleeper.ingest.IngestFactory;
import sleeper.query.executor.QueryExecutor;
import sleeper.query.model.Query;
import sleeper.statestore.StateStoreProvider;

import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Executors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static sleeper.configuration.properties.instance.SystemDefinedInstanceProperty.CONFIG_BUCKET;
import static sleeper.configuration.properties.table.TableProperty.ACTIVE_FILEINFO_TABLENAME;
import static sleeper.configuration.properties.table.TableProperty.DATA_BUCKET;
import static sleeper.configuration.properties.table.TableProperty.PARTITION_TABLENAME;
import static sleeper.configuration.properties.table.TableProperty.READY_FOR_GC_FILEINFO_TABLENAME;
import static sleeper.configuration.properties.table.TableProperty.TABLE_NAME;
import static sleeper.configuration.testutils.LocalStackAwsV1ClientHelper.buildAwsV1Client;
import static sleeper.ingest.testutils.LocalStackAwsV2ClientHelper.buildAwsV2Client;

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
        // Given / When
        DeployDockerInstance.deploy("test-instance", s3Client, dynamoDB);

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
        assertThatThrownBy(() -> s3Client.headBucket(new HeadBucketRequest(tableProperties.get(DATA_BUCKET))))
                .isInstanceOf(AmazonServiceException.class);
        assertThatThrownBy(() -> dynamoDB.describeTable(tableProperties.get(ACTIVE_FILEINFO_TABLENAME)))
                .isInstanceOf(ResourceNotFoundException.class);
        assertThatThrownBy(() -> dynamoDB.describeTable(tableProperties.get(READY_FOR_GC_FILEINFO_TABLENAME)))
                .isInstanceOf(ResourceNotFoundException.class);
        assertThatThrownBy(() -> dynamoDB.describeTable(tableProperties.get(PARTITION_TABLENAME)))
                .isInstanceOf(ResourceNotFoundException.class);
    }

    @Nested
    @DisplayName("Store records")
    class StoreRecords {
        @TempDir
        private Path tempDir;

        @Test
        void shouldStoreRecords() throws Exception {
            // Given
            DeployDockerInstance.deploy("test-instance-3", s3Client, dynamoDB);
            InstanceProperties instanceProperties = new InstanceProperties();
            instanceProperties.loadFromS3GivenInstanceId(s3Client, "test-instance-3");
            TableProperties tableProperties = new TableProperties(instanceProperties);
            tableProperties.loadFromS3(s3Client, "system-test");

            // When
            List<Record> records = List.of(
                    new Record(Map.of("key", "test1")),
                    new Record(Map.of("key", "test2")));
            ingestRecords(instanceProperties, tableProperties, records);

            // Then
            assertThat(queryAllRecords(instanceProperties, tableProperties))
                    .toIterable().containsExactlyElementsOf(records);
        }

        private void ingestRecords(InstanceProperties instanceProperties, TableProperties tableProperties,
                                   List<Record> records) throws Exception {
            IngestFactory.builder()
                    .instanceProperties(instanceProperties)
                    .objectFactory(ObjectFactory.noUserJars())
                    .localDir(tempDir.toString())
                    .hadoopConfiguration(getHadoopConfiguration())
                    .stateStoreProvider(new StateStoreProvider(dynamoDB, instanceProperties))
                    .s3AsyncClient(createS3AsyncClient())
                    .build().ingestFromRecordIteratorAndClose(tableProperties, new WrappedIterator<>(records.iterator()));
        }
    }

    private CloseableIterator<Record> queryAllRecords(
            InstanceProperties instanceProperties, TableProperties tableProperties) throws Exception {
        StateStore stateStore = new StateStoreProvider(dynamoDB, instanceProperties).getStateStore(tableProperties);
        PartitionTree tree = new PartitionTree(tableProperties.getSchema(), stateStore.getAllPartitions());
        QueryExecutor executor = new QueryExecutor(ObjectFactory.noUserJars(), tableProperties,
                stateStore, getHadoopConfiguration(), Executors.newSingleThreadExecutor());
        executor.init(tree.getAllPartitions(), stateStore.getPartitionToActiveFilesMap());
        return executor.execute(createQueryAllRecords(tree, tableProperties.get(TABLE_NAME)));
    }

    private static Query createQueryAllRecords(PartitionTree tree, String tableName) {
        return new Query.Builder(tableName,
                UUID.randomUUID().toString(),
                List.of(tree.getRootPartition().getRegion())).build();
    }

    private S3AsyncClient createS3AsyncClient() {
        return buildAwsV2Client(localStackContainer, LocalStackContainer.Service.S3, S3AsyncClient.builder());
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
