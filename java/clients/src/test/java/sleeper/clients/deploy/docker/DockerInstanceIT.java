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
import com.amazonaws.services.dynamodbv2.model.ResourceNotFoundException;
import com.amazonaws.services.s3.model.HeadBucketRequest;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.testcontainers.containers.localstack.LocalStackContainer;
import org.testcontainers.junit.jupiter.Testcontainers;
import software.amazon.awssdk.services.s3.S3AsyncClient;

import sleeper.clients.docker.TearDownDockerInstance;
import sleeper.configuration.jars.ObjectFactory;
import sleeper.configuration.properties.instance.InstanceProperties;
import sleeper.configuration.properties.table.S3TableProperties;
import sleeper.configuration.properties.table.TableProperties;
import sleeper.core.iterator.WrappedIterator;
import sleeper.core.record.Record;
import sleeper.ingest.IngestFactory;
import sleeper.statestore.StateStoreProvider;

import java.nio.file.Path;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static sleeper.configuration.properties.instance.CdkDefinedInstanceProperty.ACTIVE_FILEINFO_TABLENAME;
import static sleeper.configuration.properties.instance.CdkDefinedInstanceProperty.CONFIG_BUCKET;
import static sleeper.configuration.properties.instance.CdkDefinedInstanceProperty.DATA_BUCKET;
import static sleeper.configuration.properties.instance.CdkDefinedInstanceProperty.PARTITION_TABLENAME;
import static sleeper.configuration.properties.instance.CdkDefinedInstanceProperty.READY_FOR_GC_FILEINFO_TABLENAME;
import static sleeper.configuration.properties.instance.CdkDefinedInstanceProperty.REVISION_TABLENAME;
import static sleeper.configuration.properties.table.TableProperty.STATESTORE_CLASSNAME;
import static sleeper.ingest.testutils.LocalStackAwsV2ClientHelper.buildAwsV2Client;

@Testcontainers
public class DockerInstanceIT extends DockerInstanceTestBase {
    @Nested
    @DisplayName("Using DynamoDB state store")
    class UsingDynamoDBStateStore {
        @Test
        void shouldDeployInstance() throws Exception {
            // Given / When
            deployInstance("test-instance", tableProperties ->
                    tableProperties.set(STATESTORE_CLASSNAME, "sleeper.statestore.dynamodb.DynamoDBStateStore"));

            // Then
            InstanceProperties instanceProperties = new InstanceProperties();
            instanceProperties.loadFromS3GivenInstanceId(s3Client, "test-instance");
            TableProperties tableProperties = S3TableProperties.getStore(instanceProperties, s3Client, dynamoDB)
                    .loadByName("system-test").orElseThrow();
            assertThat(queryAllRecords(instanceProperties, tableProperties)).isExhausted();
            assertThatCode(() -> dynamoDB.describeTable(instanceProperties.get(ACTIVE_FILEINFO_TABLENAME)))
                    .doesNotThrowAnyException();
            assertThatCode(() -> dynamoDB.describeTable(instanceProperties.get(READY_FOR_GC_FILEINFO_TABLENAME)))
                    .doesNotThrowAnyException();
            assertThatCode(() -> dynamoDB.describeTable(instanceProperties.get(PARTITION_TABLENAME)))
                    .doesNotThrowAnyException();
        }

        @Test
        void shouldTearDownInstance() {
            // Given
            deployInstance("test-instance-2", tableProperties ->
                    tableProperties.set(STATESTORE_CLASSNAME, "sleeper.statestore.dynamodb.DynamoDBStateStore"));
            InstanceProperties instanceProperties = new InstanceProperties();
            instanceProperties.loadFromS3GivenInstanceId(s3Client, "test-instance-2");

            // When
            TearDownDockerInstance.tearDown("test-instance-2", s3Client, dynamoDB, sqsClient);

            // Then
            assertThatThrownBy(() -> s3Client.headBucket(new HeadBucketRequest(instanceProperties.get(CONFIG_BUCKET))))
                    .isInstanceOf(AmazonServiceException.class);
            assertThatThrownBy(() -> s3Client.headBucket(new HeadBucketRequest(instanceProperties.get(DATA_BUCKET))))
                    .isInstanceOf(AmazonServiceException.class);
            assertThatThrownBy(() -> dynamoDB.describeTable(instanceProperties.get(ACTIVE_FILEINFO_TABLENAME)))
                    .isInstanceOf(ResourceNotFoundException.class);
            assertThatThrownBy(() -> dynamoDB.describeTable(instanceProperties.get(READY_FOR_GC_FILEINFO_TABLENAME)))
                    .isInstanceOf(ResourceNotFoundException.class);
            assertThatThrownBy(() -> dynamoDB.describeTable(instanceProperties.get(PARTITION_TABLENAME)))
                    .isInstanceOf(ResourceNotFoundException.class);
        }
    }

    @Nested
    @DisplayName("Using S3 state store")
    class UsingS3StateStore {
        @Test
        void shouldDeployInstance() throws Exception {
            // Given / When
            deployInstance("test-instance-3", tableProperties ->
                    tableProperties.set(STATESTORE_CLASSNAME, "sleeper.statestore.s3.S3StateStore"));

            // Then
            InstanceProperties instanceProperties = new InstanceProperties();
            instanceProperties.loadFromS3GivenInstanceId(s3Client, "test-instance-3");
            TableProperties tableProperties = S3TableProperties.getStore(instanceProperties, s3Client, dynamoDB)
                    .loadByName("system-test").orElseThrow();
            assertThat(queryAllRecords(instanceProperties, tableProperties)).isExhausted();
            assertThatCode(() -> dynamoDB.describeTable(instanceProperties.get(REVISION_TABLENAME)))
                    .doesNotThrowAnyException();
        }

        @Test
        void shouldTearDownInstance() {
            // Given
            deployInstance("test-instance-4", tableProperties ->
                    tableProperties.set(STATESTORE_CLASSNAME, "sleeper.statestore.s3.S3StateStore"));
            InstanceProperties instanceProperties = new InstanceProperties();
            instanceProperties.loadFromS3GivenInstanceId(s3Client, "test-instance-4");

            // When
            TearDownDockerInstance.tearDown("test-instance-4", s3Client, dynamoDB, sqsClient);

            // Then
            assertThatThrownBy(() -> s3Client.headBucket(new HeadBucketRequest(instanceProperties.get(CONFIG_BUCKET))))
                    .isInstanceOf(AmazonServiceException.class);
            assertThatThrownBy(() -> s3Client.headBucket(new HeadBucketRequest(instanceProperties.get(DATA_BUCKET))))
                    .isInstanceOf(AmazonServiceException.class);
            assertThatThrownBy(() -> dynamoDB.describeTable(instanceProperties.get(REVISION_TABLENAME)))
                    .isInstanceOf(ResourceNotFoundException.class);
        }
    }

    @Nested
    @DisplayName("Store records")
    class StoreRecords {
        @TempDir
        private Path tempDir;

        @Test
        void shouldStoreRecords() throws Exception {
            // Given
            deployInstance("test-instance-5");
            InstanceProperties instanceProperties = new InstanceProperties();
            instanceProperties.loadFromS3GivenInstanceId(s3Client, "test-instance-5");
            TableProperties tableProperties = S3TableProperties.getStore(instanceProperties, s3Client, dynamoDB)
                    .loadByName("system-test").orElseThrow();

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
                    .stateStoreProvider(new StateStoreProvider(dynamoDB, instanceProperties, getHadoopConfiguration()))
                    .s3AsyncClient(createS3AsyncClient())
                    .build().ingestFromRecordIteratorAndClose(tableProperties, new WrappedIterator<>(records.iterator()));
        }
    }

    private S3AsyncClient createS3AsyncClient() {
        return buildAwsV2Client(localStackContainer, LocalStackContainer.Service.S3, S3AsyncClient.builder());
    }
}
