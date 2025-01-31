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

import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.dynamodbv2.model.AmazonDynamoDBException;
import com.amazonaws.services.dynamodbv2.model.ResourceNotFoundException;
import com.amazonaws.services.s3.model.HeadBucketRequest;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import sleeper.clients.docker.TearDownDockerInstance;
import sleeper.configuration.properties.S3InstanceProperties;
import sleeper.configuration.properties.S3TableProperties;
import sleeper.core.iterator.WrappedIterator;
import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.instance.InstanceProperty;
import sleeper.core.properties.table.TableProperties;
import sleeper.core.record.Record;
import sleeper.core.util.ObjectFactory;
import sleeper.ingest.runner.IngestFactory;
import sleeper.statestore.StateStoreFactory;
import sleeper.statestore.dynamodb.DynamoDBStateStore;
import sleeper.statestore.s3.S3StateStore;
import sleeper.statestore.transactionlog.DynamoDBTransactionLogStateStore;

import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.ACTIVE_FILES_TABLENAME;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.CONFIG_BUCKET;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.DATA_BUCKET;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.PARTITION_TABLENAME;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.REVISION_TABLENAME;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.TRANSACTION_LOG_ALL_SNAPSHOTS_TABLENAME;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.TRANSACTION_LOG_FILES_TABLENAME;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.TRANSACTION_LOG_LATEST_SNAPSHOTS_TABLENAME;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.TRANSACTION_LOG_PARTITIONS_TABLENAME;
import static sleeper.core.properties.table.TableProperty.STATESTORE_CLASSNAME;

public class DockerInstanceIT extends DockerInstanceTestBase {
    @Nested
    @DisplayName("Using DynamoDB state store")
    class UsingDynamoDBStateStore {
        @Test
        void shouldDeployInstance() throws Exception {
            // Given / When
            String instanceId = UUID.randomUUID().toString().substring(0, 18);
            deployInstance(instanceId, tableProperties -> tableProperties.set(STATESTORE_CLASSNAME, DynamoDBStateStore.class.getName()));

            // Then
            InstanceProperties instanceProperties = S3InstanceProperties.loadGivenInstanceId(s3Client, instanceId);
            TableProperties tableProperties = S3TableProperties.createStore(instanceProperties, s3Client, dynamoClient)
                    .loadByName("system-test");
            assertThat(queryAllRecords(instanceProperties, tableProperties)).isExhausted();
            assertThatCode(() -> dynamoClient.describeTable(instanceProperties.get(ACTIVE_FILES_TABLENAME)))
                    .doesNotThrowAnyException();
            assertThatCode(() -> dynamoClient.describeTable(instanceProperties.get(PARTITION_TABLENAME)))
                    .doesNotThrowAnyException();
        }

        @Test
        void shouldTearDownInstance() {
            // Given
            String instanceId = UUID.randomUUID().toString().substring(0, 18);
            deployInstance(instanceId, tableProperties -> tableProperties.set(STATESTORE_CLASSNAME, DynamoDBStateStore.class.getName()));
            InstanceProperties instanceProperties = S3InstanceProperties.loadGivenInstanceId(s3Client, instanceId);

            // When
            TearDownDockerInstance.tearDown(instanceId, s3Client, dynamoClient, sqsClientV2);

            // Then
            assertThatThrownBy(() -> s3Client.headBucket(new HeadBucketRequest(instanceProperties.get(CONFIG_BUCKET))))
                    .isInstanceOf(AmazonServiceException.class);
            assertThatThrownBy(() -> s3Client.headBucket(new HeadBucketRequest(instanceProperties.get(DATA_BUCKET))))
                    .isInstanceOf(AmazonServiceException.class);
            assertThatThrownBy(() -> dynamoClient.describeTable(instanceProperties.get(ACTIVE_FILES_TABLENAME)))
                    .isInstanceOf(ResourceNotFoundException.class);
            assertThatThrownBy(() -> dynamoClient.describeTable(instanceProperties.get(PARTITION_TABLENAME)))
                    .isInstanceOf(ResourceNotFoundException.class);
        }
    }

    @Nested
    @DisplayName("Using S3 state store")
    class UsingS3StateStore {
        @Test
        void shouldDeployInstance() throws Exception {
            // Given / When
            String instanceId = UUID.randomUUID().toString().substring(0, 18);
            deployInstance(instanceId, tableProperties -> tableProperties.set(STATESTORE_CLASSNAME, S3StateStore.class.getName()));

            // Then
            InstanceProperties instanceProperties = S3InstanceProperties.loadGivenInstanceId(s3Client, instanceId);
            TableProperties tableProperties = S3TableProperties.createStore(instanceProperties, s3Client, dynamoClient)
                    .loadByName("system-test");
            assertThat(queryAllRecords(instanceProperties, tableProperties)).isExhausted();
            assertThatCode(() -> dynamoClient.describeTable(instanceProperties.get(REVISION_TABLENAME)))
                    .doesNotThrowAnyException();
        }

        @Test
        void shouldTearDownInstance() {
            // Given
            String instanceId = UUID.randomUUID().toString().substring(0, 18);
            deployInstance(instanceId, tableProperties -> tableProperties.set(STATESTORE_CLASSNAME, S3StateStore.class.getName()));
            InstanceProperties instanceProperties = S3InstanceProperties.loadGivenInstanceId(s3Client, instanceId);

            // When
            TearDownDockerInstance.tearDown(instanceId, s3Client, dynamoClient, sqsClientV2);

            // Then
            assertThatThrownBy(() -> s3Client.headBucket(new HeadBucketRequest(instanceProperties.get(CONFIG_BUCKET))))
                    .isInstanceOf(AmazonServiceException.class);
            assertThatThrownBy(() -> s3Client.headBucket(new HeadBucketRequest(instanceProperties.get(DATA_BUCKET))))
                    .isInstanceOf(AmazonServiceException.class);
            assertThatThrownBy(() -> dynamoClient.describeTable(instanceProperties.get(REVISION_TABLENAME)))
                    .isInstanceOf(ResourceNotFoundException.class);
        }
    }

    @Nested
    @DisplayName("Using transaction log state store")
    class UsingTransactionLogStateStore {
        @Test
        void shouldDeployInstance() throws Exception {
            // Given / When
            String instanceId = UUID.randomUUID().toString().substring(0, 18);
            deployInstance(instanceId, tableProperties -> tableProperties.set(STATESTORE_CLASSNAME, DynamoDBTransactionLogStateStore.class.getName()));

            // Then
            InstanceProperties instanceProperties = S3InstanceProperties.loadGivenInstanceId(s3Client, instanceId);
            TableProperties tableProperties = S3TableProperties.createStore(instanceProperties, s3Client, dynamoClient)
                    .loadByName("system-test");
            assertThat(queryAllRecords(instanceProperties, tableProperties)).isExhausted();
            assertThatCode(() -> describeTables(instanceProperties,
                    TRANSACTION_LOG_FILES_TABLENAME,
                    TRANSACTION_LOG_PARTITIONS_TABLENAME,
                    TRANSACTION_LOG_ALL_SNAPSHOTS_TABLENAME,
                    TRANSACTION_LOG_LATEST_SNAPSHOTS_TABLENAME))
                    .doesNotThrowAnyException();
        }

        @Test
        void shouldTearDownInstance() {
            // Given
            String instanceId = UUID.randomUUID().toString().substring(0, 18);
            deployInstance(instanceId, tableProperties -> tableProperties.set(STATESTORE_CLASSNAME, DynamoDBTransactionLogStateStore.class.getName()));
            InstanceProperties instanceProperties = S3InstanceProperties.loadGivenInstanceId(s3Client, instanceId);

            // When
            TearDownDockerInstance.tearDown(instanceId, s3Client, dynamoClient, sqsClientV2);

            // Then
            assertThatThrownBy(() -> s3Client.headBucket(new HeadBucketRequest(instanceProperties.get(CONFIG_BUCKET))))
                    .isInstanceOf(AmazonServiceException.class);
            assertThatThrownBy(() -> s3Client.headBucket(new HeadBucketRequest(instanceProperties.get(DATA_BUCKET))))
                    .isInstanceOf(AmazonServiceException.class);
            assertThatThrownBy(() -> dynamoClient.describeTable(instanceProperties.get(TRANSACTION_LOG_FILES_TABLENAME)))
                    .isInstanceOf(ResourceNotFoundException.class);
            assertThatThrownBy(() -> dynamoClient.describeTable(instanceProperties.get(TRANSACTION_LOG_PARTITIONS_TABLENAME)))
                    .isInstanceOf(ResourceNotFoundException.class);
            assertThatThrownBy(() -> dynamoClient.describeTable(instanceProperties.get(TRANSACTION_LOG_ALL_SNAPSHOTS_TABLENAME)))
                    .isInstanceOf(ResourceNotFoundException.class);
            assertThatThrownBy(() -> dynamoClient.describeTable(instanceProperties.get(TRANSACTION_LOG_LATEST_SNAPSHOTS_TABLENAME)))
                    .isInstanceOf(ResourceNotFoundException.class);

        }

        private void describeTables(InstanceProperties instanceProperties, InstanceProperty... tableNameProperties) throws AmazonDynamoDBException {
            for (InstanceProperty tableNameProperty : tableNameProperties) {
                dynamoClient.describeTable(instanceProperties.get(tableNameProperty));
            }
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
            String instanceId = UUID.randomUUID().toString().substring(0, 18);
            deployInstance(instanceId);
            InstanceProperties instanceProperties = S3InstanceProperties.loadGivenInstanceId(s3Client, instanceId);
            TableProperties tableProperties = S3TableProperties.createStore(instanceProperties, s3Client, dynamoClient)
                    .loadByName("system-test");

            // When
            List<Record> records = List.of(
                    new Record(Map.of("key", "test1")),
                    new Record(Map.of("key", "test2")));
            ingestRecords(instanceProperties, tableProperties, records);

            // Then
            assertThat(queryAllRecords(instanceProperties, tableProperties))
                    .toIterable().containsExactlyElementsOf(records);
        }

        private void ingestRecords(
                InstanceProperties instanceProperties, TableProperties tableProperties, List<Record> records) throws Exception {
            IngestFactory.builder()
                    .instanceProperties(instanceProperties)
                    .objectFactory(ObjectFactory.noUserJars())
                    .localDir(tempDir.toString())
                    .hadoopConfiguration(HADOOP_CONF)
                    .stateStoreProvider(StateStoreFactory.createProvider(instanceProperties, s3Client, dynamoClient, HADOOP_CONF))
                    .s3AsyncClient(S3_ASYNC_CLIENT)
                    .build().ingestFromRecordIteratorAndClose(tableProperties, new WrappedIterator<>(records.iterator()));
        }
    }
}
