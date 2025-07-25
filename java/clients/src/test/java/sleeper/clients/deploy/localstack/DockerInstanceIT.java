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

package sleeper.clients.deploy.localstack;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import software.amazon.awssdk.services.dynamodb.model.ResourceNotFoundException;
import software.amazon.awssdk.services.s3.model.NoSuchBucketException;

import sleeper.configuration.properties.S3InstanceProperties;
import sleeper.configuration.properties.S3TableProperties;
import sleeper.core.iterator.WrappedIterator;
import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.instance.InstanceProperty;
import sleeper.core.properties.table.TableProperties;
import sleeper.core.row.Row;
import sleeper.core.util.ObjectFactory;
import sleeper.ingest.runner.IngestFactory;
import sleeper.statestore.StateStoreFactory;
import sleeper.statestore.transactionlog.DynamoDBTransactionLogStateStore;

import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.CONFIG_BUCKET;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.DATA_BUCKET;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.TABLE_ID_INDEX_DYNAMO_TABLENAME;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.TABLE_NAME_INDEX_DYNAMO_TABLENAME;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.TABLE_ONLINE_INDEX_DYNAMO_TABLENAME;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.TRANSACTION_LOG_ALL_SNAPSHOTS_TABLENAME;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.TRANSACTION_LOG_FILES_TABLENAME;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.TRANSACTION_LOG_LATEST_SNAPSHOTS_TABLENAME;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.TRANSACTION_LOG_PARTITIONS_TABLENAME;
import static sleeper.core.properties.table.TableProperty.STATESTORE_CLASSNAME;

public class DockerInstanceIT extends DockerInstanceTestBase {

    @Nested
    @DisplayName("Using transaction log state store")
    class UsingTransactionLogStateStore {
        @Test
        void shouldDeployInstance() throws Exception {
            // Given / When
            String instanceId = UUID.randomUUID().toString().substring(0, 18);
            deployInstance(instanceId, tableProperties -> tableProperties.set(STATESTORE_CLASSNAME, DynamoDBTransactionLogStateStore.class.getSimpleName()));

            // Then
            InstanceProperties instanceProperties = S3InstanceProperties.loadGivenInstanceId(s3Client, instanceId);
            TableProperties tableProperties = S3TableProperties.createStore(instanceProperties, s3Client, dynamoClient)
                    .loadByName("system-test");
            assertThat(queryAllRows(instanceProperties, tableProperties)).isExhausted();
            assertTablesExist(instanceProperties,
                    TABLE_NAME_INDEX_DYNAMO_TABLENAME,
                    TABLE_ONLINE_INDEX_DYNAMO_TABLENAME,
                    TABLE_ID_INDEX_DYNAMO_TABLENAME,
                    TRANSACTION_LOG_FILES_TABLENAME,
                    TRANSACTION_LOG_PARTITIONS_TABLENAME,
                    TRANSACTION_LOG_ALL_SNAPSHOTS_TABLENAME,
                    TRANSACTION_LOG_LATEST_SNAPSHOTS_TABLENAME);
        }

        @Test
        void shouldTearDownInstance() {
            // Given
            String instanceId = UUID.randomUUID().toString().substring(0, 18);
            deployInstance(instanceId, tableProperties -> tableProperties.set(STATESTORE_CLASSNAME, DynamoDBTransactionLogStateStore.class.getSimpleName()));
            InstanceProperties instanceProperties = S3InstanceProperties.loadGivenInstanceId(s3Client, instanceId);

            // When
            TearDownDockerInstance.tearDown(instanceId, s3Client, dynamoClient, sqsClient);

            // Then
            assertThatThrownBy(() -> s3Client.headBucket(request -> request.bucket(instanceProperties.get(CONFIG_BUCKET))))
                    .isInstanceOf(NoSuchBucketException.class);
            assertThatThrownBy(() -> s3Client.headBucket(request -> request.bucket(instanceProperties.get(DATA_BUCKET))))
                    .isInstanceOf(NoSuchBucketException.class);
            assertTablesDoNotExist(instanceProperties,
                    TABLE_NAME_INDEX_DYNAMO_TABLENAME,
                    TABLE_ONLINE_INDEX_DYNAMO_TABLENAME,
                    TABLE_ID_INDEX_DYNAMO_TABLENAME,
                    TRANSACTION_LOG_FILES_TABLENAME,
                    TRANSACTION_LOG_PARTITIONS_TABLENAME,
                    TRANSACTION_LOG_ALL_SNAPSHOTS_TABLENAME,
                    TRANSACTION_LOG_LATEST_SNAPSHOTS_TABLENAME);
        }

        private void assertTablesExist(InstanceProperties instanceProperties, InstanceProperty... tableNameProperties) {
            for (InstanceProperty tableNameProperty : tableNameProperties) {
                assertThatCode(() -> dynamoClient.describeTable(request -> request.tableName(instanceProperties.get(tableNameProperty))))
                        .describedAs("Table should exist: " + tableNameProperty)
                        .doesNotThrowAnyException();
            }
        }

        private void assertTablesDoNotExist(InstanceProperties instanceProperties, InstanceProperty... tableNameProperties) {
            for (InstanceProperty tableNameProperty : tableNameProperties) {
                assertThatThrownBy(() -> dynamoClient.describeTable(request -> request.tableName(instanceProperties.get(tableNameProperty))),
                        "Table should not exist: " + tableNameProperty)
                        .isInstanceOf(ResourceNotFoundException.class);
            }
        }

    }

    @Nested
    @DisplayName("Store rows")
    class StoreRows {
        @TempDir
        private Path tempDir;

        @Test
        void shouldStoreRows() throws Exception {
            // Given
            String instanceId = UUID.randomUUID().toString().substring(0, 18);
            deployInstance(instanceId);
            InstanceProperties instanceProperties = S3InstanceProperties.loadGivenInstanceId(s3Client, instanceId);
            TableProperties tableProperties = S3TableProperties.createStore(instanceProperties, s3Client, dynamoClient)
                    .loadByName("system-test");

            // When
            List<Row> rows = List.of(
                    new Row(Map.of("key", "test1")),
                    new Row(Map.of("key", "test2")));
            ingestRows(instanceProperties, tableProperties, rows);

            // Then
            assertThat(queryAllRows(instanceProperties, tableProperties))
                    .toIterable().containsExactlyElementsOf(rows);
        }

        private void ingestRows(
                InstanceProperties instanceProperties, TableProperties tableProperties, List<Row> rows) throws Exception {
            IngestFactory.builder()
                    .instanceProperties(instanceProperties)
                    .objectFactory(ObjectFactory.noUserJars())
                    .localDir(tempDir.toString())
                    .hadoopConfiguration(hadoopConf)
                    .stateStoreProvider(StateStoreFactory.createProvider(instanceProperties, s3Client, dynamoClient))
                    .s3AsyncClient(s3AsyncClient)
                    .build().ingestFromRecordIteratorAndClose(tableProperties, new WrappedIterator<>(rows.iterator()));
        }
    }
}
