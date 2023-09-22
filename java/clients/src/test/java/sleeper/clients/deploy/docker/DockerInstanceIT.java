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

import sleeper.clients.docker.DeployDockerInstance;
import sleeper.clients.docker.TearDownDockerInstance;
import sleeper.configuration.jars.ObjectFactory;
import sleeper.configuration.properties.instance.InstanceProperties;
import sleeper.configuration.properties.table.TableProperties;
import sleeper.core.iterator.WrappedIterator;
import sleeper.core.record.Record;
import sleeper.ingest.IngestFactory;
import sleeper.statestore.StateStoreProvider;

import java.nio.file.Path;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static sleeper.configuration.properties.instance.SystemDefinedInstanceProperty.CONFIG_BUCKET;
import static sleeper.configuration.properties.instance.SystemDefinedInstanceProperty.DATA_BUCKET;
import static sleeper.configuration.properties.table.TableProperty.ACTIVE_FILEINFO_TABLENAME;
import static sleeper.configuration.properties.table.TableProperty.PARTITION_TABLENAME;
import static sleeper.configuration.properties.table.TableProperty.READY_FOR_GC_FILEINFO_TABLENAME;
import static sleeper.ingest.testutils.LocalStackAwsV2ClientHelper.buildAwsV2Client;

@Testcontainers
public class DockerInstanceIT extends DockerInstanceTestBase {

    @Test
    void shouldDeployInstance() throws Exception {
        // Given / When
        DeployDockerInstance.deploy("test-instance", s3Client, dynamoDB, sqsClient);

        // Then
        InstanceProperties instanceProperties = new InstanceProperties();
        instanceProperties.loadFromS3GivenInstanceId(s3Client, "test-instance");
        TableProperties tableProperties = new TableProperties(instanceProperties);
        tableProperties.loadFromS3(s3Client, "system-test");
        assertThat(queryAllRecords(instanceProperties, tableProperties)).isExhausted();
    }

    @Test
    void shouldTearDownInstance() {
        // Given
        DeployDockerInstance.deploy("test-instance-2", s3Client, dynamoDB, sqsClient);
        InstanceProperties instanceProperties = new InstanceProperties();
        instanceProperties.loadFromS3GivenInstanceId(s3Client, "test-instance-2");
        TableProperties tableProperties = new TableProperties(instanceProperties);
        tableProperties.loadFromS3(s3Client, "system-test");

        // When
        TearDownDockerInstance.tearDown("test-instance-2", s3Client, dynamoDB, sqsClient);

        // Then
        assertThatThrownBy(() -> s3Client.headBucket(new HeadBucketRequest(instanceProperties.get(CONFIG_BUCKET))))
                .isInstanceOf(AmazonServiceException.class);
        assertThatThrownBy(() -> s3Client.headBucket(new HeadBucketRequest(instanceProperties.get(DATA_BUCKET))))
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
            DeployDockerInstance.deploy("test-instance-3", s3Client, dynamoDB, sqsClient);
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

    private S3AsyncClient createS3AsyncClient() {
        return buildAwsV2Client(localStackContainer, LocalStackContainer.Service.S3, S3AsyncClient.builder());
    }
}
