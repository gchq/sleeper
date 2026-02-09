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
package sleeper.systemtest.datageneration;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import sleeper.clients.api.SleeperClient;
import sleeper.configuration.table.index.DynamoDBTableIndexCreator;
import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.table.TableProperties;
import sleeper.core.statestore.FileReference;
import sleeper.ingest.core.job.IngestJob;
import sleeper.ingest.core.job.IngestJobSerDe;
import sleeper.localstack.test.LocalStackTestBase;
import sleeper.statestore.transactionlog.TransactionLogStateStoreCreator;
import sleeper.systemtest.configuration.SystemTestDataGenerationJob;
import sleeper.systemtest.configuration.SystemTestDataGenerationJobStore;
import sleeper.systemtest.configuration.SystemTestStandaloneProperties;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Path;
import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.ACCOUNT;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.CONFIG_BUCKET;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.DATA_BUCKET;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.INGEST_BY_QUEUE_ROLE_ARN;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.INGEST_DIRECT_ROLE_ARN;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.INGEST_JOB_QUEUE_URL;
import static sleeper.core.properties.instance.CommonProperty.ENDPOINT_URL;
import static sleeper.core.properties.model.IngestQueue.STANDARD_INGEST;
import static sleeper.core.properties.table.TableProperty.TABLE_NAME;
import static sleeper.core.properties.testutils.InstancePropertiesTestHelper.createTestInstanceProperties;
import static sleeper.core.properties.testutils.TablePropertiesTestHelper.createTestTableProperties;
import static sleeper.core.schema.SchemaTestHelper.createSchemaWithKey;
import static sleeper.systemtest.configuration.SystemTestIngestMode.DIRECT;
import static sleeper.systemtest.configuration.SystemTestIngestMode.QUEUE;
import static sleeper.systemtest.configuration.SystemTestProperty.SYSTEM_TEST_BUCKET_NAME;

public class ECSSystemTestTaskIT extends LocalStackTestBase {

    InstanceProperties instanceProperties = createTestInstanceProperties();
    SystemTestStandaloneProperties systemTestProperties = new SystemTestStandaloneProperties();
    TableProperties tableProperties = createTestTableProperties(instanceProperties, createSchemaWithKey("key"));
    String tableName = tableProperties.get(TABLE_NAME);
    SleeperClient sleeperClient;
    @TempDir
    Path tempDir;

    @BeforeEach
    void setUp() {
        String account = stsClient.getCallerIdentity().account();
        instanceProperties.set(ACCOUNT, account);
        instanceProperties.set(ENDPOINT_URL, localStackContainer.getEndpoint().toString());
        instanceProperties.set(INGEST_DIRECT_ROLE_ARN, "arn:aws:iam::" + account + ":role/ingest-direct");
        instanceProperties.set(INGEST_BY_QUEUE_ROLE_ARN, "arn:aws:iam::" + account + ":role/ingest-by-queue");
        instanceProperties.set(INGEST_JOB_QUEUE_URL, createSqsQueueGetUrl());
        systemTestProperties.set(SYSTEM_TEST_BUCKET_NAME, UUID.randomUUID().toString());
        createBucket(instanceProperties.get(CONFIG_BUCKET));
        createBucket(instanceProperties.get(DATA_BUCKET));
        createBucket(systemTestProperties.get(SYSTEM_TEST_BUCKET_NAME));
        DynamoDBTableIndexCreator.create(dynamoClient, instanceProperties);
        new TransactionLogStateStoreCreator(instanceProperties, dynamoClient).create();
        sleeperClient = createSleeperClient();
        sleeperClient.addTable(tableProperties, List.of());
    }

    @Test
    void shouldIngestDirectly() throws Exception {
        // Given
        SystemTestDataGenerationJob job = SystemTestDataGenerationJob.builder()
                .tableName(tableName)
                .numberOfIngests(2)
                .rowsPerIngest(12)
                .ingestMode(DIRECT)
                .build();

        // When
        createTask(job).run();

        // Then
        assertThat(sleeperClient.getStateStore(tableName).getFileReferences())
                .extracting(FileReference::getNumberOfRows)
                .containsExactly(12L, 12L);
    }

    @Test
    void shouldReuseSameDataGenerationJob() {
        // Given
        SystemTestDataGenerationJob job = SystemTestDataGenerationJob.builder()
                .tableName(tableName)
                .numberOfIngests(1)
                .rowsPerIngest(10)
                .ingestMode(QUEUE)
                .ingestQueue(STANDARD_INGEST)
                .build();

        // When
        createTask(job).run();
        createTask(job).run();

        // Then
        List<IngestJob> ingestJobs = receiveIngestJobs();
        assertThat(ingestJobs).extracting(IngestJob::getId).hasSize(2).doesNotHaveDuplicates();
        assertThat(ingestJobs).flatExtracting(IngestJob::getFiles).hasSize(2).doesNotHaveDuplicates();
    }

    List<IngestJob> receiveIngestJobs() {
        IngestJobSerDe serDe = new IngestJobSerDe();
        return receiveMessages(instanceProperties.get(INGEST_JOB_QUEUE_URL))
                .map(serDe::fromJson)
                .toList();
    }

    ECSSystemTestTask createTask(SystemTestDataGenerationJob job) {
        String jobObjectKey = new SystemTestDataGenerationJobStore(systemTestProperties, s3Client)
                .writeJobGetObjectKey(job);
        IngestRandomData ingestData = new IngestRandomData(instanceProperties, systemTestProperties, stsClient, hadoopConf, tempDir.toString());
        return new ECSSystemTestTask(systemTestProperties, s3Client, jobObjectKey, readJob -> {
            try {
                ingestData.run(readJob);
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        });
    }

    SleeperClient createSleeperClient() {
        return SleeperClient.builder()
                .instanceProperties(instanceProperties)
                .awsClients(clients -> clients
                        .s3Client(s3Client)
                        .dynamoClient(dynamoClient)
                        .sqsClient(sqsClient)
                        .awsCredentialsProvider(credentialsProvider))
                .hadoopConf(hadoopConf)
                .build();
    }

}
