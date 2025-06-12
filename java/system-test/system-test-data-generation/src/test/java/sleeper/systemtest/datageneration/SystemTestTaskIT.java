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
import sleeper.localstack.test.LocalStackTestBase;
import sleeper.statestore.transactionlog.TransactionLogStateStoreCreator;
import sleeper.systemtest.configuration.SystemTestDataGenerationJob;
import sleeper.systemtest.configuration.SystemTestDataGenerationJobSerDe;
import sleeper.systemtest.configuration.SystemTestStandaloneProperties;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Path;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.CONFIG_BUCKET;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.DATA_BUCKET;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.INGEST_BY_QUEUE_ROLE_ARN;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.INGEST_DIRECT_ROLE_ARN;
import static sleeper.core.properties.instance.CommonProperty.ENDPOINT_URL;
import static sleeper.core.properties.table.TableProperty.TABLE_NAME;
import static sleeper.core.properties.testutils.InstancePropertiesTestHelper.createTestInstanceProperties;
import static sleeper.core.properties.testutils.TablePropertiesTestHelper.createTestTableProperties;
import static sleeper.core.schema.SchemaTestHelper.createSchemaWithKey;
import static sleeper.systemtest.configuration.SystemTestIngestMode.DIRECT;
import static sleeper.systemtest.configuration.SystemTestProperty.SYSTEM_TEST_JOBS_QUEUE_URL;

public class SystemTestTaskIT extends LocalStackTestBase {

    InstanceProperties instanceProperties = createTestInstanceProperties();
    SystemTestStandaloneProperties systemTestProperties = new SystemTestStandaloneProperties();
    TableProperties tableProperties = createTestTableProperties(instanceProperties, createSchemaWithKey("key"));
    String tableName = tableProperties.get(TABLE_NAME);
    SleeperClient sleeperClient;
    @TempDir
    Path tempDir;

    @BeforeEach
    void setUp() {
        instanceProperties.set(ENDPOINT_URL, localStackContainer.getEndpoint().toString());
        instanceProperties.set(INGEST_DIRECT_ROLE_ARN, "ingest-direct");
        instanceProperties.set(INGEST_BY_QUEUE_ROLE_ARN, "ingest-by-queue");
        systemTestProperties.set(SYSTEM_TEST_JOBS_QUEUE_URL, createSqsQueueGetUrl());
        createBucket(instanceProperties.get(CONFIG_BUCKET));
        createBucket(instanceProperties.get(DATA_BUCKET));
        DynamoDBTableIndexCreator.create(dynamoClientV2, instanceProperties);
        new TransactionLogStateStoreCreator(instanceProperties, dynamoClientV2).create();
        sleeperClient = createSleeperClient();
        sleeperClient.addTable(tableProperties, List.of());
    }

    @Test
    void shouldIngestDirectly() throws Exception {
        // Given
        sendJob(SystemTestDataGenerationJob.builder()
                .jobId("test-job")
                .tableName(tableName)
                .numberOfIngests(2)
                .recordsPerIngest(12)
                .ingestMode(DIRECT)
                .build());

        // When
        createTask().run();

        // Then
        assertThat(sleeperClient.getStateStore(tableName).getFileReferences())
                .extracting(FileReference::getNumberOfRecords)
                .containsExactly(12L, 12L);
    }

    ECSSystemTestTask createTask() {
        IngestRandomData ingestData = new IngestRandomData(instanceProperties, systemTestProperties, stsClientV2, hadoopConf, tempDir.toString());
        return new ECSSystemTestTask(systemTestProperties, sqsClientV2, job -> {
            try {
                ingestData.run(job);
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        });
    }

    void sendJob(SystemTestDataGenerationJob job) {
        sqsClientV2.sendMessage(builder -> builder
                .queueUrl(systemTestProperties.get(SYSTEM_TEST_JOBS_QUEUE_URL))
                .messageBody(new SystemTestDataGenerationJobSerDe().toJson(job)));
    }

    SleeperClient createSleeperClient() {
        return SleeperClient.builder()
                .instanceProperties(instanceProperties)
                .awsClients(clients -> clients
                        .s3Client(s3ClientV2)
                        .dynamoClient(dynamoClientV2)
                        .sqsClient(sqsClientV2))
                .hadoopConf(hadoopConf)
                .build();
    }

}
