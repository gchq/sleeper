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
package sleeper.ingest.batcher.job.creator;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import sleeper.configuration.properties.S3InstanceProperties;
import sleeper.configuration.properties.S3TableProperties;
import sleeper.configuration.table.index.DynamoDBTableIndexCreator;
import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.instance.InstanceProperty;
import sleeper.core.properties.table.TableProperties;
import sleeper.ingest.batcher.core.IngestBatcherStore;
import sleeper.ingest.batcher.core.IngestBatcherTrackedFile;
import sleeper.ingest.batcher.store.DynamoDBIngestBatcherStore;
import sleeper.ingest.batcher.store.DynamoDBIngestBatcherStoreCreator;
import sleeper.ingest.core.job.IngestJob;
import sleeper.ingest.core.job.IngestJobSerDe;
import sleeper.localstack.test.LocalStackTestBase;

import java.time.Instant;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.CONFIG_BUCKET;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.INGEST_JOB_QUEUE_URL;
import static sleeper.core.properties.instance.TableDefaultProperty.DEFAULT_INGEST_BATCHER_INGEST_QUEUE;
import static sleeper.core.properties.instance.TableDefaultProperty.DEFAULT_INGEST_BATCHER_MIN_JOB_SIZE;
import static sleeper.core.properties.model.IngestQueue.STANDARD_INGEST;
import static sleeper.core.properties.table.TableProperty.TABLE_ID;
import static sleeper.core.properties.testutils.InstancePropertiesTestHelper.createTestInstanceProperties;
import static sleeper.core.properties.testutils.TablePropertiesTestHelper.createTestTableProperties;
import static sleeper.core.schema.SchemaTestHelper.createSchemaWithKey;
import static sleeper.ingest.batcher.core.testutil.IngestBatcherTestHelper.jobIdSupplier;
import static sleeper.ingest.batcher.core.testutil.IngestBatcherTestHelper.timeSupplier;

public class IngestBatcherJobCreatorLambdaIT extends LocalStackTestBase {

    private final InstanceProperties instanceProperties = createTestInstanceProperties();
    private final TableProperties tableProperties = createTestTableProperties(instanceProperties, createSchemaWithKey("key"));

    @BeforeEach
    void setUp() {
        createBucket(instanceProperties.get(CONFIG_BUCKET));
        DynamoDBTableIndexCreator.create(dynamoClient, instanceProperties);
        DynamoDBIngestBatcherStoreCreator.create(instanceProperties, dynamoClient);
        instanceProperties.set(DEFAULT_INGEST_BATCHER_INGEST_QUEUE, STANDARD_INGEST.toString());
        instanceProperties.set(DEFAULT_INGEST_BATCHER_MIN_JOB_SIZE, "0");
        instanceProperties.set(INGEST_JOB_QUEUE_URL, createSqsQueueGetUrl());
        S3InstanceProperties.saveToS3(s3Client, instanceProperties);
        S3TableProperties.createStore(instanceProperties, s3Client, dynamoClient).save(tableProperties);
    }

    @Test
    void shouldSendOneFileFromStore() {
        // Given
        batcherStore().addFile(IngestBatcherTrackedFile.builder()
                .file("some-bucket/some-file.parquet")
                .tableId(tableProperties.get(TABLE_ID))
                .fileSizeBytes(1024)
                .receivedTime(Instant.parse("2023-05-25T14:43:00Z"))
                .build());

        // When
        lambdaWithTimesAndJobIds(
                List.of(Instant.parse("2023-05-25T14:44:00Z")),
                List.of("test-job-id"))
                .batchFiles();

        // Then
        assertThat(receiveJobs(INGEST_JOB_QUEUE_URL))
                .containsExactly(IngestJob.builder()
                        .id("test-job-id")
                        .tableId(tableProperties.get(TABLE_ID))
                        .files(List.of("some-bucket/some-file.parquet"))
                        .build());
    }

    private List<IngestJob> receiveJobs(InstanceProperty queueProperty) {
        return receiveMessages(instanceProperties.get(queueProperty))
                .map(new IngestJobSerDe()::fromJson)
                .toList();
    }

    private IngestBatcherStore batcherStore() {
        return new DynamoDBIngestBatcherStore(dynamoClient, instanceProperties,
                S3TableProperties.createProvider(instanceProperties, s3Client, dynamoClient));
    }

    private IngestBatcherJobCreatorLambda lambdaWithTimesAndJobIds(List<Instant> times, List<String> jobIds) {
        return new IngestBatcherJobCreatorLambda(
                s3Client, instanceProperties.get(CONFIG_BUCKET),
                sqsClient, dynamoClient, timeSupplier(times), jobIdSupplier(jobIds));
    }
}
