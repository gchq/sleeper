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
package sleeper.ingest.batcher.job.creator;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClientBuilder;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.ReceiveMessageRequest;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.localstack.LocalStackContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import sleeper.configuration.properties.S3InstanceProperties;
import sleeper.configuration.properties.S3TableProperties;
import sleeper.configuration.table.index.DynamoDBTableIndexCreator;
import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.instance.InstanceProperty;
import sleeper.core.properties.table.TableProperties;
import sleeper.ingest.batcher.core.FileIngestRequest;
import sleeper.ingest.batcher.core.IngestBatcherStore;
import sleeper.ingest.batcher.store.DynamoDBIngestBatcherStore;
import sleeper.ingest.batcher.store.DynamoDBIngestBatcherStoreCreator;
import sleeper.ingest.core.job.IngestJob;
import sleeper.ingest.core.job.IngestJobSerDe;
import sleeper.localstack.test.SleeperLocalStackContainer;

import java.time.Instant;
import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.CONFIG_BUCKET;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.INGEST_JOB_QUEUE_URL;
import static sleeper.core.properties.instance.TableDefaultProperty.DEFAULT_INGEST_BATCHER_INGEST_QUEUE;
import static sleeper.core.properties.instance.TableDefaultProperty.DEFAULT_INGEST_BATCHER_MIN_JOB_SIZE;
import static sleeper.core.properties.table.TableProperty.TABLE_ID;
import static sleeper.core.properties.testutils.InstancePropertiesTestHelper.createTestInstanceProperties;
import static sleeper.core.properties.testutils.TablePropertiesTestHelper.createTestTableProperties;
import static sleeper.core.properties.validation.IngestQueue.STANDARD_INGEST;
import static sleeper.core.schema.SchemaTestHelper.schemaWithKey;
import static sleeper.ingest.batcher.core.testutil.IngestBatcherTestHelper.jobIdSupplier;
import static sleeper.ingest.batcher.core.testutil.IngestBatcherTestHelper.timeSupplier;
import static sleeper.localstack.test.LocalStackAwsV1ClientHelper.buildAwsV1Client;

@Testcontainers
public class IngestBatcherJobCreatorLambdaIT {

    @Container
    public static LocalStackContainer localStackContainer = SleeperLocalStackContainer.create(
            LocalStackContainer.Service.S3, LocalStackContainer.Service.SQS, LocalStackContainer.Service.DYNAMODB);

    private final AmazonS3 s3 = createS3Client();
    private final AmazonSQS sqs = createSQSClient();
    private final AmazonDynamoDB dynamoDB = createDynamoClient();
    private final InstanceProperties instanceProperties = createTestInstanceProperties();
    private final TableProperties tableProperties = createTestTableProperties(instanceProperties, schemaWithKey("key"));

    @BeforeEach
    void setUp() {
        s3.createBucket(instanceProperties.get(CONFIG_BUCKET));
        DynamoDBTableIndexCreator.create(dynamoDB, instanceProperties);
        DynamoDBIngestBatcherStoreCreator.create(instanceProperties, dynamoDB);
        instanceProperties.set(DEFAULT_INGEST_BATCHER_INGEST_QUEUE, STANDARD_INGEST.toString());
        instanceProperties.set(DEFAULT_INGEST_BATCHER_MIN_JOB_SIZE, "0");
        instanceProperties.set(INGEST_JOB_QUEUE_URL, sqs.createQueue(UUID.randomUUID().toString()).getQueueUrl());
        S3InstanceProperties.saveToS3(s3, instanceProperties);
        S3TableProperties.createStore(instanceProperties, s3, dynamoDB).save(tableProperties);
    }

    @Test
    void shouldSendOneFileFromStore() {
        // Given
        batcherStore().addFile(FileIngestRequest.builder()
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
        assertThat(consumeQueueMessages(INGEST_JOB_QUEUE_URL))
                .extracting(this::readJobMessage)
                .containsExactly(IngestJob.builder()
                        .id("test-job-id")
                        .tableId(tableProperties.get(TABLE_ID))
                        .files(List.of("some-bucket/some-file.parquet"))
                        .build());
    }

    private List<Message> consumeQueueMessages(InstanceProperty queueProperty) {
        return sqs.receiveMessage(new ReceiveMessageRequest()
                .withQueueUrl(instanceProperties.get(queueProperty))
                .withWaitTimeSeconds(1)
                .withMaxNumberOfMessages(10))
                .getMessages();
    }

    private IngestJob readJobMessage(Message message) {
        return new IngestJobSerDe().fromJson(message.getBody());
    }

    private static AmazonS3 createS3Client() {
        return buildAwsV1Client(localStackContainer, LocalStackContainer.Service.S3, AmazonS3ClientBuilder.standard());
    }

    private static AmazonSQS createSQSClient() {
        return buildAwsV1Client(localStackContainer, LocalStackContainer.Service.SQS, AmazonSQSClientBuilder.standard());
    }

    private static AmazonDynamoDB createDynamoClient() {
        return buildAwsV1Client(localStackContainer, LocalStackContainer.Service.DYNAMODB, AmazonDynamoDBClientBuilder.standard());
    }

    private IngestBatcherStore batcherStore() {
        return new DynamoDBIngestBatcherStore(dynamoDB, instanceProperties,
                S3TableProperties.createProvider(instanceProperties, s3, dynamoDB));
    }

    private IngestBatcherJobCreatorLambda lambdaWithTimesAndJobIds(List<Instant> times, List<String> jobIds) {
        return new IngestBatcherJobCreatorLambda(
                s3, instanceProperties.get(CONFIG_BUCKET),
                sqs, dynamoDB, timeSupplier(times), jobIdSupplier(jobIds));
    }
}
