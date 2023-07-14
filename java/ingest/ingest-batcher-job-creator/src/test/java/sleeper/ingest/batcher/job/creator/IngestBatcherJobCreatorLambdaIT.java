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
package sleeper.ingest.batcher.job.creator;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClientBuilder;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.ReceiveMessageRequest;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.localstack.LocalStackContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import sleeper.configuration.properties.instance.InstanceProperties;
import sleeper.configuration.properties.instance.InstanceProperty;
import sleeper.configuration.properties.table.TableProperties;
import sleeper.configuration.properties.table.TablePropertiesProvider;
import sleeper.core.CommonTestConstants;
import sleeper.ingest.batcher.FileIngestRequest;
import sleeper.ingest.batcher.IngestBatcherStore;
import sleeper.ingest.batcher.store.DynamoDBIngestBatcherStore;
import sleeper.ingest.batcher.store.DynamoDBIngestBatcherStoreCreator;
import sleeper.ingest.job.IngestJob;
import sleeper.ingest.job.IngestJobSerDe;

import java.time.Instant;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.configuration.properties.instance.DefaultProperty.DEFAULT_INGEST_BATCHER_MIN_JOB_SIZE;
import static sleeper.configuration.properties.InstancePropertiesTestHelper.createTestInstanceProperties;
import static sleeper.configuration.properties.instance.SystemDefinedInstanceProperty.CONFIG_BUCKET;
import static sleeper.configuration.properties.instance.SystemDefinedInstanceProperty.INGEST_JOB_QUEUE_URL;
import static sleeper.configuration.properties.table.TablePropertiesTestHelper.createTestTableProperties;
import static sleeper.configuration.properties.table.TableProperty.TABLE_NAME;
import static sleeper.core.schema.SchemaTestHelper.schemaWithKey;
import static sleeper.ingest.batcher.testutil.IngestBatcherTestHelper.jobIdSupplier;
import static sleeper.ingest.batcher.testutil.IngestBatcherTestHelper.timeSupplier;

@Testcontainers
public class IngestBatcherJobCreatorLambdaIT {

    @Container
    public static LocalStackContainer localStackContainer = new LocalStackContainer(DockerImageName.parse(CommonTestConstants.LOCALSTACK_DOCKER_IMAGE)).withServices(
            LocalStackContainer.Service.S3, LocalStackContainer.Service.SQS, LocalStackContainer.Service.DYNAMODB);

    private final AmazonS3 s3 = createS3Client();
    private final AmazonSQS sqs = createSQSClient();
    private final AmazonDynamoDB dynamoDB = createDynamoClient();
    private final InstanceProperties instanceProperties = createTestInstanceProperties(s3, properties -> {
        properties.set(INGEST_JOB_QUEUE_URL, "test-ingest-job-queue");
        properties.set(DEFAULT_INGEST_BATCHER_MIN_JOB_SIZE, "0");
    });
    private final TableProperties tableProperties = createTestTableProperties(instanceProperties, schemaWithKey("key"), s3);
    private final IngestBatcherStore store = new DynamoDBIngestBatcherStore(dynamoDB, instanceProperties,
            new TablePropertiesProvider(s3, instanceProperties));

    private IngestBatcherJobCreatorLambda lambdaWithTimesAndJobIds(List<Instant> times, List<String> jobIds) {
        return new IngestBatcherJobCreatorLambda(
                s3, instanceProperties.get(CONFIG_BUCKET),
                sqs, dynamoDB, timeSupplier(times), jobIdSupplier(jobIds));
    }

    @BeforeEach
    void setUp() {
        DynamoDBIngestBatcherStoreCreator.create(instanceProperties, dynamoDB);
        sqs.createQueue(instanceProperties.get(INGEST_JOB_QUEUE_URL));
    }

    @AfterEach
    void tearDown() {
        DynamoDBIngestBatcherStoreCreator.tearDown(instanceProperties, dynamoDB);
        sqs.deleteQueue(instanceProperties.get(INGEST_JOB_QUEUE_URL));
    }

    @Test
    void shouldSendOneFileFromStore() {
        // Given
        store.addFile(FileIngestRequest.builder()
                .file("some-bucket/some-file.parquet")
                .tableName(tableProperties.get(TABLE_NAME))
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
                        .tableName(tableProperties.get(TABLE_NAME))
                        .files("some-bucket/some-file.parquet")
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
        return AmazonS3ClientBuilder.standard()
                .withEndpointConfiguration(localStackContainer.getEndpointConfiguration(LocalStackContainer.Service.S3))
                .withCredentials(localStackContainer.getDefaultCredentialsProvider())
                .build();
    }

    private static AmazonSQS createSQSClient() {
        return AmazonSQSClientBuilder.standard()
                .withEndpointConfiguration(localStackContainer.getEndpointConfiguration(LocalStackContainer.Service.SQS))
                .withCredentials(localStackContainer.getDefaultCredentialsProvider())
                .build();
    }

    private static AmazonDynamoDB createDynamoClient() {
        return AmazonDynamoDBClientBuilder.standard()
                .withCredentials(localStackContainer.getDefaultCredentialsProvider())
                .withEndpointConfiguration(localStackContainer.getEndpointConfiguration(LocalStackContainer.Service.DYNAMODB))
                .build();
    }
}
