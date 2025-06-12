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
package sleeper.ingest.batcher.submitter;

import com.github.tomakehurst.wiremock.junit5.WireMockRuntimeInfo;
import com.github.tomakehurst.wiremock.junit5.WireMockTest;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.CreateQueueRequest;
import software.amazon.awssdk.services.sqs.model.Message;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageRequest;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageResponse;

import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.table.TableProperties;
import sleeper.core.properties.testutils.FixedTablePropertiesProvider;
import sleeper.core.table.InMemoryTableIndex;
import sleeper.core.table.TableIndex;
import sleeper.core.table.TableStatusTestHelper;
import sleeper.ingest.batcher.core.IngestBatcherStore;
import sleeper.ingest.batcher.core.IngestBatcherTrackedFile;
import sleeper.ingest.batcher.store.DynamoDBIngestBatcherStore;
import sleeper.ingest.batcher.store.DynamoDBIngestBatcherStoreCreator;

import java.nio.file.Path;
import java.time.Instant;
import java.util.List;
import java.util.UUID;

import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static com.github.tomakehurst.wiremock.client.WireMock.post;
import static com.github.tomakehurst.wiremock.client.WireMock.stubFor;
import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.INGEST_BATCHER_SUBMIT_DLQ_URL;
import static sleeper.core.properties.table.TableProperty.TABLE_ID;
import static sleeper.core.properties.table.TableProperty.TABLE_NAME;
import static sleeper.core.properties.testutils.InstancePropertiesTestHelper.createTestInstanceProperties;
import static sleeper.core.properties.testutils.TablePropertiesTestHelper.createTestTableProperties;
import static sleeper.core.schema.SchemaTestHelper.createSchemaWithKey;
import static sleeper.localstack.test.WiremockAwsV2ClientHelper.wiremockAwsV2Client;

public class IngestBatcherSubmitterLambdaWireMockIT {

    private S3Client s3Client;
    private SqsClient sqsClient;
    private DynamoDbClient dynamoClient;
    private static final Instant RECEIVED_TIME = Instant.parse("2023-06-16T10:57:00Z");
    private final String testBucket = UUID.randomUUID().toString();
    private final InstanceProperties instanceProperties = createTestInstanceProperties();
    private final TableIndex tableIndex = new InMemoryTableIndex();
    private static final String TEST_TABLE_ID = "test-table-id";
    private final TableProperties tableProperties = createTestTableProperties(instanceProperties, createSchemaWithKey("key"));

    public static final String WIREMOCK_ACCESS_KEY = "wiremock-access-key";
    public static final String WIREMOCK_SECRET_KEY = "wiremock-secret-key";

    //In Progress
    //Been trying to understand how to make the s3Client call, I think I'm on the right track here but could be wrong
    //I've taken code from BuildUptimeLambdaIt and WiremockTestHelper to create a mocked object to go in the builder
    //Those didnt mock the s3Client though so it may not work for it
    //I havent found how to add a retry mechanic into the sqs for this test, over riding the default behaviour.

    @DisplayName("Network Error test")
    @WireMockTest
    class NetworkErrorTest {

        @BeforeEach
        void setUp(WireMockRuntimeInfo runtimeInfo) {
            s3Client = wiremockAwsV2Client(runtimeInfo, S3Client.builder());
            sqsClient = wiremockAwsV2Client(runtimeInfo, SqsClient.builder());
            dynamoClient = wiremockAwsV2Client(runtimeInfo, DynamoDbClient.builder());

            // find inmemory version
            tableIndex.create(TableStatusTestHelper.uniqueIdAndName(TEST_TABLE_ID, "test-table"));
            DynamoDBIngestBatcherStoreCreator.create(instanceProperties, dynamoClient);
            instanceProperties.set(INGEST_BATCHER_SUBMIT_DLQ_URL, createSqsQueueGetUrl());
            tableProperties.set(TABLE_ID, TEST_TABLE_ID);
            tableProperties.set(TABLE_NAME, "test-table");

        }

        @Test
        void shouldHandleNetworkErrorCorrectly(WireMockRuntimeInfo runtimeInfo) {
            uploadFileToS3("test-file-1.parquet");
            //Mock Network error

            final IngestBatcherSubmitterLambda lambda = new IngestBatcherSubmitterLambda(
                    batcherStore(), instanceProperties, tableIndex,
                    new IngestBatcherSubmitDeadLetterQueue(instanceProperties, sqsClient),
                    s3Client);
            stubFor(post("/").willReturn(aResponse().withStatus(200)));
            //Setup custom retry sqs for tests

            //Send job
            String json = "{" +
                    "\"files\":[\"" + testBucket + "/test-file-1.parquet\"]," +
                    "\"tableName\":\"test-table\"" +
                    "}";

            // When

            lambda.handleMessage(json, RECEIVED_TIME);

            //Assert retry attempted
            assertThat(batcherStore().getAllFilesNewestFirst())
                    .containsExactly(
                            fileRequest(testBucket + "/test-file-1.parquet"));
            assertThat(receiveDeadLetters()).isEmpty();

        }

        private void uploadFileToS3(String filePath) {
            s3Client.putObject(PutObjectRequest.builder()
                    .bucket(testBucket)
                    .key("test")
                    .build(),
                    RequestBody.fromFile(Path.of(filePath)));
        }

        private static IngestBatcherTrackedFile fileRequest(String filePath) {
            return IngestBatcherTrackedFile.builder()
                    .file(filePath)
                    .fileSizeBytes(4)
                    .tableId(TEST_TABLE_ID)
                    .receivedTime(RECEIVED_TIME).build();
        }

        private List<String> receiveDeadLetters() {
            ReceiveMessageResponse result = sqsClient.receiveMessage(ReceiveMessageRequest.builder()
                    .queueUrl(instanceProperties.get(INGEST_BATCHER_SUBMIT_DLQ_URL))
                    .maxNumberOfMessages(10)
                    .waitTimeSeconds(1)
                    .build());
            return result.messages().stream()
                    .map(Message::body)
                    .toList();
        }

        // find inmemory version
        private IngestBatcherStore batcherStore() {
            return new DynamoDBIngestBatcherStore(dynamoClient, instanceProperties,
                    new FixedTablePropertiesProvider(tableProperties));
        }

        private String createSqsQueueGetUrl() {
            return sqsClient.createQueue(CreateQueueRequest.builder()
                    .queueName(UUID.randomUUID().toString())
                    .build())
                    .queueUrl();
        }

    }
}
