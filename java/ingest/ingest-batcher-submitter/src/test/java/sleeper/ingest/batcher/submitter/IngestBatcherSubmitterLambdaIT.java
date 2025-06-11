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
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
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
import sleeper.localstack.test.LocalStackTestBase;

import java.net.URI;
import java.time.Instant;
import java.util.List;
import java.util.UUID;

import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static com.github.tomakehurst.wiremock.client.WireMock.post;
import static com.github.tomakehurst.wiremock.client.WireMock.stubFor;
import static net.javacrumbs.jsonunit.assertj.JsonAssertions.assertThatJson;
import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.INGEST_BATCHER_SUBMIT_DLQ_URL;
import static sleeper.core.properties.table.TableProperty.TABLE_ID;
import static sleeper.core.properties.table.TableProperty.TABLE_NAME;
import static sleeper.core.properties.testutils.InstancePropertiesTestHelper.createTestInstanceProperties;
import static sleeper.core.properties.testutils.TablePropertiesTestHelper.createTestTableProperties;
import static sleeper.core.schema.SchemaTestHelper.createSchemaWithKey;

public class IngestBatcherSubmitterLambdaIT extends LocalStackTestBase {

    private static final String TEST_TABLE_ID = "test-table-id";
    private static final Instant RECEIVED_TIME = Instant.parse("2023-06-16T10:57:00Z");
    private final String testBucket = UUID.randomUUID().toString();
    private final InstanceProperties instanceProperties = createTestInstanceProperties();
    private final TableIndex tableIndex = new InMemoryTableIndex();
    private final TableProperties tableProperties = createTestTableProperties(instanceProperties, createSchemaWithKey("key"));
    public static final String WIREMOCK_ACCESS_KEY = "wiremock-access-key";
    public static final String WIREMOCK_SECRET_KEY = "wiremock-secret-key";

    @BeforeEach
    void setup() {
        tableIndex.create(TableStatusTestHelper.uniqueIdAndName(TEST_TABLE_ID, "test-table"));
        createBucket(testBucket);
        DynamoDBIngestBatcherStoreCreator.create(instanceProperties, dynamoClientV2);
        instanceProperties.set(INGEST_BATCHER_SUBMIT_DLQ_URL, createSqsQueueGetUrl());
        tableProperties.set(TABLE_ID, TEST_TABLE_ID);
        tableProperties.set(TABLE_NAME, "test-table");
    }

    //In Progress
    //Been trying to understand how to make the s3Client call, I think I'm on the right track here but could be wrong
    //I've taken code from BuildUptimeLambdaIt and WiremockTestHelper to create a mocked object to go in the builder
    //Those didnt mock the s3Client though so it may not work for it
    //I havent found how to add a retry mechanic into the sqs for this test, over riding the default behaviour.
    @Nested
    @DisplayName("Network Error test")
    @WireMockTest
    class NetworkErrorTest {
        @Test
        void shouldHandleNetworkErrorCorrectly(WireMockRuntimeInfo runtimeInfo) {
            uploadFileToS3("test-file-1.parquet");
            //Mock Network error

            S3Client client = S3Client.builder().endpointOverride(URI.create(runtimeInfo.getHttpBaseUrl())) //This code was taken from BuildUptimeLambdaIt and WiremockTestHelper
                    .region(Region.US_EAST_1)
                    .credentialsProvider(StaticCredentialsProvider.create(
                            AwsBasicCredentials.create(WIREMOCK_ACCESS_KEY, WIREMOCK_SECRET_KEY)))
                    .build();

            final IngestBatcherSubmitterLambda lambda = new IngestBatcherSubmitterLambda(
                    batcherStore(), instanceProperties, tableIndex,
                    new IngestBatcherSubmitDeadLetterQueue(instanceProperties, sqsClientV2),
                    client);
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
    }

    @Nested
    @DisplayName("Store files by their full path")
    class StoreFileByFullPath {
        @Test
        void shouldStoreFileIngestRequestFromJson() {
            // Given
            uploadFileToS3("test-file-1.parquet");
            String json = "{" +
                    "\"files\":[\"" + testBucket + "/test-file-1.parquet\"]," +
                    "\"tableName\":\"test-table\"" +
                    "}";

            // When
            lambda().handleMessage(json, RECEIVED_TIME);

            // Then
            assertThat(batcherStore().getAllFilesNewestFirst())
                    .containsExactly(
                            fileRequest(testBucket + "/test-file-1.parquet"));
            assertThat(receiveDeadLetters()).isEmpty();
        }

        @Test
        void shouldStoreFileByPathNotIncludingItsExtension() {
            // Given
            uploadFileToS3("test-file-1.parquet");
            String json = "{" +
                    "\"files\":[\"" + testBucket + "/test-file-1\"]," +
                    "\"tableName\":\"test-table\"" +
                    "}";

            // When
            lambda().handleMessage(json, RECEIVED_TIME);

            // Then
            assertThat(batcherStore().getAllFilesNewestFirst())
                    .containsExactly(
                            fileRequest(testBucket + "/test-file-1.parquet"));
            assertThat(receiveDeadLetters()).isEmpty();
        }

        @Test
        void shouldStoreFileThatHasNoExtension() {
            // Given
            uploadFileToS3("test-file-1");
            String json = "{" +
                    "\"files\":[\"" + testBucket + "/test-file-1\"]," +
                    "\"tableName\":\"test-table\"" +
                    "}";

            // When
            lambda().handleMessage(json, RECEIVED_TIME);

            // Then
            assertThat(batcherStore().getAllFilesNewestFirst())
                    .containsExactly(
                            fileRequest(testBucket + "/test-file-1"));
            assertThat(receiveDeadLetters()).isEmpty();
        }

        @Test
        void shouldStoreFileWithCustomSize() {
            // Given
            putObject(testBucket, "test-file-1.parquet", "a".repeat(123));
            String json = "{" +
                    "\"files\":[\"" + testBucket + "/test-file-1.parquet\"]," +
                    "\"tableName\":\"test-table\"" +
                    "}";

            // When
            lambda().handleMessage(json, RECEIVED_TIME);

            // Then
            assertThat(batcherStore().getAllFilesNewestFirst())
                    .containsExactly(IngestBatcherTrackedFile.builder()
                            .file(testBucket + "/test-file-1.parquet")
                            .fileSizeBytes(123)
                            .tableId(TEST_TABLE_ID)
                            .receivedTime(RECEIVED_TIME)
                            .build());
        }

        @Test
        void shouldStoreFileInDirectory() {
            // Given
            uploadFileToS3("test-directory/test-file-1.parquet");
            String json = "{" +
                    "\"files\":[\"" + testBucket + "/test-directory/test-file-1.parquet\"]," +
                    "\"tableName\":\"test-table\"" +
                    "}";

            // When
            lambda().handleMessage(json, RECEIVED_TIME);

            // Then
            assertThat(batcherStore().getAllFilesNewestFirst())
                    .containsExactly(
                            fileRequest(testBucket + "/test-directory/test-file-1.parquet"));
        }

        @Test
        void shouldStoreFileInNestedDirectory() {
            // Given
            uploadFileToS3("test-directory/another-test-directory/test-file-1.parquet");
            String json = "{" +
                    "\"files\":[\"" + testBucket + "/test-directory/another-test-directory/test-file-1.parquet\"]," +
                    "\"tableName\":\"test-table\"" +
                    "}";

            // When
            lambda().handleMessage(json, RECEIVED_TIME);

            // Then
            assertThat(batcherStore().getAllFilesNewestFirst())
                    .containsExactly(
                            fileRequest(testBucket + "/test-directory/another-test-directory/test-file-1.parquet"));
        }
    }

    @Nested
    @DisplayName("Store files by a prefix/directory")
    class StoreFilesByPrefix {
        @Test
        void shouldStoreOneFileInDirectory() {
            // Given
            uploadFileToS3("test-directory/test-file-1.parquet");
            String json = "{" +
                    "\"files\":[\"" + testBucket + "/test-directory\"]," +
                    "\"tableName\":\"test-table\"" +
                    "}";

            // When
            lambda().handleMessage(json, RECEIVED_TIME);

            // Then
            assertThat(batcherStore().getAllFilesNewestFirst())
                    .containsExactly(
                            fileRequest(testBucket + "/test-directory/test-file-1.parquet"));
        }

        @Test
        void shouldStoreMultipleFilesInDirectory() {
            // Given
            uploadFileToS3("test-directory/test-file-1.parquet");
            uploadFileToS3("test-directory/test-file-2.parquet");
            String json = "{" +
                    "\"files\":[\"" + testBucket + "/test-directory\"]," +
                    "\"tableName\":\"test-table\"" +
                    "}";

            // When
            lambda().handleMessage(json, RECEIVED_TIME);

            // Then
            assertThat(batcherStore().getAllFilesNewestFirst())
                    .containsExactly(
                            fileRequest(testBucket + "/test-directory/test-file-1.parquet"),
                            fileRequest(testBucket + "/test-directory/test-file-2.parquet"));
        }

        @Test
        void shouldStoreFileByNestedDirectory() {
            // Given
            uploadFileToS3("test-directory/another-test-directory/test-file-1.parquet");
            String json = "{" +
                    "\"files\":[\"" + testBucket + "/test-directory/another-test-directory\"]," +
                    "\"tableName\":\"test-table\"" +
                    "}";

            // When
            lambda().handleMessage(json, RECEIVED_TIME);

            // Then
            assertThat(batcherStore().getAllFilesNewestFirst())
                    .containsExactly(
                            fileRequest(testBucket + "/test-directory/another-test-directory/test-file-1.parquet"));
        }

        @Test
        void shouldStoreFileInNestedDirectory() {
            // Given
            uploadFileToS3("test-directory/nested/test-file-1.parquet");
            String json = "{" +
                    "\"files\":[\"" + testBucket + "/test-directory\"]," +
                    "\"tableName\":\"test-table\"" +
                    "}";

            // When
            lambda().handleMessage(json, RECEIVED_TIME);

            // Then
            assertThat(batcherStore().getAllFilesNewestFirst())
                    .containsExactly(
                            fileRequest(testBucket + "/test-directory/nested/test-file-1.parquet"));
        }

        @Test
        void shouldStoreFileAcrossMultipleDirectories() {
            // Given
            uploadFileToS3("test-directory/nested-1/test-file-1.parquet");
            uploadFileToS3("test-directory/nested-2/test-file-2.parquet");
            String json = "{" +
                    "\"files\":[\"" + testBucket + "/test-directory\"]," +
                    "\"tableName\":\"test-table\"" +
                    "}";

            // When
            lambda().handleMessage(json, RECEIVED_TIME);

            // Then
            assertThat(batcherStore().getAllFilesNewestFirst())
                    .containsExactly(
                            fileRequest(testBucket + "/test-directory/nested-1/test-file-1.parquet"),
                            fileRequest(testBucket + "/test-directory/nested-2/test-file-2.parquet"));
        }

        @Test
        void shouldStoreAllFilesByBucketName() {
            // Given
            uploadFileToS3("test-file-1.parquet");
            uploadFileToS3("test-file-2.parquet");
            String json = "{" +
                    "\"files\":[\"" + testBucket + "\"]," +
                    "\"tableName\":\"test-table\"" +
                    "}";

            // When
            lambda().handleMessage(json, RECEIVED_TIME);

            // Then
            assertThat(batcherStore().getAllFilesNewestFirst())
                    .containsExactly(
                            fileRequest(testBucket + "/test-file-1.parquet"),
                            fileRequest(testBucket + "/test-file-2.parquet"));
        }
    }

    @Nested
    @DisplayName("Store files from multiple file paths")
    class StoreFilesFromMultipleFilePaths {
        @Test
        void shouldStoreFilesWhenJsonHasMultipleFilePaths() {
            // Given
            uploadFileToS3("test-file-1.parquet");
            uploadFileToS3("test-file-2.parquet");
            String json = "{" +
                    "\"files\":[" +
                    "\"" + testBucket + "/test-file-1.parquet\"," +
                    "\"" + testBucket + "/test-file-2.parquet\"" +
                    "]," +
                    "\"tableName\":\"test-table\"" +
                    "}";

            // When
            lambda().handleMessage(json, RECEIVED_TIME);

            // Then
            assertThat(batcherStore().getAllFilesNewestFirst())
                    .containsExactly(
                            fileRequest(testBucket + "/test-file-1.parquet"),
                            fileRequest(testBucket + "/test-file-2.parquet"));
        }

        @Test
        void shouldNotStoreFilesIfFileNotFound() {
            // Given
            uploadFileToS3("test-file-2.parquet");
            String json = "{" +
                    "\"files\":[" +
                    "\"" + testBucket + "/not-found.parquet\"," +
                    "\"" + testBucket + "/test-file-2.parquet\"" +
                    "]," +
                    "\"tableName\":\"test-table\"" +
                    "}";

            // When
            lambda().handleMessage(json, RECEIVED_TIME);

            // Then
            assertThat(batcherStore().getAllFilesNewestFirst())
                    .isEmpty();
        }
    }

    @Nested
    @DisplayName("Handle errors in messages")
    class HandleErrorsInMessage {

        @Test
        void shouldLogMessageWithInvalidJsonAndSendToDeadLetterQueue() {
            // Given
            String json = "{";

            // When
            lambda().handleMessage(json, RECEIVED_TIME);

            // Then
            assertThat(batcherStore().getAllFilesNewestFirst()).isEmpty();
            assertThat(receiveDeadLetters()).singleElement().isEqualTo(json);
        }

        @Test
        void shouldLogMessageIfTableDoesNotExistAndSendToDeadLetterQueue() {
            // Given
            String json = "{" +
                    "\"files\":[\"" + testBucket + "/test-file-1.parquet\"]," +
                    "\"tableName\":\"not-a-table\"" +
                    "}";

            // When
            lambda().handleMessage(json, RECEIVED_TIME);

            // Then
            assertThat(batcherStore().getAllFilesNewestFirst()).isEmpty();
            assertThat(receiveDeadLetters())
                    .singleElement()
                    .satisfies(deadLetter -> assertThatJson(deadLetter).isEqualTo(json));
        }

        @Test
        void shouldLogMessageIfFileDoesNotExistAndSendToDeadLetterQueue() {
            // Given
            String json = "{" +
                    "\"files\":[\"" + testBucket + "/not-exists.parquet\"]," +
                    "\"tableName\":\"test-table\"" +
                    "}";

            // When
            lambda().handleMessage(json, RECEIVED_TIME);

            // Then
            assertThat(batcherStore().getAllFilesNewestFirst()).isEmpty();
            assertThat(receiveDeadLetters())
                    .singleElement()
                    .satisfies(deadLetter -> assertThatJson(deadLetter).isEqualTo(json));
        }

        @Test
        void shouldLogMessageIfDirectoryDoesNotExistAndSendToDeadLetterQueue() {
            // Given
            String json = "{" +
                    "\"files\":[\"" + testBucket + "/not-exists\"]," +
                    "\"tableName\":\"test-table\"" +
                    "}";

            // When
            lambda().handleMessage(json, RECEIVED_TIME);

            // Then
            assertThat(batcherStore().getAllFilesNewestFirst()).isEmpty();
            assertThat(receiveDeadLetters())
                    .singleElement()
                    .satisfies(deadLetter -> assertThatJson(deadLetter).isEqualTo(json));
        }

        @Test
        void shouldIgnoreMessageIfFilesNotProvided() {
            // Given
            String json = "{" +
                    "\"files\":[]," +
                    "\"tableName\":\"test-table\"" +
                    "}";

            // When
            lambda().handleMessage(json, RECEIVED_TIME);

            // Then
            assertThat(batcherStore().getAllFilesNewestFirst()).isEmpty();
            assertThat(receiveDeadLetters()).isEmpty();
        }
    }

    protected void uploadFileToS3(String filePath) {
        s3Client.putObject(testBucket, filePath, "test");
    }

    protected static IngestBatcherTrackedFile fileRequest(String filePath) {
        return IngestBatcherTrackedFile.builder()
                .file(filePath)
                .fileSizeBytes(4)
                .tableId(TEST_TABLE_ID)
                .receivedTime(RECEIVED_TIME).build();
    }

    private List<String> receiveDeadLetters() {
        ReceiveMessageResponse result = sqsClientV2.receiveMessage(ReceiveMessageRequest.builder()
                .queueUrl(instanceProperties.get(INGEST_BATCHER_SUBMIT_DLQ_URL))
                .maxNumberOfMessages(10)
                .waitTimeSeconds(1)
                .build());
        return result.messages().stream()
                .map(Message::body)
                .toList();
    }

    private IngestBatcherSubmitterLambda lambda() {
        return new IngestBatcherSubmitterLambda(
                batcherStore(), instanceProperties, tableIndex,
                new IngestBatcherSubmitDeadLetterQueue(instanceProperties, sqsClientV2),
                s3ClientV2);
    }

    private IngestBatcherStore batcherStore() {
        return new DynamoDBIngestBatcherStore(dynamoClientV2, instanceProperties,
                new FixedTablePropertiesProvider(tableProperties));
    }
}
