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

package sleeper.ingest.batcher.submitter;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.localstack.LocalStackContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import sleeper.core.CommonTestConstants;
import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.table.InMemoryTableIndex;
import sleeper.core.table.TableIndex;
import sleeper.core.table.TableStatusTestHelper;
import sleeper.ingest.batcher.core.FileIngestRequest;
import sleeper.ingest.batcher.core.IngestBatcherStore;
import sleeper.ingest.batcher.core.testutil.InMemoryIngestBatcherStore;

import java.time.Instant;

import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.configuration.testutils.LocalStackAwsV1ClientHelper.buildAwsV1Client;
import static sleeper.core.properties.testutils.InstancePropertiesTestHelper.createTestInstanceProperties;

@Testcontainers
public class IngestBatcherSubmitterLambdaIT {
    @Container
    public static LocalStackContainer localStackContainer = new LocalStackContainer(DockerImageName.parse(CommonTestConstants.LOCALSTACK_DOCKER_IMAGE))
            .withServices(LocalStackContainer.Service.S3);

    protected final AmazonS3 s3 = buildAwsV1Client(localStackContainer, LocalStackContainer.Service.S3, AmazonS3ClientBuilder.standard());

    private static final String TEST_TABLE_ID = "test-table-id";
    private static final String TEST_BUCKET = "test-bucket";
    private static final Instant RECEIVED_TIME = Instant.parse("2023-06-16T10:57:00Z");
    private final IngestBatcherStore store = new InMemoryIngestBatcherStore();
    private final InstanceProperties instanceProperties = createTestInstanceProperties();
    private final TableIndex tableIndex = new InMemoryTableIndex();
    private final IngestBatcherSubmitterLambda lambda = new IngestBatcherSubmitterLambda(
            store, instanceProperties, tableIndex, createHadoopConfiguration());

    @BeforeEach
    void setup() {
        tableIndex.create(TableStatusTestHelper.uniqueIdAndName(TEST_TABLE_ID, "test-table"));
        s3.createBucket(TEST_BUCKET);
    }

    @AfterEach
    void tearDown() {
        s3.listObjects(TEST_BUCKET).getObjectSummaries().forEach(s3ObjectSummary -> s3.deleteObject(TEST_BUCKET, s3ObjectSummary.getKey()));
        s3.deleteBucket(TEST_BUCKET);
    }

    @Nested
    @DisplayName("Store single file")
    class StoreSingleFile {
        @Test
        void shouldStoreFileIngestRequestFromJson() {
            // Given
            uploadFileToS3("test-file-1.parquet");
            String json = "{" +
                    "\"files\":[\"test-bucket/test-file-1.parquet\"]," +
                    "\"tableName\":\"test-table\"" +
                    "}";

            // When
            lambda.handleMessage(json, RECEIVED_TIME);

            // Then
            assertThat(store.getAllFilesNewestFirst())
                    .containsExactly(
                            fileRequest("test-bucket/test-file-1.parquet"));
        }

        @Test
        void shouldStoreFileWithCustomSize() {
            // Given
            s3.putObject(TEST_BUCKET, "test-file-1.parquet", "a".repeat(123));
            String json = "{" +
                    "\"files\":[\"test-bucket/test-file-1.parquet\"]," +
                    "\"tableName\":\"test-table\"" +
                    "}";

            // When
            lambda.handleMessage(json, RECEIVED_TIME);

            // Then
            assertThat(store.getAllFilesNewestFirst())
                    .containsExactly(FileIngestRequest.builder()
                            .file(TEST_BUCKET + "/test-file-1.parquet")
                            .fileSizeBytes(123)
                            .tableId(TEST_TABLE_ID)
                            .receivedTime(RECEIVED_TIME)
                            .build());
        }
    }

    @Nested
    @DisplayName("Store all files in directory")
    class StoreFilesInDirectory {
        @Test
        void shouldStoreOneFileInDirectory() {
            // Given
            uploadFileToS3("test-directory/test-file-1.parquet");
            String json = "{" +
                    "\"files\":[\"test-bucket/test-directory\"]," +
                    "\"tableName\":\"test-table\"" +
                    "}";

            // When
            lambda.handleMessage(json, RECEIVED_TIME);

            // Then
            assertThat(store.getAllFilesNewestFirst())
                    .containsExactly(
                            fileRequest(TEST_BUCKET + "/test-directory/test-file-1.parquet"));
        }

        @Test
        void shouldStoreMultipleFilesInDirectory() {
            // Given
            uploadFileToS3("test-directory/test-file-1.parquet");
            uploadFileToS3("test-directory/test-file-2.parquet");
            String json = "{" +
                    "\"files\":[\"test-bucket/test-directory\"]," +
                    "\"tableName\":\"test-table\"" +
                    "}";

            // When
            lambda.handleMessage(json, RECEIVED_TIME);

            // Then
            assertThat(store.getAllFilesNewestFirst())
                    .containsExactly(
                            fileRequest(TEST_BUCKET + "/test-directory/test-file-1.parquet"),
                            fileRequest(TEST_BUCKET + "/test-directory/test-file-2.parquet"));
        }

        @Test
        void shouldStoreFileInNestedDirectories() {
            // Given
            uploadFileToS3("test-directory/nested/test-file-1.parquet");
            String json = "{" +
                    "\"files\":[\"test-bucket/test-directory\"]," +
                    "\"tableName\":\"test-table\"" +
                    "}";

            // When
            lambda.handleMessage(json, RECEIVED_TIME);

            // Then
            assertThat(store.getAllFilesNewestFirst())
                    .containsExactly(
                            fileRequest(TEST_BUCKET + "/test-directory/nested/test-file-1.parquet"));
        }

        @Test
        void shouldStoreFileAcrossMultipleDirectories() {
            // Given
            uploadFileToS3("test-directory/nested-1/test-file-1.parquet");
            uploadFileToS3("test-directory/nested-2/test-file-2.parquet");
            String json = "{" +
                    "\"files\":[\"test-bucket/test-directory\"]," +
                    "\"tableName\":\"test-table\"" +
                    "}";

            // When
            lambda.handleMessage(json, RECEIVED_TIME);

            // Then
            assertThat(store.getAllFilesNewestFirst())
                    .containsExactly(
                            fileRequest(TEST_BUCKET + "/test-directory/nested-1/test-file-1.parquet"),
                            fileRequest(TEST_BUCKET + "/test-directory/nested-2/test-file-2.parquet"));
        }

        @Test
        void shouldStoreAllFilesByBucketName() {
            // Given
            uploadFileToS3("test-file-1.parquet");
            uploadFileToS3("test-file-2.parquet");
            String json = "{" +
                    "\"files\":[\"test-bucket\"]," +
                    "\"tableName\":\"test-table\"" +
                    "}";

            // When
            lambda.handleMessage(json, RECEIVED_TIME);

            // Then
            assertThat(store.getAllFilesNewestFirst())
                    .containsExactly(
                            fileRequest(TEST_BUCKET + "/test-file-1.parquet"),
                            fileRequest(TEST_BUCKET + "/test-file-2.parquet"));
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
                    "\"test-bucket/test-file-1.parquet\"," +
                    "\"test-bucket/test-file-2.parquet\"" +
                    "]," +
                    "\"tableName\":\"test-table\"" +
                    "}";

            // When
            lambda.handleMessage(json, RECEIVED_TIME);

            // Then
            assertThat(store.getAllFilesNewestFirst())
                    .containsExactly(
                            fileRequest("test-bucket/test-file-1.parquet"),
                            fileRequest("test-bucket/test-file-2.parquet"));
        }

        @Test
        void shouldNotStoreFilesIfFileNotFound() {
            // Given
            uploadFileToS3("test-file-2.parquet");
            String json = "{" +
                    "\"files\":[" +
                    "\"test-bucket/not-found.parquet\"," +
                    "\"test-bucket/test-file-2.parquet\"" +
                    "]," +
                    "\"tableName\":\"test-table\"" +
                    "}";

            // When
            lambda.handleMessage(json, RECEIVED_TIME);

            // Then
            assertThat(store.getAllFilesNewestFirst())
                    .isEmpty();
        }
    }

    @Nested
    @DisplayName("Handle errors in messages")
    class HandleErrorsInMessage {

        @Test
        void shouldIgnoreAndLogMessageWithInvalidJson() {
            // Given
            String json = "{";

            // When
            lambda.handleMessage(json, RECEIVED_TIME);

            // Then
            assertThat(store.getAllFilesNewestFirst()).isEmpty();
        }

        @Test
        void shouldIgnoreAndLogMessageIfTableDoesNotExist() {
            // Given
            String json = "{" +
                    "\"files\":[\"test-bucket/test-file-1.parquet\"]," +
                    "\"tableName\":\"not-a-table\"" +
                    "}";

            // When
            lambda.handleMessage(json, RECEIVED_TIME);

            // Then
            assertThat(store.getAllFilesNewestFirst()).isEmpty();
        }

        @Test
        void shouldIgnoreMessageIfFileDoesNotExist() {
            // Given
            String json = "{" +
                    "\"files\":[\"test-bucket/not-exists.parquet\"]," +
                    "\"tableName\":\"test-table\"" +
                    "}";

            // When
            lambda.handleMessage(json, RECEIVED_TIME);

            // Then
            assertThat(store.getAllFilesNewestFirst()).isEmpty();
        }

        @Test
        void shouldIgnoreMessageIfFilesNotProvided() {
            // Given
            String json = "{" +
                    "\"files\":[]," +
                    "\"tableName\":\"test-table\"" +
                    "}";

            // When
            lambda.handleMessage(json, RECEIVED_TIME);

            // Then
            assertThat(store.getAllFilesNewestFirst()).isEmpty();
        }
    }

    private void uploadFileToS3(String filePath) {
        s3.putObject(TEST_BUCKET, filePath, "test");
    }

    private static FileIngestRequest fileRequest(String filePath) {
        return FileIngestRequest.builder()
                .file(filePath)
                .fileSizeBytes(4)
                .tableId(TEST_TABLE_ID)
                .receivedTime(RECEIVED_TIME).build();
    }

    private static Configuration createHadoopConfiguration() {
        Configuration conf = new Configuration();
        conf.set("fs.s3a.bucket.test-bucket.endpoint", localStackContainer.getEndpointOverride(LocalStackContainer.Service.S3).toString());
        conf.set("fs.s3a.access.key", localStackContainer.getAccessKey());
        conf.set("fs.s3a.secret.key", localStackContainer.getSecretKey());
        return conf;
    }
}
