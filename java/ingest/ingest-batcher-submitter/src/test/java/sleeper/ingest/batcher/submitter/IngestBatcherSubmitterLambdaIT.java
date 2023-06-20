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

package sleeper.ingest.batcher.submitter;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.localstack.LocalStackContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import sleeper.configuration.properties.table.FixedTablePropertiesProvider;
import sleeper.configuration.properties.table.TableProperties;
import sleeper.configuration.properties.table.TablePropertiesProvider;
import sleeper.core.CommonTestConstants;
import sleeper.ingest.batcher.FileIngestRequest;
import sleeper.ingest.batcher.IngestBatcherStore;
import sleeper.ingest.batcher.testutil.IngestBatcherStoreInMemory;

import java.time.Instant;

import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.configuration.properties.InstancePropertiesTestHelper.createTestInstanceProperties;
import static sleeper.configuration.properties.table.TablePropertiesTestHelper.createTestTableProperties;
import static sleeper.configuration.properties.table.TableProperty.TABLE_NAME;
import static sleeper.core.schema.SchemaTestHelper.schemaWithKey;

@Testcontainers
public class IngestBatcherSubmitterLambdaIT {
    @Container
    public static LocalStackContainer localStackContainer = new LocalStackContainer(DockerImageName.parse(CommonTestConstants.LOCALSTACK_DOCKER_IMAGE))
            .withServices(LocalStackContainer.Service.S3);

    protected final AmazonS3 s3 = AmazonS3ClientBuilder.standard()
            .withEndpointConfiguration(localStackContainer.getEndpointConfiguration(LocalStackContainer.Service.S3))
            .withCredentials(localStackContainer.getDefaultCredentialsProvider())
            .build();

    private static final String TEST_TABLE = "test-table";
    private static final String TEST_BUCKET = "test-bucket";
    private static final Instant RECEIVED_TIME = Instant.parse("2023-06-16T10:57:00Z");
    private final IngestBatcherStore store = new IngestBatcherStoreInMemory();
    private final TablePropertiesProvider tablePropertiesProvider = new FixedTablePropertiesProvider(createTableProperties());
    private final IngestBatcherSubmitterLambda lambda = new IngestBatcherSubmitterLambda(
            store, tablePropertiesProvider, s3);

    @BeforeEach
    void setup() {
        s3.createBucket(TEST_BUCKET);
    }

    @AfterEach
    void tearDown() {
        s3.listObjects(TEST_BUCKET).getObjectSummaries().forEach(s3ObjectSummary ->
                s3.deleteObject(TEST_BUCKET, s3ObjectSummary.getKey()));
        s3.deleteBucket(TEST_BUCKET);
    }

    private void uploadFileToS3(String filePath) {
        s3.putObject(TEST_BUCKET, filePath, "test");
    }

    @Nested
    @DisplayName("Store single file")
    class StoreSingleFile {
        @Test
        void shouldStoreFileIngestRequestFromJson() {
            // Given
            uploadFileToS3("test-file-1.parquet");
            String json = "{" +
                    "\"files\":[\"" + TEST_BUCKET + "/test-file-1.parquet\"]," +
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
                    "\"files\":[\"" + TEST_BUCKET + "/test-file-1.parquet\"]," +
                    "\"tableName\":\"" + TEST_TABLE + "\"" +
                    "}";

            // When
            lambda.handleMessage(json, RECEIVED_TIME);

            // Then
            assertThat(store.getAllFilesNewestFirst())
                    .containsExactly(FileIngestRequest.builder()
                            .file(TEST_BUCKET + "/test-file-1.parquet")
                            .fileSizeBytes(123)
                            .tableName(TEST_TABLE)
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
                    "\"files\":[\"" + TEST_BUCKET + "/test-directory\"]," +
                    "\"tableName\":\"" + TEST_TABLE + "\"" +
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
                    "\"files\":[\"" + TEST_BUCKET + "/test-directory\"]," +
                    "\"tableName\":\"" + TEST_TABLE + "\"" +
                    "}";

            // When
            lambda.handleMessage(json, RECEIVED_TIME);

            // Then
            assertThat(store.getAllFilesNewestFirst())
                    .containsExactly(
                            fileRequest(TEST_BUCKET + "/test-directory/test-file-2.parquet"),
                            fileRequest(TEST_BUCKET + "/test-directory/test-file-1.parquet"));
        }

        @Test
        void shouldStoreFileInNestedDirectories() {
            // Given
            uploadFileToS3("test-directory/nested/test-file-1.parquet");
            String json = "{" +
                    "\"files\":[\"" + TEST_BUCKET + "/test-directory\"]," +
                    "\"tableName\":\"" + TEST_TABLE + "\"" +
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
                    "\"files\":[\"" + TEST_BUCKET + "/test-directory\"]," +
                    "\"tableName\":\"" + TEST_TABLE + "\"" +
                    "}";

            // When
            lambda.handleMessage(json, RECEIVED_TIME);

            // Then
            assertThat(store.getAllFilesNewestFirst())
                    .containsExactly(
                            fileRequest(TEST_BUCKET + "/test-directory/nested-2/test-file-2.parquet"),
                            fileRequest(TEST_BUCKET + "/test-directory/nested-1/test-file-1.parquet"));
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
                            fileRequest("test-bucket/test-file-2.parquet"),
                            fileRequest("test-bucket/test-file-1.parquet"));
        }

        @Test
        void shouldSkipFileIfNotFound() {
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
                    .containsExactly(
                            fileRequest("test-bucket/test-file-2.parquet"));
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
                    "\"files\":[\"" + TEST_BUCKET + "/test-file-1.parquet\"]," +
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
                    "\"files\":[\"" + TEST_BUCKET + "/not-exists.parquet\"]," +
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

    private static FileIngestRequest fileRequest(String filePath) {
        return FileIngestRequest.builder()
                .file(filePath)
                .fileSizeBytes(4)
                .tableName(TEST_TABLE)
                .receivedTime(RECEIVED_TIME).build();
    }

    private static TableProperties createTableProperties() {
        TableProperties properties = createTestTableProperties(createTestInstanceProperties(), schemaWithKey("key"));
        properties.set(TABLE_NAME, TEST_TABLE);
        return properties;
    }
}
