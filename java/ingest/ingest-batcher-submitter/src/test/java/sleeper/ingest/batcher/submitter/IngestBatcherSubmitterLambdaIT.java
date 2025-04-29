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

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.table.InMemoryTableIndex;
import sleeper.core.table.TableIndex;
import sleeper.core.table.TableStatusTestHelper;
import sleeper.ingest.batcher.core.IngestBatcherStore;
import sleeper.ingest.batcher.core.IngestBatcherTrackedFile;
import sleeper.ingest.batcher.core.testutil.InMemoryIngestBatcherStore;
import sleeper.localstack.test.LocalStackTestBase;

import java.time.Instant;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.core.properties.testutils.InstancePropertiesTestHelper.createTestInstanceProperties;

public class IngestBatcherSubmitterLambdaIT extends LocalStackTestBase {

    private static final String TEST_TABLE_ID = "test-table-id";
    private static final Instant RECEIVED_TIME = Instant.parse("2023-06-16T10:57:00Z");
    private final String testBucket = UUID.randomUUID().toString();
    private final IngestBatcherStore store = new InMemoryIngestBatcherStore();
    private final InstanceProperties instanceProperties = createTestInstanceProperties();
    private final TableIndex tableIndex = new InMemoryTableIndex();
    private final IngestBatcherSubmitterLambda lambda = new IngestBatcherSubmitterLambda(
            store, instanceProperties, tableIndex, hadoopConf);

    @BeforeEach
    void setup() {
        tableIndex.create(TableStatusTestHelper.uniqueIdAndName(TEST_TABLE_ID, "test-table"));
        createBucket(testBucket);
    }

    @Nested
    @DisplayName("Store single file")
    class StoreSingleFile {
        @Test
        void shouldStoreFileIngestRequestFromJson() {
            // Given
            uploadFileToS3("test-file-1.parquet");
            String json = "{" +
                    "\"files\":[\"" + testBucket + "/test-file-1.parquet\"]," +
                    "\"tableName\":\"test-table\"" +
                    "}";

            // When
            lambda.handleMessage(json, RECEIVED_TIME);

            // Then
            assertThat(store.getAllFilesNewestFirst())
                    .containsExactly(
                            fileRequest(testBucket + "/test-file-1.parquet"));
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
            lambda.handleMessage(json, RECEIVED_TIME);

            // Then
            assertThat(store.getAllFilesNewestFirst())
                    .containsExactly(IngestBatcherTrackedFile.builder()
                            .file(testBucket + "/test-file-1.parquet")
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
                    "\"files\":[\"" + testBucket + "/test-directory\"]," +
                    "\"tableName\":\"test-table\"" +
                    "}";

            // When
            lambda.handleMessage(json, RECEIVED_TIME);

            // Then
            assertThat(store.getAllFilesNewestFirst())
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
            lambda.handleMessage(json, RECEIVED_TIME);

            // Then
            assertThat(store.getAllFilesNewestFirst())
                    .containsExactly(
                            fileRequest(testBucket + "/test-directory/test-file-1.parquet"),
                            fileRequest(testBucket + "/test-directory/test-file-2.parquet"));
        }

        @Test
        void shouldStoreFileInNestedDirectories() {
            // Given
            uploadFileToS3("test-directory/nested/test-file-1.parquet");
            String json = "{" +
                    "\"files\":[\"" + testBucket + "/test-directory\"]," +
                    "\"tableName\":\"test-table\"" +
                    "}";

            // When
            lambda.handleMessage(json, RECEIVED_TIME);

            // Then
            assertThat(store.getAllFilesNewestFirst())
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
            lambda.handleMessage(json, RECEIVED_TIME);

            // Then
            assertThat(store.getAllFilesNewestFirst())
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
            lambda.handleMessage(json, RECEIVED_TIME);

            // Then
            assertThat(store.getAllFilesNewestFirst())
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
            lambda.handleMessage(json, RECEIVED_TIME);

            // Then
            assertThat(store.getAllFilesNewestFirst())
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
                    "\"files\":[\"" + testBucket + "/test-file-1.parquet\"]," +
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
                    "\"files\":[\"" + testBucket + "/not-exists.parquet\"]," +
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
        s3Client.putObject(testBucket, filePath, "test");
    }

    private static IngestBatcherTrackedFile fileRequest(String filePath) {
        return IngestBatcherTrackedFile.builder()
                .file(filePath)
                .fileSizeBytes(4)
                .tableId(TEST_TABLE_ID)
                .receivedTime(RECEIVED_TIME).build();
    }
}
