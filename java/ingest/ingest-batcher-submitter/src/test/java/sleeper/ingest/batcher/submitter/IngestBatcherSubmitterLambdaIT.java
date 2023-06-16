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
import static sleeper.ingest.batcher.submitter.IngestBatcherSubmitterLambda.isRequestForDirectory;

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

    private static TableProperties createTableProperties() {
        TableProperties properties = createTestTableProperties(createTestInstanceProperties(), schemaWithKey("key"));
        properties.set(TABLE_NAME, TEST_TABLE);
        return properties;
    }

    @Nested
    @DisplayName("Check object is directory in s3")
    class CheckObjectIsDirectory {
        @Test
        void shouldDetectThatRequestIsForDirectory() {
            // Given
            s3.putObject(TEST_BUCKET, "test-directory/test-1.parquet", "test");
            FileIngestRequest request = createFileRequest("/test-directory");

            // When/Then
            assertThat(isRequestForDirectory(s3, request)).isTrue();
        }

        @Test
        void shouldDetectThatRequestIsForDirectoryWhenSubdirectoriesExist() {
            // Given
            s3.putObject(TEST_BUCKET, "test-directory/test-1.parquet", "test");
            s3.putObject(TEST_BUCKET, "test-directory/test-directory-2/test-2.parquet", "test");
            FileIngestRequest request = createFileRequest("/test-directory");

            // When/Then
            assertThat(isRequestForDirectory(s3, request)).isTrue();
        }

        @Test
        void shouldDetectThatRequestIsNotForDirectory() {
            // Given
            s3.putObject(TEST_BUCKET, "test-directory/test-1.parquet", "test");
            FileIngestRequest request = createFileRequest("/test-directory/test-1.parquet");

            // When/Then
            assertThat(isRequestForDirectory(s3, request)).isFalse();
        }

        FileIngestRequest createFileRequest(String fullPath) {
            return FileIngestRequest.builder()
                    .file(TEST_BUCKET + fullPath)
                    .fileSizeBytes(123)
                    .tableName(TEST_TABLE)
                    .receivedTime(Instant.parse("2023-06-15T15:30:00Z")).build();
        }
    }

    @Nested
    @DisplayName("Store all files in directory")
    class StoreFilesInDirectory {
        private final Instant receivedTime = Instant.parse("2023-06-16T10:57:00Z");

        @Test
        void shouldStoreOneFileInDirectory() {
            // Given
            s3.putObject(TEST_BUCKET, "test-directory/test-1.parquet", "test");
            String json = "{" +
                    "\"file\":\"" + TEST_BUCKET + "/test-directory\"," +
                    "\"fileSizeBytes\":100," +
                    "\"tableName\":\"" + TEST_TABLE + "\"" +
                    "}";

            // When
            lambda.handleMessage(json, receivedTime);

            // Then
            assertThat(store.getAllFilesNewestFirst())
                    .containsExactly(
                            fileRequest("test-directory/test-1.parquet", receivedTime));
        }

        @Test
        void shouldStoreMultipleFilesInDirectory() {
            // Given
            s3.putObject(TEST_BUCKET, "test-directory/test-1.parquet", "test");
            s3.putObject(TEST_BUCKET, "test-directory/test-2.parquet", "test");
            String json = "{" +
                    "\"file\":\"" + TEST_BUCKET + "/test-directory\"," +
                    "\"fileSizeBytes\":100," +
                    "\"tableName\":\"" + TEST_TABLE + "\"" +
                    "}";

            // When
            lambda.handleMessage(json, receivedTime);

            // Then
            assertThat(store.getAllFilesNewestFirst())
                    .containsExactly(
                            fileRequest("test-directory/test-2.parquet", receivedTime),
                            fileRequest("test-directory/test-1.parquet", receivedTime));
        }

        @Test
        void shouldStoreFileInNestedDirectories() {
            // Given
            s3.putObject(TEST_BUCKET, "test-directory/nested/test-1.parquet", "test");
            String json = "{" +
                    "\"file\":\"" + TEST_BUCKET + "/test-directory\"," +
                    "\"fileSizeBytes\":100," +
                    "\"tableName\":\"" + TEST_TABLE + "\"" +
                    "}";

            // When
            lambda.handleMessage(json, receivedTime);

            // Then
            assertThat(store.getAllFilesNewestFirst())
                    .containsExactly(fileRequest("test-directory/nested/test-1.parquet", receivedTime));
        }

        FileIngestRequest fileRequest(String filePath, Instant receivedTime) {
            return FileIngestRequest.builder()
                    .file(filePath)
                    .fileSizeBytes(4)
                    .tableName(TEST_TABLE)
                    .receivedTime(receivedTime).build();
        }
    }
}
