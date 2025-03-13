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

package sleeper.ingest.runner.task;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.table.InMemoryTableIndex;
import sleeper.core.table.TableIndex;
import sleeper.core.table.TableStatusTestHelper;
import sleeper.core.tracker.ingest.job.InMemoryIngestJobTracker;
import sleeper.core.tracker.ingest.job.IngestJobTracker;
import sleeper.ingest.core.job.IngestJob;
import sleeper.ingest.core.job.IngestJobMessageHandler;
import sleeper.localstack.test.LocalStackTestBase;

import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.core.tracker.ingest.job.IngestJobStatusTestData.ingestJobStatus;
import static sleeper.core.tracker.ingest.job.IngestJobStatusTestData.rejectedRun;

public class IngestJobMessageHandlerIT extends LocalStackTestBase {
    private static final String TEST_TABLE = "test-table";
    private static final String TEST_TABLE_ID = "test-table-id";

    private final String testBucket = UUID.randomUUID().toString();
    private final InstanceProperties properties = new InstanceProperties();
    private final Instant validationTime = Instant.parse("2023-10-17T14:15:00Z");
    private final TableIndex tableIndex = new InMemoryTableIndex();
    private final IngestJobTracker tracker = new InMemoryIngestJobTracker();
    private final IngestJobMessageHandler<IngestJob> ingestJobMessageHandler = IngestJobQueueConsumer.messageHandler(
            properties, hadoopConf, tableIndex, tracker)
            .jobIdSupplier(() -> "job-id")
            .timeSupplier(() -> validationTime)
            .build();

    @BeforeEach
    void setup() {
        createBucket(testBucket);
        tableIndex.create(TableStatusTestHelper.uniqueIdAndName(TEST_TABLE_ID, TEST_TABLE));
    }

    @Nested
    @DisplayName("Expand directories")
    class ExpandDirectories {
        @Test
        void shouldExpandDirectoryWithOneFileInside() {
            // Given
            uploadFileToS3("test-dir/test-1.parquet");
            String json = "{" +
                    "\"id\":\"id\"," +
                    "\"tableName\":\"test-table\"," +
                    "\"files\":[" +
                    "   \"" + testBucket + "/test-dir\"" +
                    "]}";

            // When
            Optional<IngestJob> job = ingestJobMessageHandler.deserialiseAndValidate(json);

            // Then
            assertThat(job).contains(jobWithFiles(
                    testBucket + "/test-dir/test-1.parquet"));
        }

        @Test
        void shouldExpandDirectoryWithMultipleFilesInside() {
            // Given
            uploadFileToS3("test-dir/test-1.parquet");
            uploadFileToS3("test-dir/test-2.parquet");
            String json = "{" +
                    "\"id\":\"id\"," +
                    "\"tableName\":\"test-table\"," +
                    "\"files\":[" +
                    "   \"" + testBucket + "/test-dir\"" +
                    "]}";

            // When
            Optional<IngestJob> job = ingestJobMessageHandler.deserialiseAndValidate(json);

            // Then
            assertThat(job).contains(jobWithFiles(
                    testBucket + "/test-dir/test-1.parquet",
                    testBucket + "/test-dir/test-2.parquet"));
        }

        @Test
        void shouldExpandDirectoryWithFileInsideNestedDirectory() {
            // Given
            uploadFileToS3("test-dir/nested-dir/test-1.parquet");
            String json = "{" +
                    "\"id\":\"id\"," +
                    "\"tableName\":\"test-table\"," +
                    "\"files\":[" +
                    "   \"" + testBucket + "/test-dir\"" +
                    "]}";

            // When
            Optional<IngestJob> job = ingestJobMessageHandler.deserialiseAndValidate(json);

            // Then
            assertThat(job).contains(jobWithFiles(
                    testBucket + "/test-dir/nested-dir/test-1.parquet"));
        }

        @Test
        void shouldExpandMultipleDirectories() {
            // Given
            uploadFileToS3("test-dir-1/test-1.parquet");
            uploadFileToS3("test-dir-2/test-2.parquet");
            String json = "{" +
                    "\"id\":\"id\"," +
                    "\"tableName\":\"test-table\"," +
                    "\"files\":[" +
                    "   \"" + testBucket + "/test-dir-1\"," +
                    "   \"" + testBucket + "/test-dir-2\"" +
                    "]}";

            // When
            Optional<IngestJob> job = ingestJobMessageHandler.deserialiseAndValidate(json);

            // Then
            assertThat(job).contains(jobWithFiles(
                    testBucket + "/test-dir-1/test-1.parquet",
                    testBucket + "/test-dir-2/test-2.parquet"));
        }
    }

    @Nested
    @DisplayName("Report validation failures")
    class ReportValidationFailures {
        @Test
        void shouldReportValidationFailureIfFileDoesNotExist() {
            // Given
            String json = "{" +
                    "\"id\": \"id\"," +
                    "\"tableName\": \"test-table\"," +
                    "\"files\": [" +
                    "    \"" + testBucket + "/not-a-file\"" +
                    "]}";

            // When
            Optional<IngestJob> job = ingestJobMessageHandler.deserialiseAndValidate(json);

            // Then
            assertThat(job).isNotPresent();
            assertThat(tracker.getInvalidJobs())
                    .containsExactly(ingestJobStatus("id",
                            rejectedRun("id", json, validationTime, "Could not find one or more files")));
        }

        @Test
        void shouldReportValidationFailureWhenOneFileExistsAndOneDoesNotExist() {
            // Given
            uploadFileToS3("test-file.parquet");
            String json = "{" +
                    "\"id\": \"id\"," +
                    "\"tableName\": \"test-table\"," +
                    "\"files\": [" +
                    "    \"" + testBucket + "/test-file.parquet\"," +
                    "    \"" + testBucket + "/not-a-file\"" +
                    "]}";

            // When
            Optional<IngestJob> job = ingestJobMessageHandler.deserialiseAndValidate(json);

            // Then
            assertThat(job).isNotPresent();
            assertThat(tracker.getInvalidJobs())
                    .containsExactly(ingestJobStatus("id",
                            rejectedRun("id", json, validationTime, "Could not find one or more files")));
        }
    }

    private void uploadFileToS3(String filePath) {
        s3Client.putObject(testBucket, filePath, "test");
    }

    private static IngestJob jobWithFiles(String... files) {
        return IngestJob.builder()
                .id("id")
                .tableName(TEST_TABLE).tableId(TEST_TABLE_ID)
                .files(List.of(files)).build();
    }
}
