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
package sleeper.bulkimport.starter;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.events.SQSEvent;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import sleeper.bulkimport.core.job.BulkImportJob;
import sleeper.bulkimport.core.job.BulkImportJobSerDe;
import sleeper.bulkimport.starter.executor.BulkImportExecutor;
import sleeper.configuration.utils.S3ExpandDirectories;
import sleeper.core.table.InMemoryTableIndex;
import sleeper.core.table.TableIndex;
import sleeper.core.table.TableStatusTestHelper;
import sleeper.core.tracker.ingest.job.InMemoryIngestJobTracker;
import sleeper.core.tracker.ingest.job.IngestJobStatusTestData;
import sleeper.core.tracker.ingest.job.IngestJobTracker;
import sleeper.core.tracker.job.run.JobRun;
import sleeper.ingest.core.job.IngestJobMessageHandler;
import sleeper.localstack.test.LocalStackTestBase;

import java.time.Instant;
import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static sleeper.core.tracker.ingest.job.IngestJobStatusTestData.ingestJobStatus;

public class BulkImportStarterLambdaIT extends LocalStackTestBase {
    private static final String TEST_TABLE = "test-table";
    private static final String TEST_TABLE_ID = "test-table-id";
    private final String testBucket = UUID.randomUUID().toString();

    private final BulkImportExecutor executor = mock(BulkImportExecutor.class);
    private final TableIndex tableIndex = new InMemoryTableIndex();
    private final IngestJobTracker tracker = new InMemoryIngestJobTracker();
    private final Instant validationTime = Instant.parse("2023-10-17T14:53:00Z");
    private final BulkImportStarterLambda bulkImportStarter = new BulkImportStarterLambda(
            executor, messageHandlerBuilder()
                    .jobIdSupplier(() -> "invalid-id")
                    .timeSupplier(() -> validationTime)
                    .build());

    @BeforeEach
    void setup() {
        createBucket(testBucket);
        tableIndex.create(TableStatusTestHelper.uniqueIdAndName(TEST_TABLE_ID, TEST_TABLE));
    }

    private IngestJobMessageHandler.Builder<BulkImportJob> messageHandlerBuilder() {
        return BulkImportStarterLambda.messageHandlerBuilder()
                .tableIndex(tableIndex)
                .ingestJobTracker(tracker)
                .expandDirectories(files -> new S3ExpandDirectories(s3Client).streamFilenames(files).toList());
    }

    @Nested
    @DisplayName("Expand directories")
    class ExpandDirectories {

        @Test
        void shouldExpandDirectoryWithOneFileInside() {
            // Given
            uploadFileToS3("test-dir/test-1.parquet");
            SQSEvent event = getSqsEvent(jobWithFiles(testBucket + "/test-dir"));

            // When
            bulkImportStarter.handleRequest(event, mock(Context.class));

            // Then
            verify(executor, times(1)).runJob(
                    jobWithFiles(testBucket + "/test-dir/test-1.parquet"));
        }

        @Test
        void shouldExpandDirectoryWithMultipleFilesInside() {
            // Given
            uploadFileToS3("test-dir/test-1.parquet");
            uploadFileToS3("test-dir/test-2.parquet");
            SQSEvent event = getSqsEvent(jobWithFiles(testBucket + "/test-dir"));

            // When
            bulkImportStarter.handleRequest(event, mock(Context.class));

            // Then
            verify(executor, times(1)).runJob(
                    jobWithFiles(
                            testBucket + "/test-dir/test-1.parquet",
                            testBucket + "/test-dir/test-2.parquet"));
        }

        @Test
        void shouldExpandDirectoryWithFileInsideNestedDirectory() {
            // Given
            uploadFileToS3("test-dir/nested-dir/test-1.parquet");
            SQSEvent event = getSqsEvent(jobWithFiles(testBucket + "/test-dir"));

            // When
            bulkImportStarter.handleRequest(event, mock(Context.class));

            // Then
            verify(executor, times(1)).runJob(
                    jobWithFiles(testBucket + "/test-dir/nested-dir/test-1.parquet"));
        }

        @Test
        void shouldExpandMultipleDirectories() {
            // Given
            uploadFileToS3("test-dir-1/test-1.parquet");
            uploadFileToS3("test-dir-2/test-2.parquet");
            SQSEvent event = getSqsEvent(jobWithFiles(
                    testBucket + "/test-dir-1", testBucket + "/test-dir-2"));

            // When
            bulkImportStarter.handleRequest(event, mock(Context.class));

            // Then
            verify(executor, times(1)).runJob(
                    jobWithFiles(
                            testBucket + "/test-dir-1/test-1.parquet",
                            testBucket + "/test-dir-2/test-2.parquet"));
        }

        @Test
        void shouldSkipJobIfDirectoryDoesNotExist() {
            // Given
            SQSEvent event = getSqsEvent(jobWithFiles(testBucket + "/test-dir"));

            // When
            bulkImportStarter.handleRequest(event, mock(Context.class));

            // Then
            verify(executor, times(0)).runJob(any());
        }
    }

    @Nested
    @DisplayName("Report validation failures")
    class ReportValidationFailures {
        @Test
        void shouldReportValidationFailureIfFileDoesNotExist() {
            // Given
            String json = jobJsonWithFiles(testBucket + "/test-dir");
            SQSEvent event = getSqsEvent(json);

            // When
            bulkImportStarter.handleRequest(event, mock(Context.class));

            // Then
            verify(executor, times(0)).runJob(any());
            assertThat(tracker.getInvalidJobs())
                    .containsExactly(ingestJobStatus("id",
                            rejectedRun("id", json, validationTime, "Could not find one or more files")));
        }

        @Test
        void shouldReportValidationFailureWhenOneFileExistsAndOneDoesNotExist() {
            // Given
            uploadFileToS3("test-file.parquet");
            String json = jobJsonWithFiles(
                    testBucket + "/test-file.parquet",
                    testBucket + "/not-a-file");
            SQSEvent event = getSqsEvent(json);

            // When
            bulkImportStarter.handleRequest(event, mock(Context.class));

            // Then
            verify(executor, times(0)).runJob(any());
            assertThat(tracker.getInvalidJobs())
                    .containsExactly(ingestJobStatus("id",
                            rejectedRun("id", json, validationTime, "Could not find one or more files")));
        }
    }

    @Test
    public void shouldHandleAValidRequest() {
        // Given
        uploadFileToS3("test-1.parquet");
        BulkImportExecutor executor = mock(BulkImportExecutor.class);
        BulkImportStarterLambda bulkImportStarter = new BulkImportStarterLambda(
                executor, messageHandlerBuilder().build());
        BulkImportJob job = jobWithFiles(testBucket + "/test-1.parquet");
        SQSEvent event = getSqsEvent(job);

        // When
        bulkImportStarter.handleRequest(event, mock(Context.class));

        // Then
        verify(executor, times(1)).runJob(job);
    }

    @Test
    void shouldSetJobIdToUUIDIfNotSetByUser() {
        // Given
        uploadFileToS3("test-1.parquet");
        BulkImportExecutor executor = mock(BulkImportExecutor.class);
        BulkImportStarterLambda bulkImportStarter = new BulkImportStarterLambda(executor,
                messageHandlerBuilder()
                        .jobIdSupplier(() -> "test-job")
                        .build());
        BulkImportJob job = BulkImportJob.builder()
                .tableName(TEST_TABLE).tableId(TEST_TABLE_ID)
                .files(List.of(testBucket + "/test-1.parquet"))
                .build();
        SQSEvent event = getSqsEvent(job);

        // When
        bulkImportStarter.handleRequest(event, mock(Context.class));

        // Then
        verify(executor, times(1)).runJob(BulkImportJob.builder()
                .id("test-job")
                .tableName(TEST_TABLE).tableId(TEST_TABLE_ID)
                .files(List.of(testBucket + "/test-1.parquet"))
                .build());
    }

    private SQSEvent getSqsEvent(BulkImportJob importJob) {
        BulkImportJobSerDe jobSerDe = new BulkImportJobSerDe();
        return getSqsEvent(jobSerDe.toJson(importJob));
    }

    private SQSEvent getSqsEvent(String json) {
        return BulkImportStarterLambdaTestHelper.getSqsEvent(json);
    }

    private static String jobJsonWithFiles(String... files) {
        BulkImportJobSerDe jobSerDe = new BulkImportJobSerDe();
        return jobSerDe.toJson(jobWithFiles(files));
    }

    private static JobRun rejectedRun(String jobId, String json, Instant validationTime, String... reasons) {
        return IngestJobStatusTestData.rejectedRun(jobId, json, validationTime, reasons);
    }

    private static BulkImportJob jobWithFiles(String... files) {
        return BulkImportJob.builder()
                .id("id")
                .tableName(TEST_TABLE).tableId(TEST_TABLE_ID)
                .files(List.of(files))
                .build();
    }

    private void uploadFileToS3(String filePath) {
        putObject(testBucket, filePath, "test");
    }
}
