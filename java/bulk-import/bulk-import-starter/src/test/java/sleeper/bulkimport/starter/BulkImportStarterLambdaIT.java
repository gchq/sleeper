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
package sleeper.bulkimport.starter;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.events.SQSEvent;
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

import sleeper.bulkimport.core.job.BulkImportJob;
import sleeper.bulkimport.core.job.BulkImportJobSerDe;
import sleeper.bulkimport.starter.executor.BulkImportExecutor;
import sleeper.core.CommonTestConstants;
import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.record.process.status.ProcessRun;
import sleeper.core.table.InMemoryTableIndex;
import sleeper.core.table.TableIndex;
import sleeper.core.table.TableStatusTestHelper;
import sleeper.ingest.core.job.IngestJobMessageHandler;
import sleeper.ingest.core.job.status.InMemoryIngestJobStatusStore;
import sleeper.ingest.core.job.status.IngestJobStatusStore;
import sleeper.ingest.core.job.status.IngestJobStatusTestHelper;
import sleeper.io.parquet.utils.HadoopPathUtils;

import java.time.Instant;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static sleeper.configuration.testutils.LocalStackAwsV1ClientHelper.buildAwsV1Client;
import static sleeper.ingest.core.job.status.IngestJobStatusTestHelper.jobStatus;

@Testcontainers
public class BulkImportStarterLambdaIT {
    private static final String TEST_TABLE = "test-table";
    private static final String TEST_TABLE_ID = "test-table-id";
    private static final String TEST_BUCKET = "test-bucket";
    @Container
    public static LocalStackContainer localStackContainer = new LocalStackContainer(DockerImageName.parse(CommonTestConstants.LOCALSTACK_DOCKER_IMAGE))
            .withServices(LocalStackContainer.Service.S3);

    private final AmazonS3 s3Client = createS3Client();
    private final BulkImportExecutor executor = mock(BulkImportExecutor.class);
    private final TableIndex tableIndex = new InMemoryTableIndex();
    private final IngestJobStatusStore ingestJobStatusStore = new InMemoryIngestJobStatusStore();
    private final Instant validationTime = Instant.parse("2023-10-17T14:53:00Z");
    private final Configuration hadoopConfig = createHadoopConfiguration();
    private final BulkImportStarterLambda bulkImportStarter = new BulkImportStarterLambda(
            executor, messageHandlerBuilder()
                    .jobIdSupplier(() -> "invalid-id")
                    .timeSupplier(() -> validationTime)
                    .build());

    private AmazonS3 createS3Client() {
        return buildAwsV1Client(localStackContainer, LocalStackContainer.Service.S3, AmazonS3ClientBuilder.standard());
    }

    @BeforeEach
    void setup() {
        s3Client.createBucket(TEST_BUCKET);
        tableIndex.create(TableStatusTestHelper.uniqueIdAndName(TEST_TABLE_ID, TEST_TABLE));
    }

    @AfterEach
    void tearDown() {
        s3Client.listObjects(TEST_BUCKET).getObjectSummaries().forEach(s3ObjectSummary -> s3Client.deleteObject(TEST_BUCKET, s3ObjectSummary.getKey()));
        s3Client.deleteBucket(TEST_BUCKET);
    }

    private IngestJobMessageHandler.Builder<BulkImportJob> messageHandlerBuilder() {
        return BulkImportStarterLambda.messageHandlerBuilder()
                .tableIndex(tableIndex)
                .ingestJobStatusStore(ingestJobStatusStore)
                .expandDirectories(files -> HadoopPathUtils.expandDirectories(files, hadoopConfig, new InstanceProperties()));
    }

    @Nested
    @DisplayName("Expand directories")
    class ExpandDirectories {

        @Test
        void shouldExpandDirectoryWithOneFileInside() {
            // Given
            uploadFileToS3("test-dir/test-1.parquet");
            SQSEvent event = getSqsEvent(jobWithFiles("test-bucket/test-dir"));

            // When
            bulkImportStarter.handleRequest(event, mock(Context.class));

            // Then
            verify(executor, times(1)).runJob(
                    jobWithFiles("test-bucket/test-dir/test-1.parquet"));
        }

        @Test
        void shouldExpandDirectoryWithMultipleFilesInside() {
            // Given
            uploadFileToS3("test-dir/test-1.parquet");
            uploadFileToS3("test-dir/test-2.parquet");
            SQSEvent event = getSqsEvent(jobWithFiles("test-bucket/test-dir"));

            // When
            bulkImportStarter.handleRequest(event, mock(Context.class));

            // Then
            verify(executor, times(1)).runJob(
                    jobWithFiles(
                            "test-bucket/test-dir/test-1.parquet",
                            "test-bucket/test-dir/test-2.parquet"));
        }

        @Test
        void shouldExpandDirectoryWithFileInsideNestedDirectory() {
            // Given
            uploadFileToS3("test-dir/nested-dir/test-1.parquet");
            SQSEvent event = getSqsEvent(jobWithFiles("test-bucket/test-dir"));

            // When
            bulkImportStarter.handleRequest(event, mock(Context.class));

            // Then
            verify(executor, times(1)).runJob(
                    jobWithFiles("test-bucket/test-dir/nested-dir/test-1.parquet"));
        }

        @Test
        void shouldExpandMultipleDirectories() {
            // Given
            uploadFileToS3("test-dir-1/test-1.parquet");
            uploadFileToS3("test-dir-2/test-2.parquet");
            SQSEvent event = getSqsEvent(jobWithFiles(
                    "test-bucket/test-dir-1", "test-bucket/test-dir-2"));

            // When
            bulkImportStarter.handleRequest(event, mock(Context.class));

            // Then
            verify(executor, times(1)).runJob(
                    jobWithFiles(
                            "test-bucket/test-dir-1/test-1.parquet",
                            "test-bucket/test-dir-2/test-2.parquet"));
        }

        @Test
        void shouldSkipJobIfDirectoryDoesNotExist() {
            // Given
            SQSEvent event = getSqsEvent(jobWithFiles("test-bucket/test-dir"));

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
            String json = jobJsonWithFiles("test-bucket/test-dir");
            SQSEvent event = getSqsEvent(json);

            // When
            bulkImportStarter.handleRequest(event, mock(Context.class));

            // Then
            verify(executor, times(0)).runJob(any());
            assertThat(ingestJobStatusStore.getInvalidJobs())
                    .containsExactly(jobStatus("id",
                            rejectedRun("id", json, validationTime, "Could not find one or more files")));
        }

        @Test
        void shouldReportValidationFailureWhenOneFileExistsAndOneDoesNotExist() {
            // Given
            uploadFileToS3("test-file.parquet");
            String json = jobJsonWithFiles(
                    "test-bucket/test-file.parquet",
                    "test-bucket/not-a-file");
            SQSEvent event = getSqsEvent(json);

            // When
            bulkImportStarter.handleRequest(event, mock(Context.class));

            // Then
            verify(executor, times(0)).runJob(any());
            assertThat(ingestJobStatusStore.getInvalidJobs())
                    .containsExactly(jobStatus("id",
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
        BulkImportJob job = jobWithFiles("test-bucket/test-1.parquet");
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
                .files(List.of("test-bucket/test-1.parquet"))
                .build();
        SQSEvent event = getSqsEvent(job);

        // When
        bulkImportStarter.handleRequest(event, mock(Context.class));

        // Then
        verify(executor, times(1)).runJob(BulkImportJob.builder()
                .id("test-job")
                .tableName(TEST_TABLE).tableId(TEST_TABLE_ID)
                .files(List.of("test-bucket/test-1.parquet"))
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

    private static ProcessRun rejectedRun(String jobId, String json, Instant validationTime, String... reasons) {
        return IngestJobStatusTestHelper.rejectedRun(jobId, json, validationTime, reasons);
    }

    private static BulkImportJob jobWithFiles(String... files) {
        return BulkImportJob.builder()
                .id("id")
                .tableName(TEST_TABLE).tableId(TEST_TABLE_ID)
                .files(List.of(files))
                .build();
    }

    private static Configuration createHadoopConfiguration() {
        Configuration conf = new Configuration();
        conf.set("fs.s3a.bucket.test-bucket.endpoint", localStackContainer.getEndpointOverride(LocalStackContainer.Service.S3).toString());
        conf.set("fs.s3a.access.key", localStackContainer.getAccessKey());
        conf.set("fs.s3a.secret.key", localStackContainer.getSecretKey());
        return conf;
    }

    private void uploadFileToS3(String filePath) {
        s3Client.putObject(TEST_BUCKET, filePath, "test");
    }
}
