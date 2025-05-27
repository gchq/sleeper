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
package sleeper.ingest.batcher.storev2;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.dynamodb.model.ScanRequest;

import sleeper.ingest.batcher.core.IngestBatcherTrackedFile;
import sleeper.ingest.batcher.core.testutil.FileIngestRequestTestHelper;

import java.time.Instant;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.core.properties.table.TableProperty.INGEST_BATCHER_TRACKING_TTL_MINUTES;
import static sleeper.dynamodb.toolsv2.DynamoDBAttributes.getLongAttribute;
import static sleeper.dynamodb.toolsv2.DynamoDBUtils.streamPagedItems;
import static sleeper.ingest.batcher.core.testutil.FileIngestRequestTestHelper.onJob;
import static sleeper.ingest.batcher.storev2.DynamoDBIngestRequestFormat.EXPIRY_TIME;

public class DynamoDBIngestBatcherStoreIT extends DynamoDBIngestBatcherStoreTestBase {

    private final FileIngestRequestTestHelper requests = new FileIngestRequestTestHelper();

    @Nested
    @DisplayName("Add ingest requests")
    class AddIngestRequests {

        @Test
        void shouldTrackOneFile() {
            // Given
            IngestBatcherTrackedFile fileIngestRequest = fileRequest()
                    .file("test-bucket/test.parquet").build();

            // When
            store.addFile(fileIngestRequest);

            // Then
            assertThat(store.getAllFilesNewestFirst())
                    .containsExactly(fileIngestRequest);
            assertThat(store.getPendingFilesOldestFirst())
                    .containsExactly(fileIngestRequest);
        }

        @Test
        void shouldOverwriteTrackingInformationWhenAddingTheSameFileTwice() {
            // Given
            IngestBatcherTrackedFile fileIngestRequest1 = fileRequest()
                    .file("test-bucket/test.parquet")
                    .fileSizeBytes(1024).build();
            IngestBatcherTrackedFile fileIngestRequest2 = fileRequest()
                    .file("test-bucket/test.parquet")
                    .fileSizeBytes(2048).build();

            // When
            store.addFile(fileIngestRequest1);
            store.addFile(fileIngestRequest2);

            // Then
            assertThat(store.getAllFilesNewestFirst())
                    .containsExactly(fileIngestRequest2);
            assertThat(store.getPendingFilesOldestFirst())
                    .containsExactly(fileIngestRequest2);
        }

        @Test
        void shouldTrackTheSameFileForMultipleTables() {
            // Given
            IngestBatcherTrackedFile fileIngestRequest1 = fileRequest()
                    .file("test-bucket/test.parquet")
                    .tableId(tableId1).build();
            IngestBatcherTrackedFile fileIngestRequest2 = fileRequest()
                    .file("test-bucket/test.parquet")
                    .tableId(tableId2).build();

            // When
            store.addFile(fileIngestRequest1);
            store.addFile(fileIngestRequest2);

            // Then
            assertThat(store.getAllFilesNewestFirst())
                    .containsExactlyInAnyOrder(fileIngestRequest1, fileIngestRequest2);
            assertThat(store.getPendingFilesOldestFirst())
                    .containsExactlyInAnyOrder(fileIngestRequest1, fileIngestRequest2);
        }
    }

    @Nested
    @DisplayName("Assign files to jobs")
    class AssignFilesToJobs {

        @Test
        void shouldTrackJobWasCreatedWithTwoFiles() {
            // Given
            IngestBatcherTrackedFile fileIngestRequest1 = fileRequest()
                    .file("test-bucket/test-1.parquet").build();
            IngestBatcherTrackedFile fileIngestRequest2 = fileRequest()
                    .file("test-bucket/test-2.parquet").build();

            // When
            store.addFile(fileIngestRequest1);
            store.addFile(fileIngestRequest2);
            List<String> assigned = store.assignJobGetAssigned("test-job",
                    List.of(fileIngestRequest1, fileIngestRequest2));

            // Then
            assertThat(assigned).containsExactlyInAnyOrder(
                    "test-bucket/test-1.parquet",
                    "test-bucket/test-2.parquet");
            assertThat(store.getAllFilesNewestFirst()).containsExactlyInAnyOrder(
                    onJob("test-job", fileIngestRequest1),
                    onJob("test-job", fileIngestRequest2));
            assertThat(store.getPendingFilesOldestFirst()).isEmpty();
        }

        @Test
        void shouldSendSameFileTwiceIfFirstRequestAssignedToJob() {
            // Given
            IngestBatcherTrackedFile fileIngestRequest1 = fileRequest()
                    .file("test-bucket/test.parquet").build();
            IngestBatcherTrackedFile fileIngestRequest2 = fileRequest()
                    .file("test-bucket/test.parquet").build();

            // When
            store.addFile(fileIngestRequest1);
            store.assignJobGetAssigned("test-job", List.of(fileIngestRequest1));
            store.addFile(fileIngestRequest2);

            // Then
            assertThat(store.getAllFilesNewestFirst()).containsExactlyInAnyOrder(
                    onJob("test-job", fileIngestRequest1),
                    fileIngestRequest2);
            assertThat(store.getPendingFilesOldestFirst()).containsExactly(fileIngestRequest2);
        }

        @Test
        void shouldRetainAllFileRequestParametersAfterAssigningToJob() {
            // Given
            IngestBatcherTrackedFile fileIngestRequest = fileRequest()
                    .file("test-bucket/test.parquet")
                    .fileSizeBytes(1234L)
                    .tableId(tableId)
                    .receivedTime(Instant.parse("2023-05-19T15:40:12Z"))
                    .build();

            // When
            store.addFile(fileIngestRequest);
            store.assignJobGetAssigned("test-job", List.of(fileIngestRequest));

            // Then
            assertThat(store.getAllFilesNewestFirst()).containsExactly(
                    fileRequest()
                            .file("test-bucket/test.parquet")
                            .fileSizeBytes(1234L)
                            .tableId(tableId)
                            .receivedTime(Instant.parse("2023-05-19T15:40:12Z"))
                            .jobId("test-job")
                            .build());
        }

        @Test
        void shouldFailToReassignFileWhenItIsAlreadyAssigned() {
            // Given
            IngestBatcherTrackedFile fileIngestRequest = fileRequest()
                    .file("test-bucket/test.parquet").build();
            store.addFile(fileIngestRequest);
            store.assignJobGetAssigned("test-job-1", List.of(fileIngestRequest));

            // When / Then
            List<IngestBatcherTrackedFile> job2 = List.of(fileIngestRequest);
            assertThat(store.assignJobGetAssigned("test-job-2", job2)).isEmpty();
            assertThat(store.getAllFilesNewestFirst()).containsExactly(
                    onJob("test-job-1", fileIngestRequest));
        }

        @Test
        void shouldFailToAssignFilesWhenOneIsAlreadyAssigned() {
            // Given
            IngestBatcherTrackedFile fileIngestRequest1 = fileRequest()
                    .file("test-bucket/test-1.parquet").build();
            IngestBatcherTrackedFile fileIngestRequest2 = fileRequest()
                    .file("test-bucket/test-2.parquet").build();
            store.addFile(fileIngestRequest1);
            store.addFile(fileIngestRequest2);
            store.assignJobGetAssigned("test-job-1", List.of(fileIngestRequest1));

            // When / Then
            assertThat(store.assignJobGetAssigned("test-job-2", List.of(fileIngestRequest1, fileIngestRequest2)))
                    .isEmpty();
            assertThat(store.getAllFilesNewestFirst()).containsExactlyInAnyOrder(
                    onJob("test-job-1", fileIngestRequest1),
                    fileIngestRequest2);
        }

        @Test
        void shouldFailToAssignFileWhenFileHasNotBeenTracked() {
            // Given
            IngestBatcherTrackedFile fileIngestRequest = fileRequest()
                    .file("test-bucket/test.parquet").build();

            // When / Then
            assertThat(store.assignJobGetAssigned("test-job", List.of(fileIngestRequest))).isEmpty();
            assertThat(store.getAllFilesNewestFirst()).isEmpty();
        }

        @Test
        void shouldFailToAssignFileWhenFileHasBeenDeleted() {
            // Given
            IngestBatcherTrackedFile fileIngestRequest = fileRequest()
                    .file("test-bucket/test.parquet").build();
            store.addFile(fileIngestRequest);
            store.deleteAllPending();

            // When / Then
            List<IngestBatcherTrackedFile> job = List.of(fileIngestRequest);
            assertThat(store.assignJobGetAssigned("test-job", job)).isEmpty();
            assertThat(store.getAllFilesNewestFirst()).isEmpty();
        }

        @Test
        void shouldFailToAssignFileToSameJobTwice() {
            // Given
            IngestBatcherTrackedFile fileIngestRequest = fileRequest()
                    .file("test-bucket/sendTwice.parquet").build();
            store.addFile(fileIngestRequest);
            store.assignJobGetAssigned("duplicate-job", List.of(fileIngestRequest));
            store.addFile(fileIngestRequest);

            // When / Then
            assertThat(store.assignJobGetAssigned("duplicate-job", List.of(fileIngestRequest))).isEmpty();
            assertThat(store.getAllFilesNewestFirst()).containsExactlyInAnyOrder(
                    onJob("duplicate-job", fileIngestRequest),
                    fileIngestRequest);
        }

        @Test
        void shouldAssignFilesWhenNumberOfFilesMeetsTheBatchSize() {
            // Given
            IngestBatcherTrackedFile file1 = fileRequest().build();
            IngestBatcherTrackedFile file2 = fileRequest().build();
            store.addFile(file1);
            store.addFile(file2);

            // When
            storeWithFilesInAssignJobBatch(2).assignJobGetAssigned("test-job", List.of(file1, file2));

            // Then
            assertThat(store.getPendingFilesOldestFirst()).isEmpty();
        }

        @Test
        void shouldAssignFilesWhenNumberOfFilesExceedsTheBatchSize() {
            // Given
            IngestBatcherTrackedFile file1 = fileRequest().build();
            IngestBatcherTrackedFile file2 = fileRequest().build();
            store.addFile(file1);
            store.addFile(file2);

            // When
            storeWithFilesInAssignJobBatch(1).assignJobGetAssigned("test-job", List.of(file1, file2));

            // Then
            assertThat(store.getPendingFilesOldestFirst()).isEmpty();
        }
    }

    @Nested
    @DisplayName("Order files returned from the store")
    class OrderFilesReturnedFromStore {

        final IngestBatcherTrackedFile fileIngestRequest1 = fileRequest()
                .file("test-bucket/first.parquet").build();
        final IngestBatcherTrackedFile fileIngestRequest2 = fileRequest()
                .file("test-bucket/another.parquet").build();
        final IngestBatcherTrackedFile fileIngestRequest3 = fileRequest()
                .file("test-bucket/last.parquet").build();

        @BeforeEach
        void setUp() {
            // Given
            store.addFile(fileIngestRequest1);
            store.addFile(fileIngestRequest2);
            store.addFile(fileIngestRequest3);
        }

        @Test
        void shouldReportAllFilesInOrderRequestsReceivedMostRecentFirst() {
            // When / Then
            assertThat(store.getAllFilesNewestFirst()).containsExactly(
                    fileIngestRequest3, fileIngestRequest2, fileIngestRequest1);
        }

        @Test
        void shouldListPendingFilesInOrderRequestsReceivedOldestFirst() {
            // When / Then
            assertThat(store.getPendingFilesOldestFirst()).containsExactly(
                    fileIngestRequest1, fileIngestRequest2, fileIngestRequest3);
        }

        @Test
        void shouldReportAllFilesInOrderRequestsReceivedMostRecentFirstWhenOneHasBeenAssignedToAJob() {
            // Given
            store.assignJobGetAssigned("test-job", List.of(fileIngestRequest2));

            // When / Then
            assertThat(store.getAllFilesNewestFirst()).containsExactly(
                    fileIngestRequest3, onJob("test-job", fileIngestRequest2), fileIngestRequest1);
        }
    }

    @Nested
    @DisplayName("Set expiry time")
    class SetExpiryTime {
        @Test
        void shouldSetExpiryTime() {
            // Given
            table1.set(INGEST_BATCHER_TRACKING_TTL_MINUTES, "2");
            IngestBatcherTrackedFile fileIngestRequest = fileRequest()
                    .receivedTime(Instant.parse("2023-05-24T11:06:42Z"))
                    .build();
            Instant expectedExpiryTime = Instant.parse("2023-05-24T11:08:42Z");

            // When
            store.addFile(fileIngestRequest);

            // Then
            assertThat(streamPagedItems(dynamoClientV2, ScanRequest.builder()
                    .tableName(requestsTableName)
                    .build()))
                    .extracting(item -> getLongAttribute(item, EXPIRY_TIME, 0L))
                    .containsExactly(expectedExpiryTime.getEpochSecond());
        }

        @Test
        void shouldSetExpiryTimeWhenAssigningJob() {
            // Given
            table1.set(INGEST_BATCHER_TRACKING_TTL_MINUTES, "2");
            IngestBatcherTrackedFile fileIngestRequest = fileRequest()
                    .receivedTime(Instant.parse("2023-05-24T11:06:42Z"))
                    .build();
            Instant expectedExpiryTime = Instant.parse("2023-05-24T11:08:42Z");

            // When
            store.addFile(fileIngestRequest);
            store.assignJobGetAssigned("test-job", List.of(fileIngestRequest));

            // Then
            assertThat(streamPagedItems(dynamoClientV2, ScanRequest.builder()
                    .tableName(requestsTableName)
                    .build()))
                    .extracting(item -> getLongAttribute(item, EXPIRY_TIME, 0L))
                    .containsExactly(expectedExpiryTime.getEpochSecond());
        }
    }

    @Nested
    @DisplayName("Delete all pending")
    class DeleteAllPending {
        final IngestBatcherTrackedFile fileIngestRequest = fileRequest()
                .file("test-bucket/first.parquet")
                .build();

        @Test
        void shouldDeletePendingFile() {
            // Given
            store.addFile(fileIngestRequest);

            // When
            store.deleteAllPending();

            // Then
            assertThat(store.getAllFilesNewestFirst())
                    .isEmpty();
        }

        @Test
        void shouldNotDeleteAssignedFile() {
            // Given
            store.addFile(fileIngestRequest);
            store.assignJobGetAssigned("test-job", List.of(fileIngestRequest));

            // When
            store.deleteAllPending();

            // Then
            assertThat(store.getAllFilesNewestFirst())
                    .containsExactly(fileIngestRequest.toBuilder().jobId("test-job").build());
        }
    }

    private IngestBatcherTrackedFile.Builder fileRequest() {
        return requests.fileRequest().tableId(tableId);
    }
}
