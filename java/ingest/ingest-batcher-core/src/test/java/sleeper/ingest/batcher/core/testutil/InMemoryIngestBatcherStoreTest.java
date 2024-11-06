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

package sleeper.ingest.batcher.core.testutil;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import sleeper.ingest.batcher.core.FileIngestRequest;
import sleeper.ingest.batcher.core.IngestBatcherStore;

import java.time.Instant;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.ingest.batcher.core.testutil.FileIngestRequestTestHelper.onJob;

class InMemoryIngestBatcherStoreTest {
    private final IngestBatcherStore store = new InMemoryIngestBatcherStore();
    private final FileIngestRequestTestHelper requests = new FileIngestRequestTestHelper();

    @Nested
    @DisplayName("Add ingest requests")
    class AddIngestRequests {

        @Test
        void shouldTrackOneFile() {
            // Given
            FileIngestRequest fileIngestRequest = fileRequest()
                    .file("test-bucket/test.parquet")
                    .tableId("test-table").build();

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
            FileIngestRequest fileIngestRequest1 = fileRequest()
                    .file("test-bucket/test.parquet")
                    .tableId("test-table")
                    .fileSizeBytes(1024).build();
            FileIngestRequest fileIngestRequest2 = fileRequest()
                    .file("test-bucket/test.parquet")
                    .tableId("test-table")
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
            FileIngestRequest fileIngestRequest1 = fileRequest()
                    .file("test-bucket/test.parquet")
                    .tableId("test-table-1").build();
            FileIngestRequest fileIngestRequest2 = fileRequest()
                    .file("test-bucket/test.parquet")
                    .tableId("test-table-2").build();

            // When
            store.addFile(fileIngestRequest1);
            store.addFile(fileIngestRequest2);

            // Then
            assertThat(store.getAllFilesNewestFirst())
                    .containsExactly(fileIngestRequest2, fileIngestRequest1);
            assertThat(store.getPendingFilesOldestFirst())
                    .containsExactly(fileIngestRequest1, fileIngestRequest2);
        }

        @Test
        void shouldAddMultipleFilesAtSameTime() {
            // Given
            Instant receivedTime = Instant.parse("2023-11-03T12:09:00Z");
            FileIngestRequest fileIngestRequest1 = fileRequest().receivedTime(receivedTime).build();
            FileIngestRequest fileIngestRequest2 = fileRequest().receivedTime(receivedTime).build();

            // When
            store.addFile(fileIngestRequest1);
            store.addFile(fileIngestRequest2);

            // Then
            assertThat(store.getAllFilesNewestFirst())
                    .containsExactly(fileIngestRequest1, fileIngestRequest2);
            assertThat(store.getPendingFilesOldestFirst())
                    .containsExactly(fileIngestRequest1, fileIngestRequest2);
        }
    }

    @Nested
    @DisplayName("Assign files to jobs")
    class AssignFilesToJobs {

        @Test
        void shouldTrackJobWasCreatedWithTwoFiles() {
            // Given
            FileIngestRequest fileIngestRequest1 = fileRequest()
                    .file("test-bucket/test-1.parquet")
                    .tableId("test-table-1").build();
            FileIngestRequest fileIngestRequest2 = fileRequest()
                    .file("test-bucket/test-2.parquet")
                    .tableId("test-table-1").build();

            // When
            store.addFile(fileIngestRequest1);
            store.addFile(fileIngestRequest2);
            store.assignJobGetAssigned("test-job", List.of(fileIngestRequest1, fileIngestRequest2));

            // Then
            assertThat(store.getAllFilesNewestFirst()).containsExactly(
                    onJob("test-job", fileIngestRequest2),
                    onJob("test-job", fileIngestRequest1));
            assertThat(store.getPendingFilesOldestFirst()).isEmpty();
        }

        @Test
        void shouldSendSameFileTwiceIfFirstRequestAssignedToJob() {
            // Given
            FileIngestRequest fileIngestRequest1 = fileRequest()
                    .file("test-bucket/test.parquet")
                    .tableId("test-table-1").build();
            FileIngestRequest fileIngestRequest2 = fileRequest()
                    .file("test-bucket/test.parquet")
                    .tableId("test-table-1").build();

            // When
            store.addFile(fileIngestRequest1);
            store.assignJobGetAssigned("test-job", List.of(fileIngestRequest1));
            store.addFile(fileIngestRequest2);

            // Then
            assertThat(store.getAllFilesNewestFirst()).containsExactly(
                    fileIngestRequest2,
                    onJob("test-job", fileIngestRequest1));
            assertThat(store.getPendingFilesOldestFirst()).containsExactly(fileIngestRequest2);
        }

        @Test
        void shouldRetainAllFileRequestParametersAfterAssigningToJob() {
            // Given
            FileIngestRequest fileIngestRequest = fileRequest()
                    .file("test-bucket/test.parquet")
                    .fileSizeBytes(1234L)
                    .tableId("test-table")
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
                            .tableId("test-table")
                            .receivedTime(Instant.parse("2023-05-19T15:40:12Z"))
                            .jobId("test-job")
                            .build());
        }
    }

    @Nested
    @DisplayName("Order files returned from the store")
    class OrderFilesReturnedFromStore {

        final FileIngestRequest fileIngestRequest1 = fileRequest()
                .file("test-bucket/first.parquet")
                .tableId("test-table").build();
        final FileIngestRequest fileIngestRequest2 = fileRequest()
                .file("test-bucket/another.parquet")
                .tableId("test-table").build();
        final FileIngestRequest fileIngestRequest3 = fileRequest()
                .file("test-bucket/last.parquet")
                .tableId("test-table").build();

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
    @DisplayName("Delete all pending")
    class DeleteAllPending {
        final FileIngestRequest fileIngestRequest = fileRequest()
                .file("test-bucket/first.parquet")
                .tableId("test-table").build();

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

    private FileIngestRequest.Builder fileRequest() {
        return requests.fileRequest();
    }

}
