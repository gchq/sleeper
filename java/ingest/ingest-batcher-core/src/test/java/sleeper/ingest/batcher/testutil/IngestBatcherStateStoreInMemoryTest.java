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

package sleeper.ingest.batcher.testutil;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import sleeper.ingest.batcher.FileIngestRequest;
import sleeper.ingest.batcher.IngestBatcherStateStore;

import java.time.Instant;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.ingest.batcher.testutil.FileIngestRequestTestHelper.onJob;

class IngestBatcherStateStoreInMemoryTest {
    private final IngestBatcherStateStore store = new IngestBatcherStateStoreInMemory();
    private final FileIngestRequestTestHelper requests = new FileIngestRequestTestHelper();

    @Nested
    @DisplayName("Add ingest requests")
    class AddIngestRequests {

        @Test
        void shouldTrackOneFile() {
            // Given
            FileIngestRequest fileIngestRequest = fileRequest()
                    .pathToFile("test-bucket/test.parquet")
                    .tableName("test-table").build();

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
                    .pathToFile("test-bucket/test.parquet")
                    .tableName("test-table")
                    .fileSizeBytes(1024).build();
            FileIngestRequest fileIngestRequest2 = fileRequest()
                    .pathToFile("test-bucket/test.parquet")
                    .tableName("test-table")
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
                    .pathToFile("test-bucket/test.parquet")
                    .tableName("test-table-1").build();
            FileIngestRequest fileIngestRequest2 = fileRequest()
                    .pathToFile("test-bucket/test.parquet")
                    .tableName("test-table-2").build();

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
            FileIngestRequest fileIngestRequest1 = fileRequest()
                    .pathToFile("test-bucket/test-1.parquet")
                    .tableName("test-table-1").build();
            FileIngestRequest fileIngestRequest2 = fileRequest()
                    .pathToFile("test-bucket/test-2.parquet")
                    .tableName("test-table-1").build();

            // When
            store.addFile(fileIngestRequest1);
            store.addFile(fileIngestRequest2);
            store.assignJob("test-job", List.of(fileIngestRequest1, fileIngestRequest2));

            // Then
            assertThat(store.getAllFilesNewestFirst()).containsExactlyInAnyOrder(
                    onJob("test-job", fileIngestRequest1),
                    onJob("test-job", fileIngestRequest2));
            assertThat(store.getPendingFilesOldestFirst()).isEmpty();
        }

        @Test
        void shouldSendSameFileTwiceIfFirstRequestAssignedToJob() {
            // Given
            FileIngestRequest fileIngestRequest1 = fileRequest()
                    .pathToFile("test-bucket/test.parquet")
                    .tableName("test-table-1").build();
            FileIngestRequest fileIngestRequest2 = fileRequest()
                    .pathToFile("test-bucket/test.parquet")
                    .tableName("test-table-1").build();

            // When
            store.addFile(fileIngestRequest1);
            store.assignJob("test-job", List.of(fileIngestRequest1));
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
            FileIngestRequest fileIngestRequest = fileRequest()
                    .pathToFile("test-bucket/test.parquet")
                    .fileSizeBytes(1234L)
                    .tableName("test-table")
                    .receivedTime(Instant.parse("2023-05-19T15:40:12Z"))
                    .build();

            // When
            store.addFile(fileIngestRequest);
            store.assignJob("test-job", List.of(fileIngestRequest));

            // Then
            assertThat(store.getAllFilesNewestFirst()).containsExactly(
                    fileRequest()
                            .pathToFile("test-bucket/test.parquet")
                            .fileSizeBytes(1234L)
                            .tableName("test-table")
                            .receivedTime(Instant.parse("2023-05-19T15:40:12Z"))
                            .jobId("test-job")
                            .build());
        }
    }

    @Nested
    @DisplayName("Order files returned from the store")
    class OrderFilesReturnedFromStore {

        final FileIngestRequest fileIngestRequest1 = fileRequest()
                .pathToFile("test-bucket/first.parquet")
                .tableName("test-table").build();
        final FileIngestRequest fileIngestRequest2 = fileRequest()
                .pathToFile("test-bucket/another.parquet")
                .tableName("test-table").build();
        final FileIngestRequest fileIngestRequest3 = fileRequest()
                .pathToFile("test-bucket/last.parquet")
                .tableName("test-table").build();

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
            store.assignJob("test-job", List.of(fileIngestRequest2));

            // When / Then
            assertThat(store.getAllFilesNewestFirst()).containsExactly(
                    fileIngestRequest3, onJob("test-job", fileIngestRequest2), fileIngestRequest1);
        }
    }

    private FileIngestRequest.Builder fileRequest() {
        return requests.fileRequest();
    }

}
