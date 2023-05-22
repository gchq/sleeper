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

package sleeper.ingest.batcher;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import sleeper.configuration.properties.table.TableProperties;

import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.configuration.properties.table.TableProperty.INGEST_BATCHER_MAX_JOB_FILES;
import static sleeper.configuration.properties.table.TableProperty.INGEST_BATCHER_MAX_JOB_SIZE;
import static sleeper.configuration.properties.table.TableProperty.INGEST_BATCHER_MIN_JOB_FILES;
import static sleeper.configuration.properties.table.TableProperty.INGEST_BATCHER_MIN_JOB_SIZE;
import static sleeper.ingest.batcher.FileIngestRequestTestHelper.onJob;

class IngestBatcherTest extends IngestBatcherTestBase {

    @Nested
    @DisplayName("Batch with minimum file count")
    class BatchWithMinimumFileCount {
        @Test
        void shouldBatchOneFileWhenMinimumFileCountIsOne() {
            // Given
            tableProperties.set(INGEST_BATCHER_MIN_JOB_FILES, "1");
            FileIngestRequest request = addFileToStore("test-bucket/test.parquet");

            // When
            batchFilesWithJobIds("test-job-id");

            // Then
            assertThat(store.getAllFilesNewestFirst()).containsExactly(
                    onJob("test-job-id", request));
            assertThat(queues.getMessagesByQueueUrl())
                    .isEqualTo(queueMessages(
                            jobWithFiles("test-job-id", "test-bucket/test.parquet")));
        }

        @Test
        void shouldBatchNoFilesWhenMinimumCountIsTwoAndOneFileIsTracked() {
            // Given
            tableProperties.set(INGEST_BATCHER_MIN_JOB_FILES, "2");
            FileIngestRequest request = addFileToStore("test-bucket/test.parquet");

            // When
            batchFilesWithJobIds("test-job-id");

            // Then
            assertThat(store.getPendingFilesOldestFirst()).containsExactly(request);
            assertThat(queues.getMessagesByQueueUrl()).isEqualTo(Collections.emptyMap());
        }

        @Test
        void shouldBatchNoFilesWhenMinimumCountIsTwoAndTwoFilesAreTrackedForDifferentTables() {
            // Given
            TableProperties table1 = createTableProperties("test-table-1");
            TableProperties table2 = createTableProperties("test-table-2");
            table1.set(INGEST_BATCHER_MIN_JOB_FILES, "2");
            table2.set(INGEST_BATCHER_MIN_JOB_FILES, "2");
            FileIngestRequest request1 = addFileToStore(ingestRequest()
                    .pathToFile("test-bucket/test-1.parquet")
                    .tableName("test-table-1")
                    .build());
            FileIngestRequest request2 = addFileToStore(ingestRequest()
                    .pathToFile("test-bucket/test-2.parquet")
                    .tableName("test-table-2")
                    .build());

            // When
            batchFilesWithTablesAndJobIds(List.of(table1, table2), List.of());

            // Then
            assertThat(store.getPendingFilesOldestFirst()).containsExactly(request1, request2);
            assertThat(queues.getMessagesByQueueUrl()).isEqualTo(Collections.emptyMap());
        }

        @Test
        void shouldBatchMultipleFilesWhenMinimumCountIsMet() {
            // Given
            tableProperties.set(INGEST_BATCHER_MIN_JOB_FILES, "2");
            FileIngestRequest request1 = addFileToStore("test-bucket/test-1.parquet");
            FileIngestRequest request2 = addFileToStore("test-bucket/test-2.parquet");

            // When
            batchFilesWithJobIds("test-job-id");

            // Then
            assertThat(store.getAllFilesNewestFirst()).containsExactlyInAnyOrder(
                    onJob("test-job-id", request1),
                    onJob("test-job-id", request2));
            assertThat(queues.getMessagesByQueueUrl())
                    .isEqualTo(queueMessages(
                            jobWithFiles("test-job-id",
                                    "test-bucket/test-1.parquet",
                                    "test-bucket/test-2.parquet")));
        }
    }

    @Nested
    @DisplayName("Batch with minimum file size")
    class BatchWithMinimumFileSize {

        @Test
        void shouldBatchOneFileWhenFileExceedsMinimumFileSize() {
            // Given
            tableProperties.set(INGEST_BATCHER_MIN_JOB_SIZE, "1K");
            FileIngestRequest request = addFileToStore(ingestRequest()
                    .pathToFile("test-bucket/test.parquet")
                    .fileSizeBytes(2048).build());

            // When
            batchFilesWithJobIds("test-job-id");

            // Then
            assertThat(store.getAllFilesNewestFirst()).containsExactly(
                    onJob("test-job-id", request));
            assertThat(queues.getMessagesByQueueUrl())
                    .isEqualTo(queueMessages(
                            jobWithFiles("test-job-id", "test-bucket/test.parquet")));
        }

        @Test
        void shouldBatchNoFilesWhenUnderMinimumFileSize() {
            // Given
            tableProperties.set(INGEST_BATCHER_MIN_JOB_SIZE, "1K");
            FileIngestRequest request = addFileToStore(ingestRequest()
                    .pathToFile("test-bucket/test.parquet")
                    .fileSizeBytes(512).build());

            // When
            batchFilesWithJobIds("test-job-id");

            // Then
            assertThat(store.getPendingFilesOldestFirst()).containsExactly(request);
            assertThat(queues.getMessagesByQueueUrl()).isEmpty();
        }

        @Test
        void shouldBatchMultipleFilesWhenTotalFileSizeExceedsMinimum() {
            // Given
            tableProperties.set(INGEST_BATCHER_MIN_JOB_SIZE, "1K");
            FileIngestRequest request1 = addFileToStore(ingestRequest()
                    .pathToFile("test-bucket/test-1.parquet")
                    .fileSizeBytes(600).build());
            FileIngestRequest request2 = addFileToStore(ingestRequest()
                    .pathToFile("test-bucket/test-2.parquet")
                    .fileSizeBytes(600).build());

            // When
            batchFilesWithJobIds("test-job-id");

            // Then
            assertThat(store.getAllFilesNewestFirst()).containsExactlyInAnyOrder(
                    onJob("test-job-id", request1),
                    onJob("test-job-id", request2));
            assertThat(queues.getMessagesByQueueUrl())
                    .isEqualTo(queueMessages(
                            jobWithFiles("test-job-id",
                                    "test-bucket/test-1.parquet",
                                    "test-bucket/test-2.parquet")));
        }
    }

    @Nested
    @DisplayName("Batch with combined minimum job sizes")
    class BatchWithCombinedMinimumJobSizes {
        @Test
        void shouldNotBatchFileWhenMinimumFileSizeMetButMinimumFileCountNotMet() {
            // Given
            tableProperties.set(INGEST_BATCHER_MIN_JOB_SIZE, "1K");
            tableProperties.set(INGEST_BATCHER_MIN_JOB_FILES, "2");
            FileIngestRequest request = addFileToStore(ingestRequest()
                    .pathToFile("test-bucket/test.parquet")
                    .fileSizeBytes(2048).build());

            // When
            batchFilesWithJobIds("test-job-id");

            // Then
            assertThat(store.getPendingFilesOldestFirst()).containsExactly(request);
            assertThat(queues.getMessagesByQueueUrl()).isEmpty();
        }

        @Test
        void shouldNotBatchFileWhenMinimumFileCountMetButMinimumFileSizeNotMet() {
            // Given
            tableProperties.set(INGEST_BATCHER_MIN_JOB_SIZE, "1K");
            tableProperties.set(INGEST_BATCHER_MIN_JOB_FILES, "1");
            FileIngestRequest request = addFileToStore(ingestRequest()
                    .pathToFile("test-bucket/test.parquet")
                    .fileSizeBytes(512).build());

            // When
            batchFilesWithJobIds("test-job-id");

            // Then
            assertThat(store.getPendingFilesOldestFirst()).containsExactly(request);
            assertThat(queues.getMessagesByQueueUrl()).isEmpty();
        }

        @Test
        void shouldBatchFileWhenMinimumFileSizeMetAndMinimumFileCountMet() {
            // Given
            tableProperties.set(INGEST_BATCHER_MIN_JOB_SIZE, "1K");
            tableProperties.set(INGEST_BATCHER_MIN_JOB_FILES, "1");
            FileIngestRequest request = addFileToStore(ingestRequest()
                    .pathToFile("test-bucket/test.parquet")
                    .fileSizeBytes(2048).build());

            // When
            batchFilesWithJobIds("test-job-id");

            // Then
            assertThat(store.getAllFilesNewestFirst()).containsExactly(
                    onJob("test-job-id", request));
            assertThat(queues.getMessagesByQueueUrl())
                    .isEqualTo(queueMessages(
                            jobWithFiles("test-job-id", "test-bucket/test.parquet")));
        }
    }

    @Nested
    @DisplayName("Batch with maximum file count")
    class BatchWithMaximumFileCount {
        @Test
        void shouldCreateTwoJobsForTwoFilesWhenMaximumFileCountIsOne() {
            // Given
            tableProperties.set(INGEST_BATCHER_MAX_JOB_FILES, "1");
            FileIngestRequest request1 = addFileToStore("test-bucket/test-1.parquet");
            FileIngestRequest request2 = addFileToStore("test-bucket/test-2.parquet");

            // When
            batchFilesWithJobIds("test-job-id-1", "test-job-id-2");

            // Then
            assertThat(store.getAllFilesNewestFirst()).containsExactlyInAnyOrder(
                    onJob("test-job-id-1", request1),
                    onJob("test-job-id-2", request2));
            assertThat(queues.getMessagesByQueueUrl())
                    .isEqualTo(queueMessages(
                            jobWithFiles("test-job-id-1", "test-bucket/test-1.parquet"),
                            jobWithFiles("test-job-id-2", "test-bucket/test-2.parquet")));
        }

        @Test
        void shouldCreateOneJobForOneFileWhenMaximumFileCountIsOne() {
            // Given
            tableProperties.set(INGEST_BATCHER_MAX_JOB_FILES, "1");
            FileIngestRequest request = addFileToStore("test-bucket/test.parquet");

            // When
            batchFilesWithJobIds("test-job-id");

            // Then
            assertThat(store.getAllFilesNewestFirst()).containsExactly(
                    onJob("test-job-id", request));
            assertThat(queues.getMessagesByQueueUrl())
                    .isEqualTo(queueMessages(
                            jobWithFiles("test-job-id", "test-bucket/test.parquet")));
        }

        @Test
        void shouldCreateOneJobForTwoFilesWhenMaximumFileCountIsTwo() {
            // Given
            tableProperties.set(INGEST_BATCHER_MAX_JOB_FILES, "2");
            FileIngestRequest request1 = addFileToStore("test-bucket/test-1.parquet");
            FileIngestRequest request2 = addFileToStore("test-bucket/test-2.parquet");

            // When
            batchFilesWithJobIds("test-job-id-1");

            // Then
            assertThat(store.getAllFilesNewestFirst()).containsExactlyInAnyOrder(
                    onJob("test-job-id-1", request1),
                    onJob("test-job-id-1", request2));
            assertThat(queues.getMessagesByQueueUrl())
                    .isEqualTo(queueMessages(
                            jobWithFiles("test-job-id-1",
                                    "test-bucket/test-1.parquet",
                                    "test-bucket/test-2.parquet")));
        }
    }

    @Nested
    @DisplayName("Batch with maximum file size")
    class BatchWithMaximumFileSize {
        @Test
        void shouldCreateTwoJobsForTwoFilesWhenSizesAddedTogetherExceedMaximum() {
            // Given
            tableProperties.set(INGEST_BATCHER_MAX_JOB_SIZE, "1K");
            FileIngestRequest request1 = addFileToStore(ingestRequest()
                    .pathToFile("test-bucket/test-1.parquet")
                    .fileSizeBytes(600).build());
            FileIngestRequest request2 = addFileToStore(ingestRequest()
                    .pathToFile("test-bucket/test-2.parquet")
                    .fileSizeBytes(600).build());

            // When
            batchFilesWithJobIds("test-job-id-1", "test-job-id-2");

            // Then
            assertThat(store.getAllFilesNewestFirst()).containsExactlyInAnyOrder(
                    onJob("test-job-id-1", request1),
                    onJob("test-job-id-2", request2));
            assertThat(queues.getMessagesByQueueUrl())
                    .isEqualTo(queueMessages(
                            jobWithFiles("test-job-id-1", "test-bucket/test-1.parquet"),
                            jobWithFiles("test-job-id-2", "test-bucket/test-2.parquet")));
        }

        @Test
        void shouldCreateTwoJobsForTwoFilesWhenMaximumFileSizeIsMetByFirstFile() {
            // Given
            tableProperties.set(INGEST_BATCHER_MAX_JOB_SIZE, "1K");
            FileIngestRequest request1 = addFileToStore(ingestRequest()
                    .pathToFile("test-bucket/test-1.parquet")
                    .fileSizeBytes(1024).build());
            FileIngestRequest request2 = addFileToStore(ingestRequest()
                    .pathToFile("test-bucket/test-2.parquet")
                    .fileSizeBytes(512).build());

            // When
            batchFilesWithJobIds("test-job-id-1", "test-job-id-2");

            // Then
            assertThat(store.getAllFilesNewestFirst()).containsExactlyInAnyOrder(
                    onJob("test-job-id-1", request1),
                    onJob("test-job-id-2", request2));
            assertThat(queues.getMessagesByQueueUrl())
                    .isEqualTo(queueMessages(
                            jobWithFiles("test-job-id-1", "test-bucket/test-1.parquet"),
                            jobWithFiles("test-job-id-2", "test-bucket/test-2.parquet")));
        }

        @Test
        void shouldCreateTwoJobsForTwoFilesWhenMaximumFileSizeIsExceededByEitherFile() {
            // Given
            tableProperties.set(INGEST_BATCHER_MAX_JOB_SIZE, "1K");
            FileIngestRequest request1 = addFileToStore(ingestRequest()
                    .pathToFile("test-bucket/test-1.parquet")
                    .fileSizeBytes(2048).build());
            FileIngestRequest request2 = addFileToStore(ingestRequest()
                    .pathToFile("test-bucket/test-2.parquet")
                    .fileSizeBytes(2048).build());

            // When
            batchFilesWithJobIds("test-job-id-1", "test-job-id-2");

            // Then
            assertThat(store.getAllFilesNewestFirst()).containsExactlyInAnyOrder(
                    onJob("test-job-id-1", request1),
                    onJob("test-job-id-2", request2));
            assertThat(queues.getMessagesByQueueUrl())
                    .isEqualTo(queueMessages(
                            jobWithFiles("test-job-id-1", "test-bucket/test-1.parquet"),
                            jobWithFiles("test-job-id-2", "test-bucket/test-2.parquet")));
        }
    }

    @Nested
    @DisplayName("Batch with combined maximum job sizes")
    class BatchWithCombinedMaximumJobSizes {
        @Test
        void shouldCreateFirstBatchMeetingFileCountAndSecondBatchMeetingFileSizeAndThirdBatchWithLeftoverFile() {
            // Given
            tableProperties.set(INGEST_BATCHER_MAX_JOB_FILES, "2");
            tableProperties.set(INGEST_BATCHER_MAX_JOB_SIZE, "1K");
            FileIngestRequest request1 = addFileToStore(ingestRequest()
                    .pathToFile("test-bucket/test-1.parquet")
                    .fileSizeBytes(256).build());
            FileIngestRequest request2 = addFileToStore(ingestRequest()
                    .pathToFile("test-bucket/test-2.parquet")
                    .fileSizeBytes(256).build());
            FileIngestRequest request3 = addFileToStore(ingestRequest()
                    .pathToFile("test-bucket/test-3.parquet")
                    .fileSizeBytes(2048).build());
            FileIngestRequest request4 = addFileToStore(ingestRequest()
                    .pathToFile("test-bucket/test-4.parquet")
                    .fileSizeBytes(512).build());

            // When
            batchFilesWithJobIds("test-job-id-1", "test-job-id-2", "test-job-id-3");

            // Then
            assertThat(store.getAllFilesNewestFirst()).containsExactlyInAnyOrder(
                    onJob("test-job-id-1", request1),
                    onJob("test-job-id-1", request2),
                    onJob("test-job-id-2", request3),
                    onJob("test-job-id-3", request4));
            assertThat(queues.getMessagesByQueueUrl())
                    .isEqualTo(queueMessages(
                            jobWithFiles("test-job-id-1",
                                    "test-bucket/test-1.parquet",
                                    "test-bucket/test-2.parquet"),
                            jobWithFiles("test-job-id-2",
                                    "test-bucket/test-3.parquet"),
                            jobWithFiles("test-job-id-3",
                                    "test-bucket/test-4.parquet")));
        }

        @Test
        void shouldCreateFirstBatchMeetingFileSizeAndSecondBatchMeetingFileCountAndThirdBatchWithLeftoverFile() {
            // Given
            tableProperties.set(INGEST_BATCHER_MAX_JOB_FILES, "2");
            tableProperties.set(INGEST_BATCHER_MAX_JOB_SIZE, "1K");
            FileIngestRequest request1 = addFileToStore(ingestRequest()
                    .pathToFile("test-bucket/test-1.parquet")
                    .fileSizeBytes(2048).build());
            FileIngestRequest request2 = addFileToStore(ingestRequest()
                    .pathToFile("test-bucket/test-2.parquet")
                    .fileSizeBytes(256).build());
            FileIngestRequest request3 = addFileToStore(ingestRequest()
                    .pathToFile("test-bucket/test-3.parquet")
                    .fileSizeBytes(256).build());
            FileIngestRequest request4 = addFileToStore(ingestRequest()
                    .pathToFile("test-bucket/test-4.parquet")
                    .fileSizeBytes(512).build());

            // When
            batchFilesWithJobIds("test-job-id-1", "test-job-id-2", "test-job-id-3");

            // Then
            assertThat(store.getAllFilesNewestFirst()).containsExactlyInAnyOrder(
                    onJob("test-job-id-1", request1),
                    onJob("test-job-id-2", request2),
                    onJob("test-job-id-2", request3),
                    onJob("test-job-id-3", request4));
            assertThat(queues.getMessagesByQueueUrl())
                    .isEqualTo(queueMessages(
                            jobWithFiles("test-job-id-1",
                                    "test-bucket/test-1.parquet"),
                            jobWithFiles("test-job-id-2",
                                    "test-bucket/test-2.parquet",
                                    "test-bucket/test-3.parquet"),
                            jobWithFiles("test-job-id-3",
                                    "test-bucket/test-4.parquet")));
        }
    }

    @Nested
    @DisplayName("Add to previous batches")
    class AddToPreviousBatches {
        @Test
        void shouldAddToPreviousBatchWhenSmallFileIsAddedAfterLargeFile() {
            // Given
            tableProperties.set(INGEST_BATCHER_MAX_JOB_SIZE, "1K");
            FileIngestRequest request1 = addFileToStore(ingestRequest()
                    .pathToFile("test-bucket/test-1.parquet")
                    .fileSizeBytes(256).build());
            FileIngestRequest request2 = addFileToStore(ingestRequest()
                    .pathToFile("test-bucket/test-2.parquet")
                    .fileSizeBytes(1024).build());
            FileIngestRequest request3 = addFileToStore(ingestRequest()
                    .pathToFile("test-bucket/test-3.parquet")
                    .fileSizeBytes(256).build());

            // When
            batchFilesWithJobIds("test-job-id-1", "test-job-id-2");

            // Then
            assertThat(store.getAllFilesNewestFirst()).containsExactlyInAnyOrder(
                    onJob("test-job-id-1", request1),
                    onJob("test-job-id-1", request3),
                    onJob("test-job-id-2", request2));
            assertThat(queues.getMessagesByQueueUrl())
                    .isEqualTo(queueMessages(
                            jobWithFiles("test-job-id-1",
                                    "test-bucket/test-1.parquet",
                                    "test-bucket/test-3.parquet"),
                            jobWithFiles("test-job-id-2",
                                    "test-bucket/test-2.parquet")));
        }

        @Test
        void shouldFillPreviousBatchWhenSmallFilesAreAddedAfterLargeFile() {
            // Given
            tableProperties.set(INGEST_BATCHER_MAX_JOB_SIZE, "1K");
            FileIngestRequest request1 = addFileToStore(ingestRequest()
                    .pathToFile("test-bucket/test-1.parquet")
                    .fileSizeBytes(512).build());
            FileIngestRequest request2 = addFileToStore(ingestRequest()
                    .pathToFile("test-bucket/test-2.parquet")
                    .fileSizeBytes(1024).build());
            FileIngestRequest request3 = addFileToStore(ingestRequest()
                    .pathToFile("test-bucket/test-3.parquet")
                    .fileSizeBytes(512).build());
            FileIngestRequest request4 = addFileToStore(ingestRequest()
                    .pathToFile("test-bucket/test-4.parquet")
                    .fileSizeBytes(256).build());

            // When
            batchFilesWithJobIds("test-job-id-1", "test-job-id-2", "test-job-id-3");

            // Then
            assertThat(store.getAllFilesNewestFirst()).containsExactlyInAnyOrder(
                    onJob("test-job-id-1", request1),
                    onJob("test-job-id-1", request3),
                    onJob("test-job-id-2", request2),
                    onJob("test-job-id-3", request4));
            assertThat(queues.getMessagesByQueueUrl())
                    .isEqualTo(queueMessages(
                            jobWithFiles("test-job-id-1",
                                    "test-bucket/test-1.parquet",
                                    "test-bucket/test-3.parquet"),
                            jobWithFiles("test-job-id-2",
                                    "test-bucket/test-2.parquet"),
                            jobWithFiles("test-job-id-3",
                                    "test-bucket/test-4.parquet")));
        }
    }
}
