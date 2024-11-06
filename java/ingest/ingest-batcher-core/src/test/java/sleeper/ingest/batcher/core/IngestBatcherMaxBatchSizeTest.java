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
package sleeper.ingest.batcher.core;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.core.properties.table.TableProperty.INGEST_BATCHER_MAX_JOB_FILES;
import static sleeper.core.properties.table.TableProperty.INGEST_BATCHER_MAX_JOB_SIZE;

public class IngestBatcherMaxBatchSizeTest extends IngestBatcherTestBase {
    @Nested
    @DisplayName("Batch with maximum file count")
    class BatchWithMaximumFileCount {
        @Test
        void shouldCreateTwoJobsForTwoFilesWhenMaximumFileCountIsOne() {
            // Given
            tableProperties.set(INGEST_BATCHER_MAX_JOB_FILES, "1");
            addFileToStore("test-bucket/test-1.parquet");
            addFileToStore("test-bucket/test-2.parquet");

            // When
            batchFilesWithJobIds("test-job-id-1", "test-job-id-2");

            // Then
            assertThat(queues.getMessagesByQueueUrl())
                    .isEqualTo(queueMessages(
                            jobWithFiles("test-job-id-1", "test-bucket/test-1.parquet"),
                            jobWithFiles("test-job-id-2", "test-bucket/test-2.parquet")));
        }

        @Test
        void shouldCreateOneJobForOneFileWhenMaximumFileCountIsOne() {
            // Given
            tableProperties.set(INGEST_BATCHER_MAX_JOB_FILES, "1");
            addFileToStore("test-bucket/test.parquet");

            // When
            batchFilesWithJobIds("test-job-id");

            // Then
            assertThat(queues.getMessagesByQueueUrl())
                    .isEqualTo(queueMessages(
                            jobWithFiles("test-job-id", "test-bucket/test.parquet")));
        }

        @Test
        void shouldCreateOneJobForTwoFilesWhenMaximumFileCountIsTwo() {
            // Given
            tableProperties.set(INGEST_BATCHER_MAX_JOB_FILES, "2");
            addFileToStore("test-bucket/test-1.parquet");
            addFileToStore("test-bucket/test-2.parquet");

            // When
            batchFilesWithJobIds("test-job-id-1");

            // Then
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
            addFileToStore(builder -> builder
                    .file("test-bucket/test-1.parquet")
                    .fileSizeBytes(600));
            addFileToStore(builder -> builder
                    .file("test-bucket/test-2.parquet")
                    .fileSizeBytes(600));

            // When
            batchFilesWithJobIds("test-job-id-1", "test-job-id-2");

            // Then
            assertThat(queues.getMessagesByQueueUrl())
                    .isEqualTo(queueMessages(
                            jobWithFiles("test-job-id-1", "test-bucket/test-1.parquet"),
                            jobWithFiles("test-job-id-2", "test-bucket/test-2.parquet")));
        }

        @Test
        void shouldCreateTwoJobsForTwoFilesWhenMaximumFileSizeIsMetByFirstFile() {
            // Given
            tableProperties.set(INGEST_BATCHER_MAX_JOB_SIZE, "1K");
            addFileToStore(builder -> builder
                    .file("test-bucket/test-1.parquet")
                    .fileSizeBytes(1024));
            addFileToStore(builder -> builder
                    .file("test-bucket/test-2.parquet")
                    .fileSizeBytes(512));

            // When
            batchFilesWithJobIds("test-job-id-1", "test-job-id-2");

            // Then
            assertThat(queues.getMessagesByQueueUrl())
                    .isEqualTo(queueMessages(
                            jobWithFiles("test-job-id-1", "test-bucket/test-1.parquet"),
                            jobWithFiles("test-job-id-2", "test-bucket/test-2.parquet")));
        }

        @Test
        void shouldCreateTwoJobsForTwoFilesWhenMaximumFileSizeIsExceededByEitherFile() {
            // Given
            tableProperties.set(INGEST_BATCHER_MAX_JOB_SIZE, "1K");
            addFileToStore(builder -> builder
                    .file("test-bucket/test-1.parquet")
                    .fileSizeBytes(2048));
            addFileToStore(builder -> builder
                    .file("test-bucket/test-2.parquet")
                    .fileSizeBytes(2048));

            // When
            batchFilesWithJobIds("test-job-id-1", "test-job-id-2");

            // Then
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
            addFileToStore(builder -> builder
                    .file("test-bucket/test-1.parquet")
                    .fileSizeBytes(256));
            addFileToStore(builder -> builder
                    .file("test-bucket/test-2.parquet")
                    .fileSizeBytes(256));
            addFileToStore(builder -> builder
                    .file("test-bucket/test-3.parquet")
                    .fileSizeBytes(2048));
            addFileToStore(builder -> builder
                    .file("test-bucket/test-4.parquet")
                    .fileSizeBytes(512));

            // When
            batchFilesWithJobIds("test-job-id-1", "test-job-id-2", "test-job-id-3");

            // Then
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
            addFileToStore(builder -> builder
                    .file("test-bucket/test-1.parquet")
                    .fileSizeBytes(2048));
            addFileToStore(builder -> builder
                    .file("test-bucket/test-2.parquet")
                    .fileSizeBytes(256));
            addFileToStore(builder -> builder
                    .file("test-bucket/test-3.parquet")
                    .fileSizeBytes(256));
            addFileToStore(builder -> builder
                    .file("test-bucket/test-4.parquet")
                    .fileSizeBytes(512));

            // When
            batchFilesWithJobIds("test-job-id-1", "test-job-id-2", "test-job-id-3");

            // Then
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
            addFileToStore(builder -> builder
                    .file("test-bucket/test-1.parquet")
                    .fileSizeBytes(256));
            addFileToStore(builder -> builder
                    .file("test-bucket/test-2.parquet")
                    .fileSizeBytes(1024));
            addFileToStore(builder -> builder
                    .file("test-bucket/test-3.parquet")
                    .fileSizeBytes(256));

            // When
            batchFilesWithJobIds("test-job-id-1", "test-job-id-2");

            // Then
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
            addFileToStore(builder -> builder
                    .file("test-bucket/test-1.parquet")
                    .fileSizeBytes(512).build());
            addFileToStore(builder -> builder
                    .file("test-bucket/test-2.parquet")
                    .fileSizeBytes(1024).build());
            addFileToStore(builder -> builder
                    .file("test-bucket/test-3.parquet")
                    .fileSizeBytes(512).build());
            addFileToStore(builder -> builder
                    .file("test-bucket/test-4.parquet")
                    .fileSizeBytes(256).build());

            // When
            batchFilesWithJobIds("test-job-id-1", "test-job-id-2", "test-job-id-3");

            // Then
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
