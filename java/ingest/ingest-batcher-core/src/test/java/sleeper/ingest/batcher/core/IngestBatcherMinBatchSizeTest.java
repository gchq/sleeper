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

import sleeper.core.properties.table.TableProperties;
import sleeper.ingest.core.job.IngestJob;

import java.time.Instant;
import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.core.properties.table.TableProperty.INGEST_BATCHER_MAX_FILE_AGE_SECONDS;
import static sleeper.core.properties.table.TableProperty.INGEST_BATCHER_MIN_JOB_FILES;
import static sleeper.core.properties.table.TableProperty.INGEST_BATCHER_MIN_JOB_SIZE;
import static sleeper.ingest.batcher.core.testutil.IngestBatcherTestHelper.timeSupplier;

class IngestBatcherMinBatchSizeTest extends IngestBatcherTestBase {

    @Nested
    @DisplayName("Batch with minimum file count")
    class BatchWithMinimumFileCount {
        @Test
        void shouldBatchOneFileWhenMinimumFileCountIsOne() {
            // Given
            tableProperties.set(INGEST_BATCHER_MIN_JOB_FILES, "1");
            addFileToStore("test-bucket/test.parquet");

            // When
            batchFilesWithJobIds("test-job-id");

            // Then
            assertThat(queues.getMessagesByQueueUrl())
                    .isEqualTo(queueMessages(
                            jobWithFiles("test-job-id", "test-bucket/test.parquet")));
        }

        @Test
        void shouldBatchNoFilesWhenMinimumCountIsTwoAndOneFileIsTracked() {
            // Given
            tableProperties.set(INGEST_BATCHER_MIN_JOB_FILES, "2");
            addFileToStore("test-bucket/test.parquet");

            // When
            batchFilesWithJobIds("test-job-id");

            // Then
            assertThat(queues.getMessagesByQueueUrl()).isEqualTo(Collections.emptyMap());
        }

        @Test
        void shouldBatchOneTableWhenOtherTableDoesNotMeetMinSize() {
            // Given
            TableProperties table1 = createTableProperties("test-table-1");
            TableProperties table2 = createTableProperties("test-table-2");
            table1.set(INGEST_BATCHER_MIN_JOB_FILES, "1");
            table2.set(INGEST_BATCHER_MIN_JOB_FILES, "2");
            addFileToStore(builder -> builder.tableId("test-table-1").file("test-bucket/test-1.parquet"));
            addFileToStore(builder -> builder.tableId("test-table-2").file("test-bucket/test-2.parquet"));

            // When
            batchFilesWithTablesAndJobIds(List.of(table1, table2), List.of("test-job-id"));

            // Then
            assertThat(queues.getMessagesByQueueUrl())
                    .isEqualTo(queueMessages(IngestJob.builder()
                            .tableId("test-table-1")
                            .id("test-job-id")
                            .files(List.of("test-bucket/test-1.parquet"))
                            .build()));
        }

        @Test
        void shouldBatchMultipleFilesWhenMinimumCountIsMet() {
            // Given
            tableProperties.set(INGEST_BATCHER_MIN_JOB_FILES, "2");
            addFileToStore("test-bucket/test-1.parquet");
            addFileToStore("test-bucket/test-2.parquet");

            // When
            batchFilesWithJobIds("test-job-id");

            // Then
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
            addFileToStore(builder -> builder
                    .file("test-bucket/test.parquet")
                    .fileSizeBytes(2048));

            // When
            batchFilesWithJobIds("test-job-id");

            // Then
            assertThat(queues.getMessagesByQueueUrl())
                    .isEqualTo(queueMessages(
                            jobWithFiles("test-job-id", "test-bucket/test.parquet")));
        }

        @Test
        void shouldBatchNoFilesWhenUnderMinimumFileSize() {
            // Given
            tableProperties.set(INGEST_BATCHER_MIN_JOB_SIZE, "1K");
            addFileToStore(builder -> builder
                    .file("test-bucket/test.parquet")
                    .fileSizeBytes(512));

            // When
            batchFilesWithJobIds("test-job-id");

            // Then
            assertThat(queues.getMessagesByQueueUrl()).isEmpty();
        }

        @Test
        void shouldBatchOneTableWhenOtherTableDoesNotMeetMinSize() {
            // Given
            TableProperties table1 = createTableProperties("test-table-1");
            TableProperties table2 = createTableProperties("test-table-2");
            table1.set(INGEST_BATCHER_MIN_JOB_SIZE, "1K");
            table2.set(INGEST_BATCHER_MIN_JOB_SIZE, "1K");
            addFileToStore(builder -> builder.tableId("test-table-1").fileSizeBytes(2048)
                    .file("test-bucket/test-1.parquet"));
            addFileToStore(builder -> builder.tableId("test-table-2").fileSizeBytes(600)
                    .file("test-bucket/test-2.parquet"));

            // When
            batchFilesWithTablesAndJobIds(List.of(table1, table2), List.of("test-job-id"));

            // Then
            assertThat(queues.getMessagesByQueueUrl())
                    .isEqualTo(queueMessages(IngestJob.builder()
                            .tableId("test-table-1")
                            .id("test-job-id")
                            .files(List.of("test-bucket/test-1.parquet"))
                            .build()));
        }

        @Test
        void shouldBatchMultipleFilesWhenTotalFileSizeExceedsMinimum() {
            // Given
            tableProperties.set(INGEST_BATCHER_MIN_JOB_SIZE, "1K");
            addFileToStore(builder -> builder
                    .file("test-bucket/test-1.parquet")
                    .fileSizeBytes(600));
            addFileToStore(builder -> builder
                    .file("test-bucket/test-2.parquet")
                    .fileSizeBytes(600));

            // When
            batchFilesWithJobIds("test-job-id");

            // Then
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
            addFileToStore(builder -> builder
                    .file("test-bucket/test.parquet")
                    .fileSizeBytes(2048));

            // When
            batchFilesWithJobIds("test-job-id");

            // Then
            assertThat(queues.getMessagesByQueueUrl()).isEmpty();
        }

        @Test
        void shouldNotBatchFileWhenMinimumFileCountMetButMinimumFileSizeNotMet() {
            // Given
            tableProperties.set(INGEST_BATCHER_MIN_JOB_SIZE, "1K");
            tableProperties.set(INGEST_BATCHER_MIN_JOB_FILES, "1");
            addFileToStore(builder -> builder
                    .file("test-bucket/test.parquet")
                    .fileSizeBytes(512));

            // When
            batchFilesWithJobIds("test-job-id");

            // Then
            assertThat(queues.getMessagesByQueueUrl()).isEmpty();
        }

        @Test
        void shouldBatchFileWhenMinimumFileSizeMetAndMinimumFileCountMet() {
            // Given
            tableProperties.set(INGEST_BATCHER_MIN_JOB_SIZE, "1K");
            tableProperties.set(INGEST_BATCHER_MIN_JOB_FILES, "1");
            addFileToStore(builder -> builder
                    .file("test-bucket/test.parquet")
                    .fileSizeBytes(2048));

            // When
            batchFilesWithJobIds("test-job-id");

            // Then
            assertThat(queues.getMessagesByQueueUrl())
                    .isEqualTo(queueMessages(
                            jobWithFiles("test-job-id", "test-bucket/test.parquet")));
        }
    }

    @Nested
    @DisplayName("Ignore minimum batch size when max age is met")
    class ApplyMaxFileAge {

        @Test
        void shouldSendFileWhenOverMaximumAgeAndMinimumJobSizeIsNotMet() {
            // Given
            tableProperties.set(INGEST_BATCHER_MAX_FILE_AGE_SECONDS, "10");
            tableProperties.set(INGEST_BATCHER_MIN_JOB_FILES, "2");
            tableProperties.set(INGEST_BATCHER_MIN_JOB_SIZE, "1K");
            addFileToStore(builder -> builder
                    .file("test-bucket/test.parquet").fileSizeBytes(256)
                    .receivedTime(Instant.parse("2023-05-22T11:11:00Z")).build());
            Instant batchTime = Instant.parse("2023-05-22T11:11:10.001Z");

            // When
            batchFilesWithJobIds(List.of("test-job-id"), builder -> builder.timeSupplier(timeSupplier(batchTime)));

            // Then
            assertThat(queues.getMessagesByQueueUrl())
                    .isEqualTo(queueMessages(
                            jobWithFiles("test-job-id", "test-bucket/test.parquet")));
        }

        @Test
        void shouldSendTwoFilesWhenOneOfThemIsOverMaximumAgeAndMinimumJobSizeIsNotMet() {
            // Given
            tableProperties.set(INGEST_BATCHER_MAX_FILE_AGE_SECONDS, "10");
            tableProperties.set(INGEST_BATCHER_MIN_JOB_FILES, "3");
            tableProperties.set(INGEST_BATCHER_MIN_JOB_SIZE, "1K");
            addFileToStore(builder -> builder
                    .file("test-bucket/test-1.parquet").fileSizeBytes(256)
                    .receivedTime(Instant.parse("2023-05-22T11:11:00Z")).build());
            addFileToStore(builder -> builder
                    .file("test-bucket/test-2.parquet").fileSizeBytes(256)
                    .receivedTime(Instant.parse("2023-05-22T11:11:05Z")).build());
            Instant batchTime = Instant.parse("2023-05-22T11:11:10.001Z");

            // When
            batchFilesWithJobIds(List.of("test-job-id"), builder -> builder.timeSupplier(timeSupplier(batchTime)));

            // Then
            assertThat(queues.getMessagesByQueueUrl())
                    .isEqualTo(queueMessages(
                            jobWithFiles("test-job-id",
                                    "test-bucket/test-1.parquet",
                                    "test-bucket/test-2.parquet")));
        }
    }
}
