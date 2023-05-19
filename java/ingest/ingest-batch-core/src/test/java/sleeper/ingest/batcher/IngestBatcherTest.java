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

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import sleeper.configuration.properties.InstanceProperties;
import sleeper.configuration.properties.table.FixedTablePropertiesProvider;
import sleeper.configuration.properties.table.TableProperties;
import sleeper.configuration.properties.validation.BatchIngestMode;
import sleeper.ingest.job.IngestJob;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.configuration.properties.InstancePropertiesTestHelper.createTestInstanceProperties;
import static sleeper.configuration.properties.SystemDefinedInstanceProperty.INGEST_JOB_QUEUE_URL;
import static sleeper.configuration.properties.table.TablePropertiesTestHelper.createTestTableProperties;
import static sleeper.configuration.properties.table.TableProperty.INGEST_BATCHER_INGEST_MODE;
import static sleeper.configuration.properties.table.TableProperty.INGEST_BATCHER_MAX_JOB_FILES;
import static sleeper.configuration.properties.table.TableProperty.INGEST_BATCHER_MAX_JOB_SIZE;
import static sleeper.configuration.properties.table.TableProperty.INGEST_BATCHER_MIN_JOB_FILES;
import static sleeper.configuration.properties.table.TableProperty.INGEST_BATCHER_MIN_JOB_SIZE;
import static sleeper.configuration.properties.table.TableProperty.TABLE_NAME;
import static sleeper.core.schema.SchemaTestHelper.schemaWithKey;
import static sleeper.ingest.batcher.FileIngestRequestTestHelper.onJob;

class IngestBatcherTest {
    private static final String DEFAULT_TABLE_NAME = "test-table";
    private final InstanceProperties instanceProperties = createTestInstanceProperties();
    private final TableProperties tableProperties = createTableProperties(DEFAULT_TABLE_NAME);
    private final IngestBatcherStateStore store = new IngestBatcherStateStoreInMemory();
    private final IngestBatcherQueuesInMemory queues = new IngestBatcherQueuesInMemory();

    @BeforeEach
    void setUp() {
        instanceProperties.set(INGEST_JOB_QUEUE_URL, "test-ingest-queue-url");
    }

    private Map<String, List<Object>> queueMessages(IngestJob... jobs) {
        return Map.of("test-ingest-queue-url", List.of(jobs));
    }

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
            assertThat(store.getAllFiles()).containsExactly(
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
            assertThat(store.getPendingFiles()).containsExactly(request);
            assertThat(queues.getMessagesByQueueUrl()).isEqualTo(Collections.emptyMap());
        }

        @Test
        void shouldBatchNoFilesWhenMinimumCountIsTwoAndTwoFilesAreTrackedForDifferentTables() {
            // Given
            TableProperties table1 = createTableProperties("test-table-1");
            TableProperties table2 = createTableProperties("test-table-2");
            table1.set(INGEST_BATCHER_MIN_JOB_FILES, "2");
            table2.set(INGEST_BATCHER_MIN_JOB_FILES, "2");
            FileIngestRequest request1 = addFileToStore(FileIngestRequest.builder()
                    .pathToFile("test-bucket/test-1.parquet")
                    .tableName("test-table-1")
                    .build());
            FileIngestRequest request2 = addFileToStore(FileIngestRequest.builder()
                    .pathToFile("test-bucket/test-2.parquet")
                    .tableName("test-table-2")
                    .build());

            // When
            batchFilesWithTablesAndJobIds(List.of(table1, table2), List.of());

            // Then
            assertThat(store.getPendingFiles()).containsExactly(request1, request2);
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
            assertThat(store.getAllFiles()).containsExactly(
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
            assertThat(store.getAllFiles()).containsExactly(
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
            assertThat(store.getPendingFiles()).containsExactly(request);
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
            assertThat(store.getAllFiles()).containsExactly(
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
            assertThat(store.getPendingFiles()).containsExactly(request);
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
            assertThat(store.getPendingFiles()).containsExactly(request);
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
            assertThat(store.getAllFiles()).containsExactly(
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
            assertThat(store.getAllFiles()).containsExactly(
                    onJob("test-job-id-1", request1),
                    onJob("test-job-id-2", request2));
            assertThat(queues.getMessagesByQueueUrl())
                    .isEqualTo(queueMessages(
                            jobWithFiles("test-job-id-1", "test-bucket/test-1.parquet"),
                            jobWithFiles("test-job-id-2", "test-bucket/test-2.parquet")));
        }

        @Test
        void shouldCreateOneJobForOneFileWhenMaximumFileCountIsMet() {
            // Given
            tableProperties.set(INGEST_BATCHER_MAX_JOB_FILES, "1");
            FileIngestRequest request = addFileToStore("test-bucket/test.parquet");

            // When
            batchFilesWithJobIds("test-job-id");

            // Then
            assertThat(store.getAllFiles()).containsExactly(
                    onJob("test-job-id", request));
            assertThat(queues.getMessagesByQueueUrl())
                    .isEqualTo(queueMessages(
                            jobWithFiles("test-job-id", "test-bucket/test.parquet")));
        }

        @Test
        void shouldCreateOneJobForTwoFileWhenMaximumFileCountIsMet() {
            // Given
            tableProperties.set(INGEST_BATCHER_MAX_JOB_FILES, "2");
            FileIngestRequest request1 = addFileToStore("test-bucket/test-1.parquet");
            FileIngestRequest request2 = addFileToStore("test-bucket/test-2.parquet");

            // When
            batchFilesWithJobIds("test-job-id-1");

            // Then
            assertThat(store.getAllFiles()).containsExactly(
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
            assertThat(store.getAllFiles()).containsExactly(
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
            assertThat(store.getAllFiles()).containsExactly(
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
            assertThat(store.getAllFiles()).containsExactly(
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
            assertThat(store.getAllFiles()).containsExactly(
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
            assertThat(store.getAllFiles()).containsExactly(
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

    private TableProperties createTableProperties(String tableName) {
        TableProperties properties = createTestTableProperties(instanceProperties, schemaWithKey("key"));
        properties.set(INGEST_BATCHER_INGEST_MODE, BatchIngestMode.STANDARD_INGEST.toString());
        properties.set(INGEST_BATCHER_MIN_JOB_SIZE, "0");
        properties.set(INGEST_BATCHER_MIN_JOB_FILES, "0");
        properties.set(TABLE_NAME, tableName);
        return properties;
    }

    private IngestJob jobWithFiles(String jobId, String... files) {
        return IngestJob.builder()
                .files(files)
                .tableName(DEFAULT_TABLE_NAME)
                .id(jobId)
                .build();
    }

    private FileIngestRequest addFileToStore(String pathToFile) {
        return addFileToStore(ingestRequest()
                .pathToFile(pathToFile).build());
    }

    private FileIngestRequest.Builder ingestRequest() {
        return FileIngestRequest.builder()
                .tableName(DEFAULT_TABLE_NAME)
                .fileSizeBytes(1024);
    }

    private FileIngestRequest addFileToStore(FileIngestRequest request) {
        store.addFile(request);
        return request;
    }

    private void batchFilesWithJobIds(String... jobIds) {
        batchFilesWithTablesAndJobIds(List.of(tableProperties), List.of(jobIds));
    }

    private void batchFilesWithTablesAndJobIds(List<TableProperties> tables, List<String> jobIds) {
        IngestBatcher.builder()
                .instanceProperties(instanceProperties)
                .tablePropertiesProvider(new FixedTablePropertiesProvider(tables))
                .jobIdSupplier(jobIdSupplier(jobIds))
                .store(store).queueClient(queues)
                .build().batchFiles();
    }

    private static Supplier<String> jobIdSupplier(List<String> jobIds) {
        return Stream.concat(jobIds.stream(), infiniteIdsForUnexpectedJobs())
                .iterator()::next;
    }

    private static Stream<String> infiniteIdsForUnexpectedJobs() {
        return IntStream.iterate(1, n -> n + 1)
                .mapToObj(num -> "unexpected-job-" + num);
    }
}
