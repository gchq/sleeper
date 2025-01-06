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

package sleeper.ingest.core.job;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import sleeper.core.table.InMemoryTableIndex;
import sleeper.core.table.TableIndex;
import sleeper.core.table.TableStatus;
import sleeper.core.table.TableStatusTestHelper;
import sleeper.core.tracker.ingest.job.InMemoryIngestJobStatusStore;
import sleeper.core.tracker.ingest.job.IngestJobStatus;
import sleeper.core.tracker.ingest.job.IngestJobStatusStore;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.core.tracker.ingest.job.IngestJobStatusTestData.ingestJobStatus;
import static sleeper.core.tracker.ingest.job.IngestJobStatusTestData.rejectedRun;

public class IngestJobMessageHandlerTest {

    private final TableIndex tableIndex = new InMemoryTableIndex();
    private final IngestJobStatusStore ingestJobStatusStore = new InMemoryIngestJobStatusStore();
    private final TableStatus table = TableStatusTestHelper.uniqueIdAndName("test-table-id", "test-table");
    private final String tableId = table.getTableUniqueId();

    @BeforeEach
    void setUp() {
        tableIndex.create(table);
    }

    @Nested
    @DisplayName("Read job")
    class ReadJob {

        @Test
        void shouldReadJob() {
            // Given
            IngestJobMessageHandler<IngestJob> ingestJobMessageHandler = messageHandler().build();
            String json = "{" +
                    "\"id\":\"test-job-id\"," +
                    "\"tableName\":\"test-table\"," +
                    "\"files\":[\"file1.parquet\",\"file2.parquet\"]" +
                    "}";

            // When / Then
            assertThat(ingestJobMessageHandler.deserialiseAndValidate(json))
                    .contains(IngestJob.builder()
                            .id("test-job-id")
                            .tableName("test-table")
                            .tableId("test-table-id")
                            .files(List.of("file1.parquet", "file2.parquet"))
                            .build());
            assertThat(ingestJobStatusStore.getInvalidJobs()).isEmpty();
            assertThat(ingestJobStatusStore.getAllJobs(tableId)).isEmpty();
        }

        @Test
        void shouldGenerateId() {
            // Given
            IngestJobMessageHandler<IngestJob> ingestJobMessageHandler = messageHandler()
                    .jobIdSupplier(() -> "test-job-id")
                    .build();
            String json = "{" +
                    "\"tableName\":\"test-table\"," +
                    "\"files\":[\"file1.parquet\",\"file2.parquet\"]" +
                    "}";

            // When / Then
            assertThat(ingestJobMessageHandler.deserialiseAndValidate(json))
                    .contains(IngestJob.builder()
                            .id("test-job-id")
                            .tableName("test-table")
                            .tableId("test-table-id")
                            .files(List.of("file1.parquet", "file2.parquet"))
                            .build());
            assertThat(ingestJobStatusStore.getInvalidJobs()).isEmpty();
            assertThat(ingestJobStatusStore.getAllJobs(tableId)).isEmpty();
        }

        @Test
        void shouldGenerateIdWhenEmpty() {
            // Given
            IngestJobMessageHandler<IngestJob> ingestJobMessageHandler = messageHandler()
                    .jobIdSupplier(() -> "test-job-id")
                    .build();
            String json = "{" +
                    "\"id\":\"\"," +
                    "\"tableName\":\"test-table\"," +
                    "\"files\":[\"file1.parquet\",\"file2.parquet\"]" +
                    "}";

            // When / Then
            assertThat(ingestJobMessageHandler.deserialiseAndValidate(json))
                    .contains(IngestJob.builder()
                            .id("test-job-id")
                            .tableName("test-table")
                            .tableId("test-table-id")
                            .files(List.of("file1.parquet", "file2.parquet"))
                            .build());
            assertThat(ingestJobStatusStore.getInvalidJobs()).isEmpty();
            assertThat(ingestJobStatusStore.getAllJobs(tableId)).isEmpty();
        }

        @Test
        void shouldReadJobByTableId() {
            // Given
            IngestJobMessageHandler<IngestJob> ingestJobMessageHandler = messageHandler().build();
            String json = "{" +
                    "\"id\":\"test-job-id\"," +
                    "\"tableId\":\"test-table-id\"," +
                    "\"files\":[\"file1.parquet\",\"file2.parquet\"]" +
                    "}";

            // When / Then
            assertThat(ingestJobMessageHandler.deserialiseAndValidate(json))
                    .contains(IngestJob.builder()
                            .id("test-job-id")
                            .tableName("test-table")
                            .tableId("test-table-id")
                            .files(List.of("file1.parquet", "file2.parquet"))
                            .build());
            assertThat(ingestJobStatusStore.getInvalidJobs()).isEmpty();
            assertThat(ingestJobStatusStore.getAllJobs(tableId)).isEmpty();
        }
    }

    @Nested
    @DisplayName("Expand directories")
    class ExpandDirectories {

        @Test
        void shouldExpandDirectories() {
            // Given
            IngestJobMessageHandler<IngestJob> ingestJobMessageHandler = messageHandlerWithDirectories(
                    Map.of("dir1", List.of("file1a.parquet", "file1b.parquet"),
                            "dir2", List.of("file2.parquet")))
                    .build();
            String json = "{" +
                    "\"id\":\"test-job-id\"," +
                    "\"tableName\":\"test-table\"," +
                    "\"files\":[\"dir1\",\"dir2\"]" +
                    "}";

            // When / Then
            assertThat(ingestJobMessageHandler.deserialiseAndValidate(json))
                    .get().extracting(IngestJob::getTableName, IngestJob::getFiles)
                    .containsExactly("test-table", List.of("dir1/file1a.parquet", "dir1/file1b.parquet", "dir2/file2.parquet"));
            assertThat(ingestJobStatusStore.getInvalidJobs()).isEmpty();
            assertThat(ingestJobStatusStore.getAllJobs(tableId)).isEmpty();
        }

        @Test
        void shouldFailValidationWhenDirectoryIsEmpty() {
            // Given
            Instant validationTime = Instant.parse("2023-07-03T16:14:00Z");
            IngestJobMessageHandler<IngestJob> ingestJobMessageHandler = messageHandlerWithDirectories(
                    Map.of("dir", List.of()))
                    .timeSupplier(() -> validationTime)
                    .build();
            String json = "{" +
                    "\"id\":\"test-job-id\"," +
                    "\"tableName\":\"test-table\"," +
                    "\"files\":[\"dir\"]" +
                    "}";

            // When / Then
            IngestJobStatus expected = ingestJobStatus("test-job-id",
                    rejectedRun("test-job-id", json, validationTime,
                            "Could not find one or more files"));
            assertThat(ingestJobMessageHandler.deserialiseAndValidate(json)).isEmpty();
            assertThat(ingestJobStatusStore.getInvalidJobs())
                    .containsExactly(expected);
            assertThat(ingestJobStatusStore.getAllJobs(tableId))
                    .containsExactly(expected);
        }

        @Test
        void shouldFailValidationWhenDirectoryIsEmptyAndIdIsGenerated() {
            // Given
            Instant validationTime = Instant.parse("2023-07-03T16:14:00Z");
            IngestJobMessageHandler<IngestJob> ingestJobMessageHandler = messageHandlerWithDirectories(
                    Map.of("dir", List.of()))
                    .jobIdSupplier(() -> "test-job-id")
                    .timeSupplier(() -> validationTime)
                    .build();
            String json = "{" +
                    "\"tableName\":\"test-table\"," +
                    "\"files\":[\"dir\"]" +
                    "}";

            // When / Then
            IngestJobStatus expected = ingestJobStatus("test-job-id",
                    rejectedRun("test-job-id", json, validationTime,
                            "Could not find one or more files"));
            assertThat(ingestJobMessageHandler.deserialiseAndValidate(json)).isEmpty();
            assertThat(ingestJobStatusStore.getInvalidJobs()).containsExactly(expected);
            assertThat(ingestJobStatusStore.getAllJobs(tableId)).containsExactly(expected);
        }
    }

    @Nested
    @DisplayName("Validate job")
    class ValidateJob {
        @Test
        void shouldReportValidationFailureIfJsonInvalid() {
            // Given
            Instant validationTime = Instant.parse("2023-07-03T16:14:00Z");
            IngestJobMessageHandler<IngestJob> ingestJobMessageHandler = messageHandler()
                    .jobIdSupplier(() -> "test-job-id")
                    .timeSupplier(() -> validationTime)
                    .build();
            String json = "{";

            // When / Then
            assertThat(ingestJobMessageHandler.deserialiseAndValidate(json)).isEmpty();
            assertThat(ingestJobStatusStore.getInvalidJobs())
                    .containsExactly(ingestJobStatus("test-job-id",
                            rejectedRun("test-job-id", json, validationTime,
                                    "Error parsing JSON. Reason: End of input at line 1 column 2 path $.")));
            assertThat(ingestJobStatusStore.getAllJobs(tableId))
                    .isEmpty();
        }

        @Test
        void shouldReportModelValidationFailureIfTableNameNotProvided() {
            // Given
            Instant validationTime = Instant.parse("2023-07-03T16:14:00Z");
            IngestJobMessageHandler<IngestJob> ingestJobMessageHandler = messageHandler()
                    .jobIdSupplier(() -> "test-job-id")
                    .timeSupplier(() -> validationTime)
                    .build();
            String json = "{" +
                    "\"files\":[]" +
                    "}";

            // When / Then
            assertThat(ingestJobMessageHandler.deserialiseAndValidate(json)).isEmpty();
            assertThat(ingestJobStatusStore.getInvalidJobs())
                    .containsExactly(ingestJobStatus("test-job-id",
                            rejectedRun("test-job-id", json, validationTime,
                                    "Table not found")));
            assertThat(ingestJobStatusStore.getAllJobs(tableId))
                    .isEmpty();
        }

        @Test
        void shouldReportModelValidationFailureIfFilesNotProvided() {
            // Given
            Instant validationTime = Instant.parse("2023-07-03T16:14:00Z");
            IngestJobMessageHandler<IngestJob> ingestJobMessageHandler = messageHandler()
                    .jobIdSupplier(() -> "test-job-id")
                    .timeSupplier(() -> validationTime)
                    .build();
            String json = "{" +
                    "\"tableName\":\"test-table\"" +
                    "}";

            // When / Then
            IngestJobStatus expected = ingestJobStatus("test-job-id",
                    rejectedRun("test-job-id", json, validationTime,
                            "Missing property \"files\""));
            assertThat(ingestJobMessageHandler.deserialiseAndValidate(json)).isEmpty();
            assertThat(ingestJobStatusStore.getInvalidJobs())
                    .containsExactly(expected);
            assertThat(ingestJobStatusStore.getAllJobs(tableId))
                    .containsExactly(expected);
        }

        @Test
        void shouldReportValidationFailureIfTableDoesNotExistByName() {
            // Given
            Instant validationTime = Instant.parse("2023-07-03T16:14:00Z");
            IngestJobMessageHandler<IngestJob> ingestJobMessageHandler = messageHandler()
                    .jobIdSupplier(() -> "test-job-id")
                    .timeSupplier(() -> validationTime)
                    .build();
            String json = "{" +
                    "\"id\":\"test-job-id\"," +
                    "\"tableName\":\"non-existent-table\"," +
                    "\"files\":[\"file1.parquet\",\"file2.parquet\"]" +
                    "}";

            // When / Then
            IngestJobStatus expectedStatus = ingestJobStatus("test-job-id",
                    rejectedRun("test-job-id", json, validationTime,
                            "Table not found"));
            assertThat(ingestJobMessageHandler.deserialiseAndValidate(json)).isEmpty();
            assertThat(ingestJobStatusStore.getInvalidJobs())
                    .containsExactly(expectedStatus);
        }

        @Test
        void shouldReportValidationFailureIfTableDoesNotExistById() {
            // Given
            Instant validationTime = Instant.parse("2023-07-03T16:14:00Z");
            IngestJobMessageHandler<IngestJob> ingestJobMessageHandler = messageHandler()
                    .jobIdSupplier(() -> "test-job-id")
                    .timeSupplier(() -> validationTime)
                    .build();
            String json = "{" +
                    "\"id\":\"test-job-id\"," +
                    "\"tableId\":\"non-existent-table\"," +
                    "\"files\":[\"file1.parquet\",\"file2.parquet\"]" +
                    "}";

            // When / Then
            IngestJobStatus expectedStatus = ingestJobStatus("test-job-id",
                    rejectedRun("test-job-id", json, validationTime,
                            "Table not found"));
            assertThat(ingestJobMessageHandler.deserialiseAndValidate(json)).isEmpty();
            assertThat(ingestJobStatusStore.getInvalidJobs())
                    .containsExactly(expectedStatus);
        }

        @Test
        void shouldReportMultipleModelValidationFailures() {
            // Given
            Instant validationTime = Instant.parse("2023-07-03T16:14:00Z");
            IngestJobMessageHandler<IngestJob> ingestJobMessageHandler = messageHandler()
                    .jobIdSupplier(() -> "test-job-id")
                    .timeSupplier(() -> validationTime)
                    .build();
            String json = "{}";

            // When / Then
            assertThat(ingestJobMessageHandler.deserialiseAndValidate(json)).isEmpty();
            assertThat(ingestJobStatusStore.getInvalidJobs())
                    .containsExactly(ingestJobStatus("test-job-id",
                            rejectedRun("test-job-id", json, validationTime,
                                    "Missing property \"files\"",
                                    "Table not found")));
            assertThat(ingestJobStatusStore.getAllJobs(tableId))
                    .isEmpty();
        }

        @Test
        void shouldReportValidationFailureIfFileIsNull() {
            // Given
            Instant validationTime = Instant.parse("2023-07-03T16:14:00Z");
            IngestJobMessageHandler<IngestJob> ingestJobMessageHandler = messageHandler()
                    .timeSupplier(() -> validationTime)
                    .build();
            String json = "{" +
                    "\"id\": \"invalid-job-1\"," +
                    "\"tableName\": \"test-table\"," +
                    "\"files\": [,]" +
                    "}";

            // When / Then
            IngestJobStatus expected = ingestJobStatus("invalid-job-1",
                    rejectedRun("invalid-job-1", json, validationTime,
                            "One of the files was null"));
            assertThat(ingestJobMessageHandler.deserialiseAndValidate(json)).isEmpty();
            assertThat(ingestJobStatusStore.getInvalidJobs())
                    .containsExactly(expected);
            assertThat(ingestJobStatusStore.getAllJobs(tableId))
                    .containsExactly(expected);
        }
    }

    private IngestJobMessageHandler.Builder<IngestJob> messageHandler() {
        return messageHandlerWithDirectories(Map.of());
    }

    private IngestJobMessageHandler.Builder<IngestJob> messageHandlerWithDirectories(Map<String, List<String>> directoryContents) {
        return IngestJobMessageHandler.forIngestJob()
                .tableIndex(tableIndex)
                .ingestJobStatusStore(ingestJobStatusStore)
                .expandDirectories(files -> expandDirFiles(files, directoryContents));
    }

    private static List<String> expandDirFiles(List<String> files, Map<String, List<String>> directoryContents) {
        return files.stream()
                .flatMap(dir -> Optional.ofNullable(directoryContents.get(dir))
                        .map(dirFiles -> dirFiles.stream().map(file -> dir + "/" + file))
                        .orElseGet(() -> Stream.of(dir)))
                .collect(Collectors.toUnmodifiableList());
    }
}
