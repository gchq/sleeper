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

package sleeper.ingest.job;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import sleeper.ingest.job.status.IngestJobStatusStore;
import sleeper.ingest.job.status.WriteToMemoryIngestJobStatusStore;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.ingest.job.status.IngestJobStatusTestData.jobStatus;
import static sleeper.ingest.job.status.IngestJobStatusTestData.rejectedRun;

public class IngestJobMessageHandlerTest {

    @Nested
    @DisplayName("Read job")
    class ReadJob {

        @Test
        void shouldReadJob() {
            // Given
            IngestJobStatusStore ingestJobStatusStore = new WriteToMemoryIngestJobStatusStore();
            IngestJobMessageHandler<IngestJob> ingestJobMessageHandler = messageHandler(ingestJobStatusStore).build();
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
                            .files("file1.parquet", "file2.parquet")
                            .build());
            assertThat(ingestJobStatusStore.getInvalidJobs()).isEmpty();
            assertThat(ingestJobStatusStore.getAllJobs("test-table")).isEmpty();
        }

        @Test
        void shouldGenerateId() {
            // Given
            IngestJobStatusStore ingestJobStatusStore = new WriteToMemoryIngestJobStatusStore();
            IngestJobMessageHandler<IngestJob> ingestJobMessageHandler = messageHandler(ingestJobStatusStore)
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
                            .files("file1.parquet", "file2.parquet")
                            .build());
            assertThat(ingestJobStatusStore.getInvalidJobs()).isEmpty();
            assertThat(ingestJobStatusStore.getAllJobs("test-table")).isEmpty();
        }

        @Test
        void shouldGenerateIdWhenEmpty() {
            // Given
            IngestJobStatusStore ingestJobStatusStore = new WriteToMemoryIngestJobStatusStore();
            IngestJobMessageHandler<IngestJob> ingestJobMessageHandler = messageHandler(ingestJobStatusStore)
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
                            .files("file1.parquet", "file2.parquet")
                            .build());
            assertThat(ingestJobStatusStore.getInvalidJobs()).isEmpty();
            assertThat(ingestJobStatusStore.getAllJobs("test-table")).isEmpty();
        }
    }

    @Nested
    @DisplayName("Expand directories")
    class ExpandDirectories {

        @Test
        void shouldExpandDirectories() {
            // Given
            IngestJobStatusStore ingestJobStatusStore = new WriteToMemoryIngestJobStatusStore();
            IngestJobMessageHandler<IngestJob> ingestJobMessageHandler = messageHandlerWithDirectories(ingestJobStatusStore,
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
            assertThat(ingestJobStatusStore.getAllJobs("test-table")).isEmpty();
        }

        @Test
        void shouldFailValidationWhenDirectoryIsEmpty() {
            // Given
            Instant validationTime = Instant.parse("2023-07-03T16:14:00Z");
            IngestJobStatusStore ingestJobStatusStore = new WriteToMemoryIngestJobStatusStore();
            IngestJobMessageHandler<IngestJob> ingestJobMessageHandler = messageHandlerWithDirectories(ingestJobStatusStore,
                    Map.of("dir", List.of()))
                    .timeSupplier(() -> validationTime)
                    .build();
            String json = "{" +
                    "\"id\":\"test-job-id\"," +
                    "\"tableName\":\"test-table\"," +
                    "\"files\":[\"dir\"]" +
                    "}";

            // When / Then
            assertThat(ingestJobMessageHandler.deserialiseAndValidate(json)).isEmpty();
            assertThat(ingestJobStatusStore.getInvalidJobs())
                    .containsExactly(jobStatus("test-job-id",
                            rejectedRun("test-job-id", json, validationTime,
                                    "Could not find one or more files")));
            assertThat(ingestJobStatusStore.getAllJobs("test-table")).isEmpty();
        }

        @Test
        void shouldFailValidationWhenDirectoryIsEmptyAndIdIsGenerated() {
            // Given
            Instant validationTime = Instant.parse("2023-07-03T16:14:00Z");
            IngestJobStatusStore ingestJobStatusStore = new WriteToMemoryIngestJobStatusStore();
            IngestJobMessageHandler<IngestJob> ingestJobMessageHandler = messageHandlerWithDirectories(ingestJobStatusStore,
                    Map.of("dir", List.of()))
                    .jobIdSupplier(() -> "test-job-id")
                    .timeSupplier(() -> validationTime)
                    .build();
            String json = "{" +
                    "\"tableName\":\"test-table\"," +
                    "\"files\":[\"dir\"]" +
                    "}";

            // When / Then
            assertThat(ingestJobMessageHandler.deserialiseAndValidate(json)).isEmpty();
            assertThat(ingestJobStatusStore.getInvalidJobs())
                    .containsExactly(jobStatus("test-job-id",
                            rejectedRun("test-job-id", json, validationTime,
                                    "Could not find one or more files")));
            assertThat(ingestJobStatusStore.getAllJobs("test-table")).isEmpty();
        }
    }

    @Nested
    @DisplayName("Validate job")
    class ValidateJob {
        @Test
        void shouldReportValidationFailureIfJsonInvalid() {
            // Given
            Instant validationTime = Instant.parse("2023-07-03T16:14:00Z");
            IngestJobStatusStore ingestJobStatusStore = new WriteToMemoryIngestJobStatusStore();
            IngestJobMessageHandler<IngestJob> ingestJobMessageHandler = messageHandler(ingestJobStatusStore)
                    .jobIdSupplier(() -> "test-job-id")
                    .timeSupplier(() -> validationTime)
                    .build();
            String json = "{";

            // When / Then
            assertThat(ingestJobMessageHandler.deserialiseAndValidate(json)).isEmpty();
            assertThat(ingestJobStatusStore.getInvalidJobs())
                    .containsExactly(jobStatus("test-job-id",
                            rejectedRun("test-job-id", json, validationTime,
                                    "Error parsing JSON. Reason: End of input at line 1 column 2 path $.")));
        }

        @Test
        void shouldReportModelValidationFailureIfTableNameNotProvided() {
            // Given
            Instant validationTime = Instant.parse("2023-07-03T16:14:00Z");
            IngestJobStatusStore ingestJobStatusStore = new WriteToMemoryIngestJobStatusStore();
            IngestJobMessageHandler<IngestJob> ingestJobMessageHandler = messageHandler(ingestJobStatusStore)
                    .jobIdSupplier(() -> "test-job-id")
                    .timeSupplier(() -> validationTime)
                    .build();
            String json = "{" +
                    "\"files\":[]" +
                    "}";

            // When / Then
            assertThat(ingestJobMessageHandler.deserialiseAndValidate(json)).isEmpty();
            assertThat(ingestJobStatusStore.getInvalidJobs())
                    .containsExactly(jobStatus("test-job-id",
                            rejectedRun("test-job-id", json, validationTime,
                                    "Model validation failed. Missing property \"tableName\"")));
        }

        @Test
        void shouldReportModelValidationFailureIfFilesNotProvided() {
            // Given
            Instant validationTime = Instant.parse("2023-07-03T16:14:00Z");
            IngestJobStatusStore ingestJobStatusStore = new WriteToMemoryIngestJobStatusStore();
            IngestJobMessageHandler<IngestJob> ingestJobMessageHandler = messageHandler(ingestJobStatusStore)
                    .jobIdSupplier(() -> "test-job-id")
                    .timeSupplier(() -> validationTime)
                    .build();
            String json = "{" +
                    "\"tableName\":\"test-table\"" +
                    "}";

            // When / Then
            assertThat(ingestJobMessageHandler.deserialiseAndValidate(json)).isEmpty();
            assertThat(ingestJobStatusStore.getInvalidJobs())
                    .containsExactly(jobStatus("test-job-id",
                            rejectedRun("test-job-id", json, validationTime,
                                    "Model validation failed. Missing property \"files\"")));
        }

        @Test
        void shouldReportMultipleModelValidationFailures() {
            // Given
            Instant validationTime = Instant.parse("2023-07-03T16:14:00Z");
            IngestJobStatusStore ingestJobStatusStore = new WriteToMemoryIngestJobStatusStore();
            IngestJobMessageHandler<IngestJob> ingestJobMessageHandler = messageHandler(ingestJobStatusStore)
                    .jobIdSupplier(() -> "test-job-id")
                    .timeSupplier(() -> validationTime)
                    .build();
            String json = "{}";

            // When / Then
            assertThat(ingestJobMessageHandler.deserialiseAndValidate(json)).isEmpty();
            assertThat(ingestJobStatusStore.getInvalidJobs())
                    .containsExactly(jobStatus("test-job-id",
                            rejectedRun("test-job-id", json, validationTime,
                                    "Model validation failed. Missing property \"files\"",
                                    "Model validation failed. Missing property \"tableName\"")));
        }

        @Test
        void shouldReportValidationFailureIfFileIsNull() {
            // Given
            Instant validationTime = Instant.parse("2023-07-03T16:14:00Z");
            IngestJobStatusStore ingestJobStatusStore = new WriteToMemoryIngestJobStatusStore();
            IngestJobMessageHandler<IngestJob> ingestJobMessageHandler = messageHandler(ingestJobStatusStore)
                    .timeSupplier(() -> validationTime)
                    .build();
            String json = "{" +
                    "\"id\": \"invalid-job-1\"," +
                    "\"tableName\": \"system-test\"," +
                    "\"files\": [,]" +
                    "}";

            // When / Then
            assertThat(ingestJobMessageHandler.deserialiseAndValidate(json)).isEmpty();
            assertThat(ingestJobStatusStore.getInvalidJobs())
                    .containsExactly(jobStatus("invalid-job-1",
                            rejectedRun("invalid-job-1", json, validationTime,
                                    "Model validation failed. One of the files was null")));
        }
    }

    private IngestJobMessageHandler.Builder<IngestJob> messageHandler(IngestJobStatusStore ingestJobStatusStore) {
        return messageHandlerWithDirectories(ingestJobStatusStore, Map.of());
    }

    private IngestJobMessageHandler.Builder<IngestJob> messageHandlerWithDirectories(
            IngestJobStatusStore ingestJobStatusStore, Map<String, List<String>> directoryContents) {

        return IngestJobMessageHandler.forIngestJob()
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
