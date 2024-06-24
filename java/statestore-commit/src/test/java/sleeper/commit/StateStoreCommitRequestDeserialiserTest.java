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
package sleeper.commit;

import org.junit.jupiter.api.Test;

import sleeper.compaction.job.CompactionJob;
import sleeper.compaction.job.commit.CompactionJobCommitRequest;
import sleeper.compaction.job.commit.CompactionJobCommitRequestSerDe;
import sleeper.core.record.process.RecordsProcessed;
import sleeper.core.record.process.RecordsProcessedSummary;
import sleeper.core.statestore.FileReference;
import sleeper.ingest.job.IngestJob;
import sleeper.ingest.job.commit.IngestAddFilesCommitRequest;
import sleeper.ingest.job.commit.IngestAddFilesCommitRequestSerDe;

import java.time.Duration;
import java.time.Instant;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class StateStoreCommitRequestDeserialiserTest {
    StateStoreCommitRequestDeserialiser commitRequestSerDe = new StateStoreCommitRequestDeserialiser();

    @Test
    void shouldDeserialiseCompactionJobCommitRequest() {
        // Given
        CompactionJob job = CompactionJob.builder()
                .tableId("test-table")
                .jobId("test-job")
                .inputFiles(List.of("file1.parquet", "file2.parquet"))
                .outputFile("test-output.parquet")
                .partitionId("test-partition-id")
                .build();
        CompactionJobCommitRequest compactionJobCommitRequest = new CompactionJobCommitRequest(job, "test-task",
                new RecordsProcessedSummary(
                        new RecordsProcessed(120, 100),
                        Instant.parse("2024-05-01T10:58:00Z"), Duration.ofMinutes(1)));
        String jsonString = new CompactionJobCommitRequestSerDe().toJson(compactionJobCommitRequest);

        // When / Then
        assertThat(commitRequestSerDe.fromJson(jsonString))
                .isEqualTo(StateStoreCommitRequest.forCompactionJob(compactionJobCommitRequest));
    }

    @Test
    void shouldDeserialiseIngestJobCommitRequest() {
        // Given
        IngestJob job = IngestJob.builder()
                .id("test-job-id")
                .files(List.of("file1.parquet", "file2.parquet"))
                .tableId("test-table-id")
                .tableName("test-table-name")
                .build();
        FileReference file1 = FileReference.builder()
                .filename("file1.parquet")
                .partitionId("root")
                .numberOfRecords(100L)
                .onlyContainsDataForThisPartition(true)
                .build();
        FileReference file2 = FileReference.builder()
                .filename("file2.parquet")
                .partitionId("root")
                .numberOfRecords(200L)
                .onlyContainsDataForThisPartition(true)
                .build();
        IngestAddFilesCommitRequest ingestJobCommitRequest = IngestAddFilesCommitRequest.builder()
                .ingestJob(job)
                .taskId("test-task")
                .jobRunId("test-job-run")
                .fileReferences(List.of(file1, file2))
                .writtenTime(Instant.parse("2024-06-20T15:57:01Z"))
                .build();
        String jsonString = new IngestAddFilesCommitRequestSerDe().toJson(ingestJobCommitRequest);

        // When / Then
        assertThat(commitRequestSerDe.fromJson(jsonString))
                .isEqualTo(StateStoreCommitRequest.forIngestAddFiles(ingestJobCommitRequest));
    }

    @Test
    void shouldDeserialiseIngestCommitRequestWithNoJob() {
        // Given
        FileReference file1 = FileReference.builder()
                .filename("file1.parquet")
                .partitionId("root")
                .numberOfRecords(100L)
                .onlyContainsDataForThisPartition(true)
                .build();
        FileReference file2 = FileReference.builder()
                .filename("file2.parquet")
                .partitionId("root")
                .numberOfRecords(200L)
                .onlyContainsDataForThisPartition(true)
                .build();
        IngestAddFilesCommitRequest ingestJobCommitRequest = IngestAddFilesCommitRequest.builder()
                .tableId("test-table")
                .fileReferences(List.of(file1, file2))
                .build();
        String jsonString = new IngestAddFilesCommitRequestSerDe().toJson(ingestJobCommitRequest);

        // When / Then
        assertThat(commitRequestSerDe.fromJson(jsonString))
                .isEqualTo(StateStoreCommitRequest.forIngestAddFiles(ingestJobCommitRequest));
    }

    @Test
    void shouldThrowExceptionIfCommitRequestTypeInvalid() {
        // Given
        String jsonString = "{\"type\":\"invalid-type\", \"request\":{}}";

        // When / Then
        assertThatThrownBy(() -> commitRequestSerDe.fromJson(jsonString))
                .isInstanceOf(CommitRequestValidationException.class);
    }
}
