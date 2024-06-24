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
package sleeper.ingest.job.commit;

import org.approvaltests.Approvals;
import org.junit.jupiter.api.Test;

import sleeper.core.statestore.FileReference;
import sleeper.ingest.job.IngestJob;

import java.time.Instant;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class IngestAddFilesCommitRequestSerDeTest {

    private final IngestAddFilesCommitRequestSerDe serDe = new IngestAddFilesCommitRequestSerDe();

    @Test
    void shouldSerialiseIngestJobCommitRequest() throws Exception {
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
        IngestAddFilesCommitRequest commit = IngestAddFilesCommitRequest.builder()
                .ingestJob(job)
                .taskId("test-task")
                .jobRunId("test-job-run")
                .fileReferences(List.of(file1, file2))
                .writtenTime(Instant.parse("2024-06-20T14:55:01Z"))
                .build();

        // When
        String json = serDe.toJsonPrettyPrint(commit);

        // Then
        assertThat(serDe.fromJson(json)).isEqualTo(commit);
        Approvals.verify(json);
    }

    @Test
    void shouldSerialiseIngestJobCommitRequestWithNoJob() throws Exception {
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
        IngestAddFilesCommitRequest commit = IngestAddFilesCommitRequest.builder()
                .tableId("test-table")
                .fileReferences(List.of(file1, file2))
                .build();

        // When
        String json = serDe.toJsonPrettyPrint(commit);

        // Then
        assertThat(serDe.fromJson(json)).isEqualTo(commit);
        Approvals.verify(json);
    }

    @Test
    void shouldFailToDeserialiseNonIngestCommitRequest() {
        assertThatThrownBy(() -> serDe.fromJson("{\"type\": \"OTHER\", \"request\":{}}"))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
