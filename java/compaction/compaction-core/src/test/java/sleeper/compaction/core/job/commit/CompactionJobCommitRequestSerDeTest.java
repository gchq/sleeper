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
package sleeper.compaction.core.job.commit;

import org.approvaltests.Approvals;
import org.junit.jupiter.api.Test;

import sleeper.compaction.core.job.CompactionJob;
import sleeper.core.record.process.RecordsProcessed;
import sleeper.core.record.process.RecordsProcessedSummary;

import java.time.Duration;
import java.time.Instant;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class CompactionJobCommitRequestSerDeTest {

    private final CompactionJobCommitRequestSerDe serDe = new CompactionJobCommitRequestSerDe();

    @Test
    void shouldSerialiseCompactionJobCommitRequest() throws Exception {
        // Given
        CompactionJob job = CompactionJob.builder()
                .tableId("test-table")
                .jobId("test-job")
                .inputFiles(List.of("file1.parquet", "file2.parquet"))
                .outputFile("test-output.parquet")
                .partitionId("test-partition-id")
                .build();
        CompactionJobCommitRequest commit = new CompactionJobCommitRequest(job, "test-task", "test-job-run",
                new RecordsProcessedSummary(
                        new RecordsProcessed(120, 100),
                        Instant.parse("2024-05-01T10:58:00Z"), Duration.ofMinutes(1)));

        // When
        String json = serDe.toJsonPrettyPrint(commit);

        // Then
        assertThat(serDe.fromJson(json)).isEqualTo(commit);
        Approvals.verify(json);
    }

    @Test
    void shouldFailToDeserialiseNonCompactionCommitRequest() {
        assertThatThrownBy(() -> serDe.fromJson("{\"type\": \"OTHER\", \"request\":{}}"))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
