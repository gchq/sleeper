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
package sleeper.compaction.job.commit;

import org.approvaltests.Approvals;
import org.junit.jupiter.api.Test;

import sleeper.compaction.job.CompactionJob;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class CompactionJobIdAssignmentCommitRequestSerDeTest {

    private final CompactionJobIdAssignmentCommitRequestSerDe serDe = new CompactionJobIdAssignmentCommitRequestSerDe();

    @Test
    void shouldSerialiseCompactionJobIdAssignmentCommitRequest() throws Exception {
        // Given
        CompactionJob job1 = CompactionJob.builder()
                .tableId("test-table")
                .jobId("test-job-1")
                .inputFiles(List.of("file1.parquet", "file2.parquet"))
                .outputFile("test-output-1.parquet")
                .partitionId("test-partition-id")
                .build();
        CompactionJob job2 = CompactionJob.builder()
                .tableId("test-table")
                .jobId("test-job-2")
                .inputFiles(List.of("file3.parquet", "file4.parquet"))
                .outputFile("test-output-2.parquet")
                .partitionId("test-partition-id")
                .build();
        CompactionJobIdAssignmentCommitRequest commit = CompactionJobIdAssignmentCommitRequest.forJobsOnTable(List.of(job1, job2), "test-table");

        // When
        String json = serDe.toJsonPrettyPrint(commit);

        // Then
        assertThat(serDe.fromJson(json)).isEqualTo(commit);
        Approvals.verify(json);
    }

    @Test
    void shouldFailToDeserialiseNonCompactionJobIdAssignmentCommitRequest() {
        assertThatThrownBy(() -> serDe.fromJson("{\"type\": \"OTHER\", \"request\":{}}"))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
