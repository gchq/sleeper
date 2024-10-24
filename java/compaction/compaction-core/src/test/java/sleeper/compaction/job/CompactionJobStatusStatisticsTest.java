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
package sleeper.compaction.job;

import org.junit.jupiter.api.Test;

import sleeper.compaction.job.status.CompactionJobStatus;

import java.time.Instant;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.compaction.job.CompactionJobStatusTestData.finishedCompactionRun;
import static sleeper.compaction.job.CompactionJobStatusTestData.jobCreated;
import static sleeper.compaction.job.CompactionJobStatusTestData.jobFilesAssigned;
import static sleeper.compaction.job.CompactionJobStatusTestData.uncommittedCompactionRun;
import static sleeper.core.record.process.RecordsProcessedSummaryTestHelper.summary;

public class CompactionJobStatusStatisticsTest {

    private final CompactionJobTestDataHelper dataHelper = new CompactionJobTestDataHelper();

    @Test
    void shouldComputeStatisticsForDelayBetweenFinishAndCommitWhenCommitted() {
        // Given
        CompactionJobStatus status1 = jobFilesAssigned(dataHelper.singleFileCompaction(),
                Instant.parse("2024-09-09T10:29:50Z"),
                Instant.parse("2024-09-09T10:29:51Z"),
                finishedCompactionRun("test-task",
                        summary(Instant.parse("2024-09-09T10:30:00Z"), Instant.parse("2024-09-09T10:31:00Z"), 100, 100),
                        Instant.parse("2024-09-09T10:31:02Z")));
        CompactionJobStatus status2 = jobFilesAssigned(dataHelper.singleFileCompaction(),
                Instant.parse("2024-09-09T10:31:50Z"),
                Instant.parse("2024-09-09T10:31:51Z"),
                finishedCompactionRun("test-task",
                        summary(Instant.parse("2024-09-09T10:32:00Z"), Instant.parse("2024-09-09T10:33:00Z"), 100, 100),
                        Instant.parse("2024-09-09T10:33:01Z")));

        // When / Then
        assertThat(CompactionJobStatus.computeStatisticsOfDelayBetweenFinishAndCommit(List.of(status1, status2)))
                .get().hasToString("avg: 1.5s, min: 1s, 99%: 2s, 99.9%: 2s, max: 2s, std dev: 0.5s");
    }

    @Test
    void shouldComputeStatisticsForDelayBetweenFinishAndCommitWhenNoneCommitted() {
        // Given
        CompactionJobStatus status = jobFilesAssigned(dataHelper.singleFileCompaction(),
                Instant.parse("2024-09-09T10:29:50Z"),
                Instant.parse("2024-09-09T10:29:51Z"),
                uncommittedCompactionRun("test-task",
                        summary(Instant.parse("2024-09-09T10:30:00Z"), Instant.parse("2024-09-09T10:31:00Z"), 100, 100)));

        // When / Then
        assertThat(CompactionJobStatus.computeStatisticsOfDelayBetweenFinishAndCommit(List.of(status)))
                .isEmpty();
    }

    @Test
    void shouldComputeStatisticsForDelayBetweenCreationAndAssignmentWhenAssigned() {
        // Given
        CompactionJobStatus status1 = jobFilesAssigned(dataHelper.singleFileCompaction(),
                Instant.parse("2024-09-09T10:29:50Z"),
                Instant.parse("2024-09-09T10:29:52Z"));
        CompactionJobStatus status2 = jobFilesAssigned(dataHelper.singleFileCompaction(),
                Instant.parse("2024-09-09T10:31:50Z"),
                Instant.parse("2024-09-09T10:31:53Z"));

        // When / Then
        assertThat(CompactionJobStatus.computeStatisticsOfDelayBetweenCreationAndFilesAssignment(List.of(status1, status2)))
                .get().hasToString("avg: 2.5s, min: 2s, 99%: 3s, 99.9%: 3s, max: 3s, std dev: 0.5s");
    }

    @Test
    void shouldComputeStatisticsForDelayBetweenCreationAndAssignmentWhenNotAssigned() {
        // Given
        CompactionJobStatus status = jobCreated(dataHelper.singleFileCompaction(),
                Instant.parse("2024-09-09T10:29:50Z"));

        // When / Then
        assertThat(CompactionJobStatus.computeStatisticsOfDelayBetweenCreationAndFilesAssignment(List.of(status)))
                .isEmpty();
    }
}
