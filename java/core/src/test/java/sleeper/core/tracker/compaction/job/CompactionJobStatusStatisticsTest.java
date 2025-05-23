/*
 * Copyright 2022-2025 Crown Copyright
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
package sleeper.core.tracker.compaction.job;

import org.junit.jupiter.api.Test;

import sleeper.core.tracker.compaction.job.query.CompactionJobStatus;

import java.time.Instant;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.core.tracker.compaction.job.CompactionJobStatusTestData.compactionJobCreated;
import static sleeper.core.tracker.compaction.job.CompactionJobStatusTestData.finishedCompactionRun;
import static sleeper.core.tracker.compaction.job.CompactionJobStatusTestData.uncommittedCompactionRun;
import static sleeper.core.tracker.job.run.JobRunSummaryTestHelper.summary;

public class CompactionJobStatusStatisticsTest {

    @Test
    void shouldComputeStatisticsForDelayBetweenFinishAndCommitWhenCommitted() {
        // Given
        CompactionJobStatus status1 = compactionJobCreated(
                Instant.parse("2024-09-09T10:29:50Z"),
                finishedCompactionRun("test-task",
                        summary(Instant.parse("2024-09-09T10:30:00Z"), Instant.parse("2024-09-09T10:31:00Z"), 100, 100),
                        Instant.parse("2024-09-09T10:31:02Z")));
        CompactionJobStatus status2 = compactionJobCreated(
                Instant.parse("2024-09-09T10:31:50Z"),
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
        CompactionJobStatus status = compactionJobCreated(
                Instant.parse("2024-09-09T10:29:50Z"),
                uncommittedCompactionRun("test-task",
                        summary(Instant.parse("2024-09-09T10:30:00Z"), Instant.parse("2024-09-09T10:31:00Z"), 100, 100)));

        // When / Then
        assertThat(CompactionJobStatus.computeStatisticsOfDelayBetweenFinishAndCommit(List.of(status)))
                .isEmpty();
    }
}
