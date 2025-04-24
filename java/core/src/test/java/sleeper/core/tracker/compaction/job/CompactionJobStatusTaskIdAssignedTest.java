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

import sleeper.core.tracker.compaction.job.query.CompactionJobCreatedStatus;
import sleeper.core.tracker.compaction.job.query.CompactionJobStatus;
import sleeper.core.tracker.compaction.job.update.CompactionJobCreatedEvent;
import sleeper.core.tracker.job.run.JobRun;
import sleeper.core.tracker.job.run.JobRuns;

import java.time.Instant;
import java.util.List;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.core.tracker.compaction.job.CompactionJobEventTestData.defaultCompactionJobCreatedEvent;
import static sleeper.core.tracker.compaction.job.CompactionJobStatusTestData.startedCompactionRun;

public class CompactionJobStatusTaskIdAssignedTest {

    private final CompactionJobCreatedEvent job = defaultCompactionJobCreatedEvent();

    @Test
    public void shouldHaveTaskWhenOneRunHasTask() {
        // Given
        CompactionJobStatus status = statusBuilder()
                .jobRuns(runsWithTaskIds(
                        "task-id-1", "test-task-id", "task-id-3"))
                .build();

        // When / Then
        assertThat(status.isTaskIdAssigned("test-task-id")).isTrue();
    }

    @Test
    public void shouldNotHaveTaskWhenNoRunHasTask() {
        // Given
        CompactionJobStatus status = statusBuilder()
                .jobRuns(runsWithTaskIds(
                        "task-id-1", "task-id-1", "task-id-3"))
                .build();

        // When / Then
        assertThat(status.isTaskIdAssigned("test-task-id")).isFalse();
    }

    @Test
    public void shouldNotHaveTaskWhenNoRunsSet() {
        // Given
        CompactionJobStatus status = statusBuilder()
                .jobRuns(JobRuns.latestFirst(List.of()))
                .build();

        // When / Then
        assertThat(status.isTaskIdAssigned("test-task-id")).isFalse();
    }

    private CompactionJobStatus.Builder statusBuilder() {
        return CompactionJobStatus.builder().jobId(job.getJobId())
                .createdStatus(CompactionJobCreatedStatus.from(job,
                        Instant.parse("2022-10-12T11:29:00.000Z")));
    }

    private static JobRuns runsWithTaskIds(String... taskIds) {
        return JobRuns.latestFirst(Stream.of(taskIds)
                .map(CompactionJobStatusTaskIdAssignedTest::runWithTaskId)
                .toList());
    }

    private static JobRun runWithTaskId(String taskId) {
        return startedCompactionRun(taskId, Instant.now());
    }
}
