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
package sleeper.compaction.core.job;

import org.junit.jupiter.api.Test;

import sleeper.compaction.core.job.status.CompactionJobCreatedStatus;
import sleeper.compaction.core.job.status.CompactionJobStatus;
import sleeper.core.record.process.status.ProcessRun;

import java.time.Instant;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.compaction.core.job.CompactionJobStatusTestData.startedCompactionRun;

public class CompactionJobStatusTaskIdAssignedTest {

    private final CompactionJob job = new CompactionJobTestDataHelper().singleFileCompaction();

    @Test
    public void shouldHaveTaskWhenOneRunHasTask() {
        // Given
        CompactionJobStatus status = statusBuilder()
                .jobRunsLatestFirst(runsWithTaskIds(
                        "task-id-1", "test-task-id", "task-id-3"))
                .build();

        // When / Then
        assertThat(status.isTaskIdAssigned("test-task-id")).isTrue();
    }

    @Test
    public void shouldNotHaveTaskWhenNoRunHasTask() {
        // Given
        CompactionJobStatus status = statusBuilder()
                .jobRunsLatestFirst(runsWithTaskIds(
                        "task-id-1", "task-id-1", "task-id-3"))
                .build();

        // When / Then
        assertThat(status.isTaskIdAssigned("test-task-id")).isFalse();
    }

    @Test
    public void shouldNotHaveTaskWhenNoRunsSet() {
        // Given
        CompactionJobStatus status = statusBuilder()
                .jobRunsLatestFirst(Collections.emptyList())
                .build();

        // When / Then
        assertThat(status.isTaskIdAssigned("test-task-id")).isFalse();
    }

    private CompactionJobStatus.Builder statusBuilder() {
        return CompactionJobStatus.builder().jobId(job.getId())
                .filesAssignedStatus(CompactionJobCreatedStatus.from(job,
                        Instant.parse("2022-10-12T11:29:00.000Z")));
    }

    private static List<ProcessRun> runsWithTaskIds(String... taskIds) {
        return Stream.of(taskIds)
                .map(CompactionJobStatusTaskIdAssignedTest::runWithTaskId)
                .collect(Collectors.toList());
    }

    private static ProcessRun runWithTaskId(String taskId) {
        return startedCompactionRun(taskId, Instant.now());
    }
}
