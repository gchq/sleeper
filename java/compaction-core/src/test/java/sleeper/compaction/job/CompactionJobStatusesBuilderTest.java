/*
 * Copyright 2022 Crown Copyright
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

import org.junit.Test;
import sleeper.compaction.job.status.CompactionJobCreatedStatus;
import sleeper.compaction.job.status.CompactionJobFinishedStatus;
import sleeper.compaction.job.status.CompactionJobStartedStatus;
import sleeper.compaction.job.status.CompactionJobStatus;
import sleeper.compaction.job.status.CompactionJobStatusesBuilder;

import java.time.Instant;
import java.util.Arrays;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.compaction.job.CompactionJobTestDataHelper.DEFAULT_TASK_ID;

public class CompactionJobStatusesBuilderTest {

    @Test
    public void shouldBuildCompactionJobStatusFromIndividualUpdates() {
        // Given
        CompactionJobCreatedStatus created1 = CompactionJobCreatedStatus.builder()
                .updateTime(Instant.parse("2022-09-23T09:23:00.012Z"))
                .partitionId("partition1").childPartitionIds(null)
                .inputFilesCount(11)
                .build();
        CompactionJobStartedStatus started1 = CompactionJobStartedStatus.updateAndStartTimeWithTaskId(
                Instant.parse("2022-09-23T09:23:30.012Z"),
                Instant.parse("2022-09-23T09:23:30.001Z"),
                DEFAULT_TASK_ID);
        CompactionJobFinishedStatus finished1 = CompactionJobFinishedStatus.updateTimeAndSummaryWithTaskId(
                Instant.parse("2022-09-23T09:24:00.012Z"),
                new CompactionJobSummary(new CompactionJobRecordsProcessed(200L, 100L),
                        Instant.parse("2022-09-23T09:23:30.001Z"),
                        Instant.parse("2022-09-23T09:24:00.001Z")),
                DEFAULT_TASK_ID);
        CompactionJobCreatedStatus created2 = CompactionJobCreatedStatus.builder()
                .updateTime(Instant.parse("2022-09-24T09:23:00.012Z"))
                .partitionId("partition2").childPartitionIds(Arrays.asList("A", "B"))
                .inputFilesCount(12)
                .build();
        CompactionJobStartedStatus started2 = CompactionJobStartedStatus.updateAndStartTimeWithTaskId(
                Instant.parse("2022-09-24T09:23:30.012Z"),
                Instant.parse("2022-09-24T09:23:30.001Z"),
                DEFAULT_TASK_ID);
        CompactionJobFinishedStatus finished2 = CompactionJobFinishedStatus.updateTimeAndSummaryWithTaskId(
                Instant.parse("2022-09-24T09:24:00.012Z"),
                new CompactionJobSummary(new CompactionJobRecordsProcessed(450L, 300L),
                        Instant.parse("2022-09-24T09:23:30.001Z"),
                        Instant.parse("2022-09-24T09:24:00.001Z")),
                DEFAULT_TASK_ID);

        // When
        List<CompactionJobStatus> statuses = new CompactionJobStatusesBuilder()
                .jobCreated("job1", created1)
                .jobStarted("job1", started1)
                .jobFinished("job1", finished1)
                .jobCreated("job2", created2)
                .jobStarted("job2", started2)
                .jobFinished("job2", finished2)
                .build();

        // Then
        assertThat(statuses).containsExactly(
                CompactionJobStatus.builder().jobId("job1").createdStatus(created1)
                        .startedStatus(started1).finishedStatus(finished1).build(),
                CompactionJobStatus.builder().jobId("job2").createdStatus(created2)
                        .startedStatus(started2).finishedStatus(finished2).build());
    }
}
