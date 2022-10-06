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
import sleeper.compaction.job.status.CompactionJobRun;
import sleeper.compaction.job.status.CompactionJobStartedStatus;
import sleeper.compaction.job.status.CompactionJobStatus;
import sleeper.compaction.job.status.CompactionJobStatusesBuilder;

import java.time.Instant;

import static org.assertj.core.api.Assertions.assertThat;

public class CompactionJobRunTest {
    private static final String DEFAULT_TASK_ID_1 = "task-id-1";
    private static final String DEFAULT_TASK_ID_2 = "task-id-2";

    @Test
    public void shouldReportNoJobRunsIfJobNotStarted() {
        // Given
        CompactionJobCreatedStatus created1 = CompactionJobCreatedStatus.builder()
                .updateTime(Instant.parse("2022-09-23T09:23:00.012Z"))
                .partitionId("partition1").childPartitionIds(null)
                .inputFilesCount(11)
                .build();
        // When
        CompactionJobStatus status = new CompactionJobStatusesBuilder()
                .jobCreated("job1", created1)
                .build().get(0);

        // Then
        assertThat(status.getJobRuns())
                .isEmpty();
    }

    @Test
    public void shouldRetrieveNoFinishedStatusIfNoMatchingStartedStatus() {
        // Given
        CompactionJobCreatedStatus created1 = CompactionJobCreatedStatus.builder()
                .updateTime(Instant.parse("2022-09-23T09:23:00.012Z"))
                .partitionId("partition1").childPartitionIds(null)
                .inputFilesCount(11)
                .build();
        CompactionJobStartedStatus started1 = CompactionJobStartedStatus.updateAndStartTime(
                Instant.parse("2022-09-23T09:23:30.012Z"),
                Instant.parse("2022-09-23T09:23:30.001Z"));

        // When
        CompactionJobStatus status = new CompactionJobStatusesBuilder()
                .jobCreated("job1", created1)
                .jobStarted("job1", started1, DEFAULT_TASK_ID_1)
                .build().get(0);

        // Then
        CompactionJobRun jobRun1 = status.getLatestJobRun();
        assertThat(jobRun1.getStartedStatus())
                .isEqualTo(started1);
        assertThat(jobRun1.getFinishedStatus())
                .isNull();
    }

    @Test
    public void shouldRetrieveCorrectFinishedStatusMatchingStartedStatus() {
        // Given
        CompactionJobCreatedStatus created1 = CompactionJobCreatedStatus.builder()
                .updateTime(Instant.parse("2022-09-23T09:23:00.012Z"))
                .partitionId("partition1").childPartitionIds(null)
                .inputFilesCount(11)
                .build();
        CompactionJobStartedStatus started2 = CompactionJobStartedStatus.updateAndStartTime(
                Instant.parse("2022-09-24T09:23:30.012Z"),
                Instant.parse("2022-09-24T09:23:30.001Z"));
        CompactionJobFinishedStatus finished2 = CompactionJobFinishedStatus.updateTimeAndSummary(
                Instant.parse("2022-09-24T09:24:00.012Z"),
                new CompactionJobSummary(new CompactionJobRecordsProcessed(450L, 300L),
                        Instant.parse("2022-09-24T09:23:30.001Z"),
                        Instant.parse("2022-09-24T09:24:00.001Z")));

        // When
        CompactionJobStatus status = new CompactionJobStatusesBuilder()
                .jobCreated("job1", created1)
                .jobStarted("job1", started2, DEFAULT_TASK_ID_1)
                .jobFinished("job1", finished2, DEFAULT_TASK_ID_1)
                .build().get(0);

        // Then
        CompactionJobRun jobRun1 = status.getLatestJobRun();

        assertThat(jobRun1.getStartedStatus())
                .isEqualTo(started2);
        assertThat(jobRun1.getFinishedStatus())
                .isEqualTo(finished2);
    }

    @Test
    public void shouldBuildCompactionJobStatusWithMultipleJobRunsSameTask() {
        // Given
        CompactionJobCreatedStatus created1 = CompactionJobCreatedStatus.builder()
                .updateTime(Instant.parse("2022-09-23T09:23:00.012Z"))
                .partitionId("partition1").childPartitionIds(null)
                .inputFilesCount(11)
                .build();
        CompactionJobStartedStatus started1 = CompactionJobStartedStatus.updateAndStartTime(
                Instant.parse("2022-09-23T09:23:30.012Z"),
                Instant.parse("2022-09-23T09:23:30.001Z"));
        CompactionJobFinishedStatus finished1 = CompactionJobFinishedStatus.updateTimeAndSummary(
                Instant.parse("2022-09-23T09:24:00.012Z"),
                new CompactionJobSummary(new CompactionJobRecordsProcessed(450L, 300L),
                        Instant.parse("2022-09-23T09:23:30.001Z"),
                        Instant.parse("2022-09-23T09:24:00.001Z")));
        CompactionJobStartedStatus started2 = CompactionJobStartedStatus.updateAndStartTime(
                Instant.parse("2022-09-24T09:23:30.012Z"),
                Instant.parse("2022-09-24T09:23:30.001Z"));
        CompactionJobFinishedStatus finished2 = CompactionJobFinishedStatus.updateTimeAndSummary(
                Instant.parse("2022-09-24T09:24:00.012Z"),
                new CompactionJobSummary(new CompactionJobRecordsProcessed(450L, 300L),
                        Instant.parse("2022-09-24T09:23:30.001Z"),
                        Instant.parse("2022-09-24T09:24:00.001Z")));

        // When
        CompactionJobStatus status = new CompactionJobStatusesBuilder()
                .jobCreated("job1", created1)
                .jobStarted("job1", started1, DEFAULT_TASK_ID_1)
                .jobFinished("job1", finished1, DEFAULT_TASK_ID_1)
                .jobStarted("job1", started2, DEFAULT_TASK_ID_1)
                .jobFinished("job1", finished2, DEFAULT_TASK_ID_1)
                .build().get(0);

        // Then
        assertThat(status.getJobRuns())
                .contains(
                        CompactionJobRun.finished(DEFAULT_TASK_ID_1, started1, finished1),
                        CompactionJobRun.finished(DEFAULT_TASK_ID_1, started2, finished2));
    }

    @Test
    public void shouldBuildCompactionJobStatusWithMultipleJobRunsDifferentTasks() {
        // Given
        CompactionJobCreatedStatus created1 = CompactionJobCreatedStatus.builder()
                .updateTime(Instant.parse("2022-09-23T09:23:00.012Z"))
                .partitionId("partition1").childPartitionIds(null)
                .inputFilesCount(11)
                .build();
        CompactionJobStartedStatus started1 = CompactionJobStartedStatus.updateAndStartTime(
                Instant.parse("2022-09-23T09:23:30.012Z"),
                Instant.parse("2022-09-23T09:23:30.001Z"));
        CompactionJobFinishedStatus finished1 = CompactionJobFinishedStatus.updateTimeAndSummary(
                Instant.parse("2022-09-23T09:24:00.012Z"),
                new CompactionJobSummary(new CompactionJobRecordsProcessed(450L, 300L),
                        Instant.parse("2022-09-23T09:23:30.001Z"),
                        Instant.parse("2022-09-23T09:24:00.001Z")));
        CompactionJobStartedStatus started2 = CompactionJobStartedStatus.updateAndStartTime(
                Instant.parse("2022-09-24T09:23:30.012Z"),
                Instant.parse("2022-09-24T09:23:30.001Z"));
        CompactionJobFinishedStatus finished2 = CompactionJobFinishedStatus.updateTimeAndSummary(
                Instant.parse("2022-09-24T09:24:00.012Z"),
                new CompactionJobSummary(new CompactionJobRecordsProcessed(450L, 300L),
                        Instant.parse("2022-09-24T09:23:30.001Z"),
                        Instant.parse("2022-09-24T09:24:00.001Z")));

        // When
        CompactionJobStatus status = new CompactionJobStatusesBuilder()
                .jobCreated("job1", created1)
                .jobStarted("job1", started1, DEFAULT_TASK_ID_1)
                .jobFinished("job1", finished1, DEFAULT_TASK_ID_1)
                .jobStarted("job1", started2, DEFAULT_TASK_ID_2)
                .jobFinished("job1", finished2, DEFAULT_TASK_ID_2)
                .build().get(0);

        // Then
        assertThat(status.getJobRuns())
                .contains(
                        CompactionJobRun.finished(DEFAULT_TASK_ID_1, started1, finished1),
                        CompactionJobRun.finished(DEFAULT_TASK_ID_2, started2, finished2));
    }
}
