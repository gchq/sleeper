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

import java.time.Instant;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.groups.Tuple.tuple;
import static sleeper.compaction.job.CompactionJobTestDataHelper.DEFAULT_TASK_ID;

public class CompactionJobRunTest {
    private static final String DEFAULT_TASK_ID_1 = "task-id-1";
    private static final String DEFAULT_TASK_ID_2 = "task-id-2";

    @Test
    public void shouldReportNoJobRunsIfJobNotStarted() {
        // Given
        CompactionJobCreatedStatus created = CompactionJobCreatedStatus.builder()
                .updateTime(Instant.parse("2022-09-23T09:23:00.012Z"))
                .partitionId("partition1").childPartitionIds(null)
                .inputFilesCount(11)
                .build();

        // When
        CompactionJobStatus status = new TestCompactionJobStatusUpdateRecords()
                .jobCreated("job1", created)
                .buildSingleStatus();

        // Then
        assertThat(status.getJobRuns())
                .isEmpty();
    }

    @Test
    public void shouldRetrieveNoFinishedStatusIfNotFinished() {
        // Given
        CompactionJobCreatedStatus created = CompactionJobCreatedStatus.builder()
                .updateTime(Instant.parse("2022-09-23T09:23:00.012Z"))
                .partitionId("partition1").childPartitionIds(null)
                .inputFilesCount(11)
                .build();
        CompactionJobStartedStatus started = CompactionJobStartedStatus.updateAndStartTime(
                Instant.parse("2022-09-23T09:23:30.012Z"),
                Instant.parse("2022-09-23T09:23:30.001Z"));

        // When
        CompactionJobStatus status = new TestCompactionJobStatusUpdateRecords()
                .updatesForJobWithTask("job1", DEFAULT_TASK_ID_1, created, started)
                .buildSingleStatus();

        // Then
        CompactionJobRun jobRun = status.getLatestJobRun();
        assertThat(jobRun.getStartedStatus())
                .isEqualTo(started);
        assertThat(jobRun.getFinishedStatus())
                .isNull();
    }

    @Test
    public void shouldRetrieveFinishedStatusIfJobFinished() {
        // Given
        CompactionJobCreatedStatus created = CompactionJobCreatedStatus.builder()
                .updateTime(Instant.parse("2022-09-23T09:23:00.012Z"))
                .partitionId("partition1").childPartitionIds(null)
                .inputFilesCount(11)
                .build();
        CompactionJobStartedStatus started = CompactionJobStartedStatus.updateAndStartTime(
                Instant.parse("2022-09-24T09:23:30.012Z"),
                Instant.parse("2022-09-24T09:23:30.001Z"));
        CompactionJobFinishedStatus finished = CompactionJobFinishedStatus.updateTimeAndSummary(
                Instant.parse("2022-09-24T09:24:00.012Z"),
                new CompactionJobSummary(new CompactionJobRecordsProcessed(450L, 300L),
                        Instant.parse("2022-09-24T09:23:30.001Z"),
                        Instant.parse("2022-09-24T09:24:00.001Z")));

        // When
        CompactionJobStatus status = new TestCompactionJobStatusUpdateRecords()
                .updatesForJobWithTask("job1", DEFAULT_TASK_ID_1, created, started, finished)
                .buildSingleStatus();

        // Then
        CompactionJobRun jobRun1 = status.getLatestJobRun();

        assertThat(jobRun1.getStartedStatus())
                .isEqualTo(started);
        assertThat(jobRun1.getFinishedStatus())
                .isEqualTo(finished);
    }

    @Test
    public void shouldReportMultipleJobRunsWhenJobStartedMultipleTimesSameTask() {
        // Given
        CompactionJobCreatedStatus created = CompactionJobCreatedStatus.builder()
                .updateTime(Instant.parse("2022-09-23T09:23:00.012Z"))
                .partitionId("partition1").childPartitionIds(null)
                .inputFilesCount(11)
                .build();
        CompactionJobStartedStatus started1 = CompactionJobStartedStatus.updateAndStartTime(
                Instant.parse("2022-09-24T09:23:30.012Z"),
                Instant.parse("2022-09-24T09:23:30.001Z"));
        CompactionJobStartedStatus started2 = CompactionJobStartedStatus.updateAndStartTime(
                Instant.parse("2022-09-24T09:24:30.012Z"),
                Instant.parse("2022-09-24T09:24:30.001Z"));

        // When
        CompactionJobStatus status = new TestCompactionJobStatusUpdateRecords()
                .updatesForJobWithTask("job1", DEFAULT_TASK_ID, created, started1, started2)
                .buildSingleStatus();

        // Then
        assertThat(status.getJobRuns())
                .containsExactly(
                        CompactionJobRun.started(DEFAULT_TASK_ID, started2),
                        CompactionJobRun.started(DEFAULT_TASK_ID, started1)
                );
    }

    @Test
    public void shouldBuildCompactionJobStatusWithMultipleJobRunsSameTask() {
        // Given
        CompactionJobCreatedStatus created = CompactionJobCreatedStatus.builder()
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
        CompactionJobStatus status = new TestCompactionJobStatusUpdateRecords()
                .updatesForJobWithTask("job1", DEFAULT_TASK_ID_1, created,
                        started1, finished1, started2, finished2)
                .buildSingleStatus();

        // Then
        assertThat(status.getJobRuns())
                .containsExactly(
                        CompactionJobRun.finished(DEFAULT_TASK_ID_1, started2, finished2),
                        CompactionJobRun.finished(DEFAULT_TASK_ID_1, started1, finished1));
    }

    @Test
    public void shouldRetrieveFinishedStatusIfReturnedFromDatabaseOutOfOrder() {
        // Given
        CompactionJobCreatedStatus created = CompactionJobCreatedStatus.builder()
                .updateTime(Instant.parse("2022-09-23T09:23:00.012Z"))
                .partitionId("partition1").childPartitionIds(null)
                .inputFilesCount(11)
                .build();
        CompactionJobStartedStatus started = CompactionJobStartedStatus.updateAndStartTime(
                Instant.parse("2022-09-24T09:23:30.012Z"),
                Instant.parse("2022-09-24T09:23:30.001Z"));
        CompactionJobFinishedStatus finished = CompactionJobFinishedStatus.updateTimeAndSummary(
                Instant.parse("2022-09-24T09:24:00.012Z"),
                new CompactionJobSummary(new CompactionJobRecordsProcessed(450L, 300L),
                        Instant.parse("2022-09-24T09:23:30.001Z"),
                        Instant.parse("2022-09-24T09:24:00.001Z")));

        // When
        CompactionJobStatus status = new TestCompactionJobStatusUpdateRecords()
                .updatesForJobWithTask("job1", DEFAULT_TASK_ID, created, finished, started)
                .buildSingleStatus();

        // Then
        CompactionJobRun jobRun1 = status.getLatestJobRun();

        assertThat(jobRun1.getStartedStatus())
                .isEqualTo(started);
        assertThat(jobRun1.getFinishedStatus())
                .isEqualTo(finished);
    }

    @Test
    public void shouldRetrieveMultipleStartedStatusIfReturnedFromDatabaseOutOfOrder() {
        // Given
        CompactionJobCreatedStatus created = CompactionJobCreatedStatus.builder()
                .updateTime(Instant.parse("2022-09-23T09:23:00.012Z"))
                .partitionId("partition1").childPartitionIds(null)
                .inputFilesCount(11)
                .build();
        CompactionJobStartedStatus started1 = CompactionJobStartedStatus.updateAndStartTime(
                Instant.parse("2022-09-24T09:23:30.012Z"),
                Instant.parse("2022-09-24T09:23:30.001Z"));
        CompactionJobStartedStatus started2 = CompactionJobStartedStatus.updateAndStartTime(
                Instant.parse("2022-09-25T09:23:30.012Z"),
                Instant.parse("2022-09-25T09:23:30.001Z"));
        CompactionJobStartedStatus started3 = CompactionJobStartedStatus.updateAndStartTime(
                Instant.parse("2022-09-26T09:23:30.012Z"),
                Instant.parse("2022-09-26T09:23:30.001Z"));
        CompactionJobFinishedStatus finished = CompactionJobFinishedStatus.updateTimeAndSummary(
                Instant.parse("2022-09-27T09:24:00.012Z"),
                new CompactionJobSummary(new CompactionJobRecordsProcessed(450L, 300L),
                        Instant.parse("2022-09-26T09:23:30.001Z"),
                        Instant.parse("2022-09-27T09:24:00.001Z")));

        // When
        CompactionJobStatus status = new TestCompactionJobStatusUpdateRecords()
                .updatesForJobWithTask("job1", DEFAULT_TASK_ID, created, started3, finished, started1, started2)
                .buildSingleStatus();

        // Then
        assertThat(status.getJobRuns())
                .extracting(CompactionJobRun::getStartedStatus, CompactionJobRun::getFinishedStatus)
                .containsExactly(
                        tuple(started3, finished),
                        tuple(started2, null),
                        tuple(started1, null));
    }

    @Test
    public void shouldBuildCompactionJobStatusWithMultipleJobRunsDifferentTasks() {
        // Given
        CompactionJobCreatedStatus created = CompactionJobCreatedStatus.builder()
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
        CompactionJobStatus status = new TestCompactionJobStatusUpdateRecords()
                .jobCreated("job1", created)
                .updatesForJobWithTask("job1", DEFAULT_TASK_ID_1, started1, finished1)
                .updatesForJobWithTask("job1", DEFAULT_TASK_ID_2, started2, finished2)
                .buildSingleStatus();

        // Then
        assertThat(status.getJobRuns())
                .contains(
                        CompactionJobRun.finished(DEFAULT_TASK_ID_2, started2, finished2),
                        CompactionJobRun.finished(DEFAULT_TASK_ID_1, started1, finished1));
    }
}
