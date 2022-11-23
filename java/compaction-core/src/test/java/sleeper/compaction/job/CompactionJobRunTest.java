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
import sleeper.compaction.job.status.CompactionJobStatus;
import sleeper.core.record.process.RecordsProcessed;
import sleeper.core.record.process.RecordsProcessedSummary;
import sleeper.core.status.ProcessFinishedStatus;
import sleeper.core.status.ProcessRun;
import sleeper.core.status.ProcessStartedStatus;

import java.time.Instant;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.groups.Tuple.tuple;

public class CompactionJobRunTest {
    private static final String DEFAULT_TASK_ID_1 = "task-id-1";
    private static final String DEFAULT_TASK_ID_2 = "task-id-2";

    @Test
    public void shouldReportNoJobRunsWhenJobNotStarted() {
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
        assertThat(status.getJobRunList())
                .isEmpty();
        assertThat(status.isFinished()).isFalse();
    }

    @Test
    public void shouldReportNoFinishedStatusWhenJobNotFinished() {
        // Given
        CompactionJobCreatedStatus created = CompactionJobCreatedStatus.builder()
                .updateTime(Instant.parse("2022-09-23T09:23:00.012Z"))
                .partitionId("partition1").childPartitionIds(null)
                .inputFilesCount(11)
                .build();
        ProcessStartedStatus started = ProcessStartedStatus.updateAndStartTime(
                Instant.parse("2022-09-23T09:23:30.012Z"),
                Instant.parse("2022-09-23T09:23:30.001Z"));

        // When
        CompactionJobStatus status = new TestCompactionJobStatusUpdateRecords()
                .updatesForJobWithTask("job1", DEFAULT_TASK_ID_1, created, started)
                .buildSingleStatus();

        // Then
        assertThat(status.getJobRunList())
                .extracting(ProcessRun::getTaskId, ProcessRun::getStartedStatus, ProcessRun::getFinishedStatus)
                .containsExactly(
                        tuple(DEFAULT_TASK_ID_1, started, null));
        assertThat(status.isFinished()).isFalse();
    }

    @Test
    public void shouldBuildCompactionJobRunWhenJobFinished() {
        // Given
        CompactionJobCreatedStatus created = CompactionJobCreatedStatus.builder()
                .updateTime(Instant.parse("2022-09-23T09:23:00.012Z"))
                .partitionId("partition1").childPartitionIds(null)
                .inputFilesCount(11)
                .build();
        ProcessStartedStatus started = ProcessStartedStatus.updateAndStartTime(
                Instant.parse("2022-09-24T09:23:30.012Z"),
                Instant.parse("2022-09-24T09:23:30.001Z"));
        ProcessFinishedStatus finished = ProcessFinishedStatus.updateTimeAndSummary(
                Instant.parse("2022-09-24T09:24:00.012Z"),
                new RecordsProcessedSummary(new RecordsProcessed(450L, 300L),
                        Instant.parse("2022-09-24T09:23:30.001Z"),
                        Instant.parse("2022-09-24T09:24:00.001Z")));

        // When
        CompactionJobStatus status = new TestCompactionJobStatusUpdateRecords()
                .updatesForJobWithTask("job1", DEFAULT_TASK_ID_1, created, started, finished)
                .buildSingleStatus();

        // Then
        assertThat(status.getJobRunList())
                .extracting(ProcessRun::getTaskId, ProcessRun::getStartedStatus, ProcessRun::getFinishedStatus)
                .containsExactly(
                        tuple(DEFAULT_TASK_ID_1, started, finished));
        assertThat(status.isFinished()).isTrue();
    }

    @Test
    public void shouldBuildCompactionJobRunsWhenJobStartedSameTask() {
        // Given
        CompactionJobCreatedStatus created = CompactionJobCreatedStatus.builder()
                .updateTime(Instant.parse("2022-09-23T09:23:00.012Z"))
                .partitionId("partition1").childPartitionIds(null)
                .inputFilesCount(11)
                .build();
        ProcessStartedStatus started1 = ProcessStartedStatus.updateAndStartTime(
                Instant.parse("2022-09-24T09:23:30.012Z"),
                Instant.parse("2022-09-24T09:23:30.001Z"));
        ProcessStartedStatus started2 = ProcessStartedStatus.updateAndStartTime(
                Instant.parse("2022-09-24T09:24:30.012Z"),
                Instant.parse("2022-09-24T09:24:30.001Z"));

        // When
        CompactionJobStatus status = new TestCompactionJobStatusUpdateRecords()
                .updatesForJobWithTask("job1", DEFAULT_TASK_ID_1, created, started1, started2)
                .buildSingleStatus();

        // Then
        assertThat(status.getJobRunList())
                .extracting(ProcessRun::getTaskId, ProcessRun::getStartedStatus, ProcessRun::getFinishedStatus)
                .containsExactly(
                        tuple(DEFAULT_TASK_ID_1, started2, null),
                        tuple(DEFAULT_TASK_ID_1, started1, null));
        assertThat(status.isFinished()).isFalse();
    }

    @Test
    public void shouldBuildCompactionJobRunsWhenJobFinishedMultipleTimesSameTask() {
        // Given
        CompactionJobCreatedStatus created = CompactionJobCreatedStatus.builder()
                .updateTime(Instant.parse("2022-09-23T09:23:00.012Z"))
                .partitionId("partition1").childPartitionIds(null)
                .inputFilesCount(11)
                .build();
        ProcessStartedStatus started1 = ProcessStartedStatus.updateAndStartTime(
                Instant.parse("2022-09-23T09:23:30.012Z"),
                Instant.parse("2022-09-23T09:23:30.001Z"));
        ProcessFinishedStatus finished1 = ProcessFinishedStatus.updateTimeAndSummary(
                Instant.parse("2022-09-23T09:24:00.012Z"),
                new RecordsProcessedSummary(new RecordsProcessed(450L, 300L),
                        Instant.parse("2022-09-23T09:23:30.001Z"),
                        Instant.parse("2022-09-23T09:24:00.001Z")));
        ProcessStartedStatus started2 = ProcessStartedStatus.updateAndStartTime(
                Instant.parse("2022-09-24T09:23:30.012Z"),
                Instant.parse("2022-09-24T09:23:30.001Z"));
        ProcessFinishedStatus finished2 = ProcessFinishedStatus.updateTimeAndSummary(
                Instant.parse("2022-09-24T09:24:00.012Z"),
                new RecordsProcessedSummary(new RecordsProcessed(450L, 300L),
                        Instant.parse("2022-09-24T09:23:30.001Z"),
                        Instant.parse("2022-09-24T09:24:00.001Z")));

        // When
        CompactionJobStatus status = new TestCompactionJobStatusUpdateRecords()
                .updatesForJobWithTask("job1", DEFAULT_TASK_ID_1, created,
                        started1, finished1, started2, finished2)
                .buildSingleStatus();

        // Then
        assertThat(status.getJobRunList())
                .extracting(ProcessRun::getTaskId, ProcessRun::getStartedStatus, ProcessRun::getFinishedStatus)
                .containsExactly(
                        tuple(DEFAULT_TASK_ID_1, started2, finished2),
                        tuple(DEFAULT_TASK_ID_1, started1, finished1));
        assertThat(status.isFinished()).isTrue();
    }

    @Test
    public void shouldBuildCompactionJobRunsWhenJobStartedDifferentTasks() {
        // Given
        CompactionJobCreatedStatus created = CompactionJobCreatedStatus.builder()
                .updateTime(Instant.parse("2022-09-23T09:23:00.012Z"))
                .partitionId("partition1").childPartitionIds(null)
                .inputFilesCount(11)
                .build();
        ProcessStartedStatus started1 = ProcessStartedStatus.updateAndStartTime(
                Instant.parse("2022-09-23T09:23:30.012Z"),
                Instant.parse("2022-09-23T09:23:30.001Z"));
        ProcessStartedStatus started2 = ProcessStartedStatus.updateAndStartTime(
                Instant.parse("2022-09-24T09:23:30.012Z"),
                Instant.parse("2022-09-24T09:23:30.001Z"));
        ProcessStartedStatus started3 = ProcessStartedStatus.updateAndStartTime(
                Instant.parse("2022-09-25T09:23:30.012Z"),
                Instant.parse("2022-09-25T09:23:30.001Z"));
        ProcessStartedStatus started4 = ProcessStartedStatus.updateAndStartTime(
                Instant.parse("2022-09-26T09:23:30.012Z"),
                Instant.parse("2022-09-26T09:23:30.001Z"));

        // When
        CompactionJobStatus status = new TestCompactionJobStatusUpdateRecords()
                .jobCreated("job1", created)
                .updatesForJobWithTask("job1", DEFAULT_TASK_ID_1, started1, started3)
                .updatesForJobWithTask("job1", DEFAULT_TASK_ID_2, started2, started4)
                .buildSingleStatus();

        // Then
        assertThat(status.getJobRunList())
                .extracting(ProcessRun::getTaskId, ProcessRun::getStartedStatus, ProcessRun::getFinishedStatus)
                .containsExactly(
                        tuple(DEFAULT_TASK_ID_2, started4, null),
                        tuple(DEFAULT_TASK_ID_1, started3, null),
                        tuple(DEFAULT_TASK_ID_2, started2, null),
                        tuple(DEFAULT_TASK_ID_1, started1, null));
        assertThat(status.isFinished()).isFalse();
    }

    @Test
    public void shouldBuildCompactionJobRunsWhenJobFinishedDifferentTasks() {
        // Given
        CompactionJobCreatedStatus created = CompactionJobCreatedStatus.builder()
                .updateTime(Instant.parse("2022-09-23T09:23:00.012Z"))
                .partitionId("partition1").childPartitionIds(null)
                .inputFilesCount(11)
                .build();
        ProcessStartedStatus started1 = ProcessStartedStatus.updateAndStartTime(
                Instant.parse("2022-09-23T09:23:30.012Z"),
                Instant.parse("2022-09-23T09:23:30.001Z"));
        ProcessFinishedStatus finished1 = ProcessFinishedStatus.updateTimeAndSummary(
                Instant.parse("2022-09-23T09:24:00.012Z"),
                new RecordsProcessedSummary(new RecordsProcessed(450L, 300L),
                        Instant.parse("2022-09-23T09:23:30.001Z"),
                        Instant.parse("2022-09-23T09:24:00.001Z")));
        ProcessStartedStatus started2 = ProcessStartedStatus.updateAndStartTime(
                Instant.parse("2022-09-24T09:23:30.012Z"),
                Instant.parse("2022-09-24T09:23:30.001Z"));
        ProcessFinishedStatus finished2 = ProcessFinishedStatus.updateTimeAndSummary(
                Instant.parse("2022-09-24T09:24:00.012Z"),
                new RecordsProcessedSummary(new RecordsProcessed(450L, 300L),
                        Instant.parse("2022-09-24T09:23:30.001Z"),
                        Instant.parse("2022-09-24T09:24:00.001Z")));

        // When
        CompactionJobStatus status = new TestCompactionJobStatusUpdateRecords()
                .jobCreated("job1", created)
                .updatesForJobWithTask("job1", DEFAULT_TASK_ID_1, started1, finished1)
                .updatesForJobWithTask("job1", DEFAULT_TASK_ID_2, started2, finished2)
                .buildSingleStatus();

        // Then
        assertThat(status.getJobRunList())
                .extracting(ProcessRun::getTaskId, ProcessRun::getStartedStatus, ProcessRun::getFinishedStatus)
                .containsExactly(
                        tuple(DEFAULT_TASK_ID_2, started2, finished2),
                        tuple(DEFAULT_TASK_ID_1, started1, finished1));
        assertThat(status.isFinished()).isTrue();
    }

    @Test
    public void shouldBuildCompactionJobRunsWhenJobFinishedReturnedFromDatabaseOutOfOrder() {
        // Given
        CompactionJobCreatedStatus created = CompactionJobCreatedStatus.builder()
                .updateTime(Instant.parse("2022-09-23T09:23:00.012Z"))
                .partitionId("partition1").childPartitionIds(null)
                .inputFilesCount(11)
                .build();
        ProcessStartedStatus started = ProcessStartedStatus.updateAndStartTime(
                Instant.parse("2022-09-24T09:23:30.012Z"),
                Instant.parse("2022-09-24T09:23:30.001Z"));
        ProcessFinishedStatus finished = ProcessFinishedStatus.updateTimeAndSummary(
                Instant.parse("2022-09-24T09:24:00.012Z"),
                new RecordsProcessedSummary(new RecordsProcessed(450L, 300L),
                        Instant.parse("2022-09-24T09:23:30.001Z"),
                        Instant.parse("2022-09-24T09:24:00.001Z")));

        // When
        CompactionJobStatus status = new TestCompactionJobStatusUpdateRecords()
                .updatesForJobWithTask("job1", DEFAULT_TASK_ID_1, created, finished, started)
                .buildSingleStatus();

        // Then
        assertThat(status.getJobRunList())
                .extracting(ProcessRun::getStartedStatus, ProcessRun::getFinishedStatus)
                .containsExactly(
                        tuple(started, finished));
        assertThat(status.isFinished()).isTrue();
    }

    @Test
    public void shouldBuildCompactionJobRunsWhenJobStartedReturnedFromDatabaseOutOfOrder() {
        // Given
        CompactionJobCreatedStatus created = CompactionJobCreatedStatus.builder()
                .updateTime(Instant.parse("2022-09-23T09:23:00.012Z"))
                .partitionId("partition1").childPartitionIds(null)
                .inputFilesCount(11)
                .build();
        ProcessStartedStatus started1 = ProcessStartedStatus.updateAndStartTime(
                Instant.parse("2022-09-24T09:23:30.012Z"),
                Instant.parse("2022-09-24T09:23:30.001Z"));
        ProcessStartedStatus started2 = ProcessStartedStatus.updateAndStartTime(
                Instant.parse("2022-09-25T09:23:30.012Z"),
                Instant.parse("2022-09-25T09:23:30.001Z"));
        ProcessStartedStatus started3 = ProcessStartedStatus.updateAndStartTime(
                Instant.parse("2022-09-26T09:23:30.012Z"),
                Instant.parse("2022-09-26T09:23:30.001Z"));

        // When
        CompactionJobStatus status = new TestCompactionJobStatusUpdateRecords()
                .updatesForJobWithTask("job1", DEFAULT_TASK_ID_1, created, started3, started1, started2)
                .buildSingleStatus();

        // Then
        assertThat(status.getJobRunList())
                .extracting(ProcessRun::getStartedStatus, ProcessRun::getFinishedStatus)
                .containsExactly(
                        tuple(started3, null),
                        tuple(started2, null),
                        tuple(started1, null));
        assertThat(status.isFinished()).isFalse();
    }

    @Test
    public void shouldBuildCompactionJobRunsWhenJobFinishedMultipleTimesReturnedFromDatabaseOutOfOrder() {
        // Given
        CompactionJobCreatedStatus created = CompactionJobCreatedStatus.builder()
                .updateTime(Instant.parse("2022-09-23T09:23:00.012Z"))
                .partitionId("partition1").childPartitionIds(null)
                .inputFilesCount(11)
                .build();
        ProcessStartedStatus started1 = ProcessStartedStatus.updateAndStartTime(
                Instant.parse("2022-09-24T09:23:30.012Z"),
                Instant.parse("2022-09-24T09:23:30.001Z"));
        ProcessStartedStatus started2 = ProcessStartedStatus.updateAndStartTime(
                Instant.parse("2022-09-25T09:23:30.012Z"),
                Instant.parse("2022-09-25T09:23:30.001Z"));
        ProcessStartedStatus started3 = ProcessStartedStatus.updateAndStartTime(
                Instant.parse("2022-09-26T09:23:30.012Z"),
                Instant.parse("2022-09-26T09:23:30.001Z"));
        ProcessFinishedStatus finished = ProcessFinishedStatus.updateTimeAndSummary(
                Instant.parse("2022-09-27T09:24:00.012Z"),
                new RecordsProcessedSummary(new RecordsProcessed(450L, 300L),
                        Instant.parse("2022-09-26T09:23:30.001Z"),
                        Instant.parse("2022-09-27T09:24:00.001Z")));

        // When
        CompactionJobStatus status = new TestCompactionJobStatusUpdateRecords()
                .updatesForJobWithTask("job1", DEFAULT_TASK_ID_1, created, started3, finished, started1, started2)
                .buildSingleStatus();

        // Then
        assertThat(status.getJobRunList())
                .extracting(ProcessRun::getStartedStatus, ProcessRun::getFinishedStatus)
                .containsExactly(
                        tuple(started3, finished),
                        tuple(started2, null),
                        tuple(started1, null));
        assertThat(status.isFinished()).isFalse();
    }

    @Test
    public void shouldBuildCompactionJobRunsWhenJobFinishedDifferentTasksFromDatabaseOutOfOrder() {
        // Given
        CompactionJobCreatedStatus created = CompactionJobCreatedStatus.builder()
                .updateTime(Instant.parse("2022-09-23T09:23:00.012Z"))
                .partitionId("partition1").childPartitionIds(null)
                .inputFilesCount(11)
                .build();
        ProcessStartedStatus started1 = ProcessStartedStatus.updateAndStartTime(
                Instant.parse("2022-09-22T09:23:30.012Z"),
                Instant.parse("2022-09-22T09:23:30.001Z"));
        ProcessFinishedStatus finished1 = ProcessFinishedStatus.updateTimeAndSummary(
                Instant.parse("2022-09-23T09:24:00.012Z"),
                new RecordsProcessedSummary(new RecordsProcessed(450L, 300L),
                        Instant.parse("2022-09-23T09:23:30.001Z"),
                        Instant.parse("2022-09-23T09:24:00.001Z")));
        ProcessStartedStatus started2 = ProcessStartedStatus.updateAndStartTime(
                Instant.parse("2022-09-24T09:23:30.012Z"),
                Instant.parse("2022-09-24T09:23:30.001Z"));
        ProcessFinishedStatus finished2 = ProcessFinishedStatus.updateTimeAndSummary(
                Instant.parse("2022-09-24T09:24:00.012Z"),
                new RecordsProcessedSummary(new RecordsProcessed(450L, 300L),
                        Instant.parse("2022-09-24T09:23:30.001Z"),
                        Instant.parse("2022-09-24T09:24:00.001Z")));

        // When
        CompactionJobStatus status = new TestCompactionJobStatusUpdateRecords()
                .updatesForJobWithTask("job1", DEFAULT_TASK_ID_1, created, finished1, started1)
                .updatesForJobWithTask("job1", DEFAULT_TASK_ID_2, created, finished2, started2)
                .buildSingleStatus();

        // Then
        assertThat(status.getJobRunList())
                .extracting(ProcessRun::getTaskId, ProcessRun::getStartedStatus, ProcessRun::getFinishedStatus)
                .containsExactly(
                        tuple(DEFAULT_TASK_ID_2, started2, finished2),
                        tuple(DEFAULT_TASK_ID_1, started1, finished1));
        assertThat(status.isFinished()).isTrue();
    }

    @Test
    public void shouldBuildCompactionJobRunsWhenJobRunStartedAndFinshedDuringAnotherRunDifferentTasks() {
        // Given
        CompactionJobCreatedStatus created = CompactionJobCreatedStatus.builder()
                .updateTime(Instant.parse("2022-09-23T09:23:00.012Z"))
                .partitionId("partition1").childPartitionIds(null)
                .inputFilesCount(11)
                .build();
        ProcessStartedStatus started1 = ProcessStartedStatus.updateAndStartTime(
                Instant.parse("2022-09-23T09:23:30.012Z"),
                Instant.parse("2022-09-23T09:23:30.001Z"));
        ProcessStartedStatus started2 = ProcessStartedStatus.updateAndStartTime(
                Instant.parse("2022-09-24T09:23:30.012Z"),
                Instant.parse("2022-09-24T09:23:30.001Z"));
        ProcessFinishedStatus finished2 = ProcessFinishedStatus.updateTimeAndSummary(
                Instant.parse("2022-09-25T09:24:00.012Z"),
                new RecordsProcessedSummary(new RecordsProcessed(450L, 300L),
                        Instant.parse("2022-09-25T09:23:30.001Z"),
                        Instant.parse("2022-09-25T09:24:00.001Z")));
        ProcessFinishedStatus finished1 = ProcessFinishedStatus.updateTimeAndSummary(
                Instant.parse("2022-09-26T09:24:00.012Z"),
                new RecordsProcessedSummary(new RecordsProcessed(450L, 300L),
                        Instant.parse("2022-09-26T09:23:30.001Z"),
                        Instant.parse("2022-09-26T09:24:00.001Z")));

        // When
        CompactionJobStatus status = new TestCompactionJobStatusUpdateRecords()
                .updatesForJobWithTask("job1", DEFAULT_TASK_ID_1, created,
                        started1, finished1)
                .updatesForJobWithTask("job1", DEFAULT_TASK_ID_2, started2, finished2)
                .buildSingleStatus();

        // Then
        assertThat(status.getJobRunList())
                .extracting(ProcessRun::getTaskId, ProcessRun::getStartedStatus, ProcessRun::getFinishedStatus)
                .containsExactly(
                        tuple(DEFAULT_TASK_ID_2, started2, finished2),
                        tuple(DEFAULT_TASK_ID_1, started1, finished1));
        assertThat(status.isFinished()).isTrue();
    }
}
