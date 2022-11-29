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
import sleeper.core.record.process.status.ProcessFinishedStatus;
import sleeper.core.record.process.status.ProcessRun;
import sleeper.core.record.process.status.ProcessStartedStatus;

import java.time.Instant;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.groups.Tuple.tuple;

public class CompactionJobRunTest {
    private static final String DEFAULT_TASK_ID = "task-id-1";

    @Test
    public void shouldReportNoRunsWhenJobNotStarted() {
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
                .updatesForJobWithTask("job1", DEFAULT_TASK_ID, created, started)
                .buildSingleStatus();

        // Then
        assertThat(status.getJobRuns())
                .extracting(ProcessRun::getTaskId, ProcessRun::getStartedStatus, ProcessRun::getFinishedStatus)
                .containsExactly(
                        tuple(DEFAULT_TASK_ID, started, null));
        assertThat(status.isFinished()).isFalse();
    }

    @Test
    public void shouldReportRunWhenJobFinished() {
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
                .updatesForJobWithTask("job1", DEFAULT_TASK_ID, created, started, finished)
                .buildSingleStatus();

        // Then
        assertThat(status.getJobRuns())
                .extracting(ProcessRun::getTaskId, ProcessRun::getStartedStatus, ProcessRun::getFinishedStatus)
                .containsExactly(
                        tuple(DEFAULT_TASK_ID, started, finished));
        assertThat(status.isFinished()).isTrue();
    }
}
