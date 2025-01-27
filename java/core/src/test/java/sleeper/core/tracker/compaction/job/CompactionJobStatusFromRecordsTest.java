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
package sleeper.core.tracker.compaction.job;

import org.junit.jupiter.api.Test;

import sleeper.core.tracker.compaction.job.query.CompactionJobCreatedStatus;
import sleeper.core.tracker.compaction.job.query.CompactionJobFinishedStatus;
import sleeper.core.tracker.compaction.job.query.CompactionJobStartedStatus;
import sleeper.core.tracker.compaction.job.query.CompactionJobStatus;

import java.time.Duration;
import java.time.Instant;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.core.tracker.compaction.job.CompactionJobStatusTestData.compactionFinishedStatus;
import static sleeper.core.tracker.compaction.job.CompactionJobStatusTestData.compactionStartedStatus;
import static sleeper.core.tracker.compaction.job.CompactionJobStatusTestData.jobStatusListFromUpdates;
import static sleeper.core.tracker.job.run.JobRunSummaryTestHelper.summary;
import static sleeper.core.tracker.job.run.JobRunTestData.runOnTask;
import static sleeper.core.tracker.job.status.TestJobStatusUpdateRecords.DEFAULT_EXPIRY;
import static sleeper.core.tracker.job.status.TestJobStatusUpdateRecords.DEFAULT_TASK_ID;
import static sleeper.core.tracker.job.status.TestJobStatusUpdateRecords.forJob;

class CompactionJobStatusFromRecordsTest {

    @Test
    void shouldBuildCompactionJobStatusFromIndividualUpdates() {
        // Given
        CompactionJobCreatedStatus created1 = CompactionJobCreatedStatus.builder()
                .updateTime(Instant.parse("2022-09-23T09:23:00.012Z"))
                .partitionId("partition1")
                .inputFilesCount(11)
                .build();
        CompactionJobStartedStatus started1 = compactionStartedStatus(Instant.parse("2022-09-23T09:23:30.001Z"));
        CompactionJobFinishedStatus finished1 = compactionFinishedStatus(summary(started1, Duration.ofSeconds(30), 200L, 100L));
        CompactionJobCreatedStatus created2 = CompactionJobCreatedStatus.builder()
                .updateTime(Instant.parse("2022-09-24T09:23:00.012Z"))
                .partitionId("partition2")
                .inputFilesCount(12)
                .build();
        CompactionJobStartedStatus started2 = compactionStartedStatus(Instant.parse("2022-09-24T09:23:30.001Z"));
        CompactionJobFinishedStatus finished2 = compactionFinishedStatus(summary(started2, Duration.ofSeconds(30), 450L, 300L));

        // When
        List<CompactionJobStatus> statuses = jobStatusListFromUpdates(
                forJob("job1", created1, started1, finished1),
                forJob("job2", created2, started2, finished2));

        // Then
        assertThat(statuses).containsExactly(
                CompactionJobStatus.builder().jobId("job2").createdStatus(created2)
                        .singleJobRun(runOnTask(DEFAULT_TASK_ID, started2, finished2))
                        .expiryDate(DEFAULT_EXPIRY).build(),
                CompactionJobStatus.builder().jobId("job1").createdStatus(created1)
                        .singleJobRun(runOnTask(DEFAULT_TASK_ID, started1, finished1))
                        .expiryDate(DEFAULT_EXPIRY).build());
    }

    @Test
    void shouldBuildJobWithNoCreatedUpdate() {
        // Given
        CompactionJobStartedStatus started = compactionStartedStatus(Instant.parse("2022-09-23T09:23:30.001Z"));
        CompactionJobFinishedStatus finished = compactionFinishedStatus(summary(started, Duration.ofSeconds(30), 200L, 100L));

        // When
        List<CompactionJobStatus> statuses = jobStatusListFromUpdates(
                forJob("test-job", started, finished));

        // Then
        assertThat(statuses).containsExactly(
                CompactionJobStatus.builder().jobId("test-job")
                        .singleJobRun(runOnTask(DEFAULT_TASK_ID, started, finished))
                        .expiryDate(DEFAULT_EXPIRY).build());
    }

    @Test
    void shouldBuildJobStatusWhenCreatedUpdateStoredAfterStartedUpdate() {
        // Given
        CompactionJobCreatedStatus created = CompactionJobCreatedStatus.builder()
                .updateTime(Instant.parse("2023-03-22T15:36:02Z"))
                .partitionId("partition1")
                .inputFilesCount(11)
                .build();
        CompactionJobStartedStatus started = compactionStartedStatus(Instant.parse("2023-03-22T15:36:01Z"));
        CompactionJobFinishedStatus finished = compactionFinishedStatus(summary(started, Duration.ofSeconds(30), 200L, 100L));

        // When
        List<CompactionJobStatus> statuses = jobStatusListFromUpdates(
                forJob("test-job", created, started, finished));

        // Then
        assertThat(statuses).containsExactly(
                CompactionJobStatus.builder().jobId("test-job").createdStatus(created)
                        .singleJobRun(runOnTask(DEFAULT_TASK_ID, started, finished))
                        .expiryDate(DEFAULT_EXPIRY).build());
    }
}
