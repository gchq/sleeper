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

import sleeper.core.tracker.compaction.job.query.CompactionJobCommittedStatus;
import sleeper.core.tracker.compaction.job.query.CompactionJobCreatedStatus;
import sleeper.core.tracker.compaction.job.query.CompactionJobFinishedStatus;
import sleeper.core.tracker.compaction.job.query.CompactionJobStartedStatus;
import sleeper.core.tracker.compaction.job.query.CompactionJobStatus;
import sleeper.core.tracker.job.status.JobRunFailedStatus;

import java.time.Duration;
import java.time.Instant;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.core.tracker.compaction.job.CompactionJobStatusTestData.compactionCommittedStatus;
import static sleeper.core.tracker.compaction.job.CompactionJobStatusTestData.compactionFinishedStatus;
import static sleeper.core.tracker.compaction.job.CompactionJobStatusTestData.compactionStartedStatus;
import static sleeper.core.tracker.compaction.job.CompactionJobStatusTestData.jobStatusFrom;
import static sleeper.core.tracker.compaction.job.CompactionJobStatusTestData.jobStatusFromSingleRunUpdates;
import static sleeper.core.tracker.compaction.job.query.CompactionJobStatusType.CREATED;
import static sleeper.core.tracker.compaction.job.query.CompactionJobStatusType.FAILED;
import static sleeper.core.tracker.compaction.job.query.CompactionJobStatusType.FINISHED;
import static sleeper.core.tracker.compaction.job.query.CompactionJobStatusType.IN_PROGRESS;
import static sleeper.core.tracker.compaction.job.query.CompactionJobStatusType.UNCOMMITTED;
import static sleeper.core.tracker.job.run.JobRunSummaryTestHelper.summary;
import static sleeper.core.tracker.job.status.JobStatusUpdateTestHelper.failedStatus;
import static sleeper.core.tracker.job.status.TestJobStatusUpdateRecords.forRunOnTask;
import static sleeper.core.tracker.job.status.TestJobStatusUpdateRecords.onNoTask;
import static sleeper.core.tracker.job.status.TestJobStatusUpdateRecords.records;

public class CompactionJobStatusFurthestStatusTest {

    @Test
    void shouldReportJobAssignedToInputFiles() {
        // Given
        CompactionJobCreatedStatus filesAssigned = CompactionJobCreatedStatus.builder()
                .updateTime(Instant.parse("2023-03-22T15:36:02Z"))
                .partitionId("partition1")
                .inputFilesCount(11)
                .build();

        // When
        CompactionJobStatus status = jobStatusFromSingleRunUpdates(filesAssigned);

        // Then
        assertThat(status.getFurthestStatusType()).isEqualTo(CREATED);
    }

    @Test
    void shouldReportJobStarted() {
        // Given
        CompactionJobCreatedStatus filesAssigned = CompactionJobCreatedStatus.builder()
                .updateTime(Instant.parse("2023-03-22T15:36:02Z"))
                .partitionId("partition1")
                .inputFilesCount(11)
                .build();
        CompactionJobStartedStatus started = compactionStartedStatus(
                Instant.parse("2023-03-22T15:36:03Z"));

        // When
        CompactionJobStatus status = jobStatusFromSingleRunUpdates(filesAssigned, started);

        // Then
        assertThat(status.getFurthestStatusType()).isEqualTo(IN_PROGRESS);
    }

    @Test
    void shouldReportJobUncommitted() {
        // Given
        CompactionJobCreatedStatus filesAssigned = CompactionJobCreatedStatus.builder()
                .updateTime(Instant.parse("2023-03-22T15:36:02Z"))
                .partitionId("partition1")
                .inputFilesCount(11)
                .build();
        CompactionJobStartedStatus started = compactionStartedStatus(
                Instant.parse("2023-03-22T15:36:03Z"));
        CompactionJobFinishedStatus finished = compactionFinishedStatus(
                summary(started, Duration.ofSeconds(30), 200L, 100L));

        // When
        CompactionJobStatus status = jobStatusFromSingleRunUpdates(filesAssigned, started, finished);

        // Then
        assertThat(status.getFurthestStatusType()).isEqualTo(UNCOMMITTED);
    }

    @Test
    void shouldReportJobCommitted() {
        // Given
        CompactionJobCreatedStatus filesAssigned = CompactionJobCreatedStatus.builder()
                .updateTime(Instant.parse("2023-03-22T15:36:02Z"))
                .partitionId("partition1")
                .inputFilesCount(11)
                .build();
        CompactionJobStartedStatus started = compactionStartedStatus(
                Instant.parse("2023-03-22T15:36:03Z"));
        CompactionJobFinishedStatus finished = compactionFinishedStatus(
                summary(started, Duration.ofSeconds(30), 200L, 100L));
        CompactionJobCommittedStatus committed = compactionCommittedStatus(
                Instant.parse("2023-03-22T15:40:00Z"));

        // When
        CompactionJobStatus status = jobStatusFromSingleRunUpdates(filesAssigned, started, finished, committed);

        // Then
        assertThat(status.getFurthestStatusType()).isEqualTo(FINISHED);
    }

    @Test
    void shouldReportJobFailed() {
        // Given
        CompactionJobCreatedStatus filesAssigned = CompactionJobCreatedStatus.builder()
                .updateTime(Instant.parse("2023-03-22T15:36:02Z"))
                .partitionId("partition1")
                .inputFilesCount(11)
                .build();
        CompactionJobStartedStatus started = compactionStartedStatus(
                Instant.parse("2023-03-22T15:36:03Z"));
        JobRunFailedStatus failed = failedStatus(started, Duration.ofSeconds(30), List.of("Some failure"));

        // When
        CompactionJobStatus status = jobStatusFromSingleRunUpdates(filesAssigned, started, failed);

        // Then
        assertThat(status.getFurthestStatusType()).isEqualTo(FAILED);
    }

    @Test
    void shouldReportJobSucceededWhenFollowedByAFailedRun() {
        // Given
        CompactionJobCreatedStatus filesAssigned = CompactionJobCreatedStatus.builder()
                .updateTime(Instant.parse("2023-03-22T15:36:02Z"))
                .partitionId("partition1")
                .inputFilesCount(11)
                .build();
        CompactionJobStartedStatus started1 = compactionStartedStatus(
                Instant.parse("2023-03-22T15:36:03Z"));
        CompactionJobFinishedStatus finished = compactionFinishedStatus(
                summary(started1, Duration.ofSeconds(30), 200L, 100L));
        CompactionJobCommittedStatus committed = compactionCommittedStatus(
                Instant.parse("2023-03-22T15:36:35Z"));
        CompactionJobStartedStatus started2 = compactionStartedStatus(
                Instant.parse("2023-03-22T15:37:01Z"));
        JobRunFailedStatus failed = failedStatus(started2, Duration.ofSeconds(30), List.of("Some failure"));

        // When
        CompactionJobStatus status = jobStatusFrom(records().fromUpdates(
                onNoTask(filesAssigned), forRunOnTask(started1, finished, committed), forRunOnTask(started2, failed)));

        // Then
        assertThat(status.getFurthestStatusType()).isEqualTo(FINISHED);
    }

    @Test
    void shouldReportJobInProgressWhenRetryingAfterFailure() {
        // Given
        CompactionJobCreatedStatus filesAssigned = CompactionJobCreatedStatus.builder()
                .updateTime(Instant.parse("2023-03-22T15:36:02Z"))
                .partitionId("partition1")
                .inputFilesCount(11)
                .build();
        CompactionJobStartedStatus started1 = compactionStartedStatus(
                Instant.parse("2023-03-22T15:36:03Z"));
        JobRunFailedStatus failed = failedStatus(started1, Duration.ofSeconds(30), List.of("Some failure"));
        CompactionJobStartedStatus started2 = compactionStartedStatus(
                Instant.parse("2023-03-22T15:37:01Z"));

        // When
        CompactionJobStatus status = jobStatusFrom(records().fromUpdates(
                onNoTask(filesAssigned), forRunOnTask(started1, failed), forRunOnTask(started2)));

        // Then
        assertThat(status.getFurthestStatusType()).isEqualTo(IN_PROGRESS);
    }
}
