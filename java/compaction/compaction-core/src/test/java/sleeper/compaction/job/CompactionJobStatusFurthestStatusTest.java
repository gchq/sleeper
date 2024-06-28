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
package sleeper.compaction.job;

import org.junit.jupiter.api.Test;

import sleeper.compaction.job.status.CompactionJobCreatedStatus;
import sleeper.compaction.job.status.CompactionJobStartedStatus;
import sleeper.compaction.job.status.CompactionJobStatus;
import sleeper.core.record.process.status.ProcessFailedStatus;
import sleeper.core.record.process.status.ProcessFinishedStatus;

import java.time.Duration;
import java.time.Instant;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.compaction.job.CompactionJobStatusTestData.compactionStartedStatus;
import static sleeper.compaction.job.CompactionJobStatusTestData.jobStatusFromUpdates;
import static sleeper.compaction.job.status.CompactionJobStatusType.FAILED;
import static sleeper.compaction.job.status.CompactionJobStatusType.FINISHED;
import static sleeper.compaction.job.status.CompactionJobStatusType.IN_PROGRESS;
import static sleeper.compaction.job.status.CompactionJobStatusType.PENDING;
import static sleeper.core.record.process.status.ProcessStatusUpdateTestHelper.failedStatus;
import static sleeper.core.record.process.status.ProcessStatusUpdateTestHelper.finishedStatus;

public class CompactionJobStatusFurthestStatusTest {

    @Test
    void shouldReportJobCreated() {
        // Given
        CompactionJobCreatedStatus created = CompactionJobCreatedStatus.builder()
                .updateTime(Instant.parse("2023-03-22T15:36:02Z"))
                .partitionId("partition1")
                .inputFilesCount(11)
                .build();

        // When
        CompactionJobStatus status = jobStatusFromUpdates(created);

        // Then
        assertThat(status.getFurthestRunStatusType()).isEqualTo(PENDING);
    }

    @Test
    void shouldReportJobStarted() {
        // Given
        CompactionJobCreatedStatus created = CompactionJobCreatedStatus.builder()
                .updateTime(Instant.parse("2023-03-22T15:36:02Z"))
                .partitionId("partition1")
                .inputFilesCount(11)
                .build();
        CompactionJobStartedStatus started = compactionStartedStatus(Instant.parse("2023-03-22T15:36:01Z"));

        // When
        CompactionJobStatus status = jobStatusFromUpdates(created, started);

        // Then
        assertThat(status.getFurthestRunStatusType()).isEqualTo(IN_PROGRESS);
    }

    @Test
    void shouldReportJobFinished() {
        // Given
        CompactionJobCreatedStatus created = CompactionJobCreatedStatus.builder()
                .updateTime(Instant.parse("2023-03-22T15:36:02Z"))
                .partitionId("partition1")
                .inputFilesCount(11)
                .build();
        CompactionJobStartedStatus started = compactionStartedStatus(Instant.parse("2023-03-22T15:36:01Z"));
        ProcessFinishedStatus finished = finishedStatus(started, Duration.ofSeconds(30), 200L, 100L);

        // When
        CompactionJobStatus status = jobStatusFromUpdates(created, started, finished);

        // Then
        assertThat(status.getFurthestRunStatusType()).isEqualTo(FINISHED);
    }

    @Test
    void shouldReportJobFailed() {
        // Given
        CompactionJobCreatedStatus created = CompactionJobCreatedStatus.builder()
                .updateTime(Instant.parse("2023-03-22T15:36:02Z"))
                .partitionId("partition1")
                .inputFilesCount(11)
                .build();
        CompactionJobStartedStatus started = compactionStartedStatus(Instant.parse("2023-03-22T15:36:01Z"));
        ProcessFailedStatus failed = failedStatus(started, Duration.ofSeconds(30), List.of("Some failure"));

        // When
        CompactionJobStatus status = jobStatusFromUpdates(created, started, failed);

        // Then
        assertThat(status.getFurthestRunStatusType()).isEqualTo(FAILED);
    }

    @Test
    void shouldReportJobSucceededWhenFollowedByAFailedRun() {
        // Given
        CompactionJobCreatedStatus created = CompactionJobCreatedStatus.builder()
                .updateTime(Instant.parse("2023-03-22T15:36:02Z"))
                .partitionId("partition1")
                .inputFilesCount(11)
                .build();
        CompactionJobStartedStatus started1 = compactionStartedStatus(Instant.parse("2023-03-22T15:36:01Z"));
        ProcessFinishedStatus finished = finishedStatus(started1, Duration.ofSeconds(30), 200L, 100L);
        CompactionJobStartedStatus started2 = compactionStartedStatus(Instant.parse("2023-03-22T15:37:01Z"));
        ProcessFailedStatus failed = failedStatus(started2, Duration.ofSeconds(30), List.of("Some failure"));

        // When
        CompactionJobStatus status = jobStatusFromUpdates(created, started1, finished, started2, failed);

        // Then
        assertThat(status.getFurthestRunStatusType()).isEqualTo(FINISHED);
    }

    @Test
    void shouldReportJobInProgressWhenRetryingAfterFailure() {
        // Given
        CompactionJobCreatedStatus created = CompactionJobCreatedStatus.builder()
                .updateTime(Instant.parse("2023-03-22T15:36:02Z"))
                .partitionId("partition1")
                .inputFilesCount(11)
                .build();
        CompactionJobStartedStatus started1 = compactionStartedStatus(Instant.parse("2023-03-22T15:36:01Z"));
        ProcessFailedStatus failed = failedStatus(started1, Duration.ofSeconds(30), List.of("Some failure"));
        CompactionJobStartedStatus started2 = compactionStartedStatus(Instant.parse("2023-03-22T15:37:01Z"));

        // When
        CompactionJobStatus status = jobStatusFromUpdates(created, started1, failed, started2);

        // Then
        assertThat(status.getFurthestRunStatusType()).isEqualTo(IN_PROGRESS);
    }
}
