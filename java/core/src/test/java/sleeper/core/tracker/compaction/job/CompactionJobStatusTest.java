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

import sleeper.core.tracker.compaction.job.query.CompactionJobStatus;
import sleeper.core.tracker.job.run.JobRunSummary;
import sleeper.core.tracker.job.run.JobRunTime;
import sleeper.core.tracker.job.run.RecordsProcessed;

import java.time.Duration;
import java.time.Instant;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.core.tracker.compaction.job.CompactionJobStatusTestData.compactionJobCreated;
import static sleeper.core.tracker.compaction.job.CompactionJobStatusTestData.failedCompactionRun;
import static sleeper.core.tracker.compaction.job.CompactionJobStatusTestData.finishedCompactionRun;
import static sleeper.core.tracker.compaction.job.CompactionJobStatusTestData.startedCompactionRun;
import static sleeper.core.tracker.job.status.TestJobStatusUpdateRecords.DEFAULT_TASK_ID;

class CompactionJobStatusTest {

    @Test
    void shouldBuildCompactionJobCreatedFromJob() {
        // Given
        Instant updateTime = Instant.parse("2022-09-22T13:33:12.001Z");

        // When
        CompactionJobStatus status = compactionJobCreated(updateTime);

        // Then
        assertThat(status).extracting("createUpdateTime", "partitionId", "inputFilesCount")
                .containsExactly(updateTime, "root", 1);
    }

    @Test
    void shouldReportCompactionJobNotStarted() {
        // Given
        Instant updateTime = Instant.parse("2022-09-22T13:33:12.001Z");

        // When
        CompactionJobStatus status = compactionJobCreated(updateTime);

        // Then
        assertThat(status).extracting(CompactionJobStatus::isStarted, CompactionJobStatus::isUnstartedOrInProgress)
                .containsExactly(false, true);
    }

    @Test
    void shouldBuildCompactionJobStarted() {
        // When
        CompactionJobStatus status = compactionJobCreated(Instant.parse("2022-09-22T13:33:12.001Z"),
                startedCompactionRun(DEFAULT_TASK_ID, Instant.parse("2022-09-22T13:33:30.001Z")));

        // Then
        assertThat(status).extracting(CompactionJobStatus::isStarted, CompactionJobStatus::isUnstartedOrInProgress)
                .containsExactly(true, true);
    }

    @Test
    void shouldBuildCompactionJobFinished() {
        // Given
        Instant startTime = Instant.parse("2022-09-22T13:33:10.001Z");
        Instant finishTime = Instant.parse("2022-09-22T13:34:10.001Z");
        Instant commitTime = Instant.parse("2022-09-22T13:34:20.001Z");
        JobRunSummary summary = new JobRunSummary(
                new RecordsProcessed(450L, 300L), startTime, finishTime);

        // When
        CompactionJobStatus status = compactionJobCreated(Instant.parse("2022-09-22T13:33:00.001Z"),
                finishedCompactionRun(DEFAULT_TASK_ID, summary, commitTime));

        // Then
        assertThat(status).extracting(CompactionJobStatus::isStarted, CompactionJobStatus::isUnstartedOrInProgress)
                .containsExactly(true, false);
    }

    @Test
    void shouldBuildCompactionJobFailedWaitingForRetry() {
        // Given
        Instant startTime = Instant.parse("2022-09-22T13:33:10.001Z");
        Instant finishTime = Instant.parse("2022-09-22T13:34:10.001Z");
        JobRunTime runTime = new JobRunTime(startTime, finishTime);
        List<String> failureReasons = List.of("Could not read input file", "Some IO failure");

        // When
        CompactionJobStatus status = compactionJobCreated(Instant.parse("2022-09-22T13:33:00.001Z"),
                failedCompactionRun(DEFAULT_TASK_ID, runTime, failureReasons));

        // Then
        assertThat(status).extracting(CompactionJobStatus::isStarted, CompactionJobStatus::isUnstartedOrInProgress)
                .containsExactly(true, true);
    }

    @Test
    void shouldBuildCompactionJobFinishedAndInProgress() {
        // Given
        JobRunSummary run1Summary = new JobRunSummary(
                new RecordsProcessed(450L, 300L),
                Instant.parse("2022-09-22T13:33:10.001Z"), Duration.ofMinutes(1));
        Instant commitTime1 = Instant.parse("2022-09-22T13:34:15.001Z");
        Instant startTime2 = Instant.parse("2022-09-22T13:33:15.001Z");

        // When
        CompactionJobStatus status = compactionJobCreated(Instant.parse("2022-09-22T13:33:00.001Z"),
                finishedCompactionRun("task-1", run1Summary, commitTime1),
                startedCompactionRun("task-2", startTime2));

        // Then
        assertThat(status).extracting(CompactionJobStatus::isStarted, CompactionJobStatus::isUnstartedOrInProgress)
                .containsExactly(true, true);
    }
}
