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

import sleeper.compaction.core.job.status.CompactionJobCommittedStatus;
import sleeper.compaction.core.job.status.CompactionJobFinishedStatus;
import sleeper.compaction.core.job.status.CompactionJobInputFilesAssignedStatus;
import sleeper.compaction.core.job.status.CompactionJobStartedStatus;
import sleeper.compaction.core.job.status.CompactionJobStatus;
import sleeper.core.record.process.status.ProcessRun;

import java.time.Duration;
import java.time.Instant;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.groups.Tuple.tuple;
import static sleeper.compaction.core.job.CompactionJobStatusTestData.compactionCommittedStatus;
import static sleeper.compaction.core.job.CompactionJobStatusTestData.compactionFinishedStatus;
import static sleeper.compaction.core.job.CompactionJobStatusTestData.compactionStartedStatus;
import static sleeper.compaction.core.job.CompactionJobStatusTestData.jobStatusFromUpdates;
import static sleeper.core.record.process.RecordsProcessedSummaryTestHelper.summary;
import static sleeper.core.record.process.status.TestProcessStatusUpdateRecords.DEFAULT_TASK_ID;

public class CompactionJobRunTest {

    @Test
    public void shouldReportNoRunsWhenJobNotStarted() {
        // Given
        CompactionJobInputFilesAssignedStatus createdStatus = CompactionJobInputFilesAssignedStatus.from(CompactionJob.builder()
                .tableId("test-table").jobId("test-job").partitionId("test-partition")
                .inputFiles(List.of("file1", "file2")).outputFile("output")
                .build(), Instant.parse("2022-09-23T09:23:00.012Z"));

        // When
        CompactionJobStatus status = jobStatusFromUpdates(createdStatus);

        // Then
        assertThat(status.getJobRuns())
                .isEmpty();
        assertThat(status.isUnstartedOrInProgress()).isTrue();
    }

    @Test
    public void shouldReportNoFinishedStatusWhenJobNotFinished() {
        // Given
        CompactionJobStartedStatus started = compactionStartedStatus(Instant.parse("2022-09-23T09:23:30.001Z"));

        // When
        CompactionJobStatus status = jobStatusFromUpdates(started);

        // Then
        assertThat(status.getJobRuns())
                .extracting(ProcessRun::getTaskId, ProcessRun::getStartedStatus, ProcessRun::getFinishedStatus)
                .containsExactly(
                        tuple(DEFAULT_TASK_ID, started, null));
        assertThat(status.isUnstartedOrInProgress()).isTrue();
    }

    @Test
    public void shouldReportRunWhenJobUncommitted() {
        // Given
        CompactionJobStartedStatus started = compactionStartedStatus(Instant.parse("2022-09-24T09:23:30.001Z"));
        CompactionJobFinishedStatus finished = compactionFinishedStatus(summary(started, Duration.ofSeconds(30), 450L, 300L));

        // When
        CompactionJobStatus status = jobStatusFromUpdates(started, finished);

        // Then
        assertThat(status.getJobRuns())
                .extracting(ProcessRun::getTaskId, ProcessRun::getStartedStatus, ProcessRun::getFinishedStatus)
                .containsExactly(
                        tuple(DEFAULT_TASK_ID, started, finished));
        assertThat(status.isUnstartedOrInProgress()).isTrue();
    }

    @Test
    public void shouldReportRunWhenJobFinished() {
        // Given
        CompactionJobStartedStatus started = compactionStartedStatus(Instant.parse("2022-09-24T09:23:30.001Z"));
        CompactionJobFinishedStatus finished = compactionFinishedStatus(summary(started, Duration.ofSeconds(30), 450L, 300L));
        CompactionJobCommittedStatus committed = compactionCommittedStatus(Instant.parse("2022-09-24T09:24:30.001Z"));

        // When
        CompactionJobStatus status = jobStatusFromUpdates(started, finished, committed);

        // Then
        assertThat(status.getJobRuns())
                .extracting(ProcessRun::getTaskId, ProcessRun::getStartedStatus, ProcessRun::getFinishedStatus)
                .containsExactly(
                        tuple(DEFAULT_TASK_ID, started, finished));
        assertThat(status.isUnstartedOrInProgress()).isFalse();
    }
}
