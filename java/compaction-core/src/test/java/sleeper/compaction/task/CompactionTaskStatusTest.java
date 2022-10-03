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

package sleeper.compaction.task;

import org.junit.Test;
import sleeper.compaction.job.CompactionJob;
import sleeper.compaction.job.CompactionJobRecordsProcessed;
import sleeper.compaction.job.CompactionJobSummary;
import sleeper.compaction.job.CompactionJobTestDataHelper;
import sleeper.compaction.job.status.CompactionJobCreatedStatus;
import sleeper.compaction.job.status.CompactionJobFinishedStatus;
import sleeper.compaction.job.status.CompactionJobStartedStatus;
import sleeper.compaction.job.status.CompactionJobStatus;
import sleeper.core.partition.Partition;

import java.time.Instant;
import java.util.Arrays;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

public class CompactionTaskStatusTest {
    private final CompactionJobTestDataHelper dataHelper = new CompactionJobTestDataHelper();

    @Test
    public void shouldCreateCompactionTaskStatus() {
        // Given
        Instant taskStartedTime = Instant.parse("2022-09-22T12:00:14.000Z");

        // When
        CompactionTaskStatus status = CompactionTaskStatus.started(taskStartedTime.toEpochMilli());

        // Then
        assertThat(status).extracting("startedStatus.startTime")
                .isEqualTo(taskStartedTime);
    }

    @Test
    public void shouldCreateCompactionTaskStatusFromFinishedStandardJobList() {
        // Given
        Instant taskStartedTime = Instant.parse("2022-09-22T12:00:14.000Z");
        Instant jobFinishTime3 = Instant.parse("2022-09-22T16:00:14.000Z");

        Partition partition1 = dataHelper.singlePartition();
        CompactionJob job1 = dataHelper.singleFileCompaction(partition1);
        Partition partition2 = dataHelper.singlePartition();
        CompactionJob job2 = dataHelper.singleFileCompaction(partition2);
        Partition partition3 = dataHelper.singlePartition();
        CompactionJob job3 = dataHelper.singleFileCompaction(partition3);

        // When
        CompactionTaskStatus status = CompactionTaskStatus.started(taskStartedTime.toEpochMilli());
        status.finished(createJobStatuses(job1, job2, job3), jobFinishTime3.toEpochMilli());

        // Then
        assertThat(status).extracting("startedStatus.startTime", "finishedStatus.finishTime",
                        "finishedStatus.totalJobs", "finishedStatus.totalRuntime", "finishedStatus.totalRecordsRead",
                        "finishedStatus.totalRecordsWritten", "finishedStatus.recordsReadPerSecond", "finishedStatus.recordsWrittenPerSecond")
                .containsExactly(taskStartedTime, jobFinishTime3, 3, 14400.0, 14400L, 7200L, 1.0, 0.5);
    }

    @Test
    public void shouldCreateCompactionTaskStatusFromFinishedSplittingJobList() {
        // Given
        Instant taskStartedTime = Instant.parse("2022-09-22T12:00:14.000Z");
        Instant jobFinishTime3 = Instant.parse("2022-09-22T16:00:14.000Z");

        Partition partition1 = dataHelper.singlePartition();
        CompactionJob job1 = dataHelper.singleFileSplittingCompaction(partition1.getId(), "A", "B");
        Partition partition2 = dataHelper.singlePartition();
        CompactionJob job2 = dataHelper.singleFileSplittingCompaction(partition2.getId(), "D", "E");
        Partition partition3 = dataHelper.singlePartition();
        CompactionJob job3 = dataHelper.singleFileSplittingCompaction(partition3.getId(), "G", "H");

        // When
        CompactionTaskStatus status = CompactionTaskStatus.started(taskStartedTime.toEpochMilli());
        status.finished(createJobStatuses(job1, job2, job3), jobFinishTime3.toEpochMilli());

        // Then
        assertThat(status).extracting("startedStatus.startTime", "finishedStatus.finishTime",
                        "finishedStatus.totalJobs", "finishedStatus.totalRuntime", "finishedStatus.totalRecordsRead",
                        "finishedStatus.totalRecordsWritten", "finishedStatus.recordsReadPerSecond", "finishedStatus.recordsWrittenPerSecond")
                .containsExactly(taskStartedTime, jobFinishTime3, 3, 14400.0, 14400L, 7200L, 1.0, 0.5);
    }

    private List<CompactionJobStatus> createJobStatuses(CompactionJob job1, CompactionJob job2, CompactionJob job3) {
        Instant jobCreationTime1 = Instant.parse("2022-09-22T14:00:00.000Z");
        Instant jobStartedTime1 = Instant.parse("2022-09-22T14:00:02.000Z");
        Instant jobStartedUpdateTime1 = Instant.parse("2022-09-22T14:00:04.000Z");
        Instant jobFinishTime1 = Instant.parse("2022-09-22T14:00:14.000Z");

        Instant jobCreationTime2 = Instant.parse("2022-09-22T15:00:00.000Z");
        Instant jobStartedTime2 = Instant.parse("2022-09-22T15:00:02.000Z");
        Instant jobStartedUpdateTime2 = Instant.parse("2022-09-22T15:00:04.000Z");
        Instant jobFinishTime2 = Instant.parse("2022-09-22T15:00:14.000Z");

        Instant jobCreationTime3 = Instant.parse("2022-09-22T16:00:00.000Z");
        Instant jobStartedTime3 = Instant.parse("2022-09-22T16:00:02.000Z");
        Instant jobStartedUpdateTime3 = Instant.parse("2022-09-22T16:00:04.000Z");
        Instant jobFinishTime3 = Instant.parse("2022-09-22T16:00:14.000Z");

        CompactionJobSummary summary1 = new CompactionJobSummary(
                new CompactionJobRecordsProcessed(4800L, 2400L), jobStartedUpdateTime1, jobFinishTime1);
        CompactionJobSummary summary2 = new CompactionJobSummary(
                new CompactionJobRecordsProcessed(4800L, 2400L), jobStartedUpdateTime2, jobFinishTime2);
        CompactionJobSummary summary3 = new CompactionJobSummary(
                new CompactionJobRecordsProcessed(4800L, 2400L), jobStartedUpdateTime3, jobFinishTime3);
        
        CompactionJobStatus status1 = CompactionJobStatus.builder().jobId(job1.getId())
                .createdStatus(CompactionJobCreatedStatus.from(job1, jobCreationTime1))
                .startedStatus(CompactionJobStartedStatus.updateAndStartTime(
                        jobStartedUpdateTime1, jobStartedTime1))
                .finishedStatus(CompactionJobFinishedStatus.updateTimeAndSummary(jobStartedUpdateTime1, summary1))
                .build();
        CompactionJobStatus status2 = CompactionJobStatus.builder().jobId(job2.getId())
                .createdStatus(CompactionJobCreatedStatus.from(job2, jobCreationTime2))
                .startedStatus(CompactionJobStartedStatus.updateAndStartTime(
                        jobStartedUpdateTime2, jobStartedTime2))
                .finishedStatus(CompactionJobFinishedStatus.updateTimeAndSummary(jobStartedUpdateTime2, summary2))
                .build();
        CompactionJobStatus status3 = CompactionJobStatus.builder().jobId(job3.getId())
                .createdStatus(CompactionJobCreatedStatus.from(job3, jobCreationTime3))
                .startedStatus(CompactionJobStartedStatus.updateAndStartTime(
                        jobStartedUpdateTime3, jobStartedTime3))
                .finishedStatus(CompactionJobFinishedStatus.updateTimeAndSummary(jobStartedUpdateTime3, summary3))
                .build();
        return Arrays.asList(status1, status2, status3);
    }
}
