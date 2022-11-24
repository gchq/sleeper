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
import sleeper.core.partition.Partition;
import sleeper.core.record.process.RecordsProcessed;
import sleeper.core.record.process.RecordsProcessedSummary;
import sleeper.core.record.process.status.ProcessFinishedStatus;
import sleeper.core.record.process.status.ProcessRun;
import sleeper.core.record.process.status.ProcessStartedStatus;

import java.time.Instant;
import java.util.Arrays;
import java.util.Collections;

import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.compaction.job.CompactionJobTestDataHelper.DEFAULT_TASK_ID;

public class CompactionJobStatusTest {

    private final CompactionJobTestDataHelper dataHelper = new CompactionJobTestDataHelper();

    @Test
    public void shouldBuildCompactionJobCreatedFromJob() {
        // Given
        Partition partition = dataHelper.singlePartition();
        CompactionJob job = dataHelper.singleFileCompaction(partition);
        Instant updateTime = Instant.parse("2022-09-22T13:33:12.001Z");

        // When
        CompactionJobStatus status = CompactionJobStatus.created(job, updateTime);

        // Then
        assertThat(status).extracting("createUpdateTime", "partitionId", "inputFilesCount", "childPartitionIds", "splittingCompaction")
                .containsExactly(updateTime, "root", 1, Collections.emptyList(), false);
    }

    @Test
    public void shouldBuildSplittingCompactionJobCreatedFromJob() {
        // Given
        CompactionJob job = dataHelper.singleFileSplittingCompaction("root", "left", "right");
        Instant updateTime = Instant.parse("2022-09-22T13:33:12.001Z");

        // When
        CompactionJobStatus status = CompactionJobStatus.created(job, updateTime);

        // Then
        assertThat(status).extracting("createUpdateTime", "partitionId", "inputFilesCount", "childPartitionIds", "splittingCompaction")
                .containsExactly(updateTime, "root", 1, Arrays.asList("left", "right"), true);
    }

    @Test
    public void shouldReportCompactionJobNotStarted() {
        // Given
        CompactionJob job = dataHelper.singleFileCompaction();
        Instant updateTime = Instant.parse("2022-09-22T13:33:12.001Z");

        // When
        CompactionJobStatus status = CompactionJobStatus.created(job, updateTime);

        // Then
        assertThat(status).extracting(CompactionJobStatus::isStarted, CompactionJobStatus::isFinished)
                .containsExactly(false, false);
    }

    @Test
    public void shouldBuildCompactionJobStarted() {
        // Given
        CompactionJob job = dataHelper.singleFileCompaction();
        Instant updateTime = Instant.parse("2022-09-22T13:33:20.001Z");
        Instant startTime = Instant.parse("2022-09-22T13:33:30.001Z");

        // When
        CompactionJobStatus status = CompactionJobStatus.builder().jobId(job.getId())
                .createdStatus(CompactionJobCreatedStatus.from(job, Instant.parse("2022-09-22T13:33:12.001Z")))
                .singleJobRun(ProcessRun.started(DEFAULT_TASK_ID,
                        ProcessStartedStatus.updateAndStartTime(updateTime, startTime)))
                .build();

        // Then
        assertThat(status).extracting(CompactionJobStatus::isStarted, CompactionJobStatus::isFinished)
                .containsExactly(true, false);
    }

    @Test
    public void shouldBuildCompactionJobFinished() {
        // Given
        CompactionJob job = dataHelper.singleFileCompaction();
        Instant updateTime = Instant.parse("2022-09-22T13:34:00.001Z");
        Instant startTime = Instant.parse("2022-09-22T13:33:10.001Z");
        Instant finishTime = Instant.parse("2022-09-22T13:34:10.001Z");
        RecordsProcessedSummary summary = new RecordsProcessedSummary(
                new RecordsProcessed(450L, 300L), startTime, finishTime);

        // When
        CompactionJobStatus status = CompactionJobStatus.builder().jobId(job.getId())
                .createdStatus(CompactionJobCreatedStatus.from(job, Instant.parse("2022-09-22T13:33:00.001Z")))
                .singleJobRun(ProcessRun.finished(DEFAULT_TASK_ID,
                        ProcessStartedStatus.updateAndStartTime(
                                Instant.parse("2022-09-22T13:33:09.001Z"), startTime),
                        ProcessFinishedStatus.updateTimeAndSummary(updateTime, summary)))
                .build();

        // Then
        assertThat(status).extracting(CompactionJobStatus::isStarted, CompactionJobStatus::isFinished)
                .containsExactly(true, true);
    }
}
