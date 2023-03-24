/*
 * Copyright 2022-2023 Crown Copyright
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

import sleeper.compaction.job.status.CompactionJobStatus;
import sleeper.core.partition.Partition;
import sleeper.core.record.process.RecordsProcessed;
import sleeper.core.record.process.RecordsProcessedSummary;

import java.time.Instant;
import java.util.Arrays;
import java.util.Collections;

import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.compaction.job.CompactionJobStatusTestData.finishedCompactionRun;
import static sleeper.compaction.job.CompactionJobStatusTestData.jobCreated;
import static sleeper.compaction.job.CompactionJobStatusTestData.startedCompactionRun;
import static sleeper.core.record.process.status.TestProcessStatusUpdateRecords.DEFAULT_TASK_ID;

class CompactionJobStatusTest {

    private final CompactionJobTestDataHelper dataHelper = new CompactionJobTestDataHelper();

    @Test
    void shouldBuildCompactionJobCreatedFromJob() {
        // Given
        Partition partition = dataHelper.singlePartition();
        CompactionJob job = dataHelper.singleFileCompaction(partition);
        Instant updateTime = Instant.parse("2022-09-22T13:33:12.001Z");

        // When
        CompactionJobStatus status = jobCreated(job, updateTime);

        // Then
        assertThat(status).extracting("createUpdateTime", "partitionId", "inputFilesCount", "childPartitionIds", "splittingCompaction")
                .containsExactly(updateTime, "root", 1, Collections.emptyList(), false);
    }

    @Test
    void shouldBuildSplittingCompactionJobCreatedFromJob() {
        // Given
        CompactionJob job = dataHelper.singleFileSplittingCompaction("root", "left", "right");
        Instant updateTime = Instant.parse("2022-09-22T13:33:12.001Z");

        // When
        CompactionJobStatus status = jobCreated(job, updateTime);

        // Then
        assertThat(status).extracting("createUpdateTime", "partitionId", "inputFilesCount", "childPartitionIds", "splittingCompaction")
                .containsExactly(updateTime, "root", 1, Arrays.asList("left", "right"), true);
    }

    @Test
    void shouldReportCompactionJobNotStarted() {
        // Given
        CompactionJob job = dataHelper.singleFileCompaction();
        Instant updateTime = Instant.parse("2022-09-22T13:33:12.001Z");

        // When
        CompactionJobStatus status = jobCreated(job, updateTime);

        // Then
        assertThat(status).extracting(CompactionJobStatus::isStarted, CompactionJobStatus::isFinished)
                .containsExactly(false, false);
    }

    @Test
    void shouldBuildCompactionJobStarted() {
        // Given
        CompactionJob job = dataHelper.singleFileCompaction();

        // When
        CompactionJobStatus status = jobCreated(job, Instant.parse("2022-09-22T13:33:12.001Z"),
                startedCompactionRun(DEFAULT_TASK_ID, Instant.parse("2022-09-22T13:33:30.001Z")));

        // Then
        assertThat(status).extracting(CompactionJobStatus::isStarted, CompactionJobStatus::isFinished)
                .containsExactly(true, false);
    }

    @Test
    void shouldBuildCompactionJobFinished() {
        // Given
        CompactionJob job = dataHelper.singleFileCompaction();
        Instant startTime = Instant.parse("2022-09-22T13:33:10.001Z");
        Instant finishTime = Instant.parse("2022-09-22T13:34:10.001Z");
        RecordsProcessedSummary summary = new RecordsProcessedSummary(
                new RecordsProcessed(450L, 300L), startTime, finishTime);

        // When
        CompactionJobStatus status = jobCreated(job, Instant.parse("2022-09-22T13:33:00.001Z"),
                finishedCompactionRun(DEFAULT_TASK_ID, summary));

        // Then
        assertThat(status).extracting(CompactionJobStatus::isStarted, CompactionJobStatus::isFinished)
                .containsExactly(true, true);
    }
}
