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

package sleeper.status.report.compactionjobstatus;

import org.junit.Test;
import sleeper.compaction.job.CompactionJob;
import sleeper.compaction.job.status.CompactionJobStatus;
import sleeper.core.partition.Partition;
import sleeper.status.report.compactionjob.CompactionJobStatusReporter;

import java.time.Instant;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

public class StatusReporterRangeQueryTest extends StatusReporterTest {

    @Test
    public void shouldReportCompactionJobStatusFinishedInRange() throws Exception {
        // Given
        Partition partition = dataHelper.singlePartition();
        CompactionJob job = dataHelper.singleFileCompaction(partition);
        Instant creationTime = Instant.parse("2022-09-22T13:33:12.001Z");
        Instant startedTime = Instant.parse("2022-09-22T13:34:12.001Z");
        Instant startedUpdateTime = Instant.parse("2022-09-22T13:39:12.001Z");
        Instant finishedTime = Instant.parse("2022-09-22T13:40:12.001Z");

        // When
        CompactionJobStatus status = jobFinished(job, creationTime, startedTime, startedUpdateTime, finishedTime);
        Instant startRange = Instant.parse("2022-09-22T00:00:00.001Z");
        Instant endRange = Instant.parse("2022-09-22T23:59:59.001Z");
        statusReporter.setRange(startRange, endRange);

        // Then
        assertThat(statusReporter.report(Collections.singletonList(status), CompactionJobStatusReporter.QueryType.RANGE))
                .isEqualTo(example("reports/compactionjobstatus/standard/range/standardJobFinishedInRange.txt")
                        .replace("$(startRange)", startRange.toString())
                        .replace("$(endRange)", endRange.toString())
                        .replace("$(jobId)", job.getId()));
    }

    @Test
    public void shouldReportSplittingCompactionJobStatusFinishedInRange() throws Exception {
        // Given
        CompactionJob job = dataHelper.singleFileSplittingCompaction("C", "A", "B");
        Instant creationTime = Instant.parse("2022-09-22T13:33:12.001Z");
        Instant startedTime = Instant.parse("2022-09-22T13:34:12.001Z");
        Instant startedUpdateTime = Instant.parse("2022-09-22T13:39:12.001Z");
        Instant finishedTime = Instant.parse("2022-09-22T13:40:12.001Z");

        // When
        CompactionJobStatus status = jobFinished(job, creationTime, startedTime, startedUpdateTime, finishedTime);
        Instant startRange = Instant.parse("2022-09-22T00:00:00.001Z");
        Instant endRange = Instant.parse("2022-09-22T23:59:59.001Z");
        statusReporter.setRange(startRange, endRange);

        // Then
        assertThat(statusReporter.report(Collections.singletonList(status), CompactionJobStatusReporter.QueryType.RANGE))
                .isEqualTo(example("reports/compactionjobstatus/standard/range/splittingJobFinishedInRange.txt")
                        .replace("$(startRange)", startRange.toString())
                        .replace("$(endRange)", endRange.toString())
                        .replace("$(jobId)", job.getId()));
    }

    @Test
    public void shouldNotReportCompactionJobStatusFinishedBeforeRange() throws Exception {
        // Given
        Partition partition = dataHelper.singlePartition();
        CompactionJob job = dataHelper.singleFileCompaction(partition);
        Instant creationTime = Instant.parse("2022-09-22T13:33:12.001Z");
        Instant startedTime = Instant.parse("2022-09-22T13:34:12.001Z");
        Instant startedUpdateTime = Instant.parse("2022-09-22T13:39:12.001Z");
        Instant finishedTime = Instant.parse("2022-09-22T13:40:12.001Z");

        // When
        CompactionJobStatus status = jobFinished(job, creationTime, startedTime, startedUpdateTime, finishedTime);
        Instant startRange = Instant.parse("2022-09-25T00:00:00.001Z");
        Instant endRange = Instant.parse("2022-09-25T23:59:59.001Z");
        statusReporter.setRange(startRange, endRange);

        // Then
        List<CompactionJobStatus> statusList = Stream.of(status).filter(statusReporter::isFinishedInRange).collect(Collectors.toList());
        assertThat(statusReporter.report(statusList, CompactionJobStatusReporter.QueryType.RANGE))
                .isEqualTo(example("reports/compactionjobstatus/standard/range/jobFinishedOutsideRange.txt")
                        .replace("$(startRange)", startRange.toString())
                        .replace("$(endRange)", endRange.toString()));
    }

    @Test
    public void shouldNotReportSplittingCompactionJobStatusFinishedBeforeRange() throws Exception {
        // Given
        CompactionJob job = dataHelper.singleFileSplittingCompaction("C", "A", "B");
        Instant creationTime = Instant.parse("2022-09-22T13:33:12.001Z");
        Instant startedTime = Instant.parse("2022-09-22T13:34:12.001Z");
        Instant startedUpdateTime = Instant.parse("2022-09-22T13:39:12.001Z");
        Instant finishedTime = Instant.parse("2022-09-22T13:40:12.001Z");

        // When
        CompactionJobStatus status = jobFinished(job, creationTime, startedTime, startedUpdateTime, finishedTime);
        Instant startRange = Instant.parse("2022-09-25T00:00:00.001Z");
        Instant endRange = Instant.parse("2022-09-25T23:59:59.001Z");
        statusReporter.setRange(startRange, endRange);

        // Then
        List<CompactionJobStatus> statusList = Stream.of(status).filter(statusReporter::isFinishedInRange).collect(Collectors.toList());
        assertThat(statusReporter.report(statusList, CompactionJobStatusReporter.QueryType.RANGE))
                .isEqualTo(example("reports/compactionjobstatus/standard/range/jobFinishedOutsideRange.txt")
                        .replace("$(startRange)", startRange.toString())
                        .replace("$(endRange)", endRange.toString()));
    }

    @Test
    public void shouldNotReportCompactionJobStatusFinishedAfterRange() throws Exception {
        // Given
        Partition partition = dataHelper.singlePartition();
        CompactionJob job = dataHelper.singleFileCompaction(partition);
        Instant creationTime = Instant.parse("2022-09-22T13:33:12.001Z");
        Instant startedTime = Instant.parse("2022-09-22T13:34:12.001Z");
        Instant startedUpdateTime = Instant.parse("2022-09-22T13:39:12.001Z");
        Instant finishedTime = Instant.parse("2022-09-22T13:40:12.001Z");

        // When
        CompactionJobStatus status = jobFinished(job, creationTime, startedTime, startedUpdateTime, finishedTime);
        Instant startRange = Instant.parse("2022-09-20T00:00:00.001Z");
        Instant endRange = Instant.parse("2022-09-20T23:59:59.001Z");
        statusReporter.setRange(startRange, endRange);

        // Then
        List<CompactionJobStatus> statusList = Stream.of(status).filter(statusReporter::isFinishedInRange).collect(Collectors.toList());
        assertThat(statusReporter.report(statusList, CompactionJobStatusReporter.QueryType.RANGE))
                .isEqualTo(example("reports/compactionjobstatus/standard/range/jobFinishedOutsideRange.txt")
                        .replace("$(startRange)", startRange.toString())
                        .replace("$(endRange)", endRange.toString()));
    }

    @Test
    public void shouldNotReportSplittingCompactionJobStatusFinishedAfterRange() throws Exception {
        // Given
        CompactionJob job = dataHelper.singleFileSplittingCompaction("C", "A", "B");
        Instant creationTime = Instant.parse("2022-09-22T13:33:12.001Z");
        Instant startedTime = Instant.parse("2022-09-22T13:34:12.001Z");
        Instant startedUpdateTime = Instant.parse("2022-09-22T13:39:12.001Z");
        Instant finishedTime = Instant.parse("2022-09-22T13:40:12.001Z");

        // When
        CompactionJobStatus status = jobFinished(job, creationTime, startedTime, startedUpdateTime, finishedTime);
        Instant startRange = Instant.parse("2022-09-20T00:00:00.001Z");
        Instant endRange = Instant.parse("2022-09-20T23:59:59.001Z");
        statusReporter.setRange(startRange, endRange);

        // Then
        List<CompactionJobStatus> statusList = Stream.of(status).filter(statusReporter::isFinishedInRange).collect(Collectors.toList());
        assertThat(statusReporter.report(statusList, CompactionJobStatusReporter.QueryType.RANGE))
                .isEqualTo(example("reports/compactionjobstatus/standard/range/jobFinishedOutsideRange.txt")
                        .replace("$(startRange)", startRange.toString())
                        .replace("$(endRange)", endRange.toString()));
    }
}
