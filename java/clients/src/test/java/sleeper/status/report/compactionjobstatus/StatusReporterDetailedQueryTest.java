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
import sleeper.status.report.compactionjob.CompactionJobStatusReporter.QueryType;
import sleeper.status.report.compactionjob.StandardCompactionJobStatusReporter;

import java.time.Instant;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

public class StatusReporterDetailedQueryTest extends StatusReporterTest {
    @Test
    public void shouldReportCompactionJobStatusCreated() throws Exception {
        // Given
        Partition partition = dataHelper.singlePartition();
        CompactionJob job = dataHelper.singleFileCompaction(partition);
        Instant creationTime = Instant.parse("2022-09-22T13:33:12.001Z");

        // When
        CompactionJobStatus status = jobCreated(job, creationTime);

        // Then
        assertThat(verboseReportString(StandardCompactionJobStatusReporter::new, Collections.singletonList(status), QueryType.DETAILED))
                .isEqualTo(example("reports/compactionjobstatus/standard/detailed/standardJobCreated.txt")
                        .replace("$(jobId)", job.getId()));
    }

    @Test
    public void shouldReportCompactionJobStatusStarted() throws Exception {
        // Given
        Partition partition = dataHelper.singlePartition();
        CompactionJob job = dataHelper.singleFileCompaction(partition);
        Instant creationTime = Instant.parse("2022-09-22T13:33:12.001Z");
        Instant startedTime = Instant.parse("2022-09-22T13:34:12.001Z");
        Instant startedUpdateTime = Instant.parse("2022-09-22T13:39:12.001Z");

        // When
        CompactionJobStatus status = jobStarted(job, creationTime, startedTime, startedUpdateTime);

        // Then
        assertThat(verboseReportString(StandardCompactionJobStatusReporter::new, Collections.singletonList(status), QueryType.DETAILED))
                .isEqualTo(example("reports/compactionjobstatus/standard/detailed/standardJobStarted.txt")
                        .replace("$(jobId)", job.getId()));
    }

    @Test
    public void shouldReportCompactionJobStatusFinished() throws Exception {
        // Given
        Partition partition = dataHelper.singlePartition();
        CompactionJob job = dataHelper.singleFileCompaction(partition);
        Instant creationTime = Instant.parse("2022-09-22T13:33:12.001Z");
        Instant startedTime = Instant.parse("2022-09-22T13:34:12.001Z");
        Instant startedUpdateTime = Instant.parse("2022-09-22T13:39:12.001Z");
        Instant finishedTime = Instant.parse("2022-09-22T13:40:12.001Z");

        // When
        CompactionJobStatus status = jobFinished(job, creationTime, startedTime, startedUpdateTime, finishedTime);

        // Then
        assertThat(verboseReportString(StandardCompactionJobStatusReporter::new, Collections.singletonList(status), QueryType.DETAILED))
                .isEqualTo(example("reports/compactionjobstatus/standard/detailed/standardJobFinished.txt")
                        .replace("$(jobId)", job.getId()));
    }

    @Test
    public void shouldReportMultipleCompactionJobStatus() throws Exception {
        // Given
        CompactionJob job1 = dataHelper.singleFileSplittingCompaction("C", "A", "B");
        Instant creationTime1 = Instant.parse("2022-09-22T13:33:12.001Z");
        CompactionJob job2 = dataHelper.singleFileSplittingCompaction("F", "D", "E");
        Instant creationTime2 = Instant.parse("2022-09-23T13:33:12.001Z");
        Instant startedTime2 = Instant.parse("2022-09-23T13:34:00.001Z");
        Instant startedUpdateTime2 = Instant.parse("2022-09-23T13:36:00.001Z");
        CompactionJob job3 = dataHelper.singleFileSplittingCompaction("I", "G", "H");
        Instant creationTime3 = Instant.parse("2022-09-24T13:33:12.001Z");
        Instant startedTime3 = Instant.parse("2022-09-24T13:34:12.001Z");
        Instant startedUpdateTime3 = Instant.parse("2022-09-24T13:39:12.001Z");
        Instant finishedTime3 = Instant.parse("2022-09-24T13:40:12.001Z");

        // When
        CompactionJobStatus status1 = jobCreated(job1, creationTime1);
        CompactionJobStatus status2 = jobStarted(job2, creationTime2, startedTime2, startedUpdateTime2);
        CompactionJobStatus status3 = jobFinished(job3, creationTime3, startedTime3, startedUpdateTime3, finishedTime3);
        List<CompactionJobStatus> statusList = Arrays.asList(status1, status2, status3);

        // Then
        assertThat(verboseReportString(StandardCompactionJobStatusReporter::new, statusList, QueryType.DETAILED))
                .isEqualTo(example("reports/compactionjobstatus/standard/detailed/multipleJobs.txt")
                        .replace("$(jobId1)", job1.getId())
                        .replace("$(jobId2)", job2.getId())
                        .replace("$(jobId3)", job3.getId()));
    }


}
