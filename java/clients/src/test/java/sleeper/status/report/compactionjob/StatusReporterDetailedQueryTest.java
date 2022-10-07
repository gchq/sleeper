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

package sleeper.status.report.compactionjob;

import org.junit.Test;
import sleeper.compaction.job.CompactionJob;
import sleeper.compaction.job.status.CompactionJobStatus;
import sleeper.core.partition.Partition;
import sleeper.status.report.compactionjob.CompactionJobStatusReporter.QueryType;

import java.time.Instant;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static net.javacrumbs.jsonunit.assertj.JsonAssertions.assertThatJson;
import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.ClientTestUtils.example;

public class StatusReporterDetailedQueryTest extends StatusReporterTestBase {
    @Test
    public void shouldReportCompactionJobStatusCreated() throws Exception {
        // Given
        Partition partition = dataHelper.singlePartition();
        CompactionJob job = dataHelper.singleFileCompaction(partition);
        Instant creationTime = Instant.parse("2022-09-22T13:33:12.001Z");

        // When
        CompactionJobStatus status = jobCreated(job, creationTime);

        // Then
        List<CompactionJobStatus> statusList = Collections.singletonList(status);
        assertThat(verboseReportString(StandardCompactionJobStatusReporter::new, statusList, QueryType.DETAILED))
                .isEqualTo(example("reports/compactionjobstatus/standard/detailed/standardJobCreated.txt")
                        .replace("$(jobId)", job.getId()));
        assertThatJson(verboseReportString(JsonCompactionJobStatusReporter::new, statusList, QueryType.DETAILED))
                .isEqualTo(example("reports/compactionjobstatus/json/standardJobCreated.json")
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
        CompactionJobStatus status = jobStarted(job, DEFAULT_TASK_ID, creationTime, startedTime, startedUpdateTime);

        // Then
        List<CompactionJobStatus> statusList = Collections.singletonList(status);
        assertThat(verboseReportString(StandardCompactionJobStatusReporter::new, statusList, QueryType.DETAILED))
                .isEqualTo(example("reports/compactionjobstatus/standard/detailed/standardJobStarted.txt")
                        .replace("$(jobId)", job.getId()));
        assertThatJson(verboseReportString(JsonCompactionJobStatusReporter::new, statusList, QueryType.DETAILED))
                .isEqualTo(example("reports/compactionjobstatus/json/standardJobStarted.json")
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
        CompactionJobStatus status = jobFinished(job, DEFAULT_TASK_ID, creationTime, startedTime, startedUpdateTime, finishedTime);

        // Then
        List<CompactionJobStatus> statusList = Collections.singletonList(status);
        assertThat(verboseReportString(StandardCompactionJobStatusReporter::new, statusList, QueryType.DETAILED))
                .isEqualTo(example("reports/compactionjobstatus/standard/detailed/standardJobFinished.txt")
                        .replace("$(jobId)", job.getId()));
        assertThatJson(verboseReportString(JsonCompactionJobStatusReporter::new, statusList, QueryType.DETAILED))
                .isEqualTo(example("reports/compactionjobstatus/json/standardJobFinished.json")
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
        CompactionJobStatus status2 = jobStarted(job2, DEFAULT_TASK_ID, creationTime2, startedTime2, startedUpdateTime2);
        CompactionJobStatus status3 = jobFinished(job3, DEFAULT_TASK_ID, creationTime3, startedTime3, startedUpdateTime3, finishedTime3);

        // Then
        List<CompactionJobStatus> statusList = Arrays.asList(status1, status2, status3);
        assertThat(verboseReportString(StandardCompactionJobStatusReporter::new, statusList, QueryType.DETAILED))
                .isEqualTo(example("reports/compactionjobstatus/standard/detailed/multipleJobs.txt")
                        .replace("$(jobId1)", job1.getId())
                        .replace("$(jobId2)", job2.getId())
                        .replace("$(jobId3)", job3.getId()));
        assertThatJson(verboseReportString(JsonCompactionJobStatusReporter::new, statusList, QueryType.DETAILED))
                .isEqualTo(example("reports/compactionjobstatus/json/multipleJobs.json")
                        .replace("$(jobId1)", job1.getId())
                        .replace("$(jobId2)", job2.getId())
                        .replace("$(jobId3)", job3.getId()));
    }

    @Test
    public void shouldReportNoCompactionJobStatusIfNoMatchingId() throws Exception {
        // Given
        String searchingJobId = "non-existent-job";
        CompactionJob job = dataHelper.singleFileSplittingCompaction("C", "A", "B");
        Instant creationTime = Instant.parse("2022-09-22T13:33:12.001Z");

        // When
        CompactionJobStatus status = jobCreated(job, creationTime);

        // Then
        List<CompactionJobStatus> statusList = Stream.of(status)
                .filter(j -> j.getJobId().equals(searchingJobId))
                .collect(Collectors.toList());
        assertThat(verboseReportString(StandardCompactionJobStatusReporter::new, statusList, QueryType.DETAILED))
                .isEqualTo(example("reports/compactionjobstatus/standard/detailed/noJobFound.txt"));
        assertThatJson(verboseReportString(JsonCompactionJobStatusReporter::new, statusList, QueryType.DETAILED))
                .isEqualTo(example("reports/compactionjobstatus/json/noJobs.json"));

    }
}
