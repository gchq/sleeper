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

import java.time.Instant;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static net.javacrumbs.jsonunit.assertj.JsonAssertions.assertThatJson;
import static org.assertj.core.api.Assertions.assertThat;

public class StatusReporterAllQueryTest extends StatusReporterTest {
    @Test
    public void shouldReportCompactionJobStatusStandardJobsOnly() throws Exception {
        // Given
        Partition partition1 = dataHelper.singlePartition();
        CompactionJob job1 = dataHelper.singleFileCompaction(partition1);
        Instant creationTime1 = Instant.parse("2022-09-17T13:33:12.001Z");
        Partition partition2 = dataHelper.singlePartition();
        CompactionJob job2 = dataHelper.singleFileCompaction(partition2);
        Instant creationTime2 = Instant.parse("2022-09-18T13:33:12.001Z");
        Instant startedTime2 = Instant.parse("2022-09-18T13:34:12.001Z");
        Instant startedUpdateTime2 = Instant.parse("2022-09-18T13:39:12.001Z");
        Partition partition3 = dataHelper.singlePartition();
        CompactionJob job3 = dataHelper.singleFileCompaction(partition3);
        Instant creationTime3 = Instant.parse("2022-09-19T13:33:12.001Z");
        Instant startedTime3 = Instant.parse("2022-09-19T13:34:12.001Z");
        Instant startedUpdateTime3 = Instant.parse("2022-09-19T13:39:12.001Z");
        Instant finishedTime3 = Instant.parse("2022-09-19T13:40:12.001Z");

        // When
        CompactionJobStatus status1 = jobCreated(job1, creationTime1);
        CompactionJobStatus status2 = jobStarted(job2, DEFAULT_TASK_ID, creationTime2, startedTime2, startedUpdateTime2);
        CompactionJobStatus status3 = jobFinished(job3, DEFAULT_TASK_ID, creationTime3, startedTime3, startedUpdateTime3, finishedTime3);

        // Then
        List<CompactionJobStatus> statusList = Arrays.asList(status1, status2, status3);
        assertThat(verboseReportString(StandardCompactionJobStatusReporter::new, statusList, CompactionJobStatusReporter.QueryType.ALL))
                .isEqualTo(example("reports/compactionjobstatus/standard/all/allStandardJobs.txt")
                        .replace("$(jobId1)", job1.getId())
                        .replace("$(jobId2)", job2.getId())
                        .replace("$(jobId3)", job3.getId()));
        assertThatJson(verboseReportString(JsonCompactionJobStatusReporter::new, statusList, CompactionJobStatusReporter.QueryType.ALL))
                .isEqualTo(example("reports/compactionjobstatus/json/allStandardJobs.json")
                        .replace("$(jobId1)", job1.getId())
                        .replace("$(jobId2)", job2.getId())
                        .replace("$(jobId3)", job3.getId()));

    }

    @Test
    public void shouldReportCompactionJobStatusSplittingJobsOnly() throws Exception {
        // Given
        CompactionJob job1 = dataHelper.singleFileSplittingCompaction("C", "A", "B");
        Instant creationTime1 = Instant.parse("2022-09-20T13:33:12.001Z");
        CompactionJob job2 = dataHelper.singleFileSplittingCompaction("F", "D", "E");
        Instant creationTime2 = Instant.parse("2022-09-21T13:33:12.001Z");
        Instant startedTime2 = Instant.parse("2022-09-21T13:34:12.001Z");
        Instant startedUpdateTime2 = Instant.parse("2022-09-21T13:39:12.001Z");
        CompactionJob job3 = dataHelper.singleFileSplittingCompaction("I", "G", "H");
        Instant creationTime3 = Instant.parse("2022-09-22T13:33:12.001Z");
        Instant startedTime3 = Instant.parse("2022-09-22T13:34:12.001Z");
        Instant startedUpdateTime3 = Instant.parse("2022-09-22T13:39:12.001Z");
        Instant finishedTime3 = Instant.parse("2022-09-22T13:40:12.001Z");

        // When
        CompactionJobStatus status1 = jobCreated(job1, creationTime1);
        CompactionJobStatus status2 = jobStarted(job2, DEFAULT_TASK_ID, creationTime2, startedTime2, startedUpdateTime2);
        CompactionJobStatus status3 = jobFinished(job3, DEFAULT_TASK_ID, creationTime3, startedTime3, startedUpdateTime3, finishedTime3);

        // Then
        List<CompactionJobStatus> statusList = Arrays.asList(status1, status2, status3);
        assertThat(verboseReportString(StandardCompactionJobStatusReporter::new, statusList, CompactionJobStatusReporter.QueryType.ALL))
                .isEqualTo(example("reports/compactionjobstatus/standard/all/allSplittingJobs.txt")
                        .replace("$(jobId1)", job1.getId())
                        .replace("$(jobId2)", job2.getId())
                        .replace("$(jobId3)", job3.getId()));
        assertThatJson(verboseReportString(JsonCompactionJobStatusReporter::new, statusList, CompactionJobStatusReporter.QueryType.ALL))
                .isEqualTo(example("reports/compactionjobstatus/json/allSplittingJobs.json")
                        .replace("$(jobId1)", job1.getId())
                        .replace("$(jobId2)", job2.getId())
                        .replace("$(jobId3)", job3.getId()));
    }

    @Test
    public void shouldReportCompactionJobStatusMixedJobs() throws Exception {
        // Given
        Partition partition1 = dataHelper.singlePartition();
        CompactionJob job1 = dataHelper.singleFileCompaction(partition1);
        Instant creationTime1 = Instant.parse("2022-09-17T13:33:12.001Z");
        Partition partition2 = dataHelper.singlePartition();
        CompactionJob job2 = dataHelper.singleFileCompaction(partition2);
        Instant creationTime2 = Instant.parse("2022-09-18T13:33:12.001Z");
        Instant startedTime2 = Instant.parse("2022-09-18T13:34:12.001Z");
        Instant startedUpdateTime2 = Instant.parse("2022-09-18T13:39:12.001Z");
        Partition partition3 = dataHelper.singlePartition();
        CompactionJob job3 = dataHelper.singleFileCompaction(partition3);
        Instant creationTime3 = Instant.parse("2022-09-19T13:33:12.001Z");
        Instant startedTime3 = Instant.parse("2022-09-19T13:34:12.001Z");
        Instant startedUpdateTime3 = Instant.parse("2022-09-19T13:39:12.001Z");
        Instant finishedTime3 = Instant.parse("2022-09-19T13:40:12.001Z");

        CompactionJob job4 = dataHelper.singleFileSplittingCompaction("C", "A", "B");
        Instant creationTime4 = Instant.parse("2022-09-20T13:33:12.001Z");
        CompactionJob job5 = dataHelper.singleFileSplittingCompaction("F", "D", "E");
        Instant creationTime5 = Instant.parse("2022-09-21T13:33:12.001Z");
        Instant startedTime5 = Instant.parse("2022-09-21T13:34:12.001Z");
        Instant startedUpdateTime5 = Instant.parse("2022-09-21T13:39:12.001Z");
        CompactionJob job6 = dataHelper.singleFileSplittingCompaction("I", "G", "H");
        Instant creationTime6 = Instant.parse("2022-09-22T13:33:12.001Z");
        Instant startedTime6 = Instant.parse("2022-09-22T13:34:12.001Z");
        Instant startedUpdateTime6 = Instant.parse("2022-09-22T13:39:12.001Z");
        Instant finishedTime6 = Instant.parse("2022-09-22T13:40:12.001Z");

        // When
        CompactionJobStatus status1 = jobCreated(job1, creationTime1);
        CompactionJobStatus status2 = jobStarted(job2, DEFAULT_TASK_ID, creationTime2, startedTime2, startedUpdateTime2);
        CompactionJobStatus status3 = jobFinished(job3, DEFAULT_TASK_ID, creationTime3, startedTime3, startedUpdateTime3, finishedTime3);
        CompactionJobStatus status4 = jobCreated(job4, creationTime4);
        CompactionJobStatus status5 = jobStarted(job5, DEFAULT_TASK_ID, creationTime5, startedTime5, startedUpdateTime5);
        CompactionJobStatus status6 = jobFinished(job6, DEFAULT_TASK_ID, creationTime6, startedTime6, startedUpdateTime6, finishedTime6);

        // Then
        List<CompactionJobStatus> statusList = Arrays.asList(status1, status2, status3, status4, status5, status6);
        assertThat(verboseReportString(StandardCompactionJobStatusReporter::new, statusList, CompactionJobStatusReporter.QueryType.ALL))
                .isEqualTo(example("reports/compactionjobstatus/standard/all/mixedJobs.txt")
                        .replace("$(jobId1)", job1.getId())
                        .replace("$(jobId2)", job2.getId())
                        .replace("$(jobId3)", job3.getId())
                        .replace("$(jobId4)", job4.getId())
                        .replace("$(jobId5)", job5.getId())
                        .replace("$(jobId6)", job6.getId()));
        assertThatJson(verboseReportString(JsonCompactionJobStatusReporter::new, statusList, CompactionJobStatusReporter.QueryType.ALL))
                .isEqualTo(example("reports/compactionjobstatus/json/mixedJobs.json")
                        .replace("$(jobId1)", job1.getId())
                        .replace("$(jobId2)", job2.getId())
                        .replace("$(jobId3)", job3.getId())
                        .replace("$(jobId4)", job4.getId())
                        .replace("$(jobId5)", job5.getId())
                        .replace("$(jobId6)", job6.getId()));
    }

    @Test
    public void shouldReportNoCompactionJobStatusIfNoJobsExist() throws Exception {
        // Given/When
        List<CompactionJobStatus> statusList = Collections.emptyList();

        //Then
        assertThat(verboseReportString(StandardCompactionJobStatusReporter::new, statusList, CompactionJobStatusReporter.QueryType.ALL))
                .isEqualTo(example("reports/compactionjobstatus/standard/all/noJobs.txt"));
        assertThatJson(verboseReportString(JsonCompactionJobStatusReporter::new, statusList, CompactionJobStatusReporter.QueryType.ALL))
                .isEqualTo(example("reports/compactionjobstatus/json/noJobs.json"));
    }

}
