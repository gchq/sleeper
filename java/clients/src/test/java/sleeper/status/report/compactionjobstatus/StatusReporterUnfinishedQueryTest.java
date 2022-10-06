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
import sleeper.status.report.compactionjob.JsonCompactionJobStatusReporter;
import sleeper.status.report.compactionjob.StandardCompactionJobStatusReporter;

import java.time.Instant;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static net.javacrumbs.jsonunit.assertj.JsonAssertions.assertThatJson;
import static org.assertj.core.api.Assertions.assertThat;

public class StatusReporterUnfinishedQueryTest extends StatusReporterTest {

    @Test
    public void shouldReportCompactionJobStatusWithCreatedJobs() throws Exception {
        // Given
        Partition partition = dataHelper.singlePartition();
        CompactionJob job = dataHelper.singleFileCompaction(partition);
        Instant creationTime = Instant.parse("2022-09-22T13:33:12.001Z");

        // When
        CompactionJobStatus status = jobCreated(job, creationTime);

        // Then
        List<CompactionJobStatus> statusList = Stream.of(status).filter(s -> !s.isFinished()).collect(Collectors.toList());
        assertThat(verboseReportString(StandardCompactionJobStatusReporter::new, statusList, QueryType.UNFINISHED))
                .isEqualTo(example("reports/compactionjobstatus/standard/unfinished/standardJobCreated.txt")
                        .replace("$(jobId)", job.getId()));
        assertThatJson(verboseReportString(JsonCompactionJobStatusReporter::new, statusList, QueryType.UNFINISHED))
                .isEqualTo(example("reports/compactionjobstatus/json/standardJobCreated.json")
                        .replace("$(jobId)", job.getId()));
    }

    @Test
    public void shouldReportSplittingCompactionJobStatusWithCreatedJobs() throws Exception {
        // Given
        CompactionJob job = dataHelper.singleFileSplittingCompaction("C", "A", "B");
        Instant creationTime = Instant.parse("2022-09-22T13:33:12.001Z");

        // When
        CompactionJobStatus status = jobCreated(job, creationTime);

        // Then
        List<CompactionJobStatus> statusList = Stream.of(status).filter(s -> !s.isFinished()).collect(Collectors.toList());
        assertThat(verboseReportString(StandardCompactionJobStatusReporter::new, statusList, QueryType.UNFINISHED))
                .isEqualTo(example("reports/compactionjobstatus/standard/unfinished/splittingJobCreated.txt")
                        .replace("$(jobId)", job.getId()));
        assertThatJson(verboseReportString(JsonCompactionJobStatusReporter::new, statusList, QueryType.UNFINISHED))
                .isEqualTo(example("reports/compactionjobstatus/json/splittingJobCreated.json")
                        .replace("$(jobId)", job.getId()));
    }

    @Test
    public void shouldReportCompactionJobStatusWithStartedJobs() throws Exception {
        // Given
        Partition partition = dataHelper.singlePartition();
        CompactionJob job = dataHelper.singleFileCompaction(partition);
        Instant creationTime = Instant.parse("2022-09-22T13:33:12.001Z");
        Instant startedTime = Instant.parse("2022-09-22T13:34:12.001Z");
        Instant startedUpdateTime = Instant.parse("2022-09-22T13:39:12.001Z");

        // When
        CompactionJobStatus status = jobStarted(job, DEFAULT_TASK_ID, creationTime, startedTime, startedUpdateTime);

        // Then
        List<CompactionJobStatus> statusList = Stream.of(status).filter(s -> !s.isFinished()).collect(Collectors.toList());
        assertThat(verboseReportString(StandardCompactionJobStatusReporter::new, statusList, QueryType.UNFINISHED))
                .isEqualTo(example("reports/compactionjobstatus/standard/unfinished/standardJobStarted.txt")
                        .replace("$(jobId)", job.getId()));
        assertThatJson(verboseReportString(JsonCompactionJobStatusReporter::new, statusList, QueryType.UNFINISHED))
                .isEqualTo(example("reports/compactionjobstatus/json/standardJobStarted.json")
                        .replace("$(jobId)", job.getId()));
    }

    @Test
    public void shouldReportSplittingCompactionJobStatusWithStartedJobs() throws Exception {
        // Given
        CompactionJob job = dataHelper.singleFileSplittingCompaction("C", "A", "B");
        Instant creationTime = Instant.parse("2022-09-22T13:33:12.001Z");
        Instant startedTime = Instant.parse("2022-09-22T13:34:12.001Z");
        Instant startedUpdateTime = Instant.parse("2022-09-22T13:39:12.001Z");

        // When
        CompactionJobStatus status = jobStarted(job, DEFAULT_TASK_ID, creationTime, startedTime, startedUpdateTime);


        // Then
        List<CompactionJobStatus> statusList = Stream.of(status).filter(s -> !s.isFinished()).collect(Collectors.toList());
        assertThat(verboseReportString(StandardCompactionJobStatusReporter::new, statusList, QueryType.UNFINISHED))
                .isEqualTo(example("reports/compactionjobstatus/standard/unfinished/splittingJobStarted.txt")
                        .replace("$(jobId)", job.getId()));
        assertThatJson(verboseReportString(JsonCompactionJobStatusReporter::new, statusList, QueryType.UNFINISHED))
                .isEqualTo(example("reports/compactionjobstatus/json/splittingJobStarted.json")
                        .replace("$(jobId)", job.getId()));
    }

    @Test
    public void shouldNotReportAnyCompactionJobStatusWithOnlyFinishedJobs() throws Exception {
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
        List<CompactionJobStatus> statusList = Stream.of(status).filter(s -> !s.isFinished()).collect(Collectors.toList());
        assertThat(verboseReportString(StandardCompactionJobStatusReporter::new, statusList, QueryType.UNFINISHED))
                .isEqualTo(example("reports/compactionjobstatus/standard/unfinished/standardJobFinished.txt"));
        assertThatJson(verboseReportString(JsonCompactionJobStatusReporter::new, statusList, QueryType.UNFINISHED))
                .isEqualTo(example("reports/compactionjobstatus/json/noJobs.json")
                        .replace("$(jobId)", job.getId()));
    }

    @Test
    public void shouldNotReportAnySplittingCompactionJobStatusWithOnlyFinishedJobs() throws Exception {
        // Given
        CompactionJob job = dataHelper.singleFileSplittingCompaction("C", "A", "B");
        Instant creationTime = Instant.parse("2022-09-22T13:33:12.001Z");
        Instant startedTime = Instant.parse("2022-09-22T13:34:12.001Z");
        Instant startedUpdateTime = Instant.parse("2022-09-22T13:39:12.001Z");
        Instant finishedTime = Instant.parse("2022-09-22T13:40:12.001Z");

        // When
        CompactionJobStatus status = jobFinished(job, DEFAULT_TASK_ID, creationTime, startedTime, startedUpdateTime, finishedTime);

        // Then
        List<CompactionJobStatus> statusList = Stream.of(status).filter(s -> !s.isFinished()).collect(Collectors.toList());
        assertThat(verboseReportString(StandardCompactionJobStatusReporter::new, statusList, QueryType.UNFINISHED))
                .isEqualTo(example("reports/compactionjobstatus/standard/unfinished/splittingJobFinished.txt")
                        .replace("$(jobId)", job.getId()));
        assertThatJson(verboseReportString(JsonCompactionJobStatusReporter::new, statusList, QueryType.UNFINISHED))
                .isEqualTo(example("reports/compactionjobstatus/json/noJobs.json")
                        .replace("$(jobId)", job.getId()));
    }
}
