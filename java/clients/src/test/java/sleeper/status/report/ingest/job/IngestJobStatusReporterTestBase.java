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

package sleeper.status.report.ingest.job;

import sleeper.core.record.process.RecordsProcessed;
import sleeper.core.record.process.RecordsProcessedSummary;
import sleeper.core.record.process.status.ProcessFinishedStatus;
import sleeper.core.record.process.status.ProcessRun;
import sleeper.core.record.process.status.ProcessStartedStatus;
import sleeper.ingest.job.IngestJob;
import sleeper.ingest.job.status.IngestJobStatus;

import java.time.Instant;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static sleeper.status.report.StatusReporterTestHelper.job;
import static sleeper.status.report.StatusReporterTestHelper.task;

public abstract class IngestJobStatusReporterTestBase {
    List<IngestJobStatus> mixedJobStatuses() {
        IngestJob job1 = createJob(2, 1);
        Instant updateTime1 = Instant.parse("2022-09-18T13:34:12.001Z");
        Instant startTime1 = Instant.parse("2022-09-18T13:34:12.001Z");

        IngestJob job2 = createJob(3, 2);
        Instant updateTime2 = Instant.parse("2022-09-19T13:34:12.001Z");
        Instant startTime2 = Instant.parse("2022-09-19T13:34:12.001Z");
        Instant finishTime2 = Instant.parse("2022-09-19T13:35:12.001Z");

        IngestJob job3 = createJob(5, 3);
        Instant updateTime3 = Instant.parse("2022-09-21T13:34:12.001Z");
        Instant startTime3 = Instant.parse("2022-09-21T13:34:12.001Z");

        IngestJob job4 = createJob(6, 4);
        Instant updateTime4 = Instant.parse("2022-09-22T13:34:12.001Z");
        Instant startTime4 = Instant.parse("2022-09-22T13:34:12.001Z");
        Instant finishTime4 = Instant.parse("2022-09-22T13:35:12.001Z");

        IngestJobStatus status1 = jobStarted(job1, task(1), startTime1, updateTime1);
        IngestJobStatus status2 = jobFinished(job2, task(1), startTime2, updateTime2, finishTime2);
        IngestJobStatus status3 = jobStarted(job3, task(2), startTime3, updateTime3);
        IngestJobStatus status4 = jobFinished(job4, task(2), startTime4, updateTime4, finishTime4);
        return Arrays.asList(status1, status2, status3, status4);
    }

    public IngestJob createJob(int jobNum, int inputFileCount) {
        return IngestJob.builder()
                .id(job(jobNum))
                .files(IntStream.range(1, inputFileCount + 1)
                        .mapToObj(f -> String.format("test%1d.parquet", f))
                        .collect(Collectors.toList()))
                .build();
    }

    public IngestJobStatus jobStarted(IngestJob job, String taskId, Instant startTime, Instant startUpdateTime) {
        return IngestJobStatus.from(job)
                .jobRun(ProcessRun.started(taskId,
                        ProcessStartedStatus.updateAndStartTime(startUpdateTime, startTime)))
                .build();
    }

    public IngestJobStatus jobFinished(IngestJob job, String taskId, Instant startTime, Instant startUpdateTime, Instant finishTime) {
        return jobFinished(job, taskId, startTime, startUpdateTime, finishTime, 600L, 300L);
    }

    public IngestJobStatus jobFinished(IngestJob job, String taskId, Instant startTime, Instant startUpdateTime, Instant finishTime,
                                       long linesRead, long linesWritten) {
        RecordsProcessedSummary summary = new RecordsProcessedSummary(
                new RecordsProcessed(linesRead, linesWritten), startUpdateTime, finishTime);
        return IngestJobStatus.from(job)
                .jobRun(ProcessRun.finished(taskId,
                        ProcessStartedStatus.updateAndStartTime(startUpdateTime, startTime),
                        ProcessFinishedStatus.updateTimeAndSummary(finishTime, summary)))
                .build();
    }


}
