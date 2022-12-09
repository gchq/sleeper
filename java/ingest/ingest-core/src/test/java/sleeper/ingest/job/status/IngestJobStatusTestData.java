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
package sleeper.ingest.job.status;

import sleeper.core.record.process.RecordsProcessedSummary;
import sleeper.core.record.process.status.ProcessFinishedStatus;
import sleeper.core.record.process.status.ProcessRun;
import sleeper.core.record.process.status.ProcessRuns;
import sleeper.ingest.job.IngestJob;

import java.time.Instant;
import java.time.temporal.ChronoField;
import java.util.Arrays;

public class IngestJobStatusTestData {

    private IngestJobStatusTestData() {
    }

    public static IngestJobStatus jobStatus(IngestJob job, ProcessRun... runs) {
        return IngestJobStatus.builder()
                .jobId(job.getId())
                .jobRuns(ProcessRuns.latestFirst(Arrays.asList(runs)))
                .build();
    }

    public static IngestJobStatus startedIngestJob(IngestJob job, String taskId, Instant startTime) {
        return jobStatus(job, startedIngestRun(job, taskId, startTime));
    }

    public static IngestJobStatus finishedIngestJob(IngestJob job, String taskId, RecordsProcessedSummary summary) {
        return jobStatus(job, finishedIngestRun(job, taskId, summary));
    }

    public static ProcessRun startedIngestRun(IngestJob job, String taskId, Instant startTime) {
        return startedIngestRun(job, taskId, startTime, startTime.with(ChronoField.MILLI_OF_SECOND, 123));
    }

    public static ProcessRun startedIngestRun(IngestJob job, String taskId, Instant startTime, Instant updateTime) {
        return ProcessRun.started(taskId,
                IngestJobStartedStatus.updateAndStartTime(job, updateTime, startTime));
    }

    public static ProcessRun finishedIngestRun(
            IngestJob job, String taskId, RecordsProcessedSummary summary) {
        return finishedIngestRun(job, taskId,
                summary.getStartTime().with(ChronoField.MILLI_OF_SECOND, 123),
                summary.getFinishTime().with(ChronoField.MILLI_OF_SECOND, 123),
                summary);
    }

    public static ProcessRun finishedIngestRun(
            IngestJob job, String taskId, Instant startUpdateTime, Instant finishUpdateTime, RecordsProcessedSummary summary) {

        return ProcessRun.finished(taskId,
                IngestJobStartedStatus.updateAndStartTime(job, startUpdateTime, summary.getStartTime()),
                ProcessFinishedStatus.updateTimeAndSummary(finishUpdateTime, summary));
    }
}
