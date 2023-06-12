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
package sleeper.ingest.job.status;

import sleeper.core.record.process.RecordsProcessedSummary;
import sleeper.core.record.process.status.ProcessFinishedStatus;
import sleeper.core.record.process.status.ProcessRun;
import sleeper.core.record.process.status.ProcessRuns;
import sleeper.core.record.process.status.TestProcessStatusUpdateRecords;
import sleeper.ingest.job.IngestJob;

import java.time.Instant;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static sleeper.core.record.process.status.TestRunStatusUpdates.defaultUpdateTime;

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

    public static IngestJobStatus finishedIngestJobWithValidation(IngestJob job, String taskId, Instant validationTime, RecordsProcessedSummary summary) {
        return jobStatus(job, acceptedRunWhichFinished(job, taskId, validationTime, summary));
    }

    public static ProcessRun acceptedRunWhichStarted(
            IngestJob job, String taskId, Instant validationTime, Instant startTime) {
        return ProcessRun.builder()
                .taskId(taskId)
                .startedStatus(IngestJobAcceptedStatus.from(validationTime,
                        defaultUpdateTime(validationTime)))
                .statusUpdate(
                        IngestJobStartedStatus.withStartOfRun(false)
                                .inputFileCount(job.getFiles().size())
                                .startTime(startTime).updateTime(defaultUpdateTime(startTime)).build())
                .build();
    }

    public static ProcessRun acceptedRunWhichFinished(
            IngestJob job, String taskId, Instant validationTime, RecordsProcessedSummary summary) {
        return ProcessRun.builder()
                .taskId(taskId)
                .startedStatus(IngestJobAcceptedStatus.from(validationTime,
                        defaultUpdateTime(validationTime)))
                .statusUpdate(
                        IngestJobStartedStatus.withStartOfRun(false)
                                .inputFileCount(job.getFiles().size())
                                .startTime(summary.getStartTime()).updateTime(defaultUpdateTime(summary.getStartTime())).build())
                .finishedStatus(ProcessFinishedStatus
                        .updateTimeAndSummary(defaultUpdateTime(summary.getFinishTime()), summary))
                .build();
    }

    public static ProcessRun acceptedRun(Instant validationTime) {
        return ProcessRun.builder()
                .startedStatus(IngestJobAcceptedStatus.from(validationTime,
                        defaultUpdateTime(validationTime)))
                .build();
    }

    public static ProcessRun rejectedRun(Instant validationTime, String reason) {
        return rejectedRun(validationTime, List.of(reason));
    }

    public static ProcessRun rejectedRun(Instant validationTime, List<String> reasons) {
        return ProcessRun.builder()
                .startedStatus(IngestJobRejectedStatus.builder()
                        .validationTime(validationTime)
                        .updateTime(defaultUpdateTime(validationTime))
                        .reasons(reasons).build())
                .build();
    }

    public static ProcessRun startedIngestRun(IngestJob job, String taskId, Instant startTime) {
        return ProcessRun.started(taskId,
                IngestJobStartedStatus.startAndUpdateTime(job, startTime, defaultUpdateTime(startTime)));
    }

    public static ProcessRun finishedIngestRun(
            IngestJob job, String taskId, RecordsProcessedSummary summary) {
        return ProcessRun.finished(taskId,
                IngestJobStartedStatus.startAndUpdateTime(job, summary.getStartTime(), defaultUpdateTime(summary.getStartTime())),
                ProcessFinishedStatus.updateTimeAndSummary(defaultUpdateTime(summary.getFinishTime()), summary));
    }

    public static List<IngestJobStatus> jobStatusListFrom(TestProcessStatusUpdateRecords records) {
        return IngestJobStatus.streamFrom(records.stream()).collect(Collectors.toList());
    }

    public static IngestJobStatus singleJobStatusFrom(TestProcessStatusUpdateRecords records) {
        List<IngestJobStatus> jobs = jobStatusListFrom(records);
        if (jobs.size() != 1) {
            throw new IllegalStateException("Expected single job, found " + jobs.size());
        }
        return jobs.get(0);
    }
}
