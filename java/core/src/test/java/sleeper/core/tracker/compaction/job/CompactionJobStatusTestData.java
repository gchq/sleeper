/*
 * Copyright 2022-2024 Crown Copyright
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

package sleeper.core.tracker.compaction.job;

import sleeper.core.tracker.compaction.job.query.CompactionJobCommittedStatus;
import sleeper.core.tracker.compaction.job.query.CompactionJobCreatedStatus;
import sleeper.core.tracker.compaction.job.query.CompactionJobFinishedStatus;
import sleeper.core.tracker.compaction.job.query.CompactionJobStartedStatus;
import sleeper.core.tracker.compaction.job.query.CompactionJobStatus;
import sleeper.core.tracker.compaction.job.update.CompactionJobCreatedEvent;
import sleeper.core.tracker.job.run.JobRun;
import sleeper.core.tracker.job.run.JobRunSummary;
import sleeper.core.tracker.job.run.JobRunTime;
import sleeper.core.tracker.job.status.JobRunFailedStatus;
import sleeper.core.tracker.job.status.JobStatusUpdate;
import sleeper.core.tracker.job.status.TestJobStatusUpdateRecords;

import java.time.Instant;
import java.util.Arrays;
import java.util.List;

import static sleeper.core.tracker.compaction.job.CompactionJobEventTestData.defaultCompactionJobCreatedEvent;
import static sleeper.core.tracker.job.run.JobRunSummaryTestHelper.summary;
import static sleeper.core.tracker.job.status.JobStatusUpdateTestHelper.defaultUpdateTime;
import static sleeper.core.tracker.job.status.TestJobStatusUpdateRecords.records;

public class CompactionJobStatusTestData {
    private CompactionJobStatusTestData() {
    }

    public static CompactionJobStatus compactionJobCreated(Instant createdTime, JobRun... runsLatestFirst) {
        return compactionJobCreated(defaultCompactionJobCreatedEvent(), createdTime, runsLatestFirst);
    }

    public static CompactionJobStatus compactionJobCreated(CompactionJobCreatedEvent event, Instant createdTime, JobRun... runsLatestFirst) {
        return CompactionJobStatus.builder()
                .jobId(event.getJobId())
                .createdStatus(CompactionJobCreatedStatus.from(event, createdTime))
                .jobRunsLatestFirst(Arrays.asList(runsLatestFirst))
                .build();
    }

    public static JobRun startedCompactionRun(String taskId, Instant startTime) {
        return JobRun.started(taskId, compactionStartedStatus(startTime));
    }

    public static JobRun uncommittedCompactionRun(String taskId, JobRunSummary summary) {
        return JobRun.finished(taskId,
                compactionStartedStatus(summary.getStartTime()),
                compactionFinishedStatus(summary));
    }

    public static JobRun finishedCompactionRun(String taskId, JobRunSummary summary, Instant commitTime) {
        return JobRun.builder().taskId(taskId)
                .startedStatus(compactionStartedStatus(summary.getStartTime()))
                .finishedStatus(compactionFinishedStatus(summary))
                .statusUpdate(compactionCommittedStatus(commitTime))
                .build();
    }

    public static JobRun.Builder finishedCompactionRunBuilder(String taskId, JobRunSummary summary) {
        return JobRun.builder().taskId(taskId)
                .startedStatus(compactionStartedStatus(summary.getStartTime()))
                .finishedStatus(compactionFinishedStatus(summary));
    }

    public static JobRun failedCompactionRun(String taskId, JobRunTime runTime, List<String> failureReasons) {
        return JobRun.finished(taskId,
                compactionStartedStatus(runTime.getStartTime()),
                compactionFailedStatus(runTime, failureReasons));
    }

    public static JobRun failedCompactionRun(String taskId, Instant startTime, Instant finishTime, Instant failureTime, List<String> failureReasons) {
        return JobRun.builder().taskId(taskId)
                .startedStatus(compactionStartedStatus(startTime))
                .finishedStatus(compactionFinishedStatus(summary(startTime, finishTime, 10L, 10L)))
                .statusUpdate(compactionFailedStatus(new JobRunTime(startTime, failureTime), failureReasons))
                .build();
    }

    public static CompactionJobStartedStatus compactionStartedStatus(Instant startTime) {
        return CompactionJobStartedStatus.startAndUpdateTime(startTime, defaultUpdateTime(startTime));
    }

    public static CompactionJobFinishedStatus compactionFinishedStatus(JobRunSummary summary) {
        return CompactionJobFinishedStatus.builder()
                .updateTime(defaultUpdateTime(summary.getFinishTime()))
                .finishTime(summary.getFinishTime())
                .recordsProcessed(summary.getRecordsProcessed())
                .timeInProcess(summary.getTimeInProcess())
                .build();
    }

    public static CompactionJobCommittedStatus compactionCommittedStatus(Instant committedTime) {
        return CompactionJobCommittedStatus.commitAndUpdateTime(committedTime, defaultUpdateTime(committedTime));
    }

    public static JobRunFailedStatus compactionFailedStatus(JobRunTime runTime, List<String> failureReasons) {
        return JobRunFailedStatus.builder()
                .updateTime(defaultUpdateTime(runTime.getFinishTime()))
                .failureTime(runTime.getFinishTime())
                .failureReasons(failureReasons)
                .timeInProcess(runTime.getTimeInProcess())
                .build();
    }

    public static CompactionJobStatus jobStatusFromUpdates(JobStatusUpdate... updates) {
        return jobStatusFrom(records().fromUpdates(updates));
    }

    public static List<CompactionJobStatus> jobStatusListFromUpdates(
            TestJobStatusUpdateRecords.TaskUpdates... updates) {
        return CompactionJobStatus.listFrom(records().fromUpdates(updates).stream());
    }

    public static CompactionJobStatus jobStatusFrom(TestJobStatusUpdateRecords records) {
        List<CompactionJobStatus> built = CompactionJobStatus.listFrom(records.stream());
        if (built.size() != 1) {
            throw new IllegalStateException("Expected single status");
        }
        return built.get(0);
    }
}
