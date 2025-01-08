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
import sleeper.core.tracker.job.ProcessRunTime;
import sleeper.core.tracker.job.RecordsProcessedSummary;
import sleeper.core.tracker.job.status.ProcessFailedStatus;
import sleeper.core.tracker.job.status.ProcessRun;
import sleeper.core.tracker.job.status.ProcessStatusUpdate;
import sleeper.core.tracker.job.status.TestProcessStatusUpdateRecords;

import java.time.Instant;
import java.util.Arrays;
import java.util.List;

import static sleeper.core.tracker.compaction.job.CompactionJobEventTestData.defaultCompactionJobCreatedEvent;
import static sleeper.core.tracker.job.RecordsProcessedSummaryTestHelper.summary;
import static sleeper.core.tracker.job.status.ProcessStatusUpdateTestHelper.defaultUpdateTime;
import static sleeper.core.tracker.job.status.TestProcessStatusUpdateRecords.records;

public class CompactionJobStatusTestData {
    private CompactionJobStatusTestData() {
    }

    public static CompactionJobStatus compactionJobCreated(Instant createdTime, ProcessRun... runsLatestFirst) {
        return compactionJobCreated(defaultCompactionJobCreatedEvent(), createdTime, runsLatestFirst);
    }

    public static CompactionJobStatus compactionJobCreated(CompactionJobCreatedEvent event, Instant createdTime, ProcessRun... runsLatestFirst) {
        return CompactionJobStatus.builder()
                .jobId(event.getJobId())
                .createdStatus(CompactionJobCreatedStatus.from(event, createdTime))
                .jobRunsLatestFirst(Arrays.asList(runsLatestFirst))
                .build();
    }

    public static ProcessRun startedCompactionRun(String taskId, Instant startTime) {
        return ProcessRun.started(taskId, compactionStartedStatus(startTime));
    }

    public static ProcessRun uncommittedCompactionRun(String taskId, RecordsProcessedSummary summary) {
        return ProcessRun.finished(taskId,
                compactionStartedStatus(summary.getStartTime()),
                compactionFinishedStatus(summary));
    }

    public static ProcessRun finishedCompactionRun(String taskId, RecordsProcessedSummary summary, Instant commitTime) {
        return ProcessRun.builder().taskId(taskId)
                .startedStatus(compactionStartedStatus(summary.getStartTime()))
                .finishedStatus(compactionFinishedStatus(summary))
                .statusUpdate(compactionCommittedStatus(commitTime))
                .build();
    }

    public static ProcessRun failedCompactionRun(String taskId, ProcessRunTime runTime, List<String> failureReasons) {
        return ProcessRun.finished(taskId,
                compactionStartedStatus(runTime.getStartTime()),
                compactionFailedStatus(runTime, failureReasons));
    }

    public static ProcessRun failedCompactionRun(String taskId, Instant startTime, Instant finishTime, Instant failureTime, List<String> failureReasons) {
        return ProcessRun.builder().taskId(taskId)
                .startedStatus(compactionStartedStatus(startTime))
                .finishedStatus(compactionFinishedStatus(summary(startTime, finishTime, 10L, 10L)))
                .statusUpdate(compactionFailedStatus(new ProcessRunTime(startTime, failureTime), failureReasons))
                .build();
    }

    public static CompactionJobStartedStatus compactionStartedStatus(Instant startTime) {
        return CompactionJobStartedStatus.startAndUpdateTime(startTime, defaultUpdateTime(startTime));
    }

    public static CompactionJobFinishedStatus compactionFinishedStatus(RecordsProcessedSummary summary) {
        return CompactionJobFinishedStatus.updateTimeAndSummary(defaultUpdateTime(summary.getFinishTime()), summary).build();
    }

    public static CompactionJobCommittedStatus compactionCommittedStatus(Instant committedTime) {
        return CompactionJobCommittedStatus.commitAndUpdateTime(committedTime, defaultUpdateTime(committedTime));
    }

    public static ProcessFailedStatus compactionFailedStatus(ProcessRunTime runTime, List<String> failureReasons) {
        return ProcessFailedStatus.timeAndReasons(defaultUpdateTime(runTime.getFinishTime()), runTime, failureReasons);
    }

    public static CompactionJobStatus jobStatusFromUpdates(ProcessStatusUpdate... updates) {
        return jobStatusFrom(records().fromUpdates(updates));
    }

    public static List<CompactionJobStatus> jobStatusListFromUpdates(
            TestProcessStatusUpdateRecords.TaskUpdates... updates) {
        return CompactionJobStatus.listFrom(records().fromUpdates(updates).stream());
    }

    public static CompactionJobStatus jobStatusFrom(TestProcessStatusUpdateRecords records) {
        List<CompactionJobStatus> built = CompactionJobStatus.listFrom(records.stream());
        if (built.size() != 1) {
            throw new IllegalStateException("Expected single status");
        }
        return built.get(0);
    }
}
