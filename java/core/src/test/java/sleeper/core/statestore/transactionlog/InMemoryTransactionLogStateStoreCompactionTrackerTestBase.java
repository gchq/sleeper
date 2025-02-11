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
package sleeper.core.statestore.transactionlog;

import sleeper.core.statestore.FileReference;
import sleeper.core.statestore.ReplaceFileReferencesRequest;
import sleeper.core.tracker.compaction.job.InMemoryCompactionJobTracker;
import sleeper.core.tracker.compaction.job.query.CompactionJobStatus;
import sleeper.core.tracker.compaction.job.update.CompactionJobCreatedEvent;
import sleeper.core.tracker.job.run.JobRun;
import sleeper.core.tracker.job.run.JobRunSummary;
import sleeper.core.tracker.job.run.RecordsProcessed;

import java.time.Instant;
import java.util.List;

import static sleeper.core.tracker.compaction.job.CompactionJobEventTestData.compactionFinishedEventBuilder;
import static sleeper.core.tracker.compaction.job.CompactionJobEventTestData.compactionStartedEventBuilder;
import static sleeper.core.tracker.compaction.job.CompactionJobStatusTestData.compactionFailedStatus;
import static sleeper.core.tracker.compaction.job.CompactionJobStatusTestData.compactionFinishedStatus;
import static sleeper.core.tracker.compaction.job.CompactionJobStatusTestData.compactionJobCreated;
import static sleeper.core.tracker.compaction.job.CompactionJobStatusTestData.compactionStartedStatus;
import static sleeper.core.tracker.compaction.job.CompactionJobStatusTestData.finishedCompactionRun;
import static sleeper.core.tracker.job.run.JobRunSummaryTestHelper.summary;
import static sleeper.core.tracker.job.run.JobRunTestData.jobRunOnTask;

public abstract class InMemoryTransactionLogStateStoreCompactionTrackerTestBase extends InMemoryTransactionLogStateStoreTestBase {

    protected static final String DEFAULT_TASK_ID = "test-task";
    protected static final Instant DEFAULT_CREATE_TIME = Instant.parse("2025-01-14T11:40:00Z");
    protected static final Instant DEFAULT_START_TIME = Instant.parse("2025-01-14T11:41:00Z");
    protected static final Instant DEFAULT_FINISH_TIME = Instant.parse("2025-01-14T11:42:00Z");
    protected static final Instant DEFAULT_COMMIT_TIME = Instant.parse("2025-01-14T11:42:10Z");

    protected final InMemoryCompactionJobTracker tracker = new InMemoryCompactionJobTracker();

    protected CompactionJobCreatedEvent trackJobCreated(String jobId, String partitionId, int inputFilesCount) {
        CompactionJobCreatedEvent job = trackedJob(jobId, partitionId, inputFilesCount);
        tracker.jobCreated(job, DEFAULT_CREATE_TIME);
        return job;
    }

    protected void trackJobRun(CompactionJobCreatedEvent job, String jobRunId) {
        tracker.jobStarted(compactionStartedEventBuilder(job, DEFAULT_START_TIME).taskId(DEFAULT_TASK_ID).jobRunId(jobRunId).build());
        tracker.jobFinished(compactionFinishedEventBuilder(job, defaultSummary(100)).taskId(DEFAULT_TASK_ID).jobRunId(jobRunId).build());
    }

    protected CompactionJobCreatedEvent trackedJob(String jobId, String partitionId, int inputFilesCount) {
        return CompactionJobCreatedEvent.builder()
                .jobId(jobId)
                .tableId(tableId)
                .partitionId(partitionId)
                .inputFilesCount(inputFilesCount)
                .build();
    }

    protected JobRunSummary defaultSummary(long numberOfRecords) {
        return summary(DEFAULT_START_TIME, DEFAULT_FINISH_TIME, numberOfRecords, numberOfRecords);
    }

    protected ReplaceFileReferencesRequest.Builder replaceJobFileReferencesBuilder(String jobId, List<String> inputFiles, FileReference newReference) {
        return ReplaceFileReferencesRequest.builder()
                .jobId(jobId)
                .inputFiles(inputFiles)
                .newReference(newReference)
                .taskId(DEFAULT_TASK_ID);
    }

    protected CompactionJobStatus defaultStatus(CompactionJobCreatedEvent job, JobRun... runs) {
        return compactionJobCreated(job, DEFAULT_CREATE_TIME, runs);
    }

    protected JobRun defaultCommittedRun(int numberOfRecords) {
        return finishedCompactionRun(DEFAULT_TASK_ID, defaultSummary(numberOfRecords), DEFAULT_COMMIT_TIME);
    }

    protected JobRun defaultFailedCommitRun(int numberOfRecords, List<String> reasons) {
        return jobRunOnTask(DEFAULT_TASK_ID,
                compactionStartedStatus(DEFAULT_START_TIME),
                compactionFinishedStatus(DEFAULT_FINISH_TIME, new RecordsProcessed(numberOfRecords, numberOfRecords)),
                compactionFailedStatus(DEFAULT_COMMIT_TIME, reasons));
    }
}
