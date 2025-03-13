/*
 * Copyright 2022-2025 Crown Copyright
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
package sleeper.core.statestore.testutils;

import sleeper.core.statestore.FileReference;
import sleeper.core.tracker.ingest.job.InMemoryIngestJobTracker;
import sleeper.core.tracker.ingest.job.update.IngestJobEvent;
import sleeper.core.tracker.ingest.job.update.IngestJobStartedEvent;
import sleeper.core.tracker.job.run.JobRunSummary;

import java.time.Instant;

import static sleeper.core.tracker.ingest.job.IngestJobEventTestData.ingestJobFinishedEventBuilder;
import static sleeper.core.tracker.ingest.job.IngestJobEventTestData.ingestJobStartedEventBuilder;
import static sleeper.core.tracker.job.run.JobRunSummaryTestHelper.summary;

public abstract class InMemoryTransactionLogStateStoreIngestTrackerTestBase extends InMemoryTransactionLogStateStoreTestBase {

    protected static final String DEFAULT_TASK_ID = "test-task";
    protected static final Instant DEFAULT_START_TIME = Instant.parse("2025-01-14T11:41:00Z");
    protected static final Instant DEFAULT_FINISH_TIME = Instant.parse("2025-01-14T11:42:00Z");
    protected static final Instant DEFAULT_COMMIT_TIME = Instant.parse("2025-01-14T11:42:10Z");

    protected final InMemoryIngestJobTracker tracker = new InMemoryIngestJobTracker();

    protected IngestJobEvent trackJobRun(String jobId, String jobRunId, int inputFileCount, FileReference outputFile) {
        IngestJobStartedEvent job = trackedJob(jobId, jobRunId, inputFileCount);
        tracker.jobStarted(job);
        tracker.jobFinished(ingestJobFinishedEventBuilder(job, defaultSummary(outputFile.getNumberOfRecords()))
                .numFilesWrittenByJob(1).committedBySeparateFileUpdates(true).build());
        return job;
    }

    private IngestJobStartedEvent trackedJob(String jobId, String jobRunId, int fileCount) {
        return ingestJobStartedEventBuilder(DEFAULT_START_TIME)
                .jobId(jobId)
                .jobRunId(jobRunId)
                .taskId(DEFAULT_TASK_ID)
                .tableId(tableId)
                .fileCount(fileCount)
                .build();
    }

    protected JobRunSummary defaultSummary(long numberOfRecords) {
        return summary(DEFAULT_START_TIME, DEFAULT_FINISH_TIME, numberOfRecords, numberOfRecords);
    }

}
