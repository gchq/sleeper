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
package sleeper.core.tracker.compaction.job;

import sleeper.core.tracker.compaction.job.update.CompactionJobCommittedEvent;
import sleeper.core.tracker.compaction.job.update.CompactionJobCreatedEvent;
import sleeper.core.tracker.compaction.job.update.CompactionJobFailedEvent;
import sleeper.core.tracker.compaction.job.update.CompactionJobFinishedEvent;
import sleeper.core.tracker.compaction.job.update.CompactionJobStartedEvent;
import sleeper.core.tracker.job.run.JobRunSummary;

import java.time.Instant;
import java.util.UUID;

/**
 * This class is used to create test data compaction job events.
 */
public class CompactionJobEventTestData {

    private CompactionJobEventTestData() {
    }

    /**
     * This method builds a CompactionJobCreatedEvent using the default settings including tableId 'test-table'.
     *
     * @return CompactionJobCreatedEvent built using the default settings including tableId 'test-table'.
     */
    public static CompactionJobCreatedEvent defaultCompactionJobCreatedEvent() {
        return defaultCompactionJobCreatedEventForTable("test-table");
    }

    /**
     * This method builds a CompactionJobCreatedEvent using the provided tableId and default settings.
     *
     * @param  tableId String of the tableId to be used in the built event.
     * @return         CompactionJobCreatedEvent built using the provided tableId and default settings.
     */
    public static CompactionJobCreatedEvent defaultCompactionJobCreatedEventForTable(String tableId) {
        return CompactionJobCreatedEvent.builder()
                .jobId(UUID.randomUUID().toString())
                .tableId(tableId)
                .partitionId("root")
                .inputFilesCount(1)
                .build();
    }

    /**
     * This method builds a CompactionJobStartedEvent with the provident Created Event and commit time.
     *
     * @param  created   CompactionJobCreatedEvent that contains the job id and table id to build the Started event
     *                   with.
     * @param  startTime Instant start time of the job.
     * @return           CompactionJobStartedEvent Builder containing the given values.
     */
    public static CompactionJobStartedEvent.Builder compactionStartedEventBuilder(CompactionJobCreatedEvent created, Instant startTime) {
        return CompactionJobStartedEvent.builder().jobId(created.getJobId()).tableId(created.getTableId()).startTime(startTime).jobRunId(UUID.randomUUID().toString());
    }

    /**
     * This method builds a CompactionJobFinishedEvent with the provident started Event and JobRunSummary.
     *
     * @param  started CompactionJobStartedEvent that contains the job, table, task and jobRun Ids to build the Finished
     *                 event with.
     * @param  summary JobRunSummary to be used as the summary for the finished event.
     * @return         CompactionJobFinishedEvent built containing the given values.
     */
    public static CompactionJobFinishedEvent compactionFinishedEvent(CompactionJobStartedEvent started, JobRunSummary summary) {
        return CompactionJobFinishedEvent.builder().jobId(started.getJobId()).tableId(started.getTableId()).taskId(started.getTaskId()).jobRunId(started.getJobRunId()).summary(summary).build();
    }

    /**
     * This method builds a CompactionJobFinishedEvent with the provident Created Event and JobRunSummary.
     *
     * @param  created CompactionJobCreatedEvent that contains the job id and table id to build the Finished event with.
     * @param  summary JobRunSummary to be used as the summary for the finished event.
     * @return         CompactionJobFinishedEvent Builde containing the given valuesr.
     */
    public static CompactionJobFinishedEvent.Builder compactionFinishedEventBuilder(CompactionJobCreatedEvent created, JobRunSummary summary) {
        return CompactionJobFinishedEvent.builder().jobId(created.getJobId()).tableId(created.getTableId()).summary(summary);
    }

    /**
     * This method builds a CompactionJobCommittedEvent with the provident Started Event and commit time.
     *
     * @param  started    CompactionJobStartedEvent that contains the job, table, task and jobRun Ids to build the
     *                    Committed event with.
     * @param  commitTime Instant time the job was committed at.
     * @return            CompactionJobCommittedEvent built containing the given values.
     */
    public static CompactionJobCommittedEvent compactionCommittedEvent(CompactionJobStartedEvent started, Instant commitTime) {
        return CompactionJobCommittedEvent.builder().jobId(started.getJobId()).tableId(started.getTableId()).taskId(started.getTaskId()).jobRunId(started.getJobRunId()).commitTime(commitTime).build();
    }

    /**
     * This method builds a CompactionJobCommittedEvent with the provident Created Event and commit time.
     *
     * @param  created    CompactionJobCreatedEvent that contains the job id and table id to build the Committed event
     *                    with.
     * @param  commitTime Instant time the job was committed at.
     * @return            CompactionJobCommittedEvent Builder containing the given values.
     */
    public static CompactionJobCommittedEvent.Builder compactionCommittedEventBuilder(CompactionJobCreatedEvent created, Instant commitTime) {
        return CompactionJobCommittedEvent.builder().jobId(created.getJobId()).tableId(created.getTableId()).commitTime(commitTime);
    }

    /**
     * This method builds a CompactionJobFailedEvent with the provident Started Event and failure time.
     *
     * @param  started     CompactionJobStartedEvent hat contains the job, table, task and jobRun Ids to build the
     *                     Failed event with.
     * @param  failureTime Instant the time the job failed at.
     * @return             CompactionJobFailedEvent Builder containing the given values.
     */
    public static CompactionJobFailedEvent.Builder compactionFailedEventBuilder(CompactionJobStartedEvent started, Instant failureTime) {
        return CompactionJobFailedEvent.builder().jobId(started.getJobId()).tableId(started.getTableId()).taskId(started.getTaskId()).jobRunId(started.getJobRunId()).failureTime(failureTime);
    }

    /**
     * This method builds a CompactionJobFailedEvent with the provident Created Event and failure time.
     *
     * @param  created     CompactionJobCreatedEvent that contains the job id and table id to build the Failed event
     *                     with.
     * @param  failureTime Instant the time the job failed at.
     * @return             CompactionJobFailedEvent Builder containing the given values.
     */
    public static CompactionJobFailedEvent.Builder compactionFailedEventBuilder(CompactionJobCreatedEvent created, Instant failureTime) {
        return CompactionJobFailedEvent.builder().jobId(created.getJobId()).tableId(created.getTableId()).failureTime(failureTime);
    }

}
