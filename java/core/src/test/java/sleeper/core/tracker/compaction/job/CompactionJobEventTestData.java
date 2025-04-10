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
     * This method build a CompactionJobCreatedEvent using the default settings including tableId 'test-table'.
     *
     * @return CompactionJobCreatedEvent.
     */
    public static CompactionJobCreatedEvent defaultCompactionJobCreatedEvent() {
        return defaultCompactionJobCreatedEventForTable("test-table");
    }

    /**
     * This method build a CompactionJobCreatedEvent using the provided tableId and default settings.
     *
     * @param  tableId String.
     * @return         CompactionJobCreatedEvent.
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
     * @param  created   CompactionJobCreatedEvent.
     * @param  startTime Instant.
     * @return           CompactionJobStartedEvent Builder.
     */
    public static CompactionJobStartedEvent.Builder compactionStartedEventBuilder(CompactionJobCreatedEvent created, Instant startTime) {
        return CompactionJobStartedEvent.builder().jobId(created.getJobId()).tableId(created.getTableId()).startTime(startTime).jobRunId(UUID.randomUUID().toString());
    }

    /**
     * This method builds a CompactionJobFinishedEvent with the provident started Event and JobRunSummary.
     *
     * @param  started CompactionJobStartedEvent.
     * @param  summary JobRunSummary.
     * @return         CompactionJobFinishedEvent.
     */
    public static CompactionJobFinishedEvent compactionFinishedEvent(CompactionJobStartedEvent started, JobRunSummary summary) {
        return CompactionJobFinishedEvent.builder().jobId(started.getJobId()).tableId(started.getTableId()).taskId(started.getTaskId()).jobRunId(started.getJobRunId()).summary(summary).build();
    }

    /**
     * This method builds a CompactionJobFinishedEvent with the provident Created Event and JobRunSummary.
     *
     * @param  created CompactionJobCreatedEvent.
     * @param  summary JobRunSummary.
     * @return         CompactionJobFinishedEvent Builder.
     */
    public static CompactionJobFinishedEvent.Builder compactionFinishedEventBuilder(CompactionJobCreatedEvent created, JobRunSummary summary) {
        return CompactionJobFinishedEvent.builder().jobId(created.getJobId()).tableId(created.getTableId()).summary(summary);
    }

    /**
     * This method builds a CompactionJobCommittedEvent with the provident Started Event and commit time.
     *
     * @param  started    CompactionJobStartedEvent.
     * @param  commitTime Instant.
     * @return            CompactionJobCommittedEvent.
     */
    public static CompactionJobCommittedEvent compactionCommittedEvent(CompactionJobStartedEvent started, Instant commitTime) {
        return CompactionJobCommittedEvent.builder().jobId(started.getJobId()).tableId(started.getTableId()).taskId(started.getTaskId()).jobRunId(started.getJobRunId()).commitTime(commitTime).build();
    }

    /**
     * This method builds a CompactionJobCommittedEvent with the provident Created Event and commit time.
     *
     * @param  created    CompactionJobCreatedEvent.
     * @param  commitTime Instant.
     * @return            CompactionJobCommittedEvent Builder.
     */
    public static CompactionJobCommittedEvent.Builder compactionCommittedEventBuilder(CompactionJobCreatedEvent created, Instant commitTime) {
        return CompactionJobCommittedEvent.builder().jobId(created.getJobId()).tableId(created.getTableId()).commitTime(commitTime);
    }

    /**
     * This method builds a CompactionJobFailedEvent with the provident Started Event and failure time.
     *
     * @param  started     CompactionJobStartedEvent.
     * @param  failureTime Instant.
     * @return             CompactionJobFailedEvent Builder.
     */
    public static CompactionJobFailedEvent.Builder compactionFailedEventBuilder(CompactionJobStartedEvent started, Instant failureTime) {
        return CompactionJobFailedEvent.builder().jobId(started.getJobId()).tableId(started.getTableId()).taskId(started.getTaskId()).jobRunId(started.getJobRunId()).failureTime(failureTime);
    }

    /**
     * This method builds a CompactionJobFailedEvent with the provident Created Event and failure time.
     *
     * @param  created     CompactionJobCreatedEvent.
     * @param  failureTime Instant.
     * @return             CompactionJobFailedEvent Builder.
     */
    public static CompactionJobFailedEvent.Builder compactionFailedEventBuilder(CompactionJobCreatedEvent created, Instant failureTime) {
        return CompactionJobFailedEvent.builder().jobId(created.getJobId()).tableId(created.getTableId()).failureTime(failureTime);
    }

}
