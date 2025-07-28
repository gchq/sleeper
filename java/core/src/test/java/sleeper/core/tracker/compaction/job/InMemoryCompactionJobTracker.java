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

import sleeper.core.tracker.compaction.job.query.CompactionJobCommittedStatus;
import sleeper.core.tracker.compaction.job.query.CompactionJobCreatedStatus;
import sleeper.core.tracker.compaction.job.query.CompactionJobFinishedStatus;
import sleeper.core.tracker.compaction.job.query.CompactionJobStartedStatus;
import sleeper.core.tracker.compaction.job.query.CompactionJobStatus;
import sleeper.core.tracker.compaction.job.update.CompactionJobCommittedEvent;
import sleeper.core.tracker.compaction.job.update.CompactionJobCreatedEvent;
import sleeper.core.tracker.compaction.job.update.CompactionJobFailedEvent;
import sleeper.core.tracker.compaction.job.update.CompactionJobFinishedEvent;
import sleeper.core.tracker.compaction.job.update.CompactionJobStartedEvent;
import sleeper.core.tracker.job.status.JobRunFailedStatus;
import sleeper.core.tracker.job.status.JobStatusUpdateRecord;

import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Supplier;
import java.util.stream.Stream;

import static sleeper.core.tracker.job.status.JobStatusUpdateTestHelper.defaultUpdateTime;

/**
 * This classs is for tracking the in memory compaction jobs.
 */
public class InMemoryCompactionJobTracker implements CompactionJobTracker {
    private final Map<String, TableJobs> tableIdToJobs = new HashMap<>();
    private Supplier<Instant> timeSupplier;

    /**
     * This method sets the time supplier to the input param.
     *
     * @param now an Instant containing the current time
     */
    public void fixUpdateTime(Instant now) {
        setTimeSupplier(() -> now);
    }

    public void setTimeSupplier(Supplier<Instant> timeSupplier) {
        this.timeSupplier = timeSupplier;
    }

    private Instant getUpdateTimeOrDefault(Supplier<Instant> defaultTimeSupplier) {
        if (null != timeSupplier) {
            return timeSupplier.get();
        } else {
            return defaultTimeSupplier.get();
        }
    }

    @Override
    public void jobCreated(CompactionJobCreatedEvent event) {
        jobCreated(event, getUpdateTimeOrDefault(Instant::now));
    }

    /**
     * This method adds a job from an event to the update table.
     *
     * @param event        This event contains information about the job to be created.
     * @param assignedTime Instant detailing to time the job should be assigned from.
     */
    public void jobCreated(CompactionJobCreatedEvent event, Instant assignedTime) {
        add(event.getTableId(), JobStatusUpdateRecord.builder()
                .jobId(event.getJobId())
                .statusUpdate(CompactionJobCreatedStatus.from(event, assignedTime))
                .build());
    }

    @Override
    public void jobStarted(CompactionJobStartedEvent event) {
        add(event.getTableId(), JobStatusUpdateRecord.builder()
                .jobId(event.getJobId()).taskId(event.getTaskId()).jobRunId(event.getJobRunId())
                .statusUpdate(CompactionJobStartedStatus.startAndUpdateTime(
                        event.getStartTime(), getUpdateTimeOrDefault(() -> defaultUpdateTime(event.getStartTime()))))
                .build());
    }

    @Override
    public void jobFinished(CompactionJobFinishedEvent event) {
        add(event.getTableId(), JobStatusUpdateRecord.builder()
                .jobId(event.getJobId()).taskId(event.getTaskId()).jobRunId(event.getJobRunId())
                .statusUpdate(CompactionJobFinishedStatus.builder()
                        .updateTime(getUpdateTimeOrDefault(() -> defaultUpdateTime(event.getFinishTime())))
                        .finishTime(event.getFinishTime())
                        .rowsProcessed(event.getRowsProcessed())
                        .build())
                .build());
    }

    @Override
    public void jobCommitted(CompactionJobCommittedEvent event) {
        add(event.getTableId(), JobStatusUpdateRecord.builder()
                .jobId(event.getJobId()).taskId(event.getTaskId()).jobRunId(event.getJobRunId())
                .statusUpdate(CompactionJobCommittedStatus.commitAndUpdateTime(event.getCommitTime(),
                        getUpdateTimeOrDefault(() -> defaultUpdateTime(event.getCommitTime()))))
                .build());
    }

    @Override
    public void jobFailed(CompactionJobFailedEvent event) {
        add(event.getTableId(), JobStatusUpdateRecord.builder()
                .jobId(event.getJobId()).taskId(event.getTaskId()).jobRunId(event.getJobRunId())
                .statusUpdate(JobRunFailedStatus.builder()
                        .updateTime(getUpdateTimeOrDefault(() -> defaultUpdateTime(event.getFailureTime())))
                        .failureTime(event.getFailureTime())
                        .failureReasons(event.getFailureReasons())
                        .build())
                .build());
    }

    @Override
    public Optional<CompactionJobStatus> getJob(String jobId) {
        return CompactionJobStatus.streamFrom(
                tableIdToJobs.values().stream()
                        .flatMap(TableJobs::streamAllRecords)
                        .filter(record -> Objects.equals(jobId, record.getJobId())))
                .findAny();
    }

    @Override
    public Stream<CompactionJobStatus> streamAllJobs(String tableId) {
        return CompactionJobStatus.streamFrom(streamRecordsByTableId(tableId));
    }

    private void add(String tableId, JobStatusUpdateRecord record) {
        TableJobs table = tableIdToJobs.computeIfAbsent(tableId, id -> new TableJobs());
        table.jobIdToUpdateRecords
                .computeIfAbsent(record.getJobId(), jobId -> new ArrayList<>())
                .add(record);
    }

    private Stream<JobStatusUpdateRecord> streamRecordsByTableId(String tableId) {
        return jobsByTableId(tableId)
                .map(TableJobs::streamAllRecords)
                .orElse(Stream.empty());
    }

    private Optional<TableJobs> jobsByTableId(String tableId) {
        return Optional.ofNullable(tableIdToJobs.get(tableId));
    }

    /**
     * This private class contains a Map of tableId's to jobs.
     * It has one method to stream all the records in the Map.
     */
    private static class TableJobs {
        private final Map<String, List<JobStatusUpdateRecord>> jobIdToUpdateRecords = new HashMap<>();

        private Stream<JobStatusUpdateRecord> streamAllRecords() {
            return jobIdToUpdateRecords.values().stream().flatMap(List::stream);
        }
    }
}
