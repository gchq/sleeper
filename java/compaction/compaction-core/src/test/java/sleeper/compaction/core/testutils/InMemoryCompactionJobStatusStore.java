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
package sleeper.compaction.core.testutils;

import sleeper.compaction.core.job.CompactionJob;
import sleeper.compaction.core.job.CompactionJobStatusStore;
import sleeper.compaction.core.job.status.CompactionJobCommittedEvent;
import sleeper.compaction.core.job.status.CompactionJobCommittedStatus;
import sleeper.compaction.core.job.status.CompactionJobCreatedEvent;
import sleeper.compaction.core.job.status.CompactionJobFailedEvent;
import sleeper.compaction.core.job.status.CompactionJobFinishedEvent;
import sleeper.compaction.core.job.status.CompactionJobFinishedStatus;
import sleeper.compaction.core.job.status.CompactionJobInputFilesAssignedStatus;
import sleeper.compaction.core.job.status.CompactionJobStartedEvent;
import sleeper.compaction.core.job.status.CompactionJobStartedStatus;
import sleeper.compaction.core.job.status.CompactionJobStatus;
import sleeper.core.record.process.ProcessRunTime;
import sleeper.core.record.process.RecordsProcessedSummary;
import sleeper.core.record.process.status.ProcessFailedStatus;
import sleeper.core.record.process.status.ProcessStatusUpdateRecord;
import sleeper.core.statestore.AssignJobIdRequest;

import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Supplier;
import java.util.stream.Stream;

import static sleeper.core.record.process.status.ProcessStatusUpdateTestHelper.defaultUpdateTime;

public class InMemoryCompactionJobStatusStore implements CompactionJobStatusStore {
    private final Map<String, TableJobs> tableIdToJobs = new HashMap<>();
    private Supplier<Instant> timeSupplier;

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

    @Override
    public void jobInputFilesAssigned(String tableId, List<AssignJobIdRequest> requests) {
        jobInputFilesAssigned(tableId, requests, getUpdateTimeOrDefault(Instant::now));
    }

    public void jobCreated(CompactionJobCreatedEvent event, Instant assignedTime) {
        add(event.getTableId(), ProcessStatusUpdateRecord.builder()
                .jobId(event.getJobId())
                .statusUpdate(CompactionJobInputFilesAssignedStatus.from(event, assignedTime))
                .build());
    }

    public void jobInputFilesAssigned(String tableId, List<AssignJobIdRequest> requests, Instant assignedTime) {
        for (AssignJobIdRequest request : requests) {
            add(tableId, ProcessStatusUpdateRecord.builder()
                    .jobId(request.getJobId())
                    .statusUpdate(CompactionJobInputFilesAssignedStatus.from(request, assignedTime))
                    .build());
        }
    }

    @Override
    public void jobStarted(CompactionJobStartedEvent event) {
        add(event.getTableId(), ProcessStatusUpdateRecord.builder()
                .jobId(event.getJobId()).taskId(event.getTaskId()).jobRunId(event.getJobRunId())
                .statusUpdate(CompactionJobStartedStatus.startAndUpdateTime(
                        event.getStartTime(), getUpdateTimeOrDefault(() -> defaultUpdateTime(event.getStartTime()))))
                .build());
    }

    @Override
    public void jobFinished(CompactionJobFinishedEvent event) {
        RecordsProcessedSummary summary = event.getSummary();
        Instant eventTime = summary.getFinishTime();
        add(event.getTableId(), ProcessStatusUpdateRecord.builder()
                .jobId(event.getJobId()).taskId(event.getTaskId()).jobRunId(event.getJobRunId())
                .statusUpdate(CompactionJobFinishedStatus.updateTimeAndSummary(
                        getUpdateTimeOrDefault(() -> defaultUpdateTime(eventTime)), summary)
                        .build())
                .build());
    }

    @Override
    public void jobCommitted(CompactionJobCommittedEvent event) {
        add(event.getTableId(), ProcessStatusUpdateRecord.builder()
                .jobId(event.getJobId()).taskId(event.getTaskId()).jobRunId(event.getJobRunId())
                .statusUpdate(CompactionJobCommittedStatus.commitAndUpdateTime(event.getCommitTime(),
                        getUpdateTimeOrDefault(() -> defaultUpdateTime(event.getCommitTime()))))
                .build());
    }

    @Override
    public void jobFailed(CompactionJobFailedEvent event) {
        ProcessRunTime runTime = event.getRunTime();
        Instant eventTime = runTime.getFinishTime();
        add(event.getTableId(), ProcessStatusUpdateRecord.builder()
                .jobId(event.getJobId()).taskId(event.getTaskId()).jobRunId(event.getJobRunId())
                .statusUpdate(ProcessFailedStatus.timeAndReasons(
                        getUpdateTimeOrDefault(() -> defaultUpdateTime(eventTime)), runTime, event.getFailureReasons()))
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

    private void add(CompactionJob job, ProcessStatusUpdateRecord record) {
        add(job.getTableId(), record);
    }

    private void add(String tableId, ProcessStatusUpdateRecord record) {
        TableJobs table = tableIdToJobs.computeIfAbsent(tableId, id -> new TableJobs());
        table.jobIdToUpdateRecords
                .computeIfAbsent(record.getJobId(), jobId -> new ArrayList<>())
                .add(record);
    }

    private Stream<ProcessStatusUpdateRecord> streamRecordsByTableId(String tableId) {
        return jobsByTableId(tableId)
                .map(TableJobs::streamAllRecords)
                .orElse(Stream.empty());
    }

    private Optional<TableJobs> jobsByTableId(String tableId) {
        return Optional.ofNullable(tableIdToJobs.get(tableId));
    }

    private static class TableJobs {
        private final Map<String, List<ProcessStatusUpdateRecord>> jobIdToUpdateRecords = new HashMap<>();

        private Stream<ProcessStatusUpdateRecord> streamAllRecords() {
            return jobIdToUpdateRecords.values().stream().flatMap(List::stream);
        }
    }
}
