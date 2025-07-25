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
package sleeper.core.tracker.ingest.job;

import sleeper.core.tracker.ingest.job.query.IngestJobAcceptedStatus;
import sleeper.core.tracker.ingest.job.query.IngestJobAddedFilesStatus;
import sleeper.core.tracker.ingest.job.query.IngestJobFinishedStatus;
import sleeper.core.tracker.ingest.job.query.IngestJobRejectedStatus;
import sleeper.core.tracker.ingest.job.query.IngestJobStartedStatus;
import sleeper.core.tracker.ingest.job.query.IngestJobStatus;
import sleeper.core.tracker.ingest.job.query.IngestJobValidatedStatus;
import sleeper.core.tracker.ingest.job.update.IngestJobAddedFilesEvent;
import sleeper.core.tracker.ingest.job.update.IngestJobFailedEvent;
import sleeper.core.tracker.ingest.job.update.IngestJobFinishedEvent;
import sleeper.core.tracker.ingest.job.update.IngestJobStartedEvent;
import sleeper.core.tracker.ingest.job.update.IngestJobValidatedEvent;
import sleeper.core.tracker.job.status.JobRunFailedStatus;
import sleeper.core.tracker.job.status.JobStatusUpdateRecord;

import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static sleeper.core.tracker.ingest.job.query.IngestJobStatusType.REJECTED;
import static sleeper.core.tracker.job.status.JobStatusUpdateTestHelper.defaultUpdateTime;

/**
 * An in-memory implementation of the ingest job tracker.
 */
public class InMemoryIngestJobTracker implements IngestJobTracker {
    private final Map<String, TableJobs> tableIdToJobs = new HashMap<>();

    @Override
    public void jobValidated(IngestJobValidatedEvent event) {
        tableIdToJobs.computeIfAbsent(event.getTableId(), tableId -> new TableJobs()).jobIdToUpdateRecords.computeIfAbsent(event.getJobId(), jobId -> new ArrayList<>())
                .add(JobStatusUpdateRecord.builder()
                        .jobId(event.getJobId())
                        .statusUpdate(toStatusUpdate(event, defaultUpdateTime(event.getValidationTime())))
                        .jobRunId(event.getJobRunId())
                        .build());
    }

    @Override
    public void jobStarted(IngestJobStartedEvent event) {
        tableIdToJobs.computeIfAbsent(event.getTableId(), tableId -> new TableJobs()).jobIdToUpdateRecords.computeIfAbsent(event.getJobId(), jobId -> new ArrayList<>())
                .add(JobStatusUpdateRecord.builder()
                        .jobId(event.getJobId())
                        .statusUpdate(IngestJobStartedStatus.builder()
                                .inputFileCount(event.getFileCount())
                                .startTime(event.getStartTime())
                                .updateTime(defaultUpdateTime(event.getStartTime()))
                                .build())
                        .jobRunId(event.getJobRunId())
                        .taskId(event.getTaskId())
                        .build());
    }

    @Override
    public void jobAddedFiles(IngestJobAddedFilesEvent event) {
        existingJobRecords(event.getTableId(), event.getJobId())
                .add(JobStatusUpdateRecord.builder()
                        .jobId(event.getJobId())
                        .statusUpdate(IngestJobAddedFilesStatus.builder()
                                .writtenTime(event.getWrittenTime())
                                .updateTime(defaultUpdateTime(event.getWrittenTime()))
                                .fileCount(event.getFileCount())
                                .build())
                        .jobRunId(event.getJobRunId())
                        .taskId(event.getTaskId())
                        .build());
    }

    @Override
    public void jobFinished(IngestJobFinishedEvent event) {
        existingJobRecords(event.getTableId(), event.getJobId())
                .add(JobStatusUpdateRecord.builder()
                        .jobId(event.getJobId())
                        .statusUpdate(IngestJobFinishedStatus.builder()
                                .updateTime(defaultUpdateTime(event.getFinishTime()))
                                .finishTime(event.getFinishTime())
                                .rowsProcessed(event.getRowsProcessed())
                                .numFilesWrittenByJob(event.getNumFilesWrittenByJob())
                                .committedBySeparateFileUpdates(event.isCommittedBySeparateFileUpdates())
                                .build())
                        .jobRunId(event.getJobRunId())
                        .taskId(event.getTaskId())
                        .build());
    }

    @Override
    public void jobFailed(IngestJobFailedEvent event) {
        existingJobRecords(event.getTableId(), event.getJobId())
                .add(JobStatusUpdateRecord.builder()
                        .jobId(event.getJobId())
                        .statusUpdate(JobRunFailedStatus.builder()
                                .updateTime(defaultUpdateTime(event.getFailureTime()))
                                .failureTime(event.getFailureTime())
                                .failureReasons(event.getFailureReasons())
                                .build())
                        .jobRunId(event.getJobRunId())
                        .taskId(event.getTaskId())
                        .build());
    }

    @Override
    public Stream<IngestJobStatus> streamAllJobs(String tableId) {
        return IngestJobStatus.streamFrom(streamTableRecords(tableId));
    }

    @Override
    public List<IngestJobStatus> getInvalidJobs() {
        return streamAllJobs()
                .filter(status -> status.getFurthestRunStatusType().equals(REJECTED))
                .collect(Collectors.toList());
    }

    @Override
    public Optional<IngestJobStatus> getJob(String jobId) {
        return streamAllJobs()
                .filter(status -> Objects.equals(jobId, status.getJobId()))
                .findFirst();
    }

    private Stream<IngestJobStatus> streamAllJobs() {
        return IngestJobStatus.streamFrom(tableIdToJobs.values().stream()
                .flatMap(TableJobs::streamAllRecords));
    }

    /**
     * Streams all process status update records for a table.
     *
     * @param  tableId the table ID
     * @return         a stream of {@link JobStatusUpdateRecord}
     */
    public Stream<JobStatusUpdateRecord> streamTableRecords(String tableId) {
        return tableJobs(tableId)
                .map(TableJobs::streamAllRecords)
                .orElse(Stream.empty());
    }

    private Optional<TableJobs> tableJobs(String tableId) {
        return Optional.ofNullable(tableIdToJobs.get(tableId));
    }

    private List<JobStatusUpdateRecord> existingJobRecords(String tableId, String jobId) {
        return tableJobs(tableId)
                .map(jobs -> jobs.jobIdToUpdateRecords.get(jobId))
                .orElseThrow(() -> new IllegalStateException("Job not started: " + jobId));
    }

    /**
     * Stores job updates by job ID in memory.
     */
    private static class TableJobs {
        private final Map<String, List<JobStatusUpdateRecord>> jobIdToUpdateRecords = new HashMap<>();

        private Stream<JobStatusUpdateRecord> streamAllRecords() {
            return jobIdToUpdateRecords.values().stream().flatMap(List::stream);
        }
    }

    private static IngestJobValidatedStatus toStatusUpdate(IngestJobValidatedEvent event, Instant updateTime) {
        if (event.isAccepted()) {
            return IngestJobAcceptedStatus.from(
                    event.getFileCount(), event.getValidationTime(), updateTime);
        } else {
            return IngestJobRejectedStatus.builder()
                    .inputFileCount(event.getFileCount())
                    .validationTime(event.getValidationTime())
                    .updateTime(updateTime)
                    .reasons(event.getReasons())
                    .jsonMessage(event.getJsonMessage()).build();
        }
    }
}
