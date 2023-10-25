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
import sleeper.core.record.process.status.ProcessStatusUpdateRecord;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static sleeper.core.record.process.status.TestRunStatusUpdates.defaultUpdateTime;
import static sleeper.ingest.job.status.IngestJobStatusType.REJECTED;

public class WriteToMemoryIngestJobStatusStore implements IngestJobStatusStore {
    private final Map<String, TableJobs> tableNameToJobs = new HashMap<>();

    @Override
    public void jobValidated(IngestJobValidatedEvent event) {
        tableNameToJobs.computeIfAbsent(event.getTableName(), tableName -> new TableJobs())
                .jobIdToUpdateRecords.computeIfAbsent(event.getJobId(), jobId -> new ArrayList<>())
                .add(ProcessStatusUpdateRecord.builder()
                        .jobId(event.getJobId())
                        .statusUpdate(validatedStatus(event))
                        .jobRunId(event.getJobRunId())
                        .taskId(event.getTaskId())
                        .build());
    }

    private static IngestJobValidatedStatus validatedStatus(IngestJobValidatedEvent event) {
        if (event.isAccepted()) {
            return IngestJobAcceptedStatus.from(
                    event.getJob(),
                    event.getValidationTime(),
                    defaultUpdateTime(event.getValidationTime()));
        } else {
            return IngestJobRejectedStatus.builder()
                    .job(event.getJob())
                    .validationTime(event.getValidationTime())
                    .updateTime(defaultUpdateTime(event.getValidationTime()))
                    .reasons(event.getReasons())
                    .jsonMessage(event.getJsonMessage()).build();
        }
    }

    @Override
    public void jobStarted(IngestJobStartedEvent event) {
        tableNameToJobs.computeIfAbsent(event.getTableName(), tableName -> new TableJobs())
                .jobIdToUpdateRecords.computeIfAbsent(event.getJobId(), jobId -> new ArrayList<>())
                .add(ProcessStatusUpdateRecord.builder()
                        .jobId(event.getJobId())
                        .statusUpdate(IngestJobStartedStatus.withStartOfRun(event.isStartOfRun())
                                .inputFileCount(event.getFileCount())
                                .startTime(event.getStartTime())
                                .updateTime(defaultUpdateTime(event.getStartTime()))
                                .build())
                        .jobRunId(event.getJobRunId())
                        .taskId(event.getTaskId())
                        .build());
    }

    @Override
    public void jobFinished(IngestJobFinishedEvent event) {
        RecordsProcessedSummary summary = event.getSummary();
        List<ProcessStatusUpdateRecord> jobRecords = tableJobs(event.getTableName())
                .map(jobs -> jobs.jobIdToUpdateRecords.get(event.getJobId()))
                .orElseThrow(() -> new IllegalStateException("Job not started: " + event.getJobId()));
        jobRecords.add(ProcessStatusUpdateRecord.builder()
                .jobId(event.getJobId())
                .statusUpdate(ProcessFinishedStatus.updateTimeAndSummary(defaultUpdateTime(summary.getFinishTime()), summary))
                .jobRunId(event.getJobRunId())
                .taskId(event.getTaskId())
                .build());
    }

    @Override
    public List<IngestJobStatus> getAllJobs(String tableName) {
        return IngestJobStatus.streamFrom(streamTableRecords(tableName))
                .collect(Collectors.toList());
    }

    @Override
    public List<IngestJobStatus> getInvalidJobs() {
        return streamAllJobs()
                .filter(status -> status.getFurthestStatusType().equals(REJECTED))
                .collect(Collectors.toList());
    }

    @Override
    public Optional<IngestJobStatus> getJob(String jobId) {
        return streamAllJobs()
                .filter(status -> Objects.equals(jobId, status.getJobId()))
                .findFirst();
    }

    public Stream<IngestJobStatus> streamAllJobs() {
        return IngestJobStatus.streamFrom(tableNameToJobs.values().stream()
                .flatMap(TableJobs::streamAllRecords));
    }

    public Stream<ProcessStatusUpdateRecord> streamTableRecords(String tableName) {
        return tableJobs(tableName)
                .map(TableJobs::streamAllRecords)
                .orElse(Stream.empty());
    }

    private Optional<TableJobs> tableJobs(String tableName) {
        return Optional.ofNullable(tableNameToJobs.get(tableName));
    }

    private static class TableJobs {
        private final Map<String, List<ProcessStatusUpdateRecord>> jobIdToUpdateRecords = new HashMap<>();

        private Stream<ProcessStatusUpdateRecord> streamAllRecords() {
            return jobIdToUpdateRecords.values().stream().flatMap(List::stream);
        }
    }
}
