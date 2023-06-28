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
import sleeper.ingest.job.IngestJob;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static sleeper.core.record.process.status.TestRunStatusUpdates.defaultUpdateTime;

public class WriteToMemoryIngestJobStatusStore implements IngestJobStatusStore {
    private final Map<String, TableJobs> tableNameToJobs = new HashMap<>();

    @Override
    public void jobValidated(IngestJobValidatedEvent event) {
        tableNameToJobs.computeIfAbsent(event.getJob().getTableName(), tableName -> new TableJobs())
                .jobIdToUpdateRecords.computeIfAbsent(event.getJob().getId(), jobId -> new ArrayList<>())
                .add(ProcessStatusUpdateRecord.builder()
                        .jobId(event.getJob().getId())
                        .statusUpdate(validatedStatus(event))
                        .jobRunId(event.getJobRunId())
                        .taskId(event.getTaskId())
                        .build());
    }

    private static IngestJobValidatedStatus validatedStatus(IngestJobValidatedEvent event) {
        if (event.isAccepted()) {
            return IngestJobAcceptedStatus.from(event.getValidationTime(),
                    defaultUpdateTime(event.getValidationTime()));
        } else {
            return IngestJobRejectedStatus.builder().validationTime(event.getValidationTime())
                    .updateTime(defaultUpdateTime(event.getValidationTime()))
                    .reasons(event.getReasons()).build();
        }
    }

    @Override
    public void jobStarted(IngestJobStartedEvent event) {
        IngestJob job = event.getJob();
        tableNameToJobs.computeIfAbsent(job.getTableName(), tableName -> new TableJobs())
                .jobIdToUpdateRecords.computeIfAbsent(job.getId(), jobId -> new ArrayList<>())
                .add(ProcessStatusUpdateRecord.builder()
                        .jobId(job.getId())
                        .statusUpdate(IngestJobStartedStatus.withStartOfRun(event.isStartOfRun())
                                .inputFileCount(job.getFiles().size())
                                .startTime(event.getStartTime())
                                .updateTime(defaultUpdateTime(event.getStartTime()))
                                .build())
                        .jobRunId(event.getJobRunId())
                        .taskId(event.getTaskId())
                        .build());
    }

    @Override
    public void jobFinished(IngestJobFinishedEvent event) {
        IngestJob job = event.getJob();
        RecordsProcessedSummary summary = event.getSummary();
        List<ProcessStatusUpdateRecord> jobRecords = tableJobs(job.getTableName())
                .map(jobs -> jobs.jobIdToUpdateRecords.get(job.getId()))
                .orElseThrow(() -> new IllegalStateException("Job not started: " + job.getId()));
        jobRecords.add(ProcessStatusUpdateRecord.builder()
                .jobId(job.getId())
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
