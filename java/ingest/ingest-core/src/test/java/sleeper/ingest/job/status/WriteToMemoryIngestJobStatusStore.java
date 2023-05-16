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

import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static sleeper.ingest.job.status.IngestJobStatusTestData.defaultUpdateTime;

public class WriteToMemoryIngestJobStatusStore implements IngestJobStatusStore {
    private final Map<String, TableJobs> tableNameToJobs = new HashMap<>();

    @Override
    public void jobStarted(String taskId, IngestJob job, Instant startTime) {
        ProcessStatusUpdateRecord updateRecord = new ProcessStatusUpdateRecord(job.getId(), null,
                IngestJobStartedStatus.startAndUpdateTime(job, startTime, defaultUpdateTime(startTime)), taskId);
        tableNameToJobs.computeIfAbsent(job.getTableName(), tableName -> new TableJobs())
                .jobIdToUpdateRecords.computeIfAbsent(job.getId(), jobId -> new ArrayList<>()).add(updateRecord);
    }

    @Override
    public void jobStartedWithValidation(String taskId, IngestJob job, Instant startTime,
                                         Instant validationTime, ValidationData validationData) {
        ProcessStatusUpdateRecord validationRecord = new ProcessStatusUpdateRecord(job.getId(), null,
                ValidationStatus.builder().updateTime(validationTime)
                        .validationData(validationData).build(), taskId);
        ProcessStatusUpdateRecord startedRecord = new ProcessStatusUpdateRecord(job.getId(), null,
                IngestJobStartedStatus.withValidation()
                        .inputFileCount(job.getFiles().size())
                        .startTime(startTime)
                        .updateTime(defaultUpdateTime(startTime)).build(), taskId);
        tableNameToJobs.computeIfAbsent(job.getTableName(), tableName -> new TableJobs())
                .jobIdToUpdateRecords.computeIfAbsent(job.getId(), jobId -> new ArrayList<>())
                .addAll(List.of(validationRecord, startedRecord));
    }

    @Override
    public void jobFinished(String taskId, IngestJob job, RecordsProcessedSummary summary) {
        ProcessStatusUpdateRecord updateRecord = new ProcessStatusUpdateRecord(job.getId(), null,
                ProcessFinishedStatus.updateTimeAndSummary(defaultUpdateTime(summary.getFinishTime()), summary), taskId);
        List<ProcessStatusUpdateRecord> jobRecords = tableJobs(job.getTableName())
                .map(jobs -> jobs.jobIdToUpdateRecords.get(job.getId()))
                .orElseThrow(() -> new IllegalStateException("Job not started: " + job.getId()));
        jobRecords.add(updateRecord);
    }

    @Override
    public List<IngestJobStatus> getAllJobs(String tableName) {
        return IngestJobStatus.streamFrom(streamTableRecords(tableName))
                .collect(Collectors.toList());
    }

    private Stream<ProcessStatusUpdateRecord> streamTableRecords(String tableName) {
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
