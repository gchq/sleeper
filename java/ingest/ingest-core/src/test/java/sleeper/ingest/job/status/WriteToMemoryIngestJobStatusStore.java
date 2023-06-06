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
    public void jobAccepted(String taskId, IngestJob job, Instant validationTime) {
        ProcessStatusUpdateRecord validationRecord = new ProcessStatusUpdateRecord(job.getId(), null,
                IngestJobAcceptedStatus.from(validationTime, defaultUpdateTime(validationTime)), taskId);
        tableNameToJobs.computeIfAbsent(job.getTableName(), tableName -> new TableJobs())
                .jobIdToUpdateRecords.computeIfAbsent(job.getId(), jobId -> new ArrayList<>())
                .add(validationRecord);
    }

    @Override
    public void jobRejected(String taskId, IngestJob job, Instant validationTime, List<String> reasons) {
        ProcessStatusUpdateRecord validationRecord = new ProcessStatusUpdateRecord(job.getId(), null,
                IngestJobRejectedStatus.builder().validationTime(validationTime)
                        .updateTime(defaultUpdateTime(validationTime))
                        .reasons(reasons).build(), taskId);
        tableNameToJobs.computeIfAbsent(job.getTableName(), tableName -> new TableJobs())
                .jobIdToUpdateRecords.computeIfAbsent(job.getId(), jobId -> new ArrayList<>())
                .add(validationRecord);
    }

    @Override
    public void jobStarted(String taskId, IngestJob job, Instant startTime, boolean startOfRun) {
        ProcessStatusUpdateRecord updateRecord = new ProcessStatusUpdateRecord(job.getId(), null,
                IngestJobStartedStatus.withStartOfRun(startOfRun).inputFileCount(job.getFiles().size())
                        .startTime(startTime).updateTime(defaultUpdateTime(startTime)).build(), taskId);
        tableNameToJobs.computeIfAbsent(job.getTableName(), tableName -> new TableJobs())
                .jobIdToUpdateRecords.computeIfAbsent(job.getId(), jobId -> new ArrayList<>()).add(updateRecord);
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
