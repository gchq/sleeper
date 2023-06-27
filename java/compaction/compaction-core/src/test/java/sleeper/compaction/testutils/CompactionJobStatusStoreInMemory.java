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
package sleeper.compaction.testutils;

import sleeper.compaction.job.CompactionJob;
import sleeper.compaction.job.CompactionJobStatusStore;
import sleeper.compaction.job.status.CompactionJobCreatedStatus;
import sleeper.compaction.job.status.CompactionJobStartedStatus;
import sleeper.compaction.job.status.CompactionJobStatus;
import sleeper.core.record.process.RecordsProcessedSummary;
import sleeper.core.record.process.status.ProcessFinishedStatus;
import sleeper.core.record.process.status.ProcessStatusUpdateRecord;

import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Stream;

import static sleeper.core.record.process.status.TestRunStatusUpdates.defaultUpdateTime;

public class CompactionJobStatusStoreInMemory implements CompactionJobStatusStore {
    private final Map<String, TableJobs> tableNameToJobs = new HashMap<>();
    private Instant fixedUpdateTime;

    public void fixUpdateTime(Instant now) {
        fixedUpdateTime = now;
    }

    private Instant getCreatedTime() {
        if (null != fixedUpdateTime) {
            return fixedUpdateTime;
        } else {
            return Instant.now();
        }
    }

    private Instant getUpdateTimeForEventTime(Instant eventTime) {
        if (null != fixedUpdateTime) {
            return fixedUpdateTime;
        } else {
            return defaultUpdateTime(eventTime);
        }
    }

    @Override
    public void jobCreated(CompactionJob job) {
        jobCreated(job, getCreatedTime());
    }

    public void jobCreated(CompactionJob job, Instant createdTime) {
        add(job.getTableName(), ProcessStatusUpdateRecord.builder()
                .jobId(job.getId())
                .statusUpdate(CompactionJobCreatedStatus.from(job, createdTime))
                .build());
    }

    @Override
    public void jobStarted(CompactionJob job, Instant startTime, String taskId) {
        add(job.getTableName(), ProcessStatusUpdateRecord.builder()
                .jobId(job.getId()).taskId(taskId)
                .statusUpdate(CompactionJobStartedStatus.startAndUpdateTime(
                        startTime, getUpdateTimeForEventTime(startTime)))
                .build());
    }

    @Override
    public void jobFinished(CompactionJob job, RecordsProcessedSummary summary, String taskId) {
        add(job.getTableName(), ProcessStatusUpdateRecord.builder()
                .jobId(job.getId()).taskId(taskId)
                .statusUpdate(ProcessFinishedStatus.updateTimeAndSummary(
                        getUpdateTimeForEventTime(summary.getFinishTime()), summary))
                .build());
    }

    @Override
    public Optional<CompactionJobStatus> getJob(String jobId) {
        return CompactionJobStatus.streamFrom(tableNameToJobs.values().stream()
                        .flatMap(TableJobs::streamAllRecords)
                        .filter(record -> Objects.equals(jobId, record.getJobId())))
                .findAny();
    }

    @Override
    public Stream<CompactionJobStatus> streamAllJobs(String tableName) {
        return CompactionJobStatus.streamFrom(streamTableRecords(tableName));
    }

    private void add(String tableName, ProcessStatusUpdateRecord record) {
        tableNameToJobs.computeIfAbsent(tableName, name -> new TableJobs())
                .jobIdToUpdateRecords.computeIfAbsent(record.getJobId(), jobId -> new ArrayList<>())
                .add(record);
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
