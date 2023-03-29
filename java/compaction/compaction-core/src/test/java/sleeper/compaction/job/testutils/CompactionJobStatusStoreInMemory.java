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
package sleeper.compaction.job.testutils;

import sleeper.compaction.job.CompactionJob;
import sleeper.compaction.job.CompactionJobStatusStore;
import sleeper.compaction.job.status.CompactionJobCreatedStatus;
import sleeper.compaction.job.status.CompactionJobStartedStatus;
import sleeper.compaction.job.status.CompactionJobStatus;
import sleeper.core.record.process.RecordsProcessedSummary;
import sleeper.core.record.process.status.ProcessFinishedStatus;
import sleeper.core.record.process.status.ProcessStatusUpdateRecord;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;

public class CompactionJobStatusStoreInMemory implements CompactionJobStatusStore {
    private final Map<String, TableJobs> tableNameToJobs = new HashMap<>();
    private Clock clock = Clock.systemUTC();

    public void fixTime(Instant now) {
        clock = Clock.fixed(now, ZoneOffset.UTC);
    }

    @Override
    public void jobCreated(CompactionJob job) {
        add(job.getTableName(), new ProcessStatusUpdateRecord(job.getId(), null,
                CompactionJobCreatedStatus.from(job, clock.instant()), null));
    }

    @Override
    public void jobStarted(CompactionJob job, Instant startTime, String taskId) {
        add(job.getTableName(), new ProcessStatusUpdateRecord(job.getId(), null,
                CompactionJobStartedStatus.startAndUpdateTime(startTime, clock.instant()), taskId));
    }

    @Override
    public void jobFinished(CompactionJob job, RecordsProcessedSummary summary, String taskId) {
        add(job.getTableName(), new ProcessStatusUpdateRecord(job.getId(), null,
                ProcessFinishedStatus.updateTimeAndSummary(clock.instant(), summary), taskId));
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
