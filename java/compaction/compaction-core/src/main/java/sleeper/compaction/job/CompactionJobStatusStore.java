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
package sleeper.compaction.job;

import sleeper.compaction.job.status.CompactionJobStatus;
import sleeper.core.record.process.RecordsProcessedSummary;

import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public interface CompactionJobStatusStore {

    CompactionJobStatusStore NONE = new CompactionJobStatusStore() {
    };

    default void jobCreated(CompactionJob job) {
    }

    default void jobStarted(CompactionJob job, Instant startTime, String taskId) {
    }

    default void jobFinished(CompactionJob compactionJob, RecordsProcessedSummary summary, String taskId) {
    }

    default Optional<CompactionJobStatus> getJob(String jobId) {
        throw new UnsupportedOperationException("Instance has no compaction job status store");
    }

    default Stream<CompactionJobStatus> streamAllJobsByTableId(String tableId) {
        throw new UnsupportedOperationException("Instance has no compaction job status store");
    }

    default List<CompactionJobStatus> getAllJobsByTableId(String tableId) {
        return streamAllJobsByTableId(tableId).collect(Collectors.toList());
    }

    default List<CompactionJobStatus> getUnfinishedJobsByTableId(String tableId) {
        return streamAllJobsByTableId(tableId)
                .filter(job -> !job.isFinished())
                .collect(Collectors.toList());
    }

    default List<CompactionJobStatus> getJobsByTableIdAndTaskId(String tableId, String taskId) {
        return streamAllJobsByTableId(tableId)
                .filter(job -> job.isTaskIdAssigned(taskId))
                .collect(Collectors.toList());
    }

    default List<CompactionJobStatus> getJobsInTimePeriodByTableId(String tableId, Instant startTime, Instant endTime) {
        return streamAllJobsByTableId(tableId)
                .filter(job -> job.isInPeriod(startTime, endTime))
                .collect(Collectors.toList());
    }
}
