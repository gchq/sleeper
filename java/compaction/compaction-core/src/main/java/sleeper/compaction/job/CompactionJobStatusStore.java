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
package sleeper.compaction.job;

import sleeper.compaction.job.status.CompactionJobFinishedEvent;
import sleeper.compaction.job.status.CompactionJobStartedEvent;
import sleeper.compaction.job.status.CompactionJobStatus;
import sleeper.core.record.process.ProcessRunTime;

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

    default void jobStarted(CompactionJobStartedEvent event) {
    }

    default void jobFinished(CompactionJobFinishedEvent event) {
    }

    default void jobFailed(CompactionJob compactionJob, ProcessRunTime runTime, String taskId, List<String> failureReasons) {
    }

    default Optional<CompactionJobStatus> getJob(String jobId) {
        throw new UnsupportedOperationException("Instance has no compaction job status store");
    }

    default Stream<CompactionJobStatus> streamAllJobs(String tableId) {
        throw new UnsupportedOperationException("Instance has no compaction job status store");
    }

    default List<CompactionJobStatus> getAllJobs(String tableId) {
        return streamAllJobs(tableId).collect(Collectors.toList());
    }

    default List<CompactionJobStatus> getUnfinishedJobs(String tableId) {
        return streamAllJobs(tableId)
                .filter(CompactionJobStatus::isUnstartedOrInProgress)
                .collect(Collectors.toList());
    }

    default List<CompactionJobStatus> getJobsByTaskId(String tableId, String taskId) {
        return streamAllJobs(tableId)
                .filter(job -> job.isTaskIdAssigned(taskId))
                .collect(Collectors.toList());
    }

    default List<CompactionJobStatus> getJobsInTimePeriod(String tableId, Instant startTime, Instant endTime) {
        return streamAllJobs(tableId)
                .filter(job -> job.isInPeriod(startTime, endTime))
                .collect(Collectors.toList());
    }
}
