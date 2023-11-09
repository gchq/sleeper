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

import sleeper.core.table.TableIdentity;

import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public interface IngestJobStatusStore {
    IngestJobStatusStore NONE = new IngestJobStatusStore() {
    };

    default void jobValidated(IngestJobValidatedEvent event) {
    }

    default void jobStarted(IngestJobStartedEvent event) {
    }

    default void jobFinished(IngestJobFinishedEvent event) {
    }

    default Stream<IngestJobStatus> streamAllJobs(TableIdentity tableId) {
        throw new UnsupportedOperationException("Instance has no ingest job status store");
    }

    default List<IngestJobStatus> getAllJobs(TableIdentity tableId) {
        return streamAllJobs(tableId).collect(Collectors.toList());
    }

    default List<IngestJobStatus> getUnfinishedJobs(TableIdentity tableId) {
        return streamAllJobs(tableId)
                .filter(job -> !job.isFinished())
                .collect(Collectors.toList());
    }

    default List<IngestJobStatus> getJobsByTaskId(TableIdentity tableId, String taskId) {
        return streamAllJobs(tableId)
                .filter(job -> job.isTaskIdAssigned(taskId))
                .collect(Collectors.toList());
    }

    default List<IngestJobStatus> getJobsInTimePeriod(TableIdentity tableId, Instant startTime, Instant endTime) {
        return streamAllJobs(tableId)
                .filter(job -> job.isInPeriod(startTime, endTime))
                .collect(Collectors.toList());
    }

    default Optional<IngestJobStatus> getJob(String jobId) {
        throw new UnsupportedOperationException("Instance has no ingest job status store");
    }

    default List<IngestJobStatus> getInvalidJobs() {
        throw new UnsupportedOperationException("Instance has no ingest job status store");
    }
}
