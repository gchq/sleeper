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

public interface IngestJobStatusStore {
    IngestJobStatusStore NONE = new IngestJobStatusStore() {
    };

    default void jobValidated(IngestJobValidatedEvent event) {
    }

    default void jobStarted(IngestJobStartedEvent event) {
    }

    default void jobFinished(IngestJobFinishedEvent event) {
    }

    default List<IngestJobStatus> getJobsInTimePeriod(String tableName, Instant start, Instant end) {
        throw new UnsupportedOperationException("Instance has no ingest job status store");
    }

    default List<IngestJobStatus> getJobsInTimePeriod(TableIdentity tableId, Instant start, Instant end) {
        return getJobsInTimePeriod(tableId.getTableName(), start, end);
    }

    default List<IngestJobStatus> getAllJobs(String tableName) {
        throw new UnsupportedOperationException("Instance has no ingest job status store");
    }

    default List<IngestJobStatus> getAllJobs(TableIdentity tableId) {
        return getAllJobs(tableId.getTableName());
    }

    default List<IngestJobStatus> getUnfinishedJobs(String tableName) {
        throw new UnsupportedOperationException("Instance has no ingest job status store");
    }

    default List<IngestJobStatus> getUnfinishedJobs(TableIdentity tableId) {
        return getUnfinishedJobs(tableId.getTableName());
    }

    default Optional<IngestJobStatus> getJob(String jobId) {
        throw new UnsupportedOperationException("Instance has no ingest job status store");
    }

    default List<IngestJobStatus> getJobsByTaskId(String tableName, String taskId) {
        throw new UnsupportedOperationException("Instance has no ingest job status store");
    }

    default List<IngestJobStatus> getJobsByTaskId(TableIdentity tableId, String taskId) {
        return getJobsByTaskId(tableId.getTableName(), taskId);
    }

    default List<IngestJobStatus> getInvalidJobs() {
        throw new UnsupportedOperationException("Instance has no ingest job status store");
    }
}
