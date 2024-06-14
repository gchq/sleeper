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
package sleeper.commit;

import sleeper.compaction.job.CompactionJob;
import sleeper.compaction.job.commit.CompactionJobCommitRequest;
import sleeper.core.record.process.ProcessRunTime;
import sleeper.core.record.process.RecordsProcessed;
import sleeper.core.record.process.RecordsProcessedSummary;
import sleeper.core.statestore.CommitRequestType;
import sleeper.core.statestore.FileReference;
import sleeper.ingest.job.IngestJob;
import sleeper.ingest.job.commit.IngestJobCommitRequest;

import java.time.Instant;
import java.util.List;

/**
 * Represents a JSON string for a commit request. Used in deserialisation.
 */
public class StateStoreCommitRequestJson {
    private CommitRequestType type;
    private CommitRequest request;

    public StateStoreCommitRequestJson(CommitRequestType type, CommitRequest request) {
        this.type = type;
        this.request = request;
    }

    /**
     * Gets the compaction job commit request.
     *
     * @return the compaction job commit request
     */
    public StateStoreCommitRequest getCommitRequest() {
        if (CommitRequestType.COMPACTION_FINISHED == type) {
            return StateStoreCommitRequest.forCompactionJob(request.toCompactionJobCommitRequest());
        }
        if (CommitRequestType.INGEST_ADD_FILES == type) {
            return StateStoreCommitRequest.forIngestJob(request.toIngestJobCommitRequest());
        }
        throw new CommitRequestValidationException("Commit request type not recognised: " + type);
    }

    /**
     * Represents a compaction job commit request.
     */
    private static class CommitRequest {
        private CompactionJob job;
        private IngestJob ingestJob;
        private List<FileReference> fileReferenceList;
        private String taskId;
        private Instant startTime;
        private Instant finishTime;
        private long recordsRead;
        private long recordsWritten;

        CompactionJobCommitRequest toCompactionJobCommitRequest() {
            return new CompactionJobCommitRequest(job, taskId, recordsProcessedSummary());
        }

        IngestJobCommitRequest toIngestJobCommitRequest() {
            return new IngestJobCommitRequest(ingestJob, taskId, fileReferenceList, recordsProcessedSummary());
        }

        RecordsProcessedSummary recordsProcessedSummary() {
            return new RecordsProcessedSummary(
                    new RecordsProcessed(recordsRead, recordsWritten), new ProcessRunTime(startTime, finishTime));
        }
    }
}
