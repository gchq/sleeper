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
package sleeper.ingest.job.commit;

import sleeper.core.record.process.RecordsProcessed;
import sleeper.core.record.process.RecordsProcessedSummary;
import sleeper.core.statestore.FileReference;
import sleeper.ingest.job.IngestJob;

import java.time.Instant;
import java.util.List;
import java.util.Objects;

/**
 * A request to commit the results of an ingest job to the state store and job status store.
 */
public class IngestJobCommitRequest {
    private final IngestJob job;
    private final String taskId;
    private final List<FileReference> fileReferenceList;
    private final Instant startTime;
    private final Instant finishTime;
    private final long recordsRead;
    private final long recordsWritten;

    public IngestJobCommitRequest(IngestJob job, String taskId, List<FileReference> fileReferenceList, RecordsProcessedSummary recordsProcessed) {
        this.job = job;
        this.taskId = taskId;
        this.fileReferenceList = fileReferenceList;
        this.startTime = recordsProcessed.getStartTime();
        this.finishTime = recordsProcessed.getFinishTime();
        this.recordsRead = recordsProcessed.getRecordsRead();
        this.recordsWritten = recordsProcessed.getRecordsWritten();
    }

    public IngestJob getJob() {
        return job;
    }

    public String getTaskId() {
        return taskId;
    }

    public List<FileReference> getFileReferenceList() {
        return fileReferenceList;
    }

    public long getRecordsRead() {
        return recordsRead;
    }

    public long getRecordsWritten() {
        return recordsWritten;
    }

    /**
     * Creates a records processed object from the records read and written.
     *
     * @return a records processed object
     */
    public RecordsProcessed buildRecordsProcessed() {
        return new RecordsProcessed(recordsRead, recordsWritten);
    }

    /**
     * Creates a records processed summary from the records read and written, as well as the start and finish time.
     *
     * @return a records processed summary
     */
    public RecordsProcessedSummary buildRecordsProcessedSummary() {
        return new RecordsProcessedSummary(buildRecordsProcessed(), startTime, finishTime);
    }

    @Override
    public int hashCode() {
        return Objects.hash(job, taskId, startTime, finishTime, recordsRead, recordsWritten);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (!(obj instanceof IngestJobCommitRequest)) {
            return false;
        }
        IngestJobCommitRequest other = (IngestJobCommitRequest) obj;
        return Objects.equals(job, other.job)
                && Objects.equals(taskId, other.taskId)
                && Objects.equals(startTime, other.startTime)
                && Objects.equals(finishTime, other.finishTime)
                && recordsRead == other.recordsRead
                && recordsWritten == other.recordsWritten;
    }

    @Override
    public String toString() {
        return "IngestJobCommitRequest{job=" + job +
                ", taskId=" + taskId +
                ", startTime=" + startTime +
                ", finishTime=" + finishTime +
                ", recordsRead=" + recordsRead +
                ", recordsWritten=" + recordsWritten + "}";
    }

}
