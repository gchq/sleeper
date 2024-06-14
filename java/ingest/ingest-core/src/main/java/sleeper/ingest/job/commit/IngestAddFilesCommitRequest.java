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

import sleeper.core.statestore.FileReference;
import sleeper.ingest.job.IngestJob;

import java.util.List;
import java.util.Objects;

/**
 * A request to commit files to the state store that have been written during an ingest or bulk import.
 */
public class IngestAddFilesCommitRequest {
    private final IngestJob ingestJob;
    private final String taskId;
    private final String jobRunId;
    private final List<FileReference> fileReferences;

    public IngestAddFilesCommitRequest(IngestJob job, String taskId, String jobRunId, List<FileReference> fileReferences) {
        this.ingestJob = job;
        this.taskId = taskId;
        this.jobRunId = jobRunId;
        this.fileReferences = fileReferences;
    }

    public IngestJob getJob() {
        return ingestJob;
    }

    public String getTaskId() {
        return taskId;
    }

    public List<FileReference> getFileReferences() {
        return fileReferences;
    }

    @Override
    public int hashCode() {
        return Objects.hash(ingestJob, taskId, jobRunId, fileReferences);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (!(obj instanceof IngestAddFilesCommitRequest)) {
            return false;
        }
        IngestAddFilesCommitRequest other = (IngestAddFilesCommitRequest) obj;
        return Objects.equals(ingestJob, other.ingestJob) && Objects.equals(taskId, other.taskId)
                && Objects.equals(jobRunId, other.jobRunId) && Objects.equals(fileReferences, other.fileReferences);
    }

    @Override
    public String toString() {
        return "IngestAddFilesCommitRequest{ingestJob=" + ingestJob + ", taskId=" + taskId
                + ", jobRunId=" + jobRunId + ", fileReferences=" + fileReferences + "}";
    }

}
