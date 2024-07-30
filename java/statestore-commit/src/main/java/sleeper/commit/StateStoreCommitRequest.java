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

import sleeper.compaction.job.commit.CompactionJobCommitRequest;
import sleeper.compaction.job.commit.CompactionJobIdAssignmentCommitRequest;
import sleeper.core.statestore.commit.StateStoreCommitRequestInS3;
import sleeper.ingest.job.commit.IngestAddFilesCommitRequest;

import java.util.Objects;

/**
 * A request to commit updates to the state store.
 */
public class StateStoreCommitRequest {

    private final Object request;

    /**
     * Creates a request to commit the results of a compaction job.
     *
     * @param  request the compaction job commit request
     * @return         a state store commit request
     */
    public static StateStoreCommitRequest forCompactionJob(CompactionJobCommitRequest request) {
        return new StateStoreCommitRequest(request);
    }

    /**
     * Creates a request to commit the assignment of job ID to files in a compaction job.
     *
     * @param  request the compaction job ID assignment commit request
     * @return         a state store commit request
     */
    public static StateStoreCommitRequest forCompactionJobIdAssignment(CompactionJobIdAssignmentCommitRequest request) {
        return new StateStoreCommitRequest(request);
    }

    /**
     * Creates a request to commit files written during ingest or bulk import.
     *
     * @param  request the commit request
     * @return         a state store commit request
     */
    public static StateStoreCommitRequest forIngestAddFiles(IngestAddFilesCommitRequest request) {
        return new StateStoreCommitRequest(request);
    }

    /**
     * Creates a request which is stored in S3.
     *
     * @param  request the commit request
     * @return         a state store commit request
     */
    public static StateStoreCommitRequest storedInS3(StateStoreCommitRequestInS3 request) {
        return new StateStoreCommitRequest(request);
    }

    private StateStoreCommitRequest(Object request) {
        this.request = request;
    }

    public Object getRequest() {
        return request;
    }

    @Override
    public int hashCode() {
        return Objects.hash(request);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (!(obj instanceof StateStoreCommitRequest)) {
            return false;
        }
        StateStoreCommitRequest other = (StateStoreCommitRequest) obj;
        return Objects.equals(request, other.request);
    }

    @Override
    public String toString() {
        return "StateStoreCommitRequest{request=" + request + "}";
    }

}
