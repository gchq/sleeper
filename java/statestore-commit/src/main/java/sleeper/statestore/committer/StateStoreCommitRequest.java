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
package sleeper.statestore.committer;

import sleeper.compaction.job.commit.CompactionJobCommitRequest;
import sleeper.compaction.job.commit.CompactionJobIdAssignmentCommitRequest;
import sleeper.core.statestore.StateStoreException;
import sleeper.core.statestore.commit.GarbageCollectionCommitRequest;
import sleeper.core.statestore.commit.SplitPartitionCommitRequest;
import sleeper.ingest.job.commit.IngestAddFilesCommitRequest;

import java.util.Objects;

/**
 * A request to commit updates to the state store.
 */
public class StateStoreCommitRequest {

    private final Object request;
    private final String tableId;
    private final ApplyRequest applyRequest;

    /**
     * Creates a request to commit the results of a compaction job.
     *
     * @param  request the compaction job commit request
     * @return         a state store commit request
     */
    public static StateStoreCommitRequest forCompactionJob(CompactionJobCommitRequest request) {
        return new StateStoreCommitRequest(request, request.getJob().getTableId(), committer -> committer.commitCompaction(request));
    }

    /**
     * Creates a request to commit the assignment of job ID to files in a compaction job.
     *
     * @param  request the compaction job ID assignment commit request
     * @return         a state store commit request
     */
    public static StateStoreCommitRequest forCompactionJobIdAssignment(CompactionJobIdAssignmentCommitRequest request) {
        return new StateStoreCommitRequest(request, request.getTableId(), committer -> committer.assignCompactionInputFiles(request));
    }

    /**
     * Creates a request to commit files written during ingest or bulk import.
     *
     * @param  request the commit request
     * @return         a state store commit request
     */
    public static StateStoreCommitRequest forIngestAddFiles(IngestAddFilesCommitRequest request) {
        return new StateStoreCommitRequest(request, request.getTableId(), committer -> committer.addFiles(request));
    }

    /**
     * Creates a request to commit a partition split to add new child partitions.
     *
     * @param  request the commit request
     * @return         a state store commit request
     */
    public static StateStoreCommitRequest forSplitPartition(SplitPartitionCommitRequest request) {
        return new StateStoreCommitRequest(request, request.getTableId(), committer -> committer.splitPartition(request));
    }

    /**
     * Creates a request to commit when files have been deleted by the garbage collector.
     *
     * @param  request the commit request
     * @return         a state store commit request
     */
    public static StateStoreCommitRequest forGarbageCollection(GarbageCollectionCommitRequest request) {
        return new StateStoreCommitRequest(request, request.getTableId(), committer -> committer.filesDeleted(request));
    }

    private StateStoreCommitRequest(Object request, String tableId, ApplyRequest applyRequest) {
        this.request = request;
        this.tableId = tableId;
        this.applyRequest = applyRequest;
    }

    public Object getRequest() {
        return request;
    }

    public String getTableId() {
        return tableId;
    }

    void apply(StateStoreCommitter committer) throws StateStoreException {
        applyRequest.apply(committer);
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

    /**
     * Applies the current request with a given committer.
     */
    @FunctionalInterface
    private interface ApplyRequest {
        void apply(StateStoreCommitter committer) throws StateStoreException;
    }
}
