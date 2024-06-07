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

/**
 * A request to commit updates to the state store.
 */
public class StateStoreCommitRequest {

    private String type;
    private CompactionJobCommitRequest compactionJobCommitRequest;

    /**
     * Creates a request to commit a compaction job.
     *
     * @param  jobCommitRequest the compaction job commit request
     * @return                  a state store commit request
     */
    public static StateStoreCommitRequest forCompactionJob(CompactionJobCommitRequest jobCommitRequest) {
        return new StateStoreCommitRequest(CompactionJobCommitRequest.class.getSimpleName(), jobCommitRequest);
    }

    private StateStoreCommitRequest(String type, CompactionJobCommitRequest compactionJobCommitRequest) {
        this.type = type;
        this.compactionJobCommitRequest = compactionJobCommitRequest;
    }

    /**
     * Gets the compaction job commit request.
     *
     * @return                                  the compaction job commit request
     * @throws CommitRequestValidationException if this commit request is not a compaction job commit request
     */
    public CompactionJobCommitRequest getCompactionJobCommitRequest() {
        if (CompactionJobCommitRequest.class.getSimpleName().equals(type)) {
            return compactionJobCommitRequest;
        } else {
            throw new CommitRequestValidationException("Commit request is not a compaction job commit request");
        }
    }
}
