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
import sleeper.compaction.job.commit.CompactionJobCommitter;
import sleeper.core.statestore.StateStoreException;

import java.util.Optional;

/**
 * Applies a state store commit request.
 */
public class StateStoreCommitter {
    private final CompactionJobCommitter compactionJobCommitter;

    public StateStoreCommitter(CompactionJobCommitter compactionJobCommitter) {
        this.compactionJobCommitter = compactionJobCommitter;
    }

    /**
     * Applies a state store commit request.
     *
     * @param request the commit request
     */
    public void apply(StateStoreCommitRequest request) throws StateStoreException {
        Optional<CompactionJobCommitRequest> compactionCommit = request.getCompactionJobCommitRequest();
        if (compactionCommit.isPresent()) {
            compactionJobCommitter.apply(compactionCommit.get());
        }
    }

}
