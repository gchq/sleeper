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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sleeper.compaction.job.CompactionJobStatusStore;
import sleeper.compaction.job.commit.CompactionJobCommitRequest;
import sleeper.compaction.job.commit.CompactionJobCommitter;
import sleeper.core.statestore.GetStateStoreByTableId;
import sleeper.core.statestore.StateStore;
import sleeper.core.statestore.StateStoreException;
import sleeper.ingest.job.IngestJob;
import sleeper.ingest.job.commit.IngestAddFilesCommitRequest;

/**
 * Applies a state store commit request.
 */
public class StateStoreCommitter {
    public static final Logger LOGGER = LoggerFactory.getLogger(StateStoreCommitter.class);

    private final CompactionJobCommitter compactionJobCommitter;
    private final GetStateStoreByTableId stateStoreProvider;

    public StateStoreCommitter(CompactionJobStatusStore compactionJobStatusStore, GetStateStoreByTableId stateStoreProvider) {
        this.compactionJobCommitter = new CompactionJobCommitter(compactionJobStatusStore, stateStoreProvider);
        this.stateStoreProvider = stateStoreProvider;
    }

    /**
     * Applies a state store commit request.
     *
     * @param request the commit request
     */
    public void apply(StateStoreCommitRequest request) throws StateStoreException {
        Object requestObj = request.getRequest();
        if (requestObj instanceof CompactionJobCommitRequest) {
            compactionJobCommitter.apply((CompactionJobCommitRequest) requestObj);
        } else if (requestObj instanceof IngestAddFilesCommitRequest) {
            apply((IngestAddFilesCommitRequest) requestObj);
        }
    }

    private void apply(IngestAddFilesCommitRequest request) throws StateStoreException {
        StateStore stateStore = stateStoreProvider.getByTableId(request.getTableId());
        stateStore.addFiles(request.getFileReferences());
        IngestJob job = request.getJob();
        if (job != null) {
            LOGGER.info("Successfully committed new files for ingest job {} to table with ID {}", job.getId(), request.getTableId());
        } else {
            LOGGER.info("Successfully committed new files for ingest to table with ID {}", request.getTableId());
        }
    }

}
