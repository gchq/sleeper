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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sleeper.core.statestore.FileReference;
import sleeper.core.statestore.StateStore;
import sleeper.core.statestore.StateStoreException;
import sleeper.ingest.job.IngestJob;
import sleeper.ingest.job.status.IngestJobStatusStore;

import java.util.List;

import static sleeper.ingest.job.status.IngestJobFinishedEvent.ingestJobFinished;

/**
 * Commits the result of an ingest job to the state store and reports that the ingest job has finished.
 */
public class IngestJobCommitter {
    public static final Logger LOGGER = LoggerFactory.getLogger(IngestJobCommitter.class);

    private final IngestJobStatusStore statusStore;
    private final GetStateStore stateStoreProvider;

    public IngestJobCommitter(IngestJobStatusStore statusStore, GetStateStore stateStoreProvider) {
        this.statusStore = statusStore;
        this.stateStoreProvider = stateStoreProvider;
    }

    /**
     * Commits the result of an ingest job to the state store and reports that the ingest job has finished.
     *
     * @param  request             the ingest job commit request
     * @throws StateStoreException if the state store update fails
     */
    public void apply(IngestJobCommitRequest request) throws StateStoreException {
        IngestJob job = request.getJob();
        addFilesToStateStore(request.getFileReferenceList(), stateStoreProvider.getByTableId(job.getTableId()));
        statusStore.jobFinished(ingestJobFinished(request.getTaskId(), job, request.buildRecordsProcessedSummary()));
    }

    private static void addFilesToStateStore(List<FileReference> fileReferences, StateStore stateStore) throws StateStoreException {
        stateStore.addFiles(fileReferences);
    }

    /**
     * Gets a state store from a Sleeper table ID.
     */
    @FunctionalInterface
    public interface GetStateStore {
        /**
         * Gets a state store from a Sleeper table ID.
         *
         * @param  tableId the Sleeper table ID
         * @return         the state store for that table
         */
        StateStore getByTableId(String tableId);
    }
}
