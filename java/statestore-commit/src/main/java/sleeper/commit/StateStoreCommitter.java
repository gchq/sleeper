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

import sleeper.compaction.job.CompactionJob;
import sleeper.compaction.job.CompactionJobStatusStore;
import sleeper.compaction.job.commit.CompactionJobCommitRequest;
import sleeper.compaction.job.commit.CompactionJobCommitter;
import sleeper.core.record.process.ProcessRunTime;
import sleeper.core.statestore.AllReferencesToAFile;
import sleeper.core.statestore.GetStateStoreByTableId;
import sleeper.core.statestore.StateStore;
import sleeper.core.statestore.StateStoreException;
import sleeper.core.statestore.exception.FileAlreadyExistsException;
import sleeper.core.statestore.exception.FileNotFoundException;
import sleeper.core.statestore.exception.FileReferenceNotAssignedToJobException;
import sleeper.core.statestore.exception.FileReferenceNotFoundException;
import sleeper.core.statestore.exception.NewReferenceSameAsOldReferenceException;
import sleeper.core.statestore.exception.ReplaceRequestsFailedException;
import sleeper.ingest.job.IngestJob;
import sleeper.ingest.job.commit.IngestAddFilesCommitRequest;
import sleeper.ingest.job.status.IngestJobAddedFilesEvent;
import sleeper.ingest.job.status.IngestJobStatusStore;

import java.time.Instant;
import java.util.List;
import java.util.function.Supplier;

import static sleeper.compaction.job.status.CompactionJobCommittedEvent.compactionJobCommitted;
import static sleeper.compaction.job.status.CompactionJobFailedEvent.compactionJobFailed;

/**
 * Applies a state store commit request.
 */
public class StateStoreCommitter {
    public static final Logger LOGGER = LoggerFactory.getLogger(StateStoreCommitter.class);

    private final CompactionJobStatusStore compactionJobStatusStore;
    private final IngestJobStatusStore ingestJobStatusStore;
    private final GetStateStoreByTableId stateStoreProvider;
    private final Supplier<Instant> timeSupplier;

    public StateStoreCommitter(
            CompactionJobStatusStore compactionJobStatusStore,
            IngestJobStatusStore ingestJobStatusStore,
            GetStateStoreByTableId stateStoreProvider,
            Supplier<Instant> timeSupplier) {
        this.compactionJobStatusStore = compactionJobStatusStore;
        this.ingestJobStatusStore = ingestJobStatusStore;
        this.stateStoreProvider = stateStoreProvider;
        this.timeSupplier = timeSupplier;
    }

    /**
     * Applies a state store commit request.
     *
     * @param request the commit request
     */
    public void apply(StateStoreCommitRequest request) throws StateStoreException {
        Object requestObj = request.getRequest();
        if (requestObj instanceof CompactionJobCommitRequest) {
            apply((CompactionJobCommitRequest) requestObj);
        } else if (requestObj instanceof IngestAddFilesCommitRequest) {
            apply((IngestAddFilesCommitRequest) requestObj);
        }
    }

    private void apply(CompactionJobCommitRequest request) throws StateStoreException {
        CompactionJob job = request.getJob();
        try {
            CompactionJobCommitter.updateStateStoreSuccess(job, request.getRecordsWritten(),
                    stateStoreProvider.getByTableId(job.getTableId()));
            compactionJobStatusStore.jobCommitted(compactionJobCommitted(job).commitTime(timeSupplier.get())
                    .taskId(request.getTaskId()).jobRunId(request.getJobRunId()).build());
            LOGGER.info("Successfully committed compaction job {} to table with ID {}", job.getId(), job.getTableId());
        } catch (ReplaceRequestsFailedException e) {
            Exception failure = e.getFailures().get(0);
            if (failure instanceof FileNotFoundException
                    | failure instanceof FileReferenceNotFoundException
                    | failure instanceof FileReferenceNotAssignedToJobException
                    | failure instanceof NewReferenceSameAsOldReferenceException
                    | failure instanceof FileAlreadyExistsException) {
                compactionJobStatusStore.jobFailed(compactionJobFailed(job,
                        new ProcessRunTime(request.getFinishTime(), timeSupplier.get()))
                        .failure(e.getFailures().get(0))
                        .taskId(request.getTaskId()).jobRunId(request.getJobRunId()).build());
            } else {
                throw e;
            }
        }
    }

    private void apply(IngestAddFilesCommitRequest request) throws StateStoreException {
        StateStore stateStore = stateStoreProvider.getByTableId(request.getTableId());
        List<AllReferencesToAFile> files = AllReferencesToAFile.newFilesWithReferences(request.getFileReferences());
        stateStore.addFilesWithReferences(files);
        IngestJob job = request.getJob();
        if (job != null) {
            ingestJobStatusStore.jobAddedFiles(IngestJobAddedFilesEvent.ingestJobAddedFiles(job, files, request.getWrittenTime())
                    .taskId(request.getTaskId()).jobRunId(request.getJobRunId()).build());
            LOGGER.info("Successfully committed new files for ingest job {} to table with ID {}", job.getId(), request.getTableId());
        } else {
            LOGGER.info("Successfully committed new files for ingest to table with ID {}", request.getTableId());
        }
    }

}
