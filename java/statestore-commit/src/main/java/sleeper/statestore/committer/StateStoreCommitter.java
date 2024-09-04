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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sleeper.compaction.job.CompactionJob;
import sleeper.compaction.job.CompactionJobStatusStore;
import sleeper.compaction.job.commit.CompactionJobCommitRequest;
import sleeper.compaction.job.commit.CompactionJobCommitter;
import sleeper.compaction.job.commit.CompactionJobIdAssignmentCommitRequest;
import sleeper.configuration.properties.table.TableProperties;
import sleeper.configuration.properties.table.TablePropertiesProvider;
import sleeper.configuration.statestore.StateStoreProvider;
import sleeper.core.record.process.ProcessRunTime;
import sleeper.core.statestore.AllReferencesToAFile;
import sleeper.core.statestore.StateStore;
import sleeper.core.statestore.StateStoreException;
import sleeper.core.statestore.commit.GarbageCollectionCommitRequest;
import sleeper.core.statestore.commit.SplitPartitionCommitRequest;
import sleeper.core.statestore.transactionlog.TransactionLogStateStore;
import sleeper.ingest.job.IngestJob;
import sleeper.ingest.job.commit.IngestAddFilesCommitRequest;
import sleeper.ingest.job.status.IngestJobAddedFilesEvent;
import sleeper.ingest.job.status.IngestJobStatusStore;

import java.time.Instant;
import java.util.List;
import java.util.function.Consumer;
import java.util.function.Supplier;

import static sleeper.compaction.job.status.CompactionJobCommittedEvent.compactionJobCommitted;
import static sleeper.compaction.job.status.CompactionJobFailedEvent.compactionJobFailed;
import static sleeper.configuration.properties.table.TableProperty.STATESTORE_COMMITTER_UPDATE_ON_EVERY_BATCH;

/**
 * Applies a state store commit request.
 */
public class StateStoreCommitter {
    public static final Logger LOGGER = LoggerFactory.getLogger(StateStoreCommitter.class);

    private final CompactionJobStatusStore compactionJobStatusStore;
    private final IngestJobStatusStore ingestJobStatusStore;
    private final TablePropertiesProvider tablePropertiesProvider;
    private final StateStoreProvider stateStoreProvider;
    private final Supplier<Instant> timeSupplier;

    public StateStoreCommitter(
            CompactionJobStatusStore compactionJobStatusStore,
            IngestJobStatusStore ingestJobStatusStore,
            TablePropertiesProvider tablePropertiesProvider,
            StateStoreProvider stateStoreProvider,
            Supplier<Instant> timeSupplier) {
        this.compactionJobStatusStore = compactionJobStatusStore;
        this.ingestJobStatusStore = ingestJobStatusStore;
        this.tablePropertiesProvider = tablePropertiesProvider;
        this.stateStoreProvider = stateStoreProvider;
        this.timeSupplier = timeSupplier;
    }

    /**
     * Applies a batch of state store commit requests.
     *
     * @param retryOnThrottling function to apply retries due to DynamoDB API throttling
     * @param requests          the commit requests
     */
    public void applyBatch(RetryOnThrottling retryOnThrottling, List<RequestHandle> requests) {
        updateBeforeBatch(requests);
        for (int i = 0; i < requests.size(); i++) {
            RequestHandle handle = requests.get(i);
            try {
                retryOnThrottling.doWithRetries(() -> {
                    try {
                        apply(handle.request());
                    } catch (StateStoreException e) {
                        throw new RuntimeException(e);
                    }
                });
            } catch (InterruptedException e) {
                LOGGER.error("Interrupted applying commit request", e);
                requests.subList(i, requests.size())
                        .forEach(failed -> failed.failed(e));
                Thread.currentThread().interrupt();
                break;
            } catch (RuntimeException e) {
                LOGGER.error("Failed commit request", e);
                handle.failed(e);
            }
        }
    }

    private void updateBeforeBatch(List<RequestHandle> requests) {
        requests.stream()
                .map(handle -> handle.request().getTableId()).distinct()
                .forEach(this::updateBeforeBatchForTable);
    }

    private void updateBeforeBatchForTable(String tableId) {
        TableProperties tableProperties = tablePropertiesProvider.getById(tableId);
        if (!tableProperties.getBoolean(STATESTORE_COMMITTER_UPDATE_ON_EVERY_BATCH)) {
            return;
        }
        StateStore stateStore = stateStoreProvider.getStateStore(tableProperties);
        if (!(stateStore instanceof TransactionLogStateStore)) {
            return;
        }
        TransactionLogStateStore state = (TransactionLogStateStore) stateStore;
        try {
            state.updateFromLogs();
        } catch (StateStoreException e) {
            throw new RuntimeException("Failed updating state store at start of batch", e);
        }
    }

    /**
     * Applies a state store commit request.
     *
     * @param request the commit request
     */
    public void apply(StateStoreCommitRequest request) throws StateStoreException {
        request.apply(this);
        LOGGER.info("Applied request to table ID {} with type {} at time {}",
                request.getTableId(), request.getRequest().getClass().getSimpleName(), Instant.now());
    }

    void commitCompaction(CompactionJobCommitRequest request) throws StateStoreException {
        CompactionJob job = request.getJob();
        StateStore stateStore = stateStore(job.getTableId());
        try {
            CompactionJobCommitter.updateStateStoreSuccess(job, request.getRecordsWritten(), stateStore);
        } catch (Exception e) {
            compactionJobStatusStore.jobFailed(compactionJobFailed(job,
                    new ProcessRunTime(request.getFinishTime(), timeSupplier.get()))
                    .failure(e)
                    .taskId(request.getTaskId())
                    .jobRunId(request.getJobRunId())
                    .build());
            throw e;
        }
        compactionJobStatusStore.jobCommitted(compactionJobCommitted(job, timeSupplier.get())
                .taskId(request.getTaskId()).jobRunId(request.getJobRunId()).build());
        LOGGER.debug("Successfully committed compaction job {}", job.getId());
    }

    void addFiles(IngestAddFilesCommitRequest request) throws StateStoreException {
        StateStore stateStore = stateStore(request.getTableId());
        List<AllReferencesToAFile> files = AllReferencesToAFile.newFilesWithReferences(request.getFileReferences());
        stateStore.addFilesWithReferences(files);
        IngestJob job = request.getJob();
        if (job != null) {
            ingestJobStatusStore.jobAddedFiles(IngestJobAddedFilesEvent.ingestJobAddedFiles(job, files, request.getWrittenTime())
                    .taskId(request.getTaskId()).jobRunId(request.getJobRunId()).build());
            LOGGER.debug("Successfully committed new files for ingest job {}", job.getId());
        } else {
            LOGGER.debug("Successfully committed new files for ingest with no job");
        }
    }

    void splitPartition(SplitPartitionCommitRequest request) throws StateStoreException {
        StateStore stateStore = stateStore(request.getTableId());
        stateStore.atomicallyUpdatePartitionAndCreateNewOnes(request.getParentPartition(), request.getLeftChild(), request.getRightChild());
    }

    void assignCompactionInputFiles(CompactionJobIdAssignmentCommitRequest request) throws StateStoreException {
        StateStore stateStore = stateStore(request.getTableId());
        stateStore.assignJobIds(request.getAssignJobIdRequests());
    }

    void filesDeleted(GarbageCollectionCommitRequest request) throws StateStoreException {
        StateStore stateStore = stateStore(request.getTableId());
        stateStore.deleteGarbageCollectedFileReferenceCounts(request.getFilenames());
    }

    private StateStore stateStore(String tableId) {
        TableProperties tableProperties = tablePropertiesProvider.getById(tableId);
        return stateStoreProvider.getStateStore(tableProperties);
    }

    /**
     * Wraps a commit request to track callbacks as a request handle.
     */
    public static class RequestHandle {
        private StateStoreCommitRequest request;
        private Consumer<Exception> onFail;

        private RequestHandle(StateStoreCommitRequest request, Consumer<Exception> onFail) {
            this.request = request;
            this.onFail = onFail;
        }

        /**
         * Creates a request handle.
         *
         * @param  request the request
         * @param  onFail  the callback to run if the request failed
         * @return         the handle
         */
        public static RequestHandle withCallbackOnFail(StateStoreCommitRequest request, Runnable onFail) {
            return new RequestHandle(request, e -> onFail.run());
        }

        /**
         * Creates a request handle.
         *
         * @param  request the request
         * @param  onFail  the callback to run if the request failed
         * @return         the handle
         */
        public static RequestHandle withCallbackOnFail(StateStoreCommitRequest request, Consumer<Exception> onFail) {
            return new RequestHandle(request, onFail);
        }

        private StateStoreCommitRequest request() {
            return request;
        }

        private void failed(Exception exception) {
            onFail.accept(exception);
        }
    }

    /**
     * Retries if the DynamoDB table is throttled. This is applied when updating the local state from the transaction
     * log, and when applying commit requests.
     */
    @FunctionalInterface
    public interface RetryOnThrottling {

        /**
         * Apply the given operation.
         *
         * @param  runnable             the operation to apply with retries
         * @throws InterruptedException if the retries were interrupted
         */
        void doWithRetries(Runnable runnable) throws InterruptedException;
    }
}
