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

import sleeper.compaction.core.job.CompactionJob;
import sleeper.compaction.core.job.commit.CompactionJobCommitRequest;
import sleeper.compaction.core.job.commit.CompactionJobIdAssignmentCommitRequest;
import sleeper.core.properties.table.TableProperties;
import sleeper.core.properties.table.TablePropertiesProvider;
import sleeper.core.statestore.AllReferencesToAFile;
import sleeper.core.statestore.StateStore;
import sleeper.core.statestore.StateStoreException;
import sleeper.core.statestore.StateStoreProvider;
import sleeper.core.statestore.commit.GarbageCollectionCommitRequest;
import sleeper.core.statestore.commit.SplitPartitionCommitRequest;
import sleeper.core.statestore.commit.StateStoreCommitRequestByTransaction;
import sleeper.core.statestore.transactionlog.AddTransactionRequest;
import sleeper.core.statestore.transactionlog.TransactionBodyStore;
import sleeper.core.statestore.transactionlog.TransactionBodyStoreProvider;
import sleeper.core.statestore.transactionlog.TransactionLogStateStore;
import sleeper.core.statestore.transactionlog.transactions.ReplaceFileReferencesTransaction;
import sleeper.core.statestore.transactionlog.transactions.TransactionType;
import sleeper.core.tracker.compaction.job.CompactionJobTracker;
import sleeper.core.tracker.ingest.job.IngestJobTracker;
import sleeper.core.tracker.job.run.JobRunTime;
import sleeper.ingest.core.job.IngestJob;
import sleeper.ingest.core.job.commit.IngestAddFilesCommitRequest;

import java.time.Instant;
import java.util.List;
import java.util.function.Consumer;
import java.util.function.Supplier;

import static sleeper.core.properties.table.TableProperty.STATESTORE_COMMITTER_UPDATE_ON_EVERY_BATCH;

/**
 * Applies a state store commit request.
 */
public class StateStoreCommitter {
    public static final Logger LOGGER = LoggerFactory.getLogger(StateStoreCommitter.class);

    private final CompactionJobTracker compactionJobTracker;
    private final IngestJobTracker ingestJobTracker;
    private final TablePropertiesProvider tablePropertiesProvider;
    private final StateStoreProvider stateStoreProvider;
    private final TransactionBodyStoreProvider transactionBodyStoreProvider;
    private final Supplier<Instant> timeSupplier;

    public StateStoreCommitter(
            CompactionJobTracker compactionJobTracker,
            IngestJobTracker ingestJobTracker,
            TablePropertiesProvider tablePropertiesProvider,
            StateStoreProvider stateStoreProvider,
            TransactionBodyStoreProvider transactionBodyStoreProvider,
            Supplier<Instant> timeSupplier) {
        this.compactionJobTracker = compactionJobTracker;
        this.ingestJobTracker = ingestJobTracker;
        this.tablePropertiesProvider = tablePropertiesProvider;
        this.stateStoreProvider = stateStoreProvider;
        this.transactionBodyStoreProvider = transactionBodyStoreProvider;
        this.timeSupplier = timeSupplier;
    }

    /**
     * Applies a batch of state store commit requests.
     *
     * @param retryOnThrottling function to apply retries due to DynamoDB API throttling
     * @param requests          the commit requests
     */
    public void applyBatch(RetryOnThrottling retryOnThrottling, List<RequestHandle> requests) {
        updateBeforeBatch(retryOnThrottling, requests);
        for (int i = 0; i < requests.size(); i++) {
            RequestHandle handle = requests.get(i);
            try {
                retryOnThrottling.doWithRetries(() -> apply(handle.request()));
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

    private void updateBeforeBatch(RetryOnThrottling retryOnThrottling, List<RequestHandle> requests) {
        requests.stream()
                .map(handle -> handle.request().getTableId()).distinct()
                .forEach(tableId -> updateBeforeBatchForTable(retryOnThrottling, tableId));
    }

    private void updateBeforeBatchForTable(RetryOnThrottling retryOnThrottling, String tableId) {
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
            retryOnThrottling.doWithRetries(() -> state.updateFromLogs());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
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
            stateStore.atomicallyReplaceFileReferencesWithNewOnes(
                    List.of(job.replaceFileReferencesRequestBuilder(request.getRecordsWritten()).build()));
        } catch (Exception e) {
            compactionJobTracker.jobFailed(job
                    .failedEventBuilder(new JobRunTime(request.getFinishTime(), timeSupplier.get()))
                    .failure(e)
                    .taskId(request.getTaskId())
                    .jobRunId(request.getJobRunId())
                    .build());
            throw e;
        }
        compactionJobTracker.jobCommitted(job.committedEventBuilder(timeSupplier.get())
                .taskId(request.getTaskId()).jobRunId(request.getJobRunId()).build());
        LOGGER.debug("Successfully committed compaction job {}", job.getId());
    }

    void addFiles(IngestAddFilesCommitRequest request) throws StateStoreException {
        StateStore stateStore = stateStore(request.getTableId());
        List<AllReferencesToAFile> files = AllReferencesToAFile.newFilesWithReferences(request.getFileReferences());
        stateStore.addFilesWithReferences(files);
        IngestJob job = request.getJob();
        if (job != null) {
            ingestJobTracker.jobAddedFiles(job.addedFilesEventBuilder(request.getWrittenTime()).files(files)
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

    void addTransaction(StateStoreCommitRequestByTransaction request) throws StateStoreException {
        TableProperties tableProperties = tablePropertiesProvider.getById(request.getTableId());
        StateStore stateStore = stateStoreProvider.getStateStore(tableProperties);
        TransactionBodyStore transactionBodyStore = transactionBodyStoreProvider.getTransactionBodyStore(tableProperties);
        if (stateStore instanceof TransactionLogStateStore) {
            TransactionLogStateStore transactionStateStore = (TransactionLogStateStore) stateStore;
            if (request.getTransactionType() == TransactionType.REPLACE_FILE_REFERENCES) {
                ReplaceFileReferencesTransaction transaction = request.getTransaction(transactionBodyStore);
                AddTransactionRequest addTransaction = AddTransactionRequest.withTransaction(transaction)
                        .bodyKey(request.getBodyKey())
                        .beforeApplyListener((number, state) -> transaction.reportJobCommits(
                                compactionJobTracker, tableProperties.getStatus(), state, timeSupplier.get()))
                        .build();
                transactionStateStore.addTransaction(addTransaction);
            } else {
                transactionStateStore.addTransaction(AddTransactionRequest.transactionInBucket(
                        request.getBodyKey(), request.getTransaction(transactionBodyStore)));
            }
        } else {
            throw new UnsupportedOperationException("Cannot add a transaction for a non-transaction log state store");
        }
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
         * Apply the given operation. Will examine any exception thrown by the given operation. If the exception or any
         * cause is a throttling exception, the operation will be retried up to a point.
         *
         * @param  runnable             the operation to apply with retries
         * @throws InterruptedException if the retries were interrupted
         */
        void doWithRetries(Runnable runnable) throws InterruptedException;
    }
}
