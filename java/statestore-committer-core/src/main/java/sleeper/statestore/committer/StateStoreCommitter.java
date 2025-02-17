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

import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.table.TableProperties;
import sleeper.core.properties.table.TablePropertiesProvider;
import sleeper.core.statestore.StateStore;
import sleeper.core.statestore.StateStoreException;
import sleeper.core.statestore.StateStoreProvider;
import sleeper.core.statestore.commit.StateStoreCommitRequest;
import sleeper.core.statestore.transactionlog.AddTransactionRequest;
import sleeper.core.statestore.transactionlog.TransactionLogStateStore;
import sleeper.core.statestore.transactionlog.log.TransactionBodyStore;
import sleeper.core.statestore.transactionlog.state.StateListenerBeforeApply;
import sleeper.core.statestore.transactionlog.transaction.TransactionType;
import sleeper.core.statestore.transactionlog.transaction.impl.AddFilesTransaction;
import sleeper.core.statestore.transactionlog.transaction.impl.ReplaceFileReferencesTransaction;
import sleeper.core.tracker.compaction.job.CompactionJobTracker;
import sleeper.core.tracker.ingest.job.IngestJobTracker;

import java.time.Instant;
import java.util.List;
import java.util.function.Consumer;
import java.util.function.Supplier;

import static sleeper.core.properties.instance.CompactionProperty.COMPACTION_TRACKER_ASYNC_COMMIT_UPDATES_ENABLED;
import static sleeper.core.properties.table.TableProperty.STATESTORE_COMMITTER_UPDATE_ON_EVERY_BATCH;

/**
 * Applies a state store commit request.
 */
public class StateStoreCommitter {
    public static final Logger LOGGER = LoggerFactory.getLogger(StateStoreCommitter.class);

    private final InstanceProperties instanceProperties;
    private final TablePropertiesProvider tablePropertiesProvider;
    private final StateStoreProvider stateStoreProvider;
    private final CompactionJobTracker compactionJobTracker;
    private final IngestJobTracker ingestJobTracker;
    private final TransactionBodyStore transactionBodyStore;
    private final Supplier<Instant> timeSupplier;

    public StateStoreCommitter(
            InstanceProperties instanceProperties,
            TablePropertiesProvider tablePropertiesProvider,
            StateStoreProvider stateStoreProvider,
            CompactionJobTracker compactionJobTracker,
            IngestJobTracker ingestJobTracker,
            TransactionBodyStore transactionBodyStore,
            Supplier<Instant> timeSupplier) {
        this.instanceProperties = instanceProperties;
        this.tablePropertiesProvider = tablePropertiesProvider;
        this.stateStoreProvider = stateStoreProvider;
        this.compactionJobTracker = compactionJobTracker;
        this.ingestJobTracker = ingestJobTracker;
        this.transactionBodyStore = transactionBodyStore;
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
        TableProperties tableProperties = tablePropertiesProvider.getById(request.getTableId());
        StateStore stateStore = stateStoreProvider.getStateStore(tableProperties);
        if (!(stateStore instanceof TransactionLogStateStore)) {
            throw new UnsupportedOperationException("Cannot add a transaction for a non-transaction log state store");
        }
        TransactionLogStateStore transactionStateStore = (TransactionLogStateStore) stateStore;
        if (request.getTransactionType() == TransactionType.REPLACE_FILE_REFERENCES &&
                instanceProperties.getBoolean(COMPACTION_TRACKER_ASYNC_COMMIT_UPDATES_ENABLED)) {
            commitCompaction(request, tableProperties, transactionStateStore);
        } else if (request.getTransactionType() == TransactionType.ADD_FILES) {
            commitIngest(request, tableProperties, transactionStateStore);
        } else {
            transactionStateStore.addTransaction(
                    AddTransactionRequest.withTransaction(transactionBodyStore.getTransaction(request))
                            .bodyKey(request.getBodyKey())
                            .build());
        }
        LOGGER.info("Applied request to table ID {} with type {} at time {}",
                request.getTableId(), request.getTransactionType(), Instant.now());
    }

    private void commitCompaction(StateStoreCommitRequest request, TableProperties tableProperties, TransactionLogStateStore stateStore) {
        ReplaceFileReferencesTransaction transaction = transactionBodyStore.getTransaction(request);
        AddTransactionRequest addTransaction = AddTransactionRequest.withTransaction(transaction)
                .bodyKey(request.getBodyKey())
                .beforeApplyListener(StateListenerBeforeApply.withFilesState(state -> transaction.reportJobCommits(
                        compactionJobTracker, tableProperties.getStatus(), state, timeSupplier.get())))
                .build();
        try {
            stateStore.addTransaction(addTransaction);
        } catch (Exception e) {
            transaction.reportJobsAllFailed(compactionJobTracker, tableProperties.getStatus(), timeSupplier.get(), e);
            throw e;
        }
    }

    private void commitIngest(StateStoreCommitRequest request, TableProperties tableProperties, TransactionLogStateStore stateStore) {
        AddFilesTransaction transaction = transactionBodyStore.getTransaction(request);
        AddTransactionRequest addTransaction = AddTransactionRequest.withTransaction(transaction)
                .bodyKey(request.getBodyKey())
                .beforeApplyListener(StateListenerBeforeApply.withFilesState(
                        state -> transaction.reportJobCommitOrThrow(ingestJobTracker, tableProperties.getStatus(), state)))
                .build();
        try {
            stateStore.addTransaction(addTransaction);
        } catch (Exception e) {
            transaction.reportJobFailed(ingestJobTracker, tableProperties.getStatus(), e);
            throw e;
        }
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
