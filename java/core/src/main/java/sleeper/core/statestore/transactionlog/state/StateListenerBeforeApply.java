/*
 * Copyright 2022-2025 Crown Copyright
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
package sleeper.core.statestore.transactionlog.state;

import sleeper.core.statestore.transactionlog.log.TransactionLogEntry;
import sleeper.core.statestore.transactionlog.transaction.FileReferenceTransaction;
import sleeper.core.statestore.transactionlog.transaction.PartitionTransaction;
import sleeper.core.statestore.transactionlog.transaction.StateStoreTransaction;
import sleeper.core.statestore.transactionlog.transaction.impl.AddFilesTransaction;
import sleeper.core.statestore.transactionlog.transaction.impl.ReplaceFileReferencesTransaction;
import sleeper.core.table.TableStatus;
import sleeper.core.tracker.compaction.job.CompactionJobTracker;
import sleeper.core.tracker.ingest.job.IngestJobTracker;

import java.util.List;
import java.util.function.Consumer;

/**
 * Listens to see the state before a transaction is applied to the local state. This is when the transaction is in the
 * log, but before it is applied locally.
 */
@FunctionalInterface
public interface StateListenerBeforeApply {

    /**
     * Informs the listener that the transaction is about to be applied to the local state.
     *
     * @param entry       the log entry
     * @param transaction the transaction
     * @param state       the state
     */
    void beforeApply(TransactionLogEntry entry, StateStoreTransaction<?> transaction, Object state);

    /**
     * Creates a transaction listener that does nothing.
     *
     * @return the listener
     */
    static StateListenerBeforeApply none() {
        return (entry, transaction, state) -> {
        };
    }

    /**
     * Creates a transaction listener that updates job trackers.
     *
     * @param  sleeperTable      the Sleeper table status
     * @param  ingestTracker     the ingest job tracker
     * @param  compactionTracker the compaction job tracker
     * @return                   the listener
     */
    static StateListenerBeforeApply updateTrackers(
            TableStatus sleeperTable, IngestJobTracker ingestTracker, CompactionJobTracker compactionTracker) {
        return and(List.of(
                byTransactionType(ReplaceFileReferencesTransaction.class,
                        new CompactionJobTrackerStateListener(sleeperTable, compactionTracker)),
                byTransactionType(AddFilesTransaction.class,
                        new IngestJobTrackerStateListener(sleeperTable, ingestTracker))));
    }

    /**
     * Creates a transaction listener that operates on just the state of partitions.
     *
     * @return the listener
     */
    static StateListenerBeforeApply withPartitionsState(Consumer<StateStorePartitions> run) {
        return byTransactionType(PartitionTransaction.class,
                (TransactionLogEntry entry, PartitionTransaction transaction, StateStorePartitions state) -> run.accept(state));
    }

    /**
     * Creates a transaction listener that operates on just the state of files.
     *
     * @return the listener
     */
    static StateListenerBeforeApply withFilesState(Consumer<StateStoreFiles> run) {
        return byTransactionType(FileReferenceTransaction.class,
                (TransactionLogEntry entry, FileReferenceTransaction transaction, StateStoreFiles state) -> run.accept(state));
    }

    /**
     * Creates a transaction listener that runs if the transaction is a certain type.
     *
     * @param  <S> the type of state the transaction operates on
     * @param  <T> the type of the transaction
     * @return     the listener
     */
    static <S, T extends StateStoreTransaction<S>> StateListenerBeforeApply byTransactionType(Class<T> transactionType, StateListenerBeforeApplyByType<S, T> listener) {
        return (entry, transaction, state) -> {
            if (transactionType.isInstance(transaction)) {
                listener.beforeApply(entry, transactionType.cast(transaction), (S) state);
            }
        };
    }

    /**
     * Creates a transaction listener that runs a number of other listeners in sequence.
     *
     * @param  <S>       the type of state the transaction operates on
     * @param  listeners the listeners to run
     * @return           the listener
     */
    static <S> StateListenerBeforeApply and(List<StateListenerBeforeApply> listeners) {
        return (entry, transaction, state) -> listeners.forEach(listener -> listener.beforeApply(entry, transaction, state));
    }
}
