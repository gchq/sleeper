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
package sleeper.statestore.transactionlog;

import sleeper.configuration.properties.table.TableProperties;
import sleeper.core.statestore.transactionlog.TransactionLogStore;

import java.time.Duration;
import java.time.Instant;
import java.util.function.Supplier;

import static sleeper.configuration.properties.table.TableProperty.TRANSACTION_LOG_NUMBER_BEHIND_TO_DELETE;
import static sleeper.configuration.properties.table.TableProperty.TRANSACTION_LOG_SNAPSHOT_MIN_AGE_MINUTES_TO_DELETE_TRANSACTIONS;

/**
 * Given a transaction log state store, finds transactions that are old enough to be deleted and deletes them.
 */
public class TransactionLogTransactionDeleter {
    private final TableProperties tableProperties;
    private final Supplier<Instant> timeSupplier;

    public TransactionLogTransactionDeleter(TableProperties tableProperties, Supplier<Instant> timeSupplier) {
        this.tableProperties = tableProperties;
        this.timeSupplier = timeSupplier;
    }

    /**
     * Finds transactions that are old enough to be deleted and deletes them.
     *
     * @param filesLogStore      the files transaction log store
     * @param partitionsLogStore the partitions transaction log store
     * @param getLatestSnapshots the method to find the latest snapshots
     */
    public void deleteWithLatestSnapshots(TransactionLogStore filesLogStore, TransactionLogStore partitionsLogStore, GetLatestSnapshotsBefore getLatestSnapshots) {
        Duration minSnapshotAge = Duration.ofMinutes(tableProperties.getLong(TRANSACTION_LOG_SNAPSHOT_MIN_AGE_MINUTES_TO_DELETE_TRANSACTIONS));
        Instant maxSnapshotTime = timeSupplier.get().minus(minSnapshotAge);
        LatestSnapshots latestSnapshots = getLatestSnapshots.getLatestSnapshotsBefore(maxSnapshotTime);
        latestSnapshots.getFilesSnapshot().ifPresent(snapshot -> deleteWithLatestSnapshot(filesLogStore, snapshot));
        latestSnapshots.getPartitionsSnapshot().ifPresent(snapshot -> deleteWithLatestSnapshot(partitionsLogStore, snapshot));
    }

    /**
     * Finds transactions that are old enough to be deleted and deletes them.
     *
     * @param logStore       the transaction log store
     * @param latestSnapshot the latest snapshot metadata, or null if there is no snapshot
     */
    private void deleteWithLatestSnapshot(TransactionLogStore logStore, TransactionLogSnapshotMetadata latestSnapshot) {
        long numBehindToDelete = tableProperties.getLong(TRANSACTION_LOG_NUMBER_BEHIND_TO_DELETE);
        long latestNumber = latestSnapshot.getTransactionNumber() - numBehindToDelete;
        logStore.deleteTransactionsAtOrBefore(latestNumber);
    }

    /**
     * Retrieves the latest snapshots with a minimum age.
     */
    public interface GetLatestSnapshotsBefore {

        /**
         * Get the latest snapshots with a minimum age.
         *
         * @param  time the maximum created time
         * @return      the latest snapshots
         */
        LatestSnapshots getLatestSnapshotsBefore(Instant time);
    }

}
