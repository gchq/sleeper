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

import static sleeper.configuration.properties.table.TableProperty.TRANSACTION_LOG_MINUTES_BEHIND_TO_DELETE;
import static sleeper.configuration.properties.table.TableProperty.TRANSACTION_LOG_NUMBER_BEHIND_TO_DELETE;

/**
 * Given a state store, finds transactions that are old enough to be deleted and deletes them.
 */
public class TransactionLogTransactionDeleter {
    private final TableProperties tableProperties;

    public TransactionLogTransactionDeleter(TableProperties tableProperties) {
        this.tableProperties = tableProperties;
    }

    /**
     * Finds transactions that are old enough to be deleted and deletes them.
     *
     * @param logStore       the transaction log store
     * @param latestSnapshot the latest snapshot metadata, or null if there is no snapshot
     */
    public void deleteWithLatestSnapshot(TransactionLogStore logStore, TransactionLogSnapshotMetadata latestSnapshot) {
        if (latestSnapshot == null) {
            return;
        }
        long numBehindToDelete = tableProperties.getLong(TRANSACTION_LOG_NUMBER_BEHIND_TO_DELETE);
        Duration timeBehindToDelete = Duration.ofMinutes(tableProperties.getLong(TRANSACTION_LOG_MINUTES_BEHIND_TO_DELETE));
        long latestNumber = latestSnapshot.getTransactionNumber() - numBehindToDelete;
        Instant latestTime = latestSnapshot.getCreatedTime().minus(timeBehindToDelete);
        logStore.deleteTransactionsAtOrBefore(latestNumber, latestTime);
    }

}
