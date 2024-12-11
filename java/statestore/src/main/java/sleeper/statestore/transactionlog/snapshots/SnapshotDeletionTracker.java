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
package sleeper.statestore.transactionlog.snapshots;

/**
 * Tracks and logs deletion of snapshots. Used in TransactionLogSnapshotDeleter implementation when deleting snapshots.
 */
public class SnapshotDeletionTracker {
    private Integer deletedCount = 0;
    private Long lastTransactionNumber = Long.MIN_VALUE;

    /**
     * Stores key details on the success of a deletion process. Increments the count of the deletion and stores
     * the transaction number for the last instance for use in logging.
     *
     * @param transactionNumber most recent transaction number deleted, done so as to save the last one for logging
     */
    public void deleteSuccess(long transactionNumber) {
        this.lastTransactionNumber = transactionNumber;
        deletedCount++;
    }

    public Integer getDeletedCount() {
        return deletedCount;
    }

    /**
     * Returns the number of the last transaction deleted, overwritten as only the latest value is required.
     *
     * @return if a transaction has been deleted then the id of the transaction, otherwise null
     */
    public Long getLastTransactionNumber() {
        if (lastTransactionNumber != Long.MIN_VALUE) {
            return lastTransactionNumber;
        } else {
            return null;
        }
    }
}
