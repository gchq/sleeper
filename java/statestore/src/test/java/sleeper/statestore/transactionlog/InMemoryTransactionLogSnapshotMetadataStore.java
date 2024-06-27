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

import sleeper.statestore.transactionlog.TransactionLogTransactionDeleter.GetLatestSnapshotsBefore;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * An in-memory implementation for working with snapshot metadata.
 */
public class InMemoryTransactionLogSnapshotMetadataStore implements GetLatestSnapshotsBefore {
    private final List<TransactionLogSnapshotMetadata> snapshots = new ArrayList<>();

    /**
     * Adds metadata for a files snapshot.
     *
     * @param transactionNumber the transaction number
     * @param createdTime       the created time
     */
    public void addFilesSnapshotAt(int transactionNumber, Instant createdTime) {
        snapshots.add(0, TransactionLogSnapshotMetadata.forFiles("", transactionNumber, createdTime));
    }

    /**
     * Adds metadata for a partitions snapshot.
     *
     * @param transactionNumber the transaction number
     * @param createdTime       the created time
     */
    public void addPartitionsSnapshotAt(int transactionNumber, Instant createdTime) {
        snapshots.add(0, TransactionLogSnapshotMetadata.forPartitions("", transactionNumber, createdTime));
    }

    @Override
    public LatestSnapshots getLatestSnapshotsBefore(Instant time) {
        TransactionLogSnapshotMetadata latestFilesSnapshot = getLatestSnapshotBefore(
                time, SnapshotType.FILES).orElse(null);
        TransactionLogSnapshotMetadata latestPartitionsSnapshot = getLatestSnapshotBefore(
                time, SnapshotType.PARTITIONS).orElse(null);
        return new LatestSnapshots(latestFilesSnapshot, latestPartitionsSnapshot);
    }

    private Optional<TransactionLogSnapshotMetadata> getLatestSnapshotBefore(Instant time, SnapshotType type) {
        return snapshots.stream()
                .filter(snapshot -> snapshot.getType() == type)
                .filter(snapshot -> snapshot.getCreatedTime().isBefore(time))
                .findFirst();
    }

}
