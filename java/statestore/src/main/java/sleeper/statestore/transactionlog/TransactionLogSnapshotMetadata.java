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

import java.time.Instant;
import java.util.Objects;

/**
 * Metadata about a snapshot derived from a transaction log, to be held in an index.
 */
public class TransactionLogSnapshotMetadata {
    private final String path;
    private final SnapshotType type;
    private final long transactionNumber;
    private final Instant createdTime;

    /**
     * Creates metadata about a snapshot of files. Generates a path to the Arrow file in which the snapshot will be
     * stored.
     *
     * @param  basePath          the base path under which data is held for the given Sleeper table
     * @param  transactionNumber the transaction number the snapshot was made against
     * @return                   the metadata
     */
    public static TransactionLogSnapshotMetadata forFiles(String basePath, long transactionNumber) {
        return new TransactionLogSnapshotMetadata(getFilesPath(basePath, transactionNumber), SnapshotType.FILES, transactionNumber);
    }

    /**
     * Creates metadata about a snapshot of files. Generates a path to the Arrow file in which the snapshot will be
     * stored.
     *
     * @param  basePath          the base path under which data is held for the given Sleeper table
     * @param  transactionNumber the transaction number the snapshot was made against
     * @param  createdTime       the time the snapshot was created
     * @return                   the metadata
     */
    public static TransactionLogSnapshotMetadata forFiles(String basePath, long transactionNumber, Instant createdTime) {
        return new TransactionLogSnapshotMetadata(getFilesPath(basePath, transactionNumber), SnapshotType.FILES, transactionNumber, createdTime);
    }

    /**
     * Creates metadata about a snapshot of partitions. Generates a path to the Arrow file in which the snapshot will be
     * stored.
     *
     * @param  basePath          the base path under which data is held for the given Sleeper table
     * @param  transactionNumber the transaction number the snapshot was made against
     * @return                   the metadata
     */
    public static TransactionLogSnapshotMetadata forPartitions(String basePath, long transactionNumber) {
        return new TransactionLogSnapshotMetadata(getPartitionsPath(basePath, transactionNumber), SnapshotType.PARTITIONS, transactionNumber);
    }

    /**
     * Creates metadata about a snapshot of partitions. Generates a path to the Arrow file in which the snapshot will be
     * stored.
     *
     * @param  basePath          the base path under which data is held for the given Sleeper table
     * @param  transactionNumber the transaction number the snapshot was made against
     * @param  createdTime       the time the snapshot was created
     * @return                   the metadata
     */
    public static TransactionLogSnapshotMetadata forPartitions(String basePath, long transactionNumber, Instant createdTime) {
        return new TransactionLogSnapshotMetadata(getPartitionsPath(basePath, transactionNumber), SnapshotType.PARTITIONS, transactionNumber, createdTime);
    }

    public TransactionLogSnapshotMetadata(String path, SnapshotType type, long transactionNumber) {
        this(path, type, transactionNumber, null);
    }

    public TransactionLogSnapshotMetadata(String path, SnapshotType type, long transactionNumber, Instant createdTime) {
        this.path = path;
        this.type = type;
        this.transactionNumber = transactionNumber;
        this.createdTime = createdTime;
    }

    public String getPath() {
        return path;
    }

    public SnapshotType getType() {
        return type;
    }

    public long getTransactionNumber() {
        return transactionNumber;
    }

    public Instant getCreatedTime() {
        return createdTime;
    }

    private static String getFilesPath(String basePath, long transactionNumber) {
        return basePath + "/statestore/snapshots/" + transactionNumber + "-files.arrow";
    }

    private static String getPartitionsPath(String basePath, long transactionNumber) {
        return basePath + "/statestore/snapshots/" + transactionNumber + "-partitions.arrow";
    }

    @Override
    public int hashCode() {
        return Objects.hash(path, type, transactionNumber, createdTime);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (!(obj instanceof TransactionLogSnapshotMetadata)) {
            return false;
        }
        TransactionLogSnapshotMetadata other = (TransactionLogSnapshotMetadata) obj;
        return Objects.equals(path, other.path) && type == other.type && transactionNumber == other.transactionNumber && Objects.equals(createdTime, other.createdTime);
    }

    @Override
    public String toString() {
        return "TransactionLogSnapshot{path=" + path + ", type=" + type + ", transactionNumber=" + transactionNumber + ", createdTime=" + createdTime + "}";
    }
}
