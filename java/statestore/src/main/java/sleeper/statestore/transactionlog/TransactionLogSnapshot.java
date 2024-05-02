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

import java.util.Objects;

public class TransactionLogSnapshot {
    private final String path;
    private final SnapshotType type;
    private final long transactionNumber;

    public static TransactionLogSnapshot forFiles(String basePath, long transactionNumber) {
        return new TransactionLogSnapshot(getFilesPath(basePath, transactionNumber), SnapshotType.FILES, transactionNumber);
    }

    public static TransactionLogSnapshot forPartitions(String basePath, long transactionNumber) {
        return new TransactionLogSnapshot(getPartitionsPath(basePath, transactionNumber), SnapshotType.PARTITIONS, transactionNumber);
    }

    public TransactionLogSnapshot(String path, SnapshotType type, long transactionNumber) {
        this.path = path;
        this.type = type;
        this.transactionNumber = transactionNumber;
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

    private static String getFilesPath(String basePath, long transactionNumber) {
        return basePath + "/statestore/snapshots/" + transactionNumber + "-files.parquet";
    }

    private static String getPartitionsPath(String basePath, long transactionNumber) {
        return basePath + "/statestore/snapshots/" + transactionNumber + "-partitions.parquet";
    }

    @Override
    public int hashCode() {
        return Objects.hash(path, type, transactionNumber);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (!(obj instanceof TransactionLogSnapshot)) {
            return false;
        }
        TransactionLogSnapshot other = (TransactionLogSnapshot) obj;
        return Objects.equals(path, other.path) && type == other.type && transactionNumber == other.transactionNumber;
    }

    @Override
    public String toString() {
        return "TransactionLogSnapshot{path=" + path + ", type=" + type + ", transactionNumber=" + transactionNumber + "}";
    }
}
