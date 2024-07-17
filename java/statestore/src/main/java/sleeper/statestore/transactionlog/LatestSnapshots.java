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
import java.util.Optional;

/**
 * Metadata about the latest snapshots in a Sleeper table.
 */
public class LatestSnapshots {
    private final TransactionLogSnapshotMetadata filesSnapshot;
    private final TransactionLogSnapshotMetadata partitionsSnapshot;

    /**
     * Builds metadata for the state where no snapshots have been made for a Sleeper table.
     *
     * @return the metadata
     */
    public static LatestSnapshots empty() {
        return new LatestSnapshots(null, null);
    }

    public LatestSnapshots(TransactionLogSnapshotMetadata filesSnapshot, TransactionLogSnapshotMetadata partitionsSnapshot) {
        this.filesSnapshot = filesSnapshot;
        this.partitionsSnapshot = partitionsSnapshot;
    }

    public Optional<TransactionLogSnapshotMetadata> getFilesSnapshot() {
        return Optional.ofNullable(filesSnapshot);
    }

    public Optional<TransactionLogSnapshotMetadata> getPartitionsSnapshot() {
        return Optional.ofNullable(partitionsSnapshot);
    }

    @Override
    public int hashCode() {
        return Objects.hash(filesSnapshot, partitionsSnapshot);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (!(obj instanceof LatestSnapshots)) {
            return false;
        }
        LatestSnapshots other = (LatestSnapshots) obj;
        return Objects.equals(filesSnapshot, other.filesSnapshot) && Objects.equals(partitionsSnapshot, other.partitionsSnapshot);
    }

    @Override
    public String toString() {
        return "LatestSnapshots{filesSnapshot=" + filesSnapshot + ", partitionsSnapshot=" + partitionsSnapshot + "}";
    }

}
