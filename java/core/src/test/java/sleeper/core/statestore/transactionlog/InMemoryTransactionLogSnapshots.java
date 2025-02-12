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
package sleeper.core.statestore.transactionlog;

import sleeper.core.statestore.transactionlog.log.TransactionLogRange;
import sleeper.core.statestore.transactionlog.snapshot.TransactionLogSnapshot;
import sleeper.core.statestore.transactionlog.snapshot.TransactionLogSnapshotLoader;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * An in-memory implementation of snapshots derived from a transaction log.
 */
public class InMemoryTransactionLogSnapshots implements TransactionLogSnapshotLoader {

    private List<TransactionLogSnapshot> snapshots = new ArrayList<>();

    /**
     * Adds a new snapshot if it is later than all previous ones.
     *
     * @param latestSnapshot the snapshot
     */
    public void setLatestSnapshot(TransactionLogSnapshot latestSnapshot) {
        if (isLatest(latestSnapshot)) {
            snapshots.add(latestSnapshot);
        }
    }

    @Override
    public Optional<TransactionLogSnapshot> loadLatestSnapshotInRange(TransactionLogRange range) {
        for (int i = snapshots.size() - 1; i >= 0; i--) { // Reverse order to get the latest first
            TransactionLogSnapshot snapshot = snapshots.get(i);
            long number = snapshot.getTransactionNumber();
            if (number >= range.startInclusive() && (!range.isMaxTransactionBounded() || number < range.endExclusive())) {
                return Optional.of(snapshot);
            }
        }
        return Optional.empty();
    }

    private boolean isLatest(TransactionLogSnapshot snapshot) {
        return snapshots.isEmpty() || snapshots.get(snapshots.size() - 1).getTransactionNumber() < snapshot.getTransactionNumber();
    }
}
