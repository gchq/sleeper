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
package sleeper.core.statestore.transactionlog.snapshot;

import sleeper.core.statestore.transactionlog.log.TransactionLogRange;

import java.util.Optional;

/**
 * Loads the latest snapshot of state derived from a transaction log.
 */
@FunctionalInterface
public interface TransactionLogSnapshotLoader {

    /**
     * Loads the latest snapshot if it's within a given range in the log. This range may be restricted to avoid loading
     * a snapshot if it would be quicker to just seek through the log.
     *
     * @param  range the range to find the latest snapshot within
     * @return       the latest snapshot if there is one within the range
     */
    Optional<TransactionLogSnapshot> loadLatestSnapshotInRange(TransactionLogRange range);

    /**
     * Creates a loader that will always return no snapshot.
     *
     * @return the loader
     */
    static TransactionLogSnapshotLoader neverLoad() {
        return range -> Optional.empty();
    }
}
