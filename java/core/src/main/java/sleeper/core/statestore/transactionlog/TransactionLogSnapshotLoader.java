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

import java.util.Optional;

/**
 * Loads the latest snapshot of state derived from a transaction log.
 */
@FunctionalInterface
public interface TransactionLogSnapshotLoader {

    /**
     * Loads the latest snapshot if it's beyond a certain point in the log. This is used to avoid loading a snapshot
     * if it would be quicker to just seek through the log.
     *
     * @param  transactionNumber the minimum transaction number to load a snapshot
     * @return                   the latest snapshot if it's after the given point in the log
     */
    Optional<TransactionLogSnapshot> loadLatestSnapshotIfAtMinimumTransaction(long transactionNumber);

    /**
     * Creates a loader that will always return no snapshot.
     *
     * @return the loader
     */
    static TransactionLogSnapshotLoader neverLoad() {
        return transactionNumber -> Optional.empty();
    }
}
