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
package sleeper.core.statestore.transactionlog.state;

import sleeper.core.statestore.transactionlog.log.TransactionLogEntry;
import sleeper.core.statestore.transactionlog.transaction.impl.ReplaceFileReferencesTransaction;
import sleeper.core.table.TableStatus;
import sleeper.core.tracker.compaction.job.CompactionJobTracker;

/**
 * A listener to update the compaction job tracker based on compaction commit transactions.
 */
public class CompactionJobTrackerStateListener implements StateListenerBeforeApplyByType<StateStoreFiles, ReplaceFileReferencesTransaction> {

    private final TableStatus sleeperTable;
    private final CompactionJobTracker tracker;

    public CompactionJobTrackerStateListener(TableStatus sleeperTable, CompactionJobTracker tracker) {
        this.sleeperTable = sleeperTable;
        this.tracker = tracker;
    }

    @Override
    public void beforeApply(TransactionLogEntry entry, ReplaceFileReferencesTransaction transaction, StateStoreFiles state) {
        transaction.reportJobCommits(tracker, sleeperTable, state, entry.getUpdateTime());
    }

}
