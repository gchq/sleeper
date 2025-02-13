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
import sleeper.core.statestore.transactionlog.transaction.impl.AddFilesTransaction;
import sleeper.core.table.TableStatus;
import sleeper.core.tracker.ingest.job.IngestJobTracker;

/**
 * A listener to update the ingest job tracker based on ingest commit transactions.
 */
public class IngestJobTrackerStateListener implements StateListenerBeforeApplyByType<StateStoreFiles, AddFilesTransaction> {

    private final TableStatus sleeperTable;
    private final IngestJobTracker tracker;

    public IngestJobTrackerStateListener(TableStatus sleeperTable, IngestJobTracker tracker) {
        this.sleeperTable = sleeperTable;
        this.tracker = tracker;
    }

    @Override
    public void beforeApply(TransactionLogEntry entry, AddFilesTransaction transaction, StateStoreFiles state) {
        transaction.reportJobCommitted(tracker, sleeperTable);
    }

}
