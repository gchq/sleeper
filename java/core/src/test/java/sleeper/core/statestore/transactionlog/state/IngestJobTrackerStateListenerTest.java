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

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import sleeper.core.partition.PartitionsBuilder;
import sleeper.core.schema.type.LongType;
import sleeper.core.statestore.FileReference;
import sleeper.core.statestore.transactionlog.TransactionLogStateStore;
import sleeper.core.statestore.transactionlog.log.TransactionLogEntry;
import sleeper.core.tracker.compaction.job.CompactionJobTracker;
import sleeper.core.tracker.ingest.job.update.IngestJobEvent;

import java.util.List;

import static sleeper.core.schema.SchemaTestHelper.schemaWithKey;

public class IngestJobTrackerStateListenerTest extends InMemoryTransactionLogStateStoreIngestTrackerTestBase {

    private TransactionLogStateStore committerStore;
    private TransactionLogStateStore followerStore;

    @BeforeEach
    void setUp() {
        initialiseWithPartitions(new PartitionsBuilder(schemaWithKey("key", new LongType())).singlePartition("root"));
        committerStore = (TransactionLogStateStore) super.store;
        followerStore = stateStoreBuilder(schemaWithKey("key", new LongType())).build();
    }

    @Test
    void shouldUpdateIngestJobTrackerBasedOnTransaction() {
        // Given
        FileReference file = factory.rootFile("file.parquet", 100L);
        IngestJobEvent job = trackJobRun("test-job", 1, file);
        committerStore.addFiles(List.of(file));
        TransactionLogEntry logEntry = filesLogStore.getLastEntry();

        // When
        loadNextTransaction(logEntry);

        // Then
        // TODO
    }

    private void loadNextTransaction(TransactionLogEntry entry) {
        followerStore.applyEntryFromLog(entry, StateListenerBeforeApply.updateTrackers(sleeperTable, tracker, CompactionJobTracker.NONE));
    }

}
