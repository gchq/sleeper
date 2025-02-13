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
import sleeper.core.tracker.compaction.job.update.CompactionJobCreatedEvent;
import sleeper.core.tracker.ingest.job.IngestJobTracker;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.core.schema.SchemaTestHelper.schemaWithKey;
import static sleeper.core.statestore.AssignJobIdRequest.assignJobOnPartitionToFiles;

public class CompactionJobCommitFollowTransactionTest extends InMemoryTransactionLogStateStoreCompactionTrackerTestBase {

    private TransactionLogStateStore committerStore;
    private TransactionLogStateStore followerStore;

    @BeforeEach
    void setUp() {
        initialiseWithPartitions(new PartitionsBuilder(schemaWithKey("key", new LongType())).singlePartition("root"));
        committerStore = (TransactionLogStateStore) super.store;
        followerStore = stateStoreBuilder(schemaWithKey("key", new LongType())).build();
    }

    @Test
    void shouldUpdateCompactionJobTrackerBasedOnTransaction() {
        // Given
        FileReference oldFile = factory.rootFile("oldFile", 100L);
        FileReference newFile = factory.rootFile("newFile", 100L);
        committerStore.addFiles(List.of(oldFile));
        committerStore.assignJobIds(List.of(
                assignJobOnPartitionToFiles("job1", "root", List.of("oldFile"))));
        CompactionJobCreatedEvent trackedJob = trackJobCreated("job1", "root", 1);
        trackJobRun(trackedJob, "test-run");
        committerStore.fixFileUpdateTime(DEFAULT_COMMIT_TIME);
        committerStore.atomicallyReplaceFileReferencesWithNewOnes(List.of(
                replaceJobFileReferencesBuilder("job1", List.of("oldFile"), newFile).jobRunId("test-run").build()));
        TransactionLogEntry logEntry = filesLogStore.getLastEntry();

        // When
        loadNextTransaction(logEntry);

        // Then
        assertThat(tracker.getAllJobs(sleeperTable.getTableUniqueId()))
                .containsExactly(defaultStatus(trackedJob, defaultCommittedRun(100)));
    }

    private void loadNextTransaction(TransactionLogEntry entry) {
        followerStore.applyEntryFromLog(entry, StateListenerBeforeApply.updateTrackers(sleeperTable, IngestJobTracker.NONE, tracker));
    }

}
