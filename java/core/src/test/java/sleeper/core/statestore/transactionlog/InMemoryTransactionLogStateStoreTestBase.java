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

import sleeper.core.partition.PartitionsBuilder;
import sleeper.core.schema.Schema;
import sleeper.core.statestore.FileReferenceFactory;
import sleeper.core.statestore.StateStore;
import sleeper.core.statestore.StateStoreException;
import sleeper.core.table.TableStatus;
import sleeper.core.util.ExponentialBackoffWithJitter;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

import static sleeper.core.statestore.FileReferenceTestData.DEFAULT_UPDATE_TIME;
import static sleeper.core.table.TableStatusTestHelper.uniqueIdAndName;
import static sleeper.core.util.ExponentialBackoffWithJitterTestHelper.fixJitterSeed;
import static sleeper.core.util.ExponentialBackoffWithJitterTestHelper.recordWaits;

public class InMemoryTransactionLogStateStoreTestBase {

    protected final TableStatus sleeperTable = uniqueIdAndName("test-table-id", "test-table");
    private PartitionsBuilder partitions;
    protected FileReferenceFactory factory;
    protected InMemoryTransactionLogStore filesLogStore = new InMemoryTransactionLogStore();
    protected InMemoryTransactionLogStore partitionsLogStore = new InMemoryTransactionLogStore();
    protected InMemoryTransactionLogSnapshots fileSnapshots = new InMemoryTransactionLogSnapshots();
    protected InMemoryTransactionLogSnapshots partitionSnapshots = new InMemoryTransactionLogSnapshots();
    protected StateStore store;
    protected final List<Duration> retryWaits = new ArrayList<>();

    protected void initialiseWithSchema(Schema schema) throws Exception {
        createStore(new PartitionsBuilder(schema).singlePartition("root"));
        store.initialise();
    }

    protected void initialiseWithPartitions(PartitionsBuilder partitions) throws Exception {
        createStore(partitions);
        store.initialise(partitions.buildList());
    }

    private void createStore(PartitionsBuilder partitions) {
        this.partitions = partitions;
        factory = FileReferenceFactory.fromUpdatedAt(partitions.buildTree(), DEFAULT_UPDATE_TIME);
        store = stateStore(stateStoreBuilder(partitions.getSchema()));
    }

    protected StateStore stateStore(TransactionLogStateStore.Builder builder) {
        StateStore stateStore = builder.build();
        stateStore.fixFileUpdateTime(DEFAULT_UPDATE_TIME);
        stateStore.fixPartitionUpdateTime(DEFAULT_UPDATE_TIME);
        return stateStore;
    }

    protected TransactionLogStateStore.Builder stateStoreBuilder(Schema schema) {
        return TransactionLogStateStore.builder()
                .sleeperTable(sleeperTable)
                .schema(schema)
                .filesLogStore(filesLogStore)
                .filesSnapshotLoader(fileSnapshots)
                .partitionsLogStore(partitionsLogStore)
                .partitionsSnapshotLoader(partitionSnapshots)
                .maxAddTransactionAttempts(10)
                .retryBackoff(new ExponentialBackoffWithJitter(
                        TransactionLogStateStore.DEFAULT_RETRY_WAIT_RANGE,
                        fixJitterSeed(), recordWaits(retryWaits)));
    }

    protected void splitPartition(String parentId, String leftId, String rightId, long splitPoint) throws StateStoreException {
        partitions.splitToNewChildren(parentId, leftId, rightId, splitPoint)
                .applySplit(store, parentId);
        factory = FileReferenceFactory.fromUpdatedAt(partitions.buildTree(), DEFAULT_UPDATE_TIME);
    }
}
