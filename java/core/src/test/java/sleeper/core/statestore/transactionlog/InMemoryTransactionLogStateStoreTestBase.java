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
import sleeper.core.table.TableStatus;

import java.time.Duration;
import java.util.List;

import static sleeper.core.statestore.FileReferenceTestData.DEFAULT_UPDATE_TIME;
import static sleeper.core.table.TableStatusTestHelper.uniqueIdAndName;

public class InMemoryTransactionLogStateStoreTestBase {

    protected final TableStatus sleeperTable = uniqueIdAndName("test-table-id", "test-table");
    protected final InMemoryTransactionLogs transactionLogs = new InMemoryTransactionLogs();
    protected final InMemoryTransactionLogStore filesLogStore = transactionLogs.getFilesLogStore();
    protected final InMemoryTransactionLogStore partitionsLogStore = transactionLogs.getPartitionsLogStore();
    protected final InMemoryTransactionBodyStore transactionBodyStore = transactionLogs.getTransactionBodyStore();
    protected final List<Duration> retryWaits = transactionLogs.getRetryWaits();
    protected PartitionsBuilder partitions;
    protected FileReferenceFactory factory;
    protected StateStore store;

    protected void initialiseWithSchema(Schema schema) {
        createStore(new PartitionsBuilder(schema).singlePartition("root"));
        store.initialise();
    }

    protected void initialiseWithPartitions(PartitionsBuilder partitions) {
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
        return transactionLogs.stateStoreBuilder(sleeperTable, schema);
    }

    protected void splitPartition(String parentId, String leftId, String rightId, Object splitPoint) {
        partitions.splitToNewChildren(parentId, leftId, rightId, splitPoint)
                .applySplit(store, parentId);
        factory = FileReferenceFactory.fromUpdatedAt(partitions.buildTree(), DEFAULT_UPDATE_TIME);
    }
}
