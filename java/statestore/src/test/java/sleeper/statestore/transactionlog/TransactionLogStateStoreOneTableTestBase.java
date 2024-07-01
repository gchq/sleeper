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

import sleeper.configuration.properties.table.TableProperties;
import sleeper.core.partition.PartitionsBuilder;
import sleeper.core.schema.Schema;
import sleeper.core.statestore.FileReferenceFactory;
import sleeper.core.statestore.StateStore;
import sleeper.core.statestore.StateStoreException;
import sleeper.core.statestore.transactionlog.InMemoryTransactionLogSnapshotSetup;
import sleeper.core.statestore.transactionlog.InMemoryTransactionLogSnapshotSetup.SetupStateStore;
import sleeper.core.statestore.transactionlog.TransactionLogStateStore;

import java.util.function.Consumer;

import static sleeper.configuration.properties.table.TablePropertiesTestHelper.createTestTablePropertiesWithNoSchema;
import static sleeper.core.statestore.FileReferenceTestData.DEFAULT_UPDATE_TIME;
import static sleeper.core.statestore.transactionlog.InMemoryTransactionLogSnapshotSetup.setupSnapshotWithFreshState;

public class TransactionLogStateStoreOneTableTestBase extends TransactionLogStateStoreTestBase {

    protected final TableProperties tableProperties = createTestTablePropertiesWithNoSchema(instanceProperties);
    private PartitionsBuilder partitions;
    protected FileReferenceFactory factory;
    protected StateStore store;

    protected void initialiseWithSchema(Schema schema) throws Exception {
        createStore(schema);
        setPartitions(new PartitionsBuilder(schema).singlePartition("root"));
        store.initialise();
    }

    protected void initialiseWithPartitions(PartitionsBuilder partitions) throws Exception {
        createStore(partitions.getSchema());
        setPartitions(partitions);
        store.initialise(partitions.buildList());
    }

    private void createStore(Schema schema) {
        tableProperties.setSchema(schema);
        store = createStateStore(tableProperties);
        store.fixFileUpdateTime(DEFAULT_UPDATE_TIME);
    }

    private void setPartitions(PartitionsBuilder partitions) {
        this.partitions = partitions;
        factory = FileReferenceFactory.fromUpdatedAt(partitions.buildTree(), DEFAULT_UPDATE_TIME);
    }

    protected void splitPartition(String parentId, String leftId, String rightId, long splitPoint) throws StateStoreException {
        partitions.splitToNewChildren(parentId, leftId, rightId, splitPoint)
                .applySplit(store, parentId);
        factory = FileReferenceFactory.fromUpdatedAt(partitions.buildTree(), DEFAULT_UPDATE_TIME);
    }

    protected StateStore stateStore(Consumer<TransactionLogStateStore.Builder> config) {
        TransactionLogStateStore.Builder builder = stateStoreBuilder(tableProperties);
        config.accept(builder);
        return stateStore(builder);
    }

    protected void createSnapshotWithFreshStateAtTransactionNumber(
            long transactionNumber, SetupStateStore setupState) throws Exception {
        InMemoryTransactionLogSnapshotSetup snapshotSetup = setupSnapshotWithFreshState(
                tableProperties.getStatus(), tableProperties.getSchema(), setupState);
        DynamoDBTransactionLogSnapshotStore snapshotStore = new DynamoDBTransactionLogSnapshotStore(instanceProperties, tableProperties, dynamoDBClient, configuration);
        snapshotStore.saveFilesSnapshot(snapshotSetup.createFilesSnapshot(transactionNumber));
        snapshotStore.savePartitionsSnapshot(snapshotSetup.createPartitionsSnapshot(transactionNumber));
    }
}
