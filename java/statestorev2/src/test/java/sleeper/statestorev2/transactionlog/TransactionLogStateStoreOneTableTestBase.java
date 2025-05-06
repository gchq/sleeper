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
package sleeper.statestorev2.transactionlog;

import sleeper.core.partition.PartitionsBuilder;
import sleeper.core.properties.table.TableProperties;
import sleeper.core.schema.Schema;
import sleeper.core.statestore.FileReferenceFactory;
import sleeper.core.statestore.StateStore;
import sleeper.core.statestore.testutils.InMemoryTransactionLogSnapshotSetup;
import sleeper.core.statestore.testutils.InMemoryTransactionLogSnapshotSetup.SetupStateStore;
import sleeper.core.statestore.transactionlog.TransactionLogStateStore;
import sleeper.statestorev2.transactionlog.snapshots.DynamoDBTransactionLogSnapshotSaver;

import java.util.function.Consumer;

import static sleeper.core.properties.testutils.TablePropertiesTestHelper.createTestTablePropertiesWithNoSchema;
import static sleeper.core.statestore.FileReferenceTestData.DEFAULT_UPDATE_TIME;
import static sleeper.core.statestore.testutils.InMemoryTransactionLogSnapshotSetup.setupSnapshotWithFreshState;
import static sleeper.core.statestore.testutils.StateStoreUpdatesWrapper.update;

public class TransactionLogStateStoreOneTableTestBase extends TransactionLogStateStoreTestBase {

    protected final TableProperties tableProperties = createTestTablePropertiesWithNoSchema(instanceProperties);
    private PartitionsBuilder partitions;
    protected FileReferenceFactory factory;
    protected StateStore store;

    protected void initialiseWithSchema(Schema schema) {
        createStore(schema);
        setPartitions(new PartitionsBuilder(schema).singlePartition("root"));
        update(store).initialise(schema);
    }

    protected void initialiseWithPartitions(PartitionsBuilder partitions) {
        createStore(partitions.getSchema());
        setPartitions(partitions);
        update(store).initialise(partitions.buildList());
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

    protected void splitPartition(String parentId, String leftId, String rightId, long splitPoint) {
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
        DynamoDBTransactionLogSnapshotSaver snapshotSaver = new DynamoDBTransactionLogSnapshotSaver(instanceProperties, tableProperties, dynamoClientV2, hadoopConf);
        snapshotSaver.saveFilesSnapshot(snapshotSetup.createFilesSnapshot(transactionNumber));
        snapshotSaver.savePartitionsSnapshot(snapshotSetup.createPartitionsSnapshot(transactionNumber));
    }
}
