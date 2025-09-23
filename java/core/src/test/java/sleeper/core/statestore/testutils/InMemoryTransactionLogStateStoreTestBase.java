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
package sleeper.core.statestore.testutils;

import org.junit.jupiter.api.BeforeEach;

import sleeper.core.partition.PartitionsBuilder;
import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.table.TableProperties;
import sleeper.core.schema.Schema;
import sleeper.core.statestore.FileReferenceFactory;
import sleeper.core.statestore.StateStore;
import sleeper.core.statestore.transactionlog.TransactionLogStateStore;

import java.time.Duration;
import java.util.List;

import static sleeper.core.properties.table.TableProperty.TABLE_ID;
import static sleeper.core.properties.table.TableProperty.TABLE_NAME;
import static sleeper.core.properties.testutils.InstancePropertiesTestHelper.createTestInstanceProperties;
import static sleeper.core.properties.testutils.TablePropertiesTestHelper.createTestTableProperties;
import static sleeper.core.schema.SchemaTestHelper.createSchemaWithKey;
import static sleeper.core.statestore.FileReferenceTestData.DEFAULT_UPDATE_TIME;
import static sleeper.core.statestore.testutils.StateStoreUpdatesWrapper.update;

public class InMemoryTransactionLogStateStoreTestBase {

    protected final InstanceProperties instanceProperties = createTestInstanceProperties();
    protected final TableProperties tableProperties = createTestTableProperties(instanceProperties, createSchemaWithKey("key"));
    protected final String tableId = "test-table-id";
    protected final InMemoryTransactionLogs transactionLogs = new InMemoryTransactionLogs();
    protected final InMemoryTransactionLogStore filesLogStore = transactionLogs.getFilesLogStore();
    protected final InMemoryTransactionLogStore partitionsLogStore = transactionLogs.getPartitionsLogStore();
    protected final InMemoryTransactionBodyStore transactionBodyStore = transactionLogs.getTransactionBodyStore();
    protected final List<Duration> retryWaits = transactionLogs.getRetryWaits();
    protected PartitionsBuilder partitions;
    protected FileReferenceFactory factory;
    protected StateStore store;

    @BeforeEach
    void setUpBase() {
        tableProperties.set(TABLE_ID, tableId);
        tableProperties.set(TABLE_NAME, "test-table");
    }

    protected void initialiseWithSchema(Schema schema) {
        createStore(new PartitionsBuilder(schema).singlePartition("root"));
        update(store).initialise(schema);
    }

    protected void initialiseWithPartitions(PartitionsBuilder partitions) {
        createStore(partitions);
        update(store).initialise(partitions.buildList());
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
        tableProperties.setSchema(schema);
        return transactionLogs.stateStoreBuilder(tableProperties);
    }

    protected void createSnapshots() {
        transactionLogs.createSnapshots(tableProperties.getStatus());
    }

    protected void splitPartition(String parentId, String leftId, String rightId, long splitPoint) {
        partitions.splitToNewChildren(parentId, leftId, rightId, splitPoint)
                .applySplit(store, parentId);
        factory = FileReferenceFactory.fromUpdatedAt(partitions.buildTree(), DEFAULT_UPDATE_TIME);
    }
}
