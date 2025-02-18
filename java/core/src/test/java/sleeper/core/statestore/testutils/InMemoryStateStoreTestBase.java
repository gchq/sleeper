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

package sleeper.core.statestore.testutils;

import sleeper.core.partition.PartitionsBuilder;
import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.table.TableProperties;
import sleeper.core.schema.Schema;
import sleeper.core.schema.type.LongType;
import sleeper.core.statestore.FileReferenceFactory;
import sleeper.core.statestore.StateStore;
import sleeper.core.statestore.StateStoreException;
import sleeper.core.statestore.transactionlog.InMemoryTransactionLogStateStore;
import sleeper.core.statestore.transactionlog.InMemoryTransactionLogs;

import static sleeper.core.properties.testutils.InstancePropertiesTestHelper.createTestInstanceProperties;
import static sleeper.core.properties.testutils.TablePropertiesTestHelper.createTestTableProperties;
import static sleeper.core.schema.SchemaTestHelper.schemaWithKey;
import static sleeper.core.statestore.FileReferenceTestData.DEFAULT_UPDATE_TIME;
import static sleeper.core.statestore.testutils.StateStoreTestHelper.inMemoryStateStoreUninitialised;

public abstract class InMemoryStateStoreTestBase {

    private PartitionsBuilder partitions;
    private final Schema schema = schemaWithKey("key", new LongType());
    private final InstanceProperties instanceProperties = createTestInstanceProperties();
    private final TableProperties tableProperties = createTestTableProperties(instanceProperties, schema);
    protected FileReferenceFactory factory;
    protected StateStore store = InMemoryTransactionLogStateStore.create(tableProperties, new InMemoryTransactionLogs());

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
        store = inMemoryStateStoreUninitialised(partitions.getSchema());
        store.fixFileUpdateTime(DEFAULT_UPDATE_TIME);
    }

    protected void splitPartition(String parentId, String leftId, String rightId, long splitPoint) throws StateStoreException {
        partitions.splitToNewChildren(parentId, leftId, rightId, splitPoint)
                .applySplit(store, parentId);
        factory = FileReferenceFactory.fromUpdatedAt(partitions.buildTree(), DEFAULT_UPDATE_TIME);
    }
}
