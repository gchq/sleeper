/*
 * Copyright 2022 Crown Copyright
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

package sleeper.ingest;

import org.junit.Before;
import org.junit.Rule;
import org.junit.rules.TemporaryFolder;
import sleeper.core.CommonTestConstants;
import sleeper.core.partition.Partition;
import sleeper.core.partition.PartitionsFromSplitPoints;
import sleeper.core.schema.Field;
import sleeper.core.schema.Schema;
import sleeper.core.schema.type.LongType;
import sleeper.ingest.testutils.IngestRecordsTestDataHelper;
import sleeper.statestore.DelegatingStateStore;
import sleeper.statestore.StateStore;
import sleeper.statestore.StateStoreException;
import sleeper.statestore.inmemory.FixedPartitionStore;
import sleeper.statestore.inmemory.InMemoryFileInfoStore;

import java.util.Collections;
import java.util.List;

import static sleeper.ingest.testutils.IngestRecordsTestDataHelper.schemaWithRowKeys;

public class IngestRecordsTestBase {
    @Rule
    public TemporaryFolder folder = new TemporaryFolder(CommonTestConstants.TMP_DIRECTORY);

    protected final Field field = new Field("key", new LongType());
    protected final Schema schema = schemaWithRowKeys(field);
    protected String inputFolderName;
    protected String sketchFolderName;

    @Before
    public void setUpBase() throws Exception {
        inputFolderName = folder.newFolder().getAbsolutePath();
        sketchFolderName = folder.newFolder().getAbsolutePath();
    }

    protected static StateStore getStateStore(Schema schema) throws StateStoreException {
        return getStateStore(schema, new PartitionsFromSplitPoints(schema, Collections.emptyList()).construct());
    }

    protected static StateStore getStateStore(Schema schema, List<Partition> initialPartitions) throws StateStoreException {
        StateStore stateStore = new DelegatingStateStore(new InMemoryFileInfoStore(), new FixedPartitionStore(schema));
        stateStore.initialise(initialPartitions);
        return stateStore;
    }

    protected IngestProperties.Builder defaultPropertiesBuilder(StateStore stateStore, Schema sleeperSchema) {
        return IngestRecordsTestDataHelper.defaultPropertiesBuilder(stateStore, sleeperSchema, inputFolderName, sketchFolderName);
    }
}
