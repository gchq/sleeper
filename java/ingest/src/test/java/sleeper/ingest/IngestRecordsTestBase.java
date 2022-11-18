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
import sleeper.configuration.properties.InstanceProperties;
import sleeper.configuration.properties.table.TableProperties;
import sleeper.core.CommonTestConstants;
import sleeper.core.record.Record;
import sleeper.core.schema.Field;
import sleeper.core.schema.Schema;
import sleeper.core.schema.type.LongType;
import sleeper.ingest.testutils.IngestRecordsTestDataHelper;
import sleeper.statestore.FixedStateStoreProvider;
import sleeper.statestore.StateStore;

import java.util.List;

import static sleeper.ingest.testutils.IngestRecordsTestDataHelper.TEST_TABLE_NAME;
import static sleeper.ingest.testutils.IngestRecordsTestDataHelper.defaultInstanceProperties;
import static sleeper.ingest.testutils.IngestRecordsTestDataHelper.defaultTableProperties;
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

    protected IngestFactory createIngestFactory(StateStore stateStore, TableProperties tableProperties, InstanceProperties instanceProperties) {
        return IngestRecordsTestDataHelper.createIngestFactory(inputFolderName, new FixedStateStoreProvider(tableProperties, stateStore), instanceProperties);
    }

    protected IngestResult ingestRecords(Schema schema, StateStore stateStore, List<Record> records) {
        InstanceProperties instanceProperties = defaultInstanceProperties();
        TableProperties tableProperties = defaultTableProperties(schema, TEST_TABLE_NAME, sketchFolderName, instanceProperties);
        IngestFactory factory = createIngestFactory(stateStore, tableProperties, instanceProperties);
        return factory.ingestRecords(tableProperties, records);
    }
}
