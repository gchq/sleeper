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
package sleeper.ingest.batcher.store;

import org.junit.jupiter.api.BeforeEach;

import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.table.TableProperties;
import sleeper.core.properties.table.TablePropertiesProvider;
import sleeper.core.properties.testutils.FixedTablePropertiesProvider;
import sleeper.ingest.batcher.core.IngestBatcherStore;
import sleeper.localstack.test.LocalStackTestBase;

import java.util.List;

import static sleeper.core.properties.instance.CommonProperty.ID;
import static sleeper.core.properties.table.TableProperty.TABLE_ID;
import static sleeper.core.properties.testutils.InstancePropertiesTestHelper.createTestInstanceProperties;
import static sleeper.core.properties.testutils.TablePropertiesTestHelper.createTestTableProperties;
import static sleeper.core.schema.SchemaTestHelper.schemaWithKey;

public class DynamoDBIngestBatcherStoreTestBase extends LocalStackTestBase {
    protected final InstanceProperties instanceProperties = createTestInstanceProperties();
    protected final TableProperties table1 = createTestTableProperties(instanceProperties, schemaWithKey("key"));
    protected final TableProperties table2 = createTestTableProperties(instanceProperties, schemaWithKey("key"));
    protected final String tableId = table1.get(TABLE_ID);
    protected final String tableId1 = table1.get(TABLE_ID);
    protected final String tableId2 = table2.get(TABLE_ID);
    private final TablePropertiesProvider tablePropertiesProvider = new FixedTablePropertiesProvider(
            List.of(table1, table2));
    protected final String requestsTableName = DynamoDBIngestBatcherStore.ingestRequestsTableName(instanceProperties.get(ID));
    protected final IngestBatcherStore store = new DynamoDBIngestBatcherStore(
            dynamoClient, instanceProperties, tablePropertiesProvider);

    @BeforeEach
    void setUp() {
        DynamoDBIngestBatcherStoreCreator.create(instanceProperties, dynamoClient);
    }

    protected IngestBatcherStore storeWithFilesInAssignJobBatch(int filesInAssignJobBatch) {
        return new DynamoDBIngestBatcherStore(dynamoClient, instanceProperties, tablePropertiesProvider, filesInAssignJobBatch);
    }
}
