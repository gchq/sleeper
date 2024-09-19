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
package sleeper.ingest.batcher.store;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;

import sleeper.configuration.properties.instance.InstanceProperties;
import sleeper.configuration.properties.table.FixedTablePropertiesProvider;
import sleeper.configuration.properties.table.TableProperties;
import sleeper.configuration.properties.table.TablePropertiesProvider;
import sleeper.dynamodb.test.DynamoDBTestBase;
import sleeper.ingest.batcher.IngestBatcherStore;

import java.util.List;

import static sleeper.configuration.properties.InstancePropertiesTestHelper.createTestInstanceProperties;
import static sleeper.configuration.properties.instance.CommonProperty.ID;
import static sleeper.configuration.properties.table.TablePropertiesTestHelper.createTestTableProperties;
import static sleeper.configuration.properties.table.TableProperty.TABLE_ID;
import static sleeper.core.schema.SchemaTestHelper.schemaWithKey;

public class DynamoDBIngestBatcherStoreTestBase extends DynamoDBTestBase {
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
            dynamoDBClient, instanceProperties, tablePropertiesProvider);

    @BeforeEach
    void setUp() {
        DynamoDBIngestBatcherStoreCreator.create(instanceProperties, dynamoDBClient);
    }

    @AfterEach
    void tearDown() {
        DynamoDBIngestBatcherStoreCreator.tearDown(instanceProperties, dynamoDBClient);
    }

    protected IngestBatcherStore storeWithFilesInAssignJobBatch(int filesInAssignJobBatch) {
        return new DynamoDBIngestBatcherStore(dynamoDBClient, instanceProperties, tablePropertiesProvider, filesInAssignJobBatch);
    }
}
