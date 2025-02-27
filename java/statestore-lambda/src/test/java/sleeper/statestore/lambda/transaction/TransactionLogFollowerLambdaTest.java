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
package sleeper.statestore.lambda.transaction;

import org.junit.jupiter.api.Test;

import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.table.TablePropertiesProvider;
import sleeper.core.properties.testutils.FixedTablePropertiesProvider;
import sleeper.core.schema.Schema;
import sleeper.core.statestore.StateStoreProvider;
import sleeper.core.statestore.testutils.InMemoryTransactionLogStateStore;
import sleeper.core.statestore.testutils.InMemoryTransactionLogsPerTable;
import sleeper.core.statestore.transactionlog.transaction.TransactionSerDeProvider;
import sleeper.core.tracker.compaction.job.CompactionJobTracker;
import sleeper.core.tracker.compaction.job.InMemoryCompactionJobTracker;
import sleeper.core.tracker.ingest.job.InMemoryIngestJobTracker;
import sleeper.core.tracker.ingest.job.IngestJobTracker;

import static sleeper.core.properties.testutils.InstancePropertiesTestHelper.createTestInstanceProperties;
import static sleeper.core.properties.testutils.TablePropertiesTestHelper.createTestTableProperties;
import static sleeper.core.schema.SchemaTestHelper.schemaWithKey;

public class TransactionLogFollowerLambdaTest {
    // Tests to create
    // - All entries success
    //      - Results in tracker update
    //      - Results in no tracker update
    // - First entry fails, all fail
    // - Second entry fails, subsequent fails

    @Test
    void shouldProcessEntrySuccessfullyTriggersTrackerUpdate() {
        // Given
        TransactionLogFollowerLambda lambda = createLambda();

        // When

        // Then
    }

    private TransactionLogFollowerLambda createLambda() {
        InstanceProperties instanceProperties = createTestInstanceProperties();
        Schema schema = schemaWithKey("key");
        TablePropertiesProvider tablePropertiesProvider = new FixedTablePropertiesProvider(createTestTableProperties(instanceProperties, schema));
        DynamoDBStreamTransactionLogEntryMapper mapper = new DynamoDBStreamTransactionLogEntryMapper(TransactionSerDeProvider.from(tablePropertiesProvider));

        StateStoreProvider stateStoreProvider = InMemoryTransactionLogStateStore.createProvider(instanceProperties, new InMemoryTransactionLogsPerTable());
        CompactionJobTracker compactionJobTracker = new InMemoryCompactionJobTracker();
        IngestJobTracker ingestJobTracker = new InMemoryIngestJobTracker();

        return new TransactionLogFollowerLambda(mapper, tablePropertiesProvider, stateStoreProvider,
                compactionJobTracker, ingestJobTracker);
    }
}
