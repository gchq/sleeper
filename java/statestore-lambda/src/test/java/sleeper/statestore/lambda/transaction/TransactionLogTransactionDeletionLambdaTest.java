/*
 * Copyright 2022-2026 Crown Copyright
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

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import sleeper.core.partition.PartitionsBuilder;
import sleeper.core.properties.PropertiesReloader;
import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.table.TableProperties;
import sleeper.core.properties.testutils.FixedTablePropertiesProvider;
import sleeper.core.schema.type.StringType;
import sleeper.core.statestore.FileReferenceFactory;
import sleeper.core.statestore.StateStore;
import sleeper.core.statestore.testutils.InMemoryTransactionLogStateStore;
import sleeper.core.statestore.testutils.InMemoryTransactionLogs;
import sleeper.core.statestore.testutils.InMemoryTransactionLogsPerTable;
import sleeper.statestore.transactionlog.snapshots.LatestSnapshots;

import static sleeper.core.properties.table.TableProperty.TRANSACTION_LOG_NUMBER_BEHIND_TO_DELETE;
import static sleeper.core.properties.table.TableProperty.TRANSACTION_LOG_SNAPSHOT_MIN_AGE_MINUTES_TO_DELETE_TRANSACTIONS;
import static sleeper.core.properties.testutils.InstancePropertiesTestHelper.createTestInstanceProperties;
import static sleeper.core.properties.testutils.TablePropertiesTestHelper.createTestTableProperties;
import static sleeper.core.schema.SchemaTestHelper.createSchemaWithKey;
import static sleeper.core.statestore.testutils.StateStoreUpdatesWrapper.update;

public class TransactionLogTransactionDeletionLambdaTest {

    private final InstanceProperties instanceProperties = createTestInstanceProperties();
    private final TableProperties tableProperties = createTestTableProperties(instanceProperties, createSchemaWithKey("key", new StringType()));
    private final InMemoryTransactionLogsPerTable transactionLogsPerTable = new InMemoryTransactionLogsPerTable();
    private final InMemoryTransactionLogs transactionLogs = transactionLogsPerTable.forTable(tableProperties);
    private final StateStore stateStore = InMemoryTransactionLogStateStore.create(tableProperties, transactionLogs);

    @BeforeEach
    void setUp() {
        tableProperties.setNumber(TRANSACTION_LOG_SNAPSHOT_MIN_AGE_MINUTES_TO_DELETE_TRANSACTIONS, 0);
        tableProperties.setNumber(TRANSACTION_LOG_NUMBER_BEHIND_TO_DELETE, 0);
    }

    @Test
    void shouldDeleteTransactions() {
        // Given
        update(stateStore).initialise(new PartitionsBuilder(tableProperties)
                .rootFirst("root")
                .splitToNewChildren("root", "L", "R", "split")
                .buildList());
        update(stateStore).addFile(FileReferenceFactory.from(stateStore).rootFile("test.parquet", 10));
        transactionLogs.createSnapshots(tableProperties);

        // When

    }

    private TransactionLogTransactionDeletionLambda lambda() {
        return new TransactionLogTransactionDeletionLambda(
                instanceProperties, new FixedTablePropertiesProvider(tableProperties), PropertiesReloader.neverReload(),
                table -> transactionLogsPerTable.forTable(table).getFilesLogStore(),
                table -> transactionLogsPerTable.forTable(table).getPartitionsLogStore(),
                table -> time -> LatestSnapshots.empty()); // TODO get latest snapshots before time
    }

}
