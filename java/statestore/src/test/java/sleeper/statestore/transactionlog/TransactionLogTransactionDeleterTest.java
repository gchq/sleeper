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

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import sleeper.configuration.properties.instance.InstanceProperties;
import sleeper.configuration.properties.table.TableProperties;
import sleeper.core.partition.PartitionsBuilder;
import sleeper.core.schema.Schema;
import sleeper.core.schema.type.StringType;
import sleeper.core.statestore.AllReferencesToAFile;
import sleeper.core.statestore.FileReference;
import sleeper.core.statestore.FileReferenceFactory;
import sleeper.core.statestore.StateStore;
import sleeper.core.statestore.transactionlog.InMemoryTransactionLogStore;
import sleeper.core.statestore.transactionlog.TransactionLogEntry;
import sleeper.core.statestore.transactionlog.TransactionLogStateStore;
import sleeper.core.statestore.transactionlog.transactions.AddFilesTransaction;
import sleeper.core.table.TableStatus;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.configuration.properties.InstancePropertiesTestHelper.createTestInstanceProperties;
import static sleeper.configuration.properties.table.TablePropertiesTestHelper.createTestTableProperties;
import static sleeper.configuration.properties.table.TableProperty.TRANSACTION_LOG_MIN_BEHIND_TO_DELETE;
import static sleeper.core.schema.SchemaTestHelper.schemaWithKey;
import static sleeper.core.statestore.FileReferenceTestData.DEFAULT_UPDATE_TIME;

public class TransactionLogTransactionDeleterTest {
    private final Schema schema = schemaWithKey("key", new StringType());
    private final InstanceProperties instanceProperties = createTestInstanceProperties();
    private final TableProperties tableProperties = createTestTableProperties(instanceProperties, schema);
    private final TableStatus tableStatus = tableProperties.getStatus();
    private final PartitionsBuilder partitions = new PartitionsBuilder(schema).rootFirst("root");
    private final InMemoryTransactionLogStore filesLogStore = new InMemoryTransactionLogStore();
    private final InMemoryTransactionLogStore partitionsLogStore = new InMemoryTransactionLogStore();
    private final StateStore stateStore = TransactionLogStateStore.builder()
            .sleeperTable(tableStatus).schema(schema)
            .filesLogStore(filesLogStore).partitionsLogStore(partitionsLogStore)
            .build();
    private LatestSnapshots latestSnapshots = LatestSnapshots.empty();

    @BeforeEach
    void setUp() {
        stateStore.fixFileUpdateTime(DEFAULT_UPDATE_TIME);
        stateStore.fixPartitionUpdateTime(DEFAULT_UPDATE_TIME);
    }

    @Test
    @Disabled("TODO")
    void shouldDeleteOldTransactionWhenTwoAreBeforeLatestSnapshot() throws Exception {
        // Given we have two file transactions
        FileReferenceFactory fileFactory = FileReferenceFactory.from(partitions.buildTree());
        FileReference file1 = fileFactory.rootFile("file1.parquet", 123L);
        FileReference file2 = fileFactory.rootFile("file2.parquet", 456L);
        stateStore.addFile(file1);
        stateStore.addFile(file2);
        // And we have a snapshot at the head of the file log
        setOnlyFilesSnapshotAtNumber(2);
        // And we configure to delete any transactions more than one before the latest snapshot
        tableProperties.setNumber(TRANSACTION_LOG_MIN_BEHIND_TO_DELETE, 1);

        // When we delete old transactions
        deleteOldFilesTransactions();

        // Then only one transaction remains
        assertThat(filesLogStore.readTransactionsAfter(0))
                .containsExactly(new TransactionLogEntry(2, DEFAULT_UPDATE_TIME,
                        new AddFilesTransaction(AllReferencesToAFile.newFilesWithReferences(List.of(file2)))));
    }

    private void setOnlyFilesSnapshotAtNumber(int transactionNumber) {
        latestSnapshots = new LatestSnapshots(TransactionLogSnapshotMetadata.forFiles("", transactionNumber), null);
    }

    private void deleteOldFilesTransactions() {
        new TransactionLogTransactionDeleter(filesLogStore, tableProperties)
                .deleteWithLatestSnapshot(latestSnapshots.getFilesSnapshot().orElse(null));
    }
}
