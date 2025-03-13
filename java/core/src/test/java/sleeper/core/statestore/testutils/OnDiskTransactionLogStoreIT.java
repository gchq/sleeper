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
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import sleeper.core.partition.PartitionTree;
import sleeper.core.partition.PartitionsBuilder;
import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.table.TableProperties;
import sleeper.core.schema.Schema;
import sleeper.core.statestore.FileReference;
import sleeper.core.statestore.FileReferenceFactory;
import sleeper.core.statestore.transactionlog.log.TransactionLogEntry;
import sleeper.core.statestore.transactionlog.log.TransactionLogRange;
import sleeper.core.statestore.transactionlog.log.TransactionLogStore;
import sleeper.core.statestore.transactionlog.transaction.TransactionSerDe;
import sleeper.core.statestore.transactionlog.transaction.TransactionType;
import sleeper.core.statestore.transactionlog.transaction.impl.AddFilesTransaction;

import java.nio.file.Path;
import java.time.Instant;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.core.properties.testutils.InstancePropertiesTestHelper.createTestInstanceProperties;
import static sleeper.core.properties.testutils.TablePropertiesTestHelper.createTestTableProperties;
import static sleeper.core.schema.SchemaTestHelper.schemaWithKey;

public class OnDiskTransactionLogStoreIT {
    @TempDir
    Path tempDir;

    InstanceProperties instanceProperties = createTestInstanceProperties();
    Schema schema = schemaWithKey("key");
    TableProperties tableProperties = createTestTableProperties(instanceProperties, schema);
    PartitionTree partitions = new PartitionsBuilder(tableProperties).singlePartition("root").buildTree();
    TransactionLogStore store;

    @BeforeEach
    void setUp() {
        store = OnDiskTransactionLogStore.inDirectory(tempDir, TransactionSerDe.forFileTransactions());
    }

    @Test
    void shouldWriteAndReadATransaction() throws Exception {
        // Given
        FileReference file = FileReferenceFactory.from(partitions).rootFile("test.parquet", 100);
        AddFilesTransaction transaction = AddFilesTransaction.fromReferences(List.of(file));
        TransactionLogEntry entry = new TransactionLogEntry(1, Instant.parse("2025-02-27T15:43:00Z"), transaction);

        // When
        store.addTransaction(entry);

        // Then
        assertThat(store.readTransactions(TransactionLogRange.fromMinimum(1)))
                .containsExactly(entry);
    }

    @Test
    void shouldWriteAndReadATransactionBodyKey() throws Exception {
        // Given
        TransactionLogEntry entry = new TransactionLogEntry(1,
                Instant.parse("2025-02-27T15:43:00Z"), TransactionType.ADD_FILES, "transactions/test");

        // When
        store.addTransaction(entry);

        // Then
        assertThat(store.readTransactions(TransactionLogRange.fromMinimum(1)))
                .containsExactly(entry);
    }

    @Test
    void shouldReadARangeOfTransactions() throws Exception {
        // Given
        TransactionLogEntry entry1 = new TransactionLogEntry(1,
                Instant.parse("2025-02-27T15:43:00Z"), TransactionType.ADD_FILES, "transactions/test1");
        TransactionLogEntry entry2 = new TransactionLogEntry(2,
                Instant.parse("2025-02-27T15:44:00Z"), TransactionType.ADD_FILES, "transactions/test2");
        TransactionLogEntry entry3 = new TransactionLogEntry(3,
                Instant.parse("2025-02-27T15:45:00Z"), TransactionType.ADD_FILES, "transactions/test3");

        // When
        store.addTransaction(entry1);
        store.addTransaction(entry2);
        store.addTransaction(entry3);

        // Then
        assertThat(store.readTransactions(new TransactionLogRange(2, 3)))
                .containsExactly(entry2);
    }

    @Test
    void shouldReadTransactionsInOrder() throws Exception {
        // Given
        TransactionLogEntry entry1 = new TransactionLogEntry(1,
                Instant.parse("2025-02-27T15:43:00Z"), TransactionType.ADD_FILES, "transactions/test1");
        TransactionLogEntry entry2 = new TransactionLogEntry(2,
                Instant.parse("2025-02-27T15:44:00Z"), TransactionType.ADD_FILES, "transactions/test2");
        TransactionLogEntry entry3 = new TransactionLogEntry(3,
                Instant.parse("2025-02-27T15:45:00Z"), TransactionType.ADD_FILES, "transactions/test3");

        // When
        store.addTransaction(entry1);
        store.addTransaction(entry2);
        store.addTransaction(entry3);

        // Then
        assertThat(store.readTransactions(TransactionLogRange.fromMinimum(1)))
                .containsExactly(entry1, entry2, entry3);
    }

    @Test
    void shouldDeleteTransactions() throws Exception {
        // Given
        TransactionLogEntry entry1 = new TransactionLogEntry(1,
                Instant.parse("2025-02-27T15:43:00Z"), TransactionType.ADD_FILES, "transactions/test1");
        TransactionLogEntry entry2 = new TransactionLogEntry(2,
                Instant.parse("2025-02-27T15:44:00Z"), TransactionType.ADD_FILES, "transactions/test2");
        TransactionLogEntry entry3 = new TransactionLogEntry(3,
                Instant.parse("2025-02-27T15:45:00Z"), TransactionType.ADD_FILES, "transactions/test3");
        store.addTransaction(entry1);
        store.addTransaction(entry2);
        store.addTransaction(entry3);

        // When
        store.deleteTransactionsAtOrBefore(2);

        // Then
        assertThat(store.readTransactions(TransactionLogRange.fromMinimum(1)))
                .containsExactly(entry3);
    }
}
