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

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
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
    @Disabled("TODO")
    void shouldWriteAndReadATransaction() throws Exception {
        // Given
        FileReference file = FileReferenceFactory.from(partitions).rootFile("test.parquet", 100);
        Instant updateTime = Instant.parse("2025-02-27T15:43:00Z");
        AddFilesTransaction transaction = AddFilesTransaction.fromReferences(List.of(file));
        TransactionLogEntry entry = new TransactionLogEntry(1, updateTime, transaction);

        // When
        store.addTransaction(entry);

        // Then
        assertThat(store.readTransactions(TransactionLogRange.fromMinimum(1)))
                .containsExactly(new TransactionLogEntry(1, updateTime, transaction));
    }
}
