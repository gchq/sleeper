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
package sleeper.core.statestore.transactionlog;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import sleeper.core.schema.type.LongType;

import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.core.schema.SchemaTestHelper.schemaWithKey;

public class TransactionLogPartitionsSnapshotTest extends InMemoryTransactionLogStateStoreTestBase {
    @TempDir
    private Path tempDir;

    @BeforeEach
    void setUp() throws Exception {
        initialiseWithSchema(schemaWithKey("key", new LongType()));
    }

    @Test
    void shouldSavePartitionsSnapshotAfterInitialising() throws Exception {
        // Given / When
        snapshot().save(tempDir);

        // Then
        assertThat(snapshot().getSavedFiles())
                .containsExactly(tempDir.resolve("snapshots/1-partitions.parquet").toString());
    }

    @Test
    void shouldSavePartitionsSnapshotAfterAddingOneTransaction() throws Exception {
        // Given
        store.addFile(factory.rootFile(123L));

        // When
        snapshot().save(tempDir);

        // Then
        assertThat(snapshot().getSavedFiles())
                .containsExactly(tempDir.resolve("snapshots/1-partitions.parquet").toString());
    }

    @Test
    void shouldSavePartitionsSnapshotAfterAddingMultipleTransactions() throws Exception {
        // Given
        store.addFile(factory.rootFile("file1.parquet", 123L));
        store.addFile(factory.rootFile("file2.parquet", 456L));
        store.addFile(factory.rootFile("file3.parquet", 789L));

        // When
        snapshot().save(tempDir);

        // Then
        assertThat(snapshot().getSavedFiles())
                .containsExactly(tempDir.resolve("snapshots/3-partitions.parquet").toString());
    }

    private InMemoryTransactionLogPartitionsSnapshot snapshot() {
        return (InMemoryTransactionLogPartitionsSnapshot) store.partitionsSnapshot();
    }
}
