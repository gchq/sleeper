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

import sleeper.core.partition.PartitionTree;
import sleeper.core.partition.PartitionsBuilder;
import sleeper.core.schema.Schema;
import sleeper.core.schema.type.StringType;
import sleeper.core.statestore.FileReference;
import sleeper.core.statestore.FileReferenceFactory;
import sleeper.core.statestore.StateStore;

import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.core.schema.SchemaTestHelper.schemaWithKey;
import static sleeper.core.statestore.FileReferenceTestData.DEFAULT_UPDATE_TIME;

public class TransactionLogHeadTest {

    private final Schema schema = schemaWithKey("key", new StringType());
    private final PartitionsBuilder partitions = new PartitionsBuilder(schema).singlePartition("root");
    private final InMemoryTransactionLogStore fileLogStore = new InMemoryTransactionLogStore();
    private final InMemoryTransactionLogStore partitionLogStore = new InMemoryTransactionLogStore();
    private final StateStore store = new TransactionLogStateStore(schema, fileLogStore, partitionLogStore);

    @BeforeEach
    void setUp() throws Exception {
        store.fixTime(DEFAULT_UPDATE_TIME);
        store.initialise(partitions.buildList());
    }

    private StateStore otherProcess() {
        StateStore otherStore = new TransactionLogStateStore(schema, fileLogStore, partitionLogStore);
        otherStore.fixTime(DEFAULT_UPDATE_TIME);
        return otherStore;
    }

    @Test
    void shouldAddTransactionWhenAnotherProcessAddedATransactionBetweenAdds() throws Exception {
        // Given
        PartitionTree afterRootSplit = partitions.splitToNewChildren("root", "L", "R", "l").buildTree();
        otherProcess().atomicallyUpdatePartitionAndCreateNewOnes(
                afterRootSplit.getPartition("root"),
                afterRootSplit.getPartition("L"), afterRootSplit.getPartition("R"));

        // When
        PartitionTree afterLeftSplit = partitions.splitToNewChildren("L", "LL", "LR", "f").buildTree();
        store.atomicallyUpdatePartitionAndCreateNewOnes(
                afterLeftSplit.getPartition("L"),
                afterLeftSplit.getPartition("LL"), afterLeftSplit.getPartition("LR"));

        // Then
        assertThat(new PartitionTree(store.getAllPartitions())).isEqualTo(afterLeftSplit);
    }

    @Test
    void shouldAddTransactionWhenAnotherProcessAddedATransactionBetweenUpdateAndAdd() throws Exception {
        // Given
        FileReferenceFactory fileFactory = FileReferenceFactory.fromUpdatedAt(partitions.buildTree(), DEFAULT_UPDATE_TIME);
        FileReference file1 = fileFactory.rootFile("file1.parquet", 100);
        FileReference file2 = fileFactory.rootFile("file2.parquet", 200);
        FileReference file3 = fileFactory.rootFile("file3.parquet", 300);
        store.addFile(file1);
        fileLogStore.beforeNextAddTransaction(() -> {
            otherProcess().addFile(file2);
        });

        // When
        store.addFile(file3);

        // Then
        assertThat(store.getFileReferences()).containsExactly(file1, file2, file3);
    }
}
