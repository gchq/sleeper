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

import org.junit.jupiter.api.Test;

import sleeper.core.partition.PartitionTree;
import sleeper.core.partition.PartitionsBuilder;
import sleeper.core.schema.type.StringType;
import sleeper.core.statestore.transactionlog.transactions.InitialisePartitionsTransaction;
import sleeper.core.statestore.transactionlog.transactions.SplitPartitionTransaction;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.core.schema.SchemaTestHelper.schemaWithKey;

public class TransactionLogHeadTest {

    @Test
    void shouldAddTransactionWhenAnotherProcessAddedATransactionToSameLog() throws Exception {
        // Given
        TransactionLogStore store = new InMemoryTransactionLogStore();
        TransactionLogHead<StateStorePartitions> head1 = TransactionLogHead.forPartitions(store);
        TransactionLogHead<StateStorePartitions> head2 = TransactionLogHead.forPartitions(store);
        PartitionsBuilder partitions = new PartitionsBuilder(schemaWithKey("key", new StringType()))
                .singlePartition("root");

        head1.addTransaction(new InitialisePartitionsTransaction(partitions.buildList()));

        PartitionTree afterRootSplit = partitions.splitToNewChildren("root", "L", "R", "l").buildTree();
        head2.addTransaction(new SplitPartitionTransaction(
                afterRootSplit.getPartition("root"),
                List.of(afterRootSplit.getPartition("L"), afterRootSplit.getPartition("R"))));

        // When
        PartitionTree afterLeftSplit = partitions.splitToNewChildren("L", "LL", "LR", "f").buildTree();
        head1.addTransaction(new SplitPartitionTransaction(
                afterLeftSplit.getPartition("L"),
                List.of(afterLeftSplit.getPartition("LL"), afterLeftSplit.getPartition("LR"))));

        // Then
        assertThat(partitionTree(head1)).isEqualTo(afterLeftSplit);
    }

    private PartitionTree partitionTree(TransactionLogHead<StateStorePartitions> head) {
        return new PartitionTree(new ArrayList<>(head.state().all()));
    }
}
