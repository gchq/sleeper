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

import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import sleeper.core.partition.PartitionTree;
import sleeper.core.partition.PartitionsBuilder;
import sleeper.core.schema.Schema;
import sleeper.core.schema.type.StringType;
import sleeper.core.statestore.StateStoreException;
import sleeper.core.statestore.transactionlog.InMemoryTransactionLogStore;
import sleeper.core.statestore.transactionlog.TransactionLogStateStore;
import sleeper.core.util.ExponentialBackoffWithJitter;
import sleeper.core.util.ExponentialBackoffWithJitter.WaitRange;

import java.nio.file.Path;
import java.util.function.Consumer;

import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.core.schema.SchemaTestHelper.schemaWithKey;
import static sleeper.core.statestore.FileReferenceTestData.DEFAULT_UPDATE_TIME;
import static sleeper.core.table.TableStatusTestHelper.uniqueIdAndName;
import static sleeper.core.util.ExponentialBackoffWithJitterTestHelper.fixJitterSeed;
import static sleeper.core.util.ExponentialBackoffWithJitterTestHelper.noWaits;

public class TransactionLogSnapshotIT {
    @TempDir
    private Path tempDir;
    private final Schema schema = schemaWithKey("key", new StringType());
    private final PartitionsBuilder partitions = new PartitionsBuilder(schema).singlePartition("root");
    private final InMemoryTransactionLogStore filesLogStore = new InMemoryTransactionLogStore();
    private final InMemoryTransactionLogStore partitionsLogStore = new InMemoryTransactionLogStore();
    private final Configuration configuration = new Configuration();

    @Test
    void shouldSaveAndLoadPartitionsState() throws StateStoreException {
        // Given
        TransactionLogStateStore stateStore = stateStore();
        PartitionTree splitTree = partitions.splitToNewChildren("root", "L", "R", "l").buildTree();
        stateStore.initialise(splitTree.getAllPartitions());

        // When
        TransactionLogSnapshot snapshot = TransactionLogSnapshot.from(schema, stateStore, configuration);
        snapshot.save(tempDir);

        // Then
        assertThat(snapshot.loadPartitionsFromTransactionNumber(tempDir, 1).all())
                .containsExactlyElementsOf(stateStore.getAllPartitions());
    }

    private TransactionLogStateStore stateStore() {
        return stateStore(builder -> {
        });
    }

    private TransactionLogStateStore stateStore(Consumer<TransactionLogStateStore.Builder> config) {
        TransactionLogStateStore.Builder builder = stateStoreBuilder();
        config.accept(builder);
        TransactionLogStateStore stateStore = builder.build();
        stateStore.fixFileUpdateTime(DEFAULT_UPDATE_TIME);
        return stateStore;
    }

    private TransactionLogStateStore.Builder stateStoreBuilder() {
        return TransactionLogStateStore.builder()
                .sleeperTable(uniqueIdAndName("test-table-id", "test-table"))
                .schema(schema)
                .filesLogStore(filesLogStore)
                .partitionsLogStore(partitionsLogStore)
                .maxAddTransactionAttempts(10)
                .retryBackoff(new ExponentialBackoffWithJitter(
                        WaitRange.firstAndMaxWaitCeilingSecs(1, 30),
                        fixJitterSeed(), noWaits()));
    }
}
