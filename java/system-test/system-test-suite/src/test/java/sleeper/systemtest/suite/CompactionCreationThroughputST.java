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
package sleeper.systemtest.suite;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import sleeper.compaction.core.job.creation.strategy.impl.BasicCompactionStrategy;
import sleeper.core.partition.PartitionTree;
import sleeper.core.partition.PartitionsBuilderSplitsFirst;
import sleeper.core.schema.Schema;
import sleeper.core.schema.type.IntType;
import sleeper.core.statestore.FileReference;
import sleeper.core.statestore.FileReferenceFactory;
import sleeper.core.statestore.transactionlog.transaction.impl.AddFilesTransaction;
import sleeper.core.util.PollWithRetries;
import sleeper.core.util.SplitIntoBatches;
import sleeper.systemtest.dsl.SleeperSystemTest;
import sleeper.systemtest.suite.testutil.SystemTest;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static sleeper.core.properties.table.TableProperty.COMPACTION_FILES_BATCH_SIZE;
import static sleeper.core.properties.table.TableProperty.COMPACTION_STRATEGY_CLASS;
import static sleeper.core.properties.table.TableProperty.TABLE_ONLINE;
import static sleeper.core.schema.SchemaTestHelper.schemaWithKey;
import static sleeper.core.statestore.testutils.StateStoreUpdatesWrapper.update;
import static sleeper.systemtest.suite.fixtures.SystemTestInstance.MAIN;

@SystemTest
public class CompactionCreationThroughputST {

    int numCompactions = 200_000;

    Schema schema = schemaWithKey("key", new IntType());
    List<String> partitionIds = IntStream.rangeClosed(1, numCompactions)
            .mapToObj(i -> "partition-" + i).toList();
    List<Object> splits = IntStream.range(1, numCompactions).mapToObj(i -> (Object) i).toList();
    PartitionTree partitions = PartitionsBuilderSplitsFirst
            .leavesWithSplits(schema, partitionIds, splits)
            .anyTreeJoiningAllLeaves().buildTree();
    FileReferenceFactory fileFactory = FileReferenceFactory.from(partitions);

    @BeforeEach
    void setUp(SleeperSystemTest sleeper) {
        sleeper.connectToInstanceNoTables(MAIN);
    }

    @Test
    void shouldFakeCompactionCreation(SleeperSystemTest sleeper) {
        // Given
        sleeper.tables().createWithProperties("compaction", schema, Map.of(
                TABLE_ONLINE, "false",
                COMPACTION_STRATEGY_CLASS, BasicCompactionStrategy.class.getName(),
                COMPACTION_FILES_BATCH_SIZE, "2"));
        sleeper.partitioning().setPartitions(partitions);
        sleeper.stateStore().fakeCommits().setupStateStore(store -> {
            Stream<FileReference> files = IntStream.rangeClosed(1, numCompactions).mapToObj(i -> i)
                    .flatMap(i -> Stream.of(
                            fileFactory.partitionFile("partition-" + i, "compaction-" + i + "file-1.parquet", 100),
                            fileFactory.partitionFile("partition-" + i, "compaction-" + i + "file-2.parquet", 200)));
            SplitIntoBatches.streamBatchesOf(10000, files)
                    .map(AddFilesTransaction::fromReferences)
                    .forEach(update(store)::addTransaction);
        });

        // When
        sleeper.compaction().createJobs(numCompactions,
                PollWithRetries.intervalAndPollingTimeout(Duration.ofSeconds(1), Duration.ofMinutes(1)));

        // Then
        // TODO drain compaction jobs queue
    }

}
