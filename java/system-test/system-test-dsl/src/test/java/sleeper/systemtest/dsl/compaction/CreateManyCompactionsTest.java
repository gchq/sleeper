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
package sleeper.systemtest.dsl.compaction;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import sleeper.compaction.core.job.creation.strategy.impl.BasicCompactionStrategy;
import sleeper.core.statestore.AllReferencesToAllFiles;
import sleeper.systemtest.dsl.SleeperDsl;
import sleeper.systemtest.dsl.testutil.InMemoryDslTest;

import java.util.Map;
import java.util.stream.LongStream;

import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.core.properties.table.TableProperty.COMPACTION_FILES_BATCH_SIZE;
import static sleeper.core.properties.table.TableProperty.COMPACTION_STRATEGY_CLASS;
import static sleeper.core.statestore.AllReferencesToAFileTestHelper.sumFileReferenceRowCounts;
import static sleeper.systemtest.dsl.testutil.InMemoryTestInstance.IN_MEMORY_MAIN;
import static sleeper.systemtest.dsl.testutil.SystemTestPartitionsTestHelper.createPartitionTreeWithRowsPerPartitionAndTotal;

@InMemoryDslTest
public class CreateManyCompactionsTest {

    @BeforeEach
    void setUp(SleeperDsl sleeper) {
        sleeper.connectToInstanceAddOnlineTable(IN_MEMORY_MAIN);
    }

    @Test
    void shouldCreateLargeQuantitiesOfCompactionJobsAtOnce(SleeperDsl sleeper) throws Exception {
        // Given
        sleeper.updateTableProperties(Map.of(
                COMPACTION_STRATEGY_CLASS, BasicCompactionStrategy.class.getName(),
                COMPACTION_FILES_BATCH_SIZE, "2"));
        sleeper.partitioning().setPartitions(
                createPartitionTreeWithRowsPerPartitionAndTotal(10, 10240, sleeper));
        sleeper.sourceFiles().inDataBucket().writeSketches()
                .createWithNumberedRows("file1.parquet", LongStream.range(0, 10240))
                .createWithNumberedRows("file2.parquet", LongStream.range(0, 10240));
        sleeper.ingest().toStateStore()
                .addFileOnEveryPartition("file1.parquet", 10240)
                .addFileOnEveryPartition("file2.parquet", 10240);

        // When
        sleeper.compaction()
                .createJobs(1024)
                .waitForTasks(1).waitForJobs();

        // Then
        AllReferencesToAllFiles files = sleeper.tableFiles().all();
        assertThat(files.getFilesWithReferences().size()).isEqualTo(1024);
        assertThat(files.getFilesWithNoReferences().size()).isEqualTo(2);
        assertThat(files.countFileReferences()).isEqualTo(1024);
        assertThat(sumFileReferenceRowCounts(files)).isEqualTo(20480);
    }
}
