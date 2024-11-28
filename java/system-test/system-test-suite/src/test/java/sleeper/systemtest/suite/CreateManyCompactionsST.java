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
import sleeper.core.statestore.AllReferencesToAllFiles;
import sleeper.systemtest.dsl.SleeperSystemTest;
import sleeper.systemtest.suite.testutil.SystemTest;

import java.util.Map;
import java.util.stream.LongStream;

import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.core.properties.table.TableProperty.COMPACTION_FILES_BATCH_SIZE;
import static sleeper.core.properties.table.TableProperty.COMPACTION_STRATEGY_CLASS;
import static sleeper.systemtest.dsl.testutil.InMemoryTestInstance.MAIN;
import static sleeper.systemtest.dsl.testutil.SystemTestPartitionsTestHelper.createPartitionTreeWithRecordsPerPartitionAndTotal;

@SystemTest
public class CreateManyCompactionsST {

    @BeforeEach
    void setUp(SleeperSystemTest sleeper) {
        sleeper.connectToInstance(MAIN);
    }

    @Test
    void shouldCreateLargeQuantitiesOfCompactionJobsAtOnce(SleeperSystemTest sleeper) throws Exception {
        // Given
        sleeper.updateTableProperties(Map.of(
                COMPACTION_STRATEGY_CLASS, BasicCompactionStrategy.class.getName(),
                COMPACTION_FILES_BATCH_SIZE, "2"));
        sleeper.partitioning().setPartitions(
                createPartitionTreeWithRecordsPerPartitionAndTotal(10, 655360, sleeper));
        sleeper.sourceFiles().inDataBucket().writeSketches()
                .createWithNumberedRecords("file1.parquet", LongStream.range(0, 655360))
                .createWithNumberedRecords("file2.parquet", LongStream.range(0, 655360));
        sleeper.ingest().toStateStore()
                .addFileOnEveryPartition("file1.parquet", 655360)
                .addFileOnEveryPartition("file2.parquet", 655360);

        // When
        sleeper.compaction()
                .createJobs(65536)
                .invokeTasks(1).waitForJobs();

        // Then
        AllReferencesToAllFiles files = sleeper.tableFiles().all();
        assertThat(files.getFilesWithReferences().size()).isEqualTo(65536);
        assertThat(files.getFilesWithNoReferences().size()).isEqualTo(2);
        assertThat(files.countFileReferences()).isEqualTo(65536);
        assertThat(files.estimateRecordsInTable()).isEqualTo(1310720);
    }
}