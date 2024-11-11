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
package sleeper.systemtest.dsl.gc;

import org.approvaltests.Approvals;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import sleeper.compaction.core.strategy.impl.BasicCompactionStrategy;
import sleeper.systemtest.dsl.SleeperSystemTest;
import sleeper.systemtest.dsl.extension.AfterTestPurgeQueues;
import sleeper.systemtest.dsl.extension.AfterTestReports;
import sleeper.systemtest.dsl.sourcedata.RecordNumbers;
import sleeper.systemtest.dsl.testutil.InMemoryDslTest;
import sleeper.systemtest.dsl.testutil.InMemorySystemTestDrivers;

import java.util.Map;
import java.util.stream.LongStream;

import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.core.properties.table.TableProperty.COMPACTION_FILES_BATCH_SIZE;
import static sleeper.core.properties.table.TableProperty.COMPACTION_STRATEGY_CLASS;
import static sleeper.core.properties.table.TableProperty.GARBAGE_COLLECTOR_DELAY_BEFORE_DELETION;
import static sleeper.core.testutils.printers.FileReferencePrinter.printFiles;
import static sleeper.systemtest.dsl.testutil.InMemoryTestInstance.MAIN;

@InMemoryDslTest
public class GarbageCollectionTest {

    @BeforeEach
    void setUp(SleeperSystemTest sleeper, AfterTestReports reporting, AfterTestPurgeQueues purgeQueues) {
        sleeper.connectToInstance(MAIN);
    }

    @Test
    void shouldGarbageCollectFilesAfterCompaction(SleeperSystemTest sleeper, InMemorySystemTestDrivers drivers) {
        // Given
        sleeper.updateTableProperties(Map.of(
                COMPACTION_STRATEGY_CLASS, BasicCompactionStrategy.class.getName(),
                COMPACTION_FILES_BATCH_SIZE, "5",
                GARBAGE_COLLECTOR_DELAY_BEFORE_DELETION, "0"));
        RecordNumbers numbers = sleeper.scrambleNumberedRecords(LongStream.range(0, 50));
        sleeper.ingest().direct(null)
                .numberedRecords(numbers.range(0, 10))
                .numberedRecords(numbers.range(10, 20))
                .numberedRecords(numbers.range(20, 30))
                .numberedRecords(numbers.range(30, 40))
                .numberedRecords(numbers.range(40, 50));
        sleeper.compaction().createJobs(1).invokeTasks(1).waitForJobs();

        // When
        sleeper.garbageCollection().invoke().waitFor();

        // Then
        assertThat(sleeper.directQuery().allRecordsInTable())
                .containsExactlyInAnyOrderElementsOf(sleeper.generateNumberedRecords(LongStream.range(0, 50)));
        Approvals.verify(printFiles(sleeper.partitioning().tree(), sleeper.tableFiles().all()));
        assertThat(drivers.data().files()).hasSize(1);
    }
}
