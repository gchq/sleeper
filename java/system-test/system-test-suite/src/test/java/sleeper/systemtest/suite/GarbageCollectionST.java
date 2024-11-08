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

import org.approvaltests.Approvals;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import sleeper.compaction.core.strategy.impl.BasicCompactionStrategy;
import sleeper.systemtest.dsl.SleeperSystemTest;
import sleeper.systemtest.dsl.extension.AfterTestPurgeQueues;
import sleeper.systemtest.dsl.extension.AfterTestReports;
import sleeper.systemtest.dsl.ingest.SystemTestDirectIngest;
import sleeper.systemtest.dsl.reporting.SystemTestReports;
import sleeper.systemtest.dsl.sourcedata.RecordNumbers;
import sleeper.systemtest.suite.testutil.SystemTest;

import java.nio.file.Path;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.stream.IntStream;
import java.util.stream.LongStream;
import java.util.stream.StreamSupport;

import static java.util.stream.Collectors.toSet;
import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.COMPACTION_JOB_QUEUE_URL;
import static sleeper.core.properties.table.TableProperty.COMPACTION_FILES_BATCH_SIZE;
import static sleeper.core.properties.table.TableProperty.COMPACTION_STRATEGY_CLASS;
import static sleeper.core.properties.table.TableProperty.GARBAGE_COLLECTOR_DELAY_BEFORE_DELETION;
import static sleeper.core.testutils.printers.FileReferencePrinter.printFiles;
import static sleeper.systemtest.suite.fixtures.SystemTestInstance.MAIN;

@SystemTest
public class GarbageCollectionST {
    @TempDir
    private Path tempDir;

    @BeforeEach
    void setUp(SleeperSystemTest sleeper, AfterTestReports reporting, AfterTestPurgeQueues purgeQueues) {
        sleeper.connectToInstance(MAIN);
        reporting.reportIfTestFailed(SystemTestReports.SystemTestBuilder::compactionTasksAndJobs);
        purgeQueues.purgeIfTestFailed(COMPACTION_JOB_QUEUE_URL);
    }

    @Test
    void shouldGarbageCollectFilesAfterCompaction(SleeperSystemTest sleeper) {
        // Given
        sleeper.updateTableProperties(Map.of(
                COMPACTION_STRATEGY_CLASS, BasicCompactionStrategy.class.getName(),
                COMPACTION_FILES_BATCH_SIZE, "10",
                GARBAGE_COLLECTOR_DELAY_BEFORE_DELETION, "0"));
        RecordNumbers numbers = sleeper.scrambleNumberedRecords(LongStream.range(0, 20_000));
        SystemTestDirectIngest ingest = sleeper.ingest().direct(tempDir);
        IntStream.range(0, 2000)
                .mapToObj(i -> numbers.range(i * 10, i * 10 + 10))
                .forEach(range -> ingest.numberedRecords(range));
        sleeper.compaction().createJobs(200).invokeTasks(1).waitForJobs();

        // When
        sleeper.garbageCollection().invoke().waitFor();

        // Then
        assertThat(new HashSet<>(sleeper.directQuery().allRecordsInTable()))
                .isEqualTo(setFrom(sleeper.generateNumberedRecords(LongStream.range(0, 20_000))));
        Approvals.verify(printFiles(sleeper.partitioning().tree(), sleeper.tableFiles().all()));
    }

    private static <T> Set<T> setFrom(Iterable<T> iterable) {
        return StreamSupport.stream(iterable.spliterator(), false).collect(toSet());
    }
}
