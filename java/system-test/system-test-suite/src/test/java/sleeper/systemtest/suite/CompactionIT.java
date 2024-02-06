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
import org.junit.jupiter.api.io.TempDir;

import sleeper.compaction.strategy.impl.BasicCompactionStrategy;
import sleeper.core.partition.PartitionTree;
import sleeper.core.statestore.FileReference;
import sleeper.core.statestore.FileReferenceFactory;
import sleeper.systemtest.datageneration.RecordNumbers;
import sleeper.systemtest.suite.dsl.SleeperSystemTest;
import sleeper.systemtest.suite.dsl.reports.SystemTestReports;
import sleeper.systemtest.suite.fixtures.SystemTestSchema;
import sleeper.systemtest.suite.testutil.AfterTestPurgeQueues;
import sleeper.systemtest.suite.testutil.AfterTestReports;
import sleeper.systemtest.suite.testutil.SystemTest;

import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.stream.LongStream;

import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.configuration.properties.instance.CdkDefinedInstanceProperty.COMPACTION_JOB_QUEUE_URL;
import static sleeper.configuration.properties.table.TableProperty.COMPACTION_FILES_BATCH_SIZE;
import static sleeper.configuration.properties.table.TableProperty.COMPACTION_STRATEGY_CLASS;
import static sleeper.core.testutils.printers.FileReferencePrinter.printFiles;
import static sleeper.systemtest.datageneration.GenerateNumberedValue.addPrefix;
import static sleeper.systemtest.datageneration.GenerateNumberedValue.numberStringAndZeroPadTo;
import static sleeper.systemtest.datageneration.GenerateNumberedValueOverrides.overrideField;
import static sleeper.systemtest.suite.fixtures.SystemTestInstance.MAIN;
import static sleeper.systemtest.suite.testutil.PartitionsTestHelper.partitionsBuilder;

@SystemTest
public class CompactionIT {
    @TempDir
    private Path tempDir;
    private FileReferenceFactory factory;
    private PartitionTree initialPartitions;

    @BeforeEach
    void setUp(SleeperSystemTest sleeper, AfterTestReports reporting, AfterTestPurgeQueues purgeQueues) {
        sleeper.connectToInstance(MAIN);
        reporting.reportIfTestFailed(SystemTestReports.SystemTestBuilder::compactionTasksAndJobs);
        purgeQueues.purgeIfTestFailed(COMPACTION_JOB_QUEUE_URL);
        initialPartitions = partitionsBuilder(sleeper).singlePartition("root").buildTree();
        factory = FileReferenceFactory.from(initialPartitions);
    }

    @Test
    void shouldCompactFilesUsingDefaultCompactionStrategy(SleeperSystemTest sleeper) throws InterruptedException {
        // Given
        sleeper.updateTableProperties(Map.of(
                COMPACTION_FILES_BATCH_SIZE, "5"));
        // Files with records 9, 9, 9, 9, 10 (which match SizeRatioStrategy criteria)
        RecordNumbers numbers = sleeper.scrambleNumberedRecords(LongStream.range(0, 46));
        sleeper.ingest().direct(tempDir)
                .numberedRecords(numbers.range(0, 9))
                .numberedRecords(numbers.range(9, 18))
                .numberedRecords(numbers.range(18, 27))
                .numberedRecords(numbers.range(27, 36))
                .numberedRecords(numbers.range(36, 46));

        // When
        sleeper.compaction().createJobs().invokeTasks(1).waitForJobs();

        // Then
        assertThat(sleeper.directQuery().allRecordsInTable())
                .containsExactlyInAnyOrderElementsOf(sleeper.generateNumberedRecords(LongStream.range(0, 46)));
        PartitionTree partitions = sleeper.partitioning().tree();
        List<FileReference> fileReferences = sleeper.tableFiles().references();
        assertThat(printFiles(partitions, fileReferences))
                .isEqualTo(printFiles(initialPartitions, List.of(
                        factory.rootFile("file1.parquet", 46)
                )));
    }

    @Test
    void shouldCompactFilesUsingBasicCompactionStrategy(SleeperSystemTest sleeper) throws InterruptedException {
        // Given
        sleeper.updateTableProperties(Map.of(
                COMPACTION_STRATEGY_CLASS, BasicCompactionStrategy.class.getName(),
                COMPACTION_FILES_BATCH_SIZE, "2"));
        RecordNumbers numbers = sleeper.scrambleNumberedRecords(LongStream.range(0, 100));
        sleeper.ingest().direct(tempDir)
                .numberedRecords(numbers.range(0, 25))
                .numberedRecords(numbers.range(25, 50))
                .numberedRecords(numbers.range(50, 75))
                .numberedRecords(numbers.range(75, 100));

        // When
        sleeper.compaction().createJobs().invokeTasks(1).waitForJobs();

        // Then
        assertThat(sleeper.directQuery().allRecordsInTable())
                .containsExactlyInAnyOrderElementsOf(sleeper.generateNumberedRecords(LongStream.range(0, 100)));
        PartitionTree partitions = sleeper.partitioning().tree();
        List<FileReference> fileReferences = sleeper.tableFiles().references();
        assertThat(printFiles(partitions, fileReferences))
                .isEqualTo(printFiles(initialPartitions, List.of(
                        factory.rootFile("file1.parquet", 50),
                        factory.rootFile("file2.parquet", 50)
                )));
    }

    @Test
    void shouldCompactOneFileIntoExistingFilesOnLeafPartitions(SleeperSystemTest sleeper) throws Exception {
        sleeper.setGeneratorOverrides(overrideField(
                SystemTestSchema.ROW_KEY_FIELD_NAME, numberStringAndZeroPadTo(2).then(addPrefix("row-"))));
        sleeper.partitioning().setPartitions(partitionsBuilder(sleeper)
                .rootFirst("root")
                .splitToNewChildren("root", "L", "R", "row-50")
                .splitToNewChildren("L", "LL", "LR", "row-25")
                .splitToNewChildren("R", "RL", "RR", "row-75")
                .buildTree());
        sleeper.sourceFiles().inDataBucket().writeSketches()
                .createWithNumberedRecords("file.parquet", LongStream.range(0, 100).filter(n -> n % 2 == 1));
        sleeper.ingest().direct(tempDir).numberedRecords(LongStream.range(0, 100).filter(n -> n % 2 == 0));
        sleeper.ingest().toStateStore().addFileWithRecordEstimatesOnPartitions(
                "file.parquet", Map.of(
                        "LL", 13L,
                        "LR", 12L,
                        "RL", 13L,
                        "RR", 12L));
    }
}
