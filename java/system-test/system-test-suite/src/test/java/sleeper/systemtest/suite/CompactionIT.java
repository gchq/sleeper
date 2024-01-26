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
import sleeper.core.schema.Schema;
import sleeper.core.statestore.FileReference;
import sleeper.core.statestore.FileReferenceFactory;
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
import static sleeper.configuration.properties.table.TableProperty.PARTITION_SPLIT_THRESHOLD;
import static sleeper.core.testutils.printers.FileReferencePrinter.printFiles;
import static sleeper.core.testutils.printers.PartitionsPrinter.printPartitions;
import static sleeper.systemtest.datageneration.GenerateNumberedValue.addPrefix;
import static sleeper.systemtest.datageneration.GenerateNumberedValue.numberStringAndZeroPadTo;
import static sleeper.systemtest.datageneration.GenerateNumberedValueOverrides.overrideField;
import static sleeper.systemtest.suite.fixtures.SystemTestInstance.MAIN;
import static sleeper.systemtest.suite.testutil.PartitionsTestHelper.partitionsBuilder;

@SystemTest
public class CompactionIT {
    @TempDir
    private Path tempDir;

    @BeforeEach
    void setUp(SleeperSystemTest sleeper, AfterTestReports reporting, AfterTestPurgeQueues purgeQueues) {
        sleeper.connectToInstance(MAIN);
        reporting.reportIfTestFailed(SystemTestReports.SystemTestBuilder::compactionTasksAndJobs);
        purgeQueues.purgeIfTestFailed(COMPACTION_JOB_QUEUE_URL);
    }

    @Test
    void shouldSplitAndCompactOneFile(SleeperSystemTest sleeper) throws InterruptedException {
        // Given
        sleeper.setGeneratorOverrides(
                overrideField(SystemTestSchema.ROW_KEY_FIELD_NAME,
                        numberStringAndZeroPadTo(2).then(addPrefix("row-"))));
        sleeper.updateTableProperties(Map.of(
                PARTITION_SPLIT_THRESHOLD, "50",
                COMPACTION_STRATEGY_CLASS, BasicCompactionStrategy.class.getName(),
                COMPACTION_FILES_BATCH_SIZE, "1"));
        sleeper.ingest().direct(tempDir).numberedRecords(LongStream.range(0, 100));

        // When
        sleeper.partitioning().split();
        sleeper.compaction().createJobs().invokeTasks(1).waitForJobs();

        // Then
        assertThat(sleeper.directQuery().allRecordsInTable())
                .containsExactlyInAnyOrderElementsOf(sleeper.generateNumberedRecords(LongStream.range(0, 100)));
        Schema schema = sleeper.tableProperties().getSchema();
        PartitionTree partitions = sleeper.partitioning().tree();
        List<FileReference> activeFiles = sleeper.tableFiles().active();
        PartitionTree expectedPartitions = partitionsBuilder(schema).rootFirst("root")
                .splitToNewChildren("root", "L", "R", "row-50")
                .buildTree();
        assertThat(printPartitions(schema, partitions))
                .isEqualTo(printPartitions(schema, expectedPartitions));
        FileReferenceFactory fileReferenceFactory = FileReferenceFactory.from(expectedPartitions);
        assertThat(printFiles(partitions, activeFiles))
                .isEqualTo(printFiles(expectedPartitions, List.of(
                        fileReferenceFactory.partitionFile("L", 50),
                        fileReferenceFactory.partitionFile("R", 50)
                )));
    }

    @Test
    void shouldSplitAndCompactMultipleFiles(SleeperSystemTest sleeper) throws InterruptedException {
        // Given
        sleeper.setGeneratorOverrides(
                overrideField(SystemTestSchema.ROW_KEY_FIELD_NAME,
                        numberStringAndZeroPadTo(2).then(addPrefix("row-"))));
        sleeper.updateTableProperties(Map.of(
                PARTITION_SPLIT_THRESHOLD, "50",
                COMPACTION_STRATEGY_CLASS, BasicCompactionStrategy.class.getName(),
                COMPACTION_FILES_BATCH_SIZE, "2"));
        sleeper.ingest().direct(tempDir).numberedRecords(LongStream.range(0, 50));
        sleeper.ingest().direct(tempDir).numberedRecords(LongStream.range(50, 100));

        // When
        sleeper.partitioning().split();
        sleeper.compaction().createJobs().invokeTasks(1).waitForJobs();

        // Then
        assertThat(sleeper.directQuery().allRecordsInTable())
                .containsExactlyInAnyOrderElementsOf(sleeper.generateNumberedRecords(LongStream.range(0, 100)));
        Schema schema = sleeper.tableProperties().getSchema();
        PartitionTree partitions = sleeper.partitioning().tree();
        List<FileReference> activeFiles = sleeper.tableFiles().active();
        PartitionTree expectedPartitions = partitionsBuilder(schema).rootFirst("root")
                .splitToNewChildren("root", "L", "R", "row-50")
                .buildTree();
        assertThat(printPartitions(schema, partitions))
                .isEqualTo(printPartitions(schema, expectedPartitions));
        FileReferenceFactory fileReferenceFactory = FileReferenceFactory.from(expectedPartitions);
        assertThat(printFiles(partitions, activeFiles))
                .isEqualTo(printFiles(expectedPartitions, List.of(
                        fileReferenceFactory.partitionFile("L", 50),
                        fileReferenceFactory.partitionFile("R", 50)
                )));
    }

    @Test
    void shouldSplitAndCompactOneFileMultipleTimes(SleeperSystemTest sleeper) throws InterruptedException {
        // Given
        sleeper.setGeneratorOverrides(
                overrideField(SystemTestSchema.ROW_KEY_FIELD_NAME,
                        numberStringAndZeroPadTo(3).then(addPrefix("row-"))));
        sleeper.updateTableProperties(Map.of(
                PARTITION_SPLIT_THRESHOLD, "50",
                COMPACTION_STRATEGY_CLASS, BasicCompactionStrategy.class.getName(),
                COMPACTION_FILES_BATCH_SIZE, "1"));
        sleeper.ingest().direct(tempDir).numberedRecords(LongStream.range(0, 200));

        // When
        sleeper.partitioning().split();
        sleeper.compaction().createJobs().invokeTasks(1).waitForJobs();
        sleeper.partitioning().split();
        sleeper.compaction().createJobs().invokeTasks(1).waitForJobs();

        // Then
        assertThat(sleeper.directQuery().allRecordsInTable())
                .containsExactlyInAnyOrderElementsOf(sleeper.generateNumberedRecords(LongStream.range(0, 200)));
        Schema schema = sleeper.tableProperties().getSchema();
        PartitionTree partitions = sleeper.partitioning().tree();
        List<FileReference> activeFiles = sleeper.tableFiles().active();
        PartitionTree expectedPartitions = partitionsBuilder(schema).rootFirst("root")
                .splitToNewChildren("root", "L", "R", "row-100")
                .splitToNewChildren("L", "LL", "LR", "row-050")
                .splitToNewChildren("R", "RL", "RR", "row-150")
                .buildTree();
        assertThat(printPartitions(schema, partitions))
                .isEqualTo(printPartitions(schema, expectedPartitions));
        FileReferenceFactory fileReferenceFactory = FileReferenceFactory.from(expectedPartitions);
        assertThat(printFiles(partitions, activeFiles))
                .isEqualTo(printFiles(expectedPartitions, List.of(
                        fileReferenceFactory.partitionFile("LL", 50),
                        fileReferenceFactory.partitionFile("LR", 50),
                        fileReferenceFactory.partitionFile("RL", 50),
                        fileReferenceFactory.partitionFile("RR", 50)
                )));
    }
}
