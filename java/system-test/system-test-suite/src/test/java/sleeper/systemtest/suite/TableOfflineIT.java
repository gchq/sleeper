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

import sleeper.core.partition.PartitionTree;
import sleeper.core.schema.Schema;
import sleeper.core.statestore.FileReference;
import sleeper.core.statestore.FileReferenceFactory;
import sleeper.systemtest.dsl.SleeperSystemTest;
import sleeper.systemtest.dsl.extension.AfterTestPurgeQueues;
import sleeper.systemtest.dsl.extension.AfterTestReports;
import sleeper.systemtest.dsl.reporting.SystemTestReports;
import sleeper.systemtest.dsl.sourcedata.RecordNumbers;
import sleeper.systemtest.suite.fixtures.SystemTestSchema;
import sleeper.systemtest.suite.testutil.SystemTest;

import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.stream.LongStream;

import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.configuration.properties.instance.CdkDefinedInstanceProperty.COMPACTION_JOB_QUEUE_URL;
import static sleeper.configuration.properties.table.TableProperty.COMPACTION_FILES_BATCH_SIZE;
import static sleeper.configuration.properties.table.TableProperty.PARTITION_SPLIT_THRESHOLD;
import static sleeper.configuration.properties.table.TableProperty.TABLE_ONLINE;
import static sleeper.core.testutils.printers.FileReferencePrinter.printFiles;
import static sleeper.core.testutils.printers.PartitionsPrinter.printPartitions;
import static sleeper.systemtest.dsl.sourcedata.GenerateNumberedValue.addPrefix;
import static sleeper.systemtest.dsl.sourcedata.GenerateNumberedValue.numberStringAndZeroPadTo;
import static sleeper.systemtest.dsl.sourcedata.GenerateNumberedValueOverrides.overrideField;
import static sleeper.systemtest.suite.fixtures.SystemTestInstance.MAIN;
import static sleeper.systemtest.suite.testutil.PartitionsTestHelper.partitionsBuilder;

@SystemTest
public class TableOfflineIT {
    @TempDir
    private Path tempDir;
    private PartitionTree expectedPartitions;
    private FileReferenceFactory fileFactory;

    @BeforeEach
    void setUp(SleeperSystemTest sleeper, AfterTestReports reporting, AfterTestPurgeQueues purgeQueues) {
        sleeper.connectToInstance(MAIN);
        reporting.reportIfTestFailed(SystemTestReports.SystemTestBuilder::compactionTasksAndJobs);
        purgeQueues.purgeIfTestFailed(COMPACTION_JOB_QUEUE_URL);
        expectedPartitions = partitionsBuilder(sleeper).singlePartition("root").buildTree();
        fileFactory = FileReferenceFactory.from(expectedPartitions);
    }

    @Test
    void shouldNotSplitPartitionsIfTableIsOffline(SleeperSystemTest sleeper) {
        // Given
        sleeper.updateTableProperties(Map.of(
                PARTITION_SPLIT_THRESHOLD, "20",
                TABLE_ONLINE, "false"));
        sleeper.setGeneratorOverrides(
                overrideField(SystemTestSchema.ROW_KEY_FIELD_NAME,
                        numberStringAndZeroPadTo(2).then(addPrefix("row-"))));
        sleeper.ingest().direct(tempDir).numberedRecords(LongStream.range(0, 100));

        // When
        sleeper.partitioning().split();

        // Then
        assertThat(sleeper.directQuery().allRecordsInTable())
                .containsExactlyInAnyOrderElementsOf(sleeper.generateNumberedRecords(LongStream.range(0, 100)));
        Schema schema = sleeper.tableProperties().getSchema();
        PartitionTree partitions = sleeper.partitioning().tree();
        List<FileReference> activeFiles = sleeper.tableFiles().references();
        assertThat(printPartitions(schema, partitions))
                .isEqualTo(printPartitions(schema, expectedPartitions));
        FileReferenceFactory fileReferenceFactory = FileReferenceFactory.from(expectedPartitions);
        assertThat(printFiles(partitions, activeFiles))
                .isEqualTo(printFiles(expectedPartitions, List.of(
                        fileReferenceFactory.rootFile(100))));
    }

    @Test
    void shouldNotCreateCompactionJobsIfTableIsOffline(SleeperSystemTest sleeper) {
        // Given
        sleeper.updateTableProperties(Map.of(
                COMPACTION_FILES_BATCH_SIZE, "5",
                TABLE_ONLINE, "false"));
        // Files with records 9, 9, 9, 9, 10 (which match SizeRatioStrategy criteria)
        RecordNumbers numbers = sleeper.scrambleNumberedRecords(LongStream.range(0, 46));
        sleeper.ingest().direct(tempDir)
                .numberedRecords(numbers.range(0, 9))
                .numberedRecords(numbers.range(9, 18))
                .numberedRecords(numbers.range(18, 27))
                .numberedRecords(numbers.range(27, 36))
                .numberedRecords(numbers.range(36, 46));

        // When
        sleeper.compaction().createJobs().invokeTasks(0);

        // Then
        assertThat(sleeper.directQuery().allRecordsInTable())
                .containsExactlyInAnyOrderElementsOf(sleeper.generateNumberedRecords(LongStream.range(0, 46)));
        assertThat(printFiles(sleeper.partitioning().tree(), sleeper.tableFiles().all()))
                .isEqualTo(printFiles(expectedPartitions, List.of(
                        fileFactory.rootFile("file1.parquet", 9),
                        fileFactory.rootFile("file2.parquet", 9),
                        fileFactory.rootFile("file3.parquet", 9),
                        fileFactory.rootFile("file4.parquet", 9),
                        fileFactory.rootFile("file5.parquet", 10))));
    }
}
