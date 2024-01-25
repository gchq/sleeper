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

import sleeper.core.partition.PartitionTree;
import sleeper.core.schema.Schema;
import sleeper.core.statestore.FileReferenceFactory;
import sleeper.systemtest.suite.dsl.SleeperSystemTest;
import sleeper.systemtest.suite.dsl.reports.SystemTestReports;
import sleeper.systemtest.suite.fixtures.SystemTestSchema;
import sleeper.systemtest.suite.testutil.AfterTestPurgeQueues;
import sleeper.systemtest.suite.testutil.AfterTestReports;
import sleeper.systemtest.suite.testutil.SystemTest;

import java.util.List;
import java.util.Map;
import java.util.stream.LongStream;

import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.configuration.properties.instance.CdkDefinedInstanceProperty.COMPACTION_JOB_QUEUE_URL;
import static sleeper.configuration.properties.table.TableProperty.PARTITION_SPLIT_THRESHOLD;
import static sleeper.core.testutils.printers.FileReferencePrinter.printExpectedFilesForAllTables;
import static sleeper.core.testutils.printers.FileReferencePrinter.printTableFilesExpectingIdentical;
import static sleeper.core.testutils.printers.PartitionsPrinter.printExpectedPartitionsForAllTables;
import static sleeper.core.testutils.printers.PartitionsPrinter.printTablePartitionsExpectingIdentical;
import static sleeper.systemtest.datageneration.GenerateNumberedValue.addPrefix;
import static sleeper.systemtest.datageneration.GenerateNumberedValue.numberStringAndZeroPadTo;
import static sleeper.systemtest.datageneration.GenerateNumberedValueOverrides.overrideField;
import static sleeper.systemtest.suite.fixtures.SystemTestInstance.MAIN;
import static sleeper.systemtest.suite.testutil.PartitionsTestHelper.partitionsBuilder;

@SystemTest
public class CompactionIT {

    private final Schema schema = SystemTestSchema.DEFAULT_SCHEMA;

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
        sleeper.updateTableProperties(Map.of(PARTITION_SPLIT_THRESHOLD, "50"));
        sleeper.sourceFiles()
                .createWithNumberedRecords("file.parquet", LongStream.range(0, 100));
        sleeper.ingest().byQueue().sendSourceFiles("file.parquet")
                .invokeTask().waitForJobs();

        // When
        sleeper.partitioning().split();
        sleeper.compaction().splitAndCompactFiles();

        // Then
        assertThat(sleeper.directQuery().byQueue().allRecordsInTable())
                .containsExactlyInAnyOrderElementsOf(
                        sleeper.generateNumberedRecords(schema, LongStream.range(0, 100)));
        var tables = sleeper.tables().loadIdentities();
        var partitionsByTable = sleeper.partitioning().treeByTable();
        var filesByTable = sleeper.tableFiles().activeByTable();
        PartitionTree expectedPartitions = partitionsBuilder(schema)
                .rootFirst("root")
                .splitToNewChildren("root", "L", "R", "row-50")
                .buildTree();
        assertThat(printTablePartitionsExpectingIdentical(schema, partitionsByTable))
                .isEqualTo(printExpectedPartitionsForAllTables(schema, tables, expectedPartitions));
        FileReferenceFactory fileReferenceFactory = FileReferenceFactory.from(expectedPartitions);
        assertThat(printTableFilesExpectingIdentical(partitionsByTable, filesByTable))
                .isEqualTo(printExpectedFilesForAllTables(tables, expectedPartitions, List.of(
                        fileReferenceFactory.partitionFile("L", 50),
                        fileReferenceFactory.partitionFile("R", 50)
                )));
    }
}
