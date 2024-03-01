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
import sleeper.systemtest.dsl.SleeperSystemTest;
import sleeper.systemtest.dsl.extension.AfterTestPurgeQueues;
import sleeper.systemtest.suite.fixtures.SystemTestSchema;
import sleeper.systemtest.suite.testutil.SystemTest;

import java.util.List;
import java.util.Map;
import java.util.stream.LongStream;

import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.configuration.properties.instance.CdkDefinedInstanceProperty.COMPACTION_JOB_QUEUE_URL;
import static sleeper.configuration.properties.instance.CdkDefinedInstanceProperty.INGEST_JOB_QUEUE_URL;
import static sleeper.configuration.properties.instance.CdkDefinedInstanceProperty.PARTITION_SPLITTING_JOB_QUEUE_URL;
import static sleeper.configuration.properties.table.TableProperty.PARTITION_SPLIT_THRESHOLD;
import static sleeper.core.testutils.printers.FileReferencePrinter.printExpectedFilesForAllTables;
import static sleeper.core.testutils.printers.FileReferencePrinter.printTableFilesExpectingIdentical;
import static sleeper.core.testutils.printers.PartitionsPrinter.printExpectedPartitionsForAllTables;
import static sleeper.core.testutils.printers.PartitionsPrinter.printTablePartitionsExpectingIdentical;
import static sleeper.systemtest.dsl.sourcedata.GenerateNumberedValue.addPrefix;
import static sleeper.systemtest.dsl.sourcedata.GenerateNumberedValue.numberStringAndZeroPadTo;
import static sleeper.systemtest.dsl.sourcedata.GenerateNumberedValueOverrides.overrideField;
import static sleeper.systemtest.suite.fixtures.SystemTestInstance.MAIN;
import static sleeper.systemtest.suite.testutil.PartitionsTestHelper.partitionsBuilder;

@SystemTest
public class MultipleTablesIT {
    private final Schema schema = SystemTestSchema.DEFAULT_SCHEMA;

    @BeforeEach
    void setUp(SleeperSystemTest sleeper, AfterTestPurgeQueues purgeQueues) {
        sleeper.connectToInstanceNoTables(MAIN);
        purgeQueues.purgeIfTestFailed(INGEST_JOB_QUEUE_URL, PARTITION_SPLITTING_JOB_QUEUE_URL, COMPACTION_JOB_QUEUE_URL);
    }

    @Test
    void shouldCreateMultipleTables(SleeperSystemTest sleeper) {
        sleeper.tables().createMany(5, schema);

        assertThat(sleeper.tables().list())
                .hasSize(5);
    }

    @Test
    void shouldIngestOneFileToMultipleTables(SleeperSystemTest sleeper) {
        // Given we have several tables
        // And we have one source file to be ingested
        sleeper.tables().createMany(5, schema);
        sleeper.sourceFiles().createWithNumberedRecords(schema, "file.parquet", LongStream.range(0, 100));

        // When we send an ingest job with the source file to all tables
        sleeper.ingest().byQueue().sendSourceFilesToAllTables("file.parquet")
                .invokeTask().waitForJobs();

        // Then all tables should contain the source file records
        // And all tables should have one active file
        assertThat(sleeper.query().byQueue().allRecordsByTable())
                .hasSize(5)
                .allSatisfy(((table, records) -> assertThat(records).containsExactlyElementsOf(
                        sleeper.generateNumberedRecords(schema, LongStream.range(0, 100)))));
        assertThat(sleeper.tableFiles().referencesByTable())
                .hasSize(5)
                .allSatisfy((table, files) -> assertThat(files).hasSize(1));
    }

    @Test
    void shouldSplitPartitionsOfMultipleTablesWith100RecordsAndThresholdOf20(SleeperSystemTest sleeper) {
        // Given we have several tables with a split threshold of 20
        // And we ingest a file of 100 records to each table
        sleeper.tables().createManyWithProperties(5, schema,
                Map.of(PARTITION_SPLIT_THRESHOLD, "20"));
        sleeper.setGeneratorOverrides(
                overrideField(SystemTestSchema.ROW_KEY_FIELD_NAME,
                        numberStringAndZeroPadTo(2).then(addPrefix("row-"))));
        sleeper.sourceFiles().createWithNumberedRecords(schema, "file.parquet", LongStream.range(0, 100));
        sleeper.ingest().byQueue().sendSourceFilesToAllTables("file.parquet")
                .invokeTask().waitForJobs();

        // When we run 3 partition splits with compactions
        sleeper.partitioning().split();
        sleeper.compaction().splitAndCompactFiles();
        sleeper.partitioning().split();
        sleeper.compaction().splitAndCompactFiles();
        sleeper.partitioning().split();
        sleeper.compaction().splitAndCompactFiles();

        // Then all tables have their records split over 8 leaf partitions
        assertThat(sleeper.directQuery().byQueue().allRecordsByTable())
                .hasSize(5)
                .allSatisfy((table, records) -> assertThat(records)
                        .containsExactlyInAnyOrderElementsOf(
                                sleeper.generateNumberedRecords(schema, LongStream.range(0, 100))));
        var tables = sleeper.tables().list();
        var partitionsByTable = sleeper.partitioning().treeByTable();
        var filesByTable = sleeper.tableFiles().referencesByTable();
        PartitionTree expectedPartitions = partitionsBuilder(schema)
                .rootFirst("root")
                .splitToNewChildren("root", "L", "R", "row-50")
                .splitToNewChildren("L", "LL", "LR", "row-25")
                .splitToNewChildren("R", "RL", "RR", "row-75")
                .splitToNewChildren("LL", "LLL", "LLR", "row-12")
                .splitToNewChildren("LR", "LRL", "LRR", "row-37")
                .splitToNewChildren("RL", "RLL", "RLR", "row-62")
                .splitToNewChildren("RR", "RRL", "RRR", "row-87")
                .buildTree();
        assertThat(printTablePartitionsExpectingIdentical(schema, partitionsByTable))
                .isEqualTo(printExpectedPartitionsForAllTables(schema, tables, expectedPartitions));
        FileReferenceFactory fileReferenceFactory = FileReferenceFactory.from(expectedPartitions);
        assertThat(printTableFilesExpectingIdentical(partitionsByTable, filesByTable))
                .isEqualTo(printExpectedFilesForAllTables(tables, expectedPartitions, List.of(
                        fileReferenceFactory.partitionFile("LLL", 12),
                        fileReferenceFactory.partitionFile("LLR", 13),
                        fileReferenceFactory.partitionFile("LRL", 12),
                        fileReferenceFactory.partitionFile("LRR", 13),
                        fileReferenceFactory.partitionFile("RLL", 12),
                        fileReferenceFactory.partitionFile("RLR", 13),
                        fileReferenceFactory.partitionFile("RRL", 12),
                        fileReferenceFactory.partitionFile("RRR", 13))));
    }
}
