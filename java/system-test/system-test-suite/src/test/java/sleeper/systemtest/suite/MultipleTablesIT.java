/*
 * Copyright 2022-2023 Crown Copyright
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
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import sleeper.core.partition.PartitionTree;
import sleeper.core.schema.Schema;
import sleeper.core.statestore.FileInfoFactory;
import sleeper.systemtest.suite.dsl.SleeperSystemTest;
import sleeper.systemtest.suite.fixtures.SystemTestSchema;
import sleeper.systemtest.suite.testutil.PurgeQueueExtension;

import java.util.List;
import java.util.Map;
import java.util.stream.LongStream;

import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.configuration.properties.instance.CdkDefinedInstanceProperty.COMPACTION_JOB_QUEUE_URL;
import static sleeper.configuration.properties.instance.CdkDefinedInstanceProperty.INGEST_JOB_QUEUE_URL;
import static sleeper.configuration.properties.instance.CdkDefinedInstanceProperty.PARTITION_SPLITTING_QUEUE_URL;
import static sleeper.configuration.properties.table.TableProperty.PARTITION_SPLIT_THRESHOLD;
import static sleeper.core.testutils.printers.FileInfoPrinter.printExpectedFilesForAllTables;
import static sleeper.core.testutils.printers.FileInfoPrinter.printTableFilesExpectingIdentical;
import static sleeper.core.testutils.printers.PartitionsPrinter.printExpectedPartitionsForAllTables;
import static sleeper.core.testutils.printers.PartitionsPrinter.printTablePartitionsExpectingIdentical;
import static sleeper.systemtest.datageneration.GenerateNumberedValue.addPrefix;
import static sleeper.systemtest.datageneration.GenerateNumberedValue.numberStringAndZeroPadTo;
import static sleeper.systemtest.datageneration.GenerateNumberedValueOverrides.overrideField;
import static sleeper.systemtest.suite.fixtures.SystemTestInstance.MAIN;
import static sleeper.systemtest.suite.testutil.PartitionsTestHelper.partitionsBuilder;

@Tag("SystemTest")
public class MultipleTablesIT {
    private final SleeperSystemTest sleeper = SleeperSystemTest.getInstance();
    private final Schema schema = SystemTestSchema.DEFAULT_SCHEMA;
    @RegisterExtension
    public final PurgeQueueExtension purgeQueue = PurgeQueueExtension
            .purgeIfTestFailed(sleeper, INGEST_JOB_QUEUE_URL, PARTITION_SPLITTING_QUEUE_URL, COMPACTION_JOB_QUEUE_URL);

    @BeforeEach
    void setUp() {
        sleeper.connectToInstanceNoTables(MAIN);
    }

    @Test
    void shouldCreate20Tables() {
        sleeper.tables().createMany(5, schema);

        assertThat(sleeper.tables().loadIdentities())
                .hasSize(5);
    }

    @Test
    void shouldIngestOneFileTo5Tables() throws Exception {
        // Given we have 5 tables
        // And we have one source file to be ingested
        sleeper.tables().createMany(5, schema);
        sleeper.sourceFiles().createWithNumberedRecords(schema, "file.parquet", LongStream.range(0, 100));

        // When we send an ingest job with the source file to all 20 tables
        sleeper.ingest().byQueue().sendSourceFilesToAllTables("file.parquet")
                .invokeTask().waitForJobs();

        // Then all tables should contain the source file records
        // And all tables should have one active file
        assertThat(sleeper.query().byQueue().allRecordsByTable())
                .hasSize(5)
                .allSatisfy(((table, records) ->
                        assertThat(records).containsExactlyElementsOf(
                                sleeper.generateNumberedRecords(schema, LongStream.range(0, 100)))));
        assertThat(sleeper.tableFiles().activeByTable())
                .hasSize(5)
                .allSatisfy((table, files) ->
                        assertThat(files).hasSize(1));
    }

    @Test
    void shouldSplitPartitionsOf5TablesWith100RecordsAndThresholdOf20() throws InterruptedException {
        // Given we have 5 tables with a split threshold of 20
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
        var tables = sleeper.tables().loadIdentities();
        var partitionsByTable = sleeper.partitioning().treeByTable();
        var filesByTable = sleeper.tableFiles().activeByTable();
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
        FileInfoFactory fileInfoFactory = FileInfoFactory.from(expectedPartitions);
        assertThat(printTableFilesExpectingIdentical(partitionsByTable, filesByTable))
                .isEqualTo(printExpectedFilesForAllTables(tables, expectedPartitions, List.of(
                        fileInfoFactory.partitionFile("LLL", 12),
                        fileInfoFactory.partitionFile("LLR", 13),
                        fileInfoFactory.partitionFile("LRL", 12),
                        fileInfoFactory.partitionFile("LRR", 13),
                        fileInfoFactory.partitionFile("RLL", 12),
                        fileInfoFactory.partitionFile("RLR", 13),
                        fileInfoFactory.partitionFile("RRL", 12),
                        fileInfoFactory.partitionFile("RRR", 13)
                )));
    }
}
