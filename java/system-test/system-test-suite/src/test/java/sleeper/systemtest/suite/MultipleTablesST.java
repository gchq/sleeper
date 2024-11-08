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

import sleeper.compaction.core.strategy.impl.BasicCompactionStrategy;
import sleeper.core.schema.Schema;
import sleeper.systemtest.dsl.SleeperSystemTest;
import sleeper.systemtest.dsl.extension.AfterTestPurgeQueues;
import sleeper.systemtest.suite.fixtures.SystemTestSchema;
import sleeper.systemtest.suite.testutil.Slow;
import sleeper.systemtest.suite.testutil.SystemTest;

import java.util.Map;
import java.util.stream.LongStream;

import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.COMPACTION_JOB_QUEUE_URL;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.INGEST_JOB_QUEUE_URL;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.PARTITION_SPLITTING_JOB_QUEUE_URL;
import static sleeper.core.properties.table.TableProperty.COMPACTION_FILES_BATCH_SIZE;
import static sleeper.core.properties.table.TableProperty.COMPACTION_STRATEGY_CLASS;
import static sleeper.core.properties.table.TableProperty.GARBAGE_COLLECTOR_DELAY_BEFORE_DELETION;
import static sleeper.core.properties.table.TableProperty.PARTITION_SPLIT_THRESHOLD;
import static sleeper.core.testutils.printers.FileReferencePrinter.printTableFilesExpectingIdentical;
import static sleeper.core.testutils.printers.PartitionsPrinter.printTablePartitionsExpectingIdentical;
import static sleeper.systemtest.dsl.sourcedata.GenerateNumberedValue.addPrefix;
import static sleeper.systemtest.dsl.sourcedata.GenerateNumberedValue.numberStringAndZeroPadTo;
import static sleeper.systemtest.dsl.sourcedata.GenerateNumberedValueOverrides.overrideField;
import static sleeper.systemtest.dsl.testutil.SystemTestTableMetricsHelper.tableMetrics;
import static sleeper.systemtest.suite.fixtures.SystemTestInstance.MAIN;

@SystemTest
@Slow // Slow because compactions run for 200 tables in one task
public class MultipleTablesST {
    private final Schema schema = SystemTestSchema.DEFAULT_SCHEMA;
    private static final int NUMBER_OF_TABLES = 200;

    @BeforeEach
    void setUp(SleeperSystemTest sleeper, AfterTestPurgeQueues purgeQueues) {
        sleeper.connectToInstanceNoTables(MAIN);
        purgeQueues.purgeIfTestFailed(INGEST_JOB_QUEUE_URL, PARTITION_SPLITTING_JOB_QUEUE_URL, COMPACTION_JOB_QUEUE_URL);
    }

    @Test
    void shouldCreateMultipleTables(SleeperSystemTest sleeper) {
        sleeper.tables().createMany(NUMBER_OF_TABLES, schema);

        assertThat(sleeper.tables().list())
                .hasSize(NUMBER_OF_TABLES);
    }

    @Test
    void shouldIngestOneFileToMultipleTables(SleeperSystemTest sleeper) {
        // Given we have several tables
        // And we have one source file to be ingested
        sleeper.tables().createMany(NUMBER_OF_TABLES, schema);
        sleeper.sourceFiles().createWithNumberedRecords(schema, "file.parquet", LongStream.range(0, 100));

        // When we send an ingest job with the source file to all tables
        sleeper.ingest().byQueue().sendSourceFilesToAllTables("file.parquet")
                .invokeTask().waitForJobs();

        // Then all tables should contain the source file records
        // And all tables should have one active file
        assertThat(sleeper.query().byQueue().allRecordsByTable())
                .hasSize(NUMBER_OF_TABLES)
                .allSatisfy(((table, records) -> assertThat(records).containsExactlyElementsOf(
                        sleeper.generateNumberedRecords(schema, LongStream.range(0, 100)))));
        assertThat(sleeper.tableFiles().referencesByTable())
                .hasSize(NUMBER_OF_TABLES)
                .allSatisfy((table, files) -> assertThat(files).hasSize(1));
    }

    @Test
    void shouldCompactAndGCMultipleTables(SleeperSystemTest sleeper) {
        // Given we have several tables
        // And we ingest two source files as separate jobs
        sleeper.tables().createManyWithProperties(NUMBER_OF_TABLES, schema, Map.of(
                COMPACTION_STRATEGY_CLASS, BasicCompactionStrategy.class.getName(),
                COMPACTION_FILES_BATCH_SIZE, "2",
                GARBAGE_COLLECTOR_DELAY_BEFORE_DELETION, "0"));
        sleeper.sourceFiles()
                .createWithNumberedRecords(schema, "file1.parquet", LongStream.range(0, 50))
                .createWithNumberedRecords(schema, "file2.parquet", LongStream.range(50, 100));
        sleeper.ingest().byQueue()
                .sendSourceFilesToAllTables("file1.parquet")
                .sendSourceFilesToAllTables("file2.parquet")
                .invokeTask().waitForJobs();

        // When we run compaction and GC
        sleeper.compaction().createJobs(NUMBER_OF_TABLES).invokeTasks(1).waitForJobs();
        sleeper.garbageCollection().invoke().waitFor();

        // Then all tables should have one active file with the expected records, and none ready for GC
        assertThat(sleeper.query().byQueue().allRecordsByTable())
                .hasSize(NUMBER_OF_TABLES)
                .allSatisfy(((table, records) -> assertThat(records).containsExactlyElementsOf(
                        sleeper.generateNumberedRecords(schema, LongStream.range(0, 100)))));
        var partitionsByTable = sleeper.partitioning().treeByTable();
        var filesByTable = sleeper.tableFiles().filesByTable();
        Approvals.verify(printTableFilesExpectingIdentical(partitionsByTable, filesByTable));
    }

    @Test
    void shouldSplitPartitionsOfMultipleTables(SleeperSystemTest sleeper) {
        // Given we have several tables with a split threshold of 20
        // And we ingest a file of 100 records to each table
        sleeper.tables().createManyWithProperties(NUMBER_OF_TABLES, schema,
                Map.of(PARTITION_SPLIT_THRESHOLD, "20"));
        sleeper.setGeneratorOverrides(
                overrideField(SystemTestSchema.ROW_KEY_FIELD_NAME,
                        numberStringAndZeroPadTo(2).then(addPrefix("row-"))));
        sleeper.sourceFiles().createWithNumberedRecords(schema, "file.parquet", LongStream.range(0, 100));
        sleeper.ingest().byQueue().sendSourceFilesToAllTables("file.parquet")
                .invokeTask().waitForJobs();

        // When we run 3 partition splits with compactions
        sleeper.partitioning().split();
        sleeper.compaction().splitFilesAndRunJobs(NUMBER_OF_TABLES * 2);
        sleeper.partitioning().split();
        sleeper.compaction().splitFilesAndRunJobs(NUMBER_OF_TABLES * 4);
        sleeper.partitioning().split();
        sleeper.compaction().splitFilesAndRunJobs(NUMBER_OF_TABLES * 8);

        // Then all tables have their records split over 8 leaf partitions
        assertThat(sleeper.directQuery().byQueue().allRecordsByTable())
                .hasSize(NUMBER_OF_TABLES)
                .allSatisfy((table, records) -> assertThat(records)
                        .containsExactlyInAnyOrderElementsOf(
                                sleeper.generateNumberedRecords(schema, LongStream.range(0, 100))));
        var partitionsByTable = sleeper.partitioning().treeByTable();
        var filesByTable = sleeper.tableFiles().filesByTable();
        Approvals.verify(printTablePartitionsExpectingIdentical(schema, partitionsByTable) + "\n" +
                printTableFilesExpectingIdentical(partitionsByTable, filesByTable));
    }

    @Test
    void shouldGenerateMetricsForMultipleTables(SleeperSystemTest sleeper) {
        // Given we have several tables
        // And we ingest two source files as separate jobs
        sleeper.tables().createManyWithProperties(NUMBER_OF_TABLES, schema, Map.of(
                COMPACTION_STRATEGY_CLASS, BasicCompactionStrategy.class.getName(),
                COMPACTION_FILES_BATCH_SIZE, "2",
                GARBAGE_COLLECTOR_DELAY_BEFORE_DELETION, "0"));
        sleeper.sourceFiles()
                .createWithNumberedRecords(schema, "file1.parquet", LongStream.range(0, 50))
                .createWithNumberedRecords(schema, "file2.parquet", LongStream.range(50, 100));
        sleeper.ingest().byQueue()
                .sendSourceFilesToAllTables("file1.parquet")
                .sendSourceFilesToAllTables("file2.parquet")
                .invokeTask().waitForJobs();

        // When we compute table metrics
        sleeper.tableMetrics().generate();

        // Then each table has the expected metrics
        sleeper.tables().forEach(() -> {
            assertThat(sleeper.tableMetrics().get()).isEqualTo(tableMetrics(sleeper)
                    .partitionCount(1).leafPartitionCount(1)
                    .fileCount(2).recordCount(100)
                    .averageFileReferencesPerPartition(2)
                    .build());
        });
    }
}
