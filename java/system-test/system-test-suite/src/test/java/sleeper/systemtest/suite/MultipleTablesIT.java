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

import sleeper.compaction.strategy.impl.BasicCompactionStrategy;
import sleeper.core.schema.Schema;
import sleeper.systemtest.suite.dsl.SleeperSystemTest;
import sleeper.systemtest.suite.fixtures.SystemTestSchema;
import sleeper.systemtest.suite.testutil.FileInfoSystemTestHelper;

import java.util.Map;
import java.util.stream.LongStream;

import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.configuration.properties.table.TableProperty.COMPACTION_FILES_BATCH_SIZE;
import static sleeper.configuration.properties.table.TableProperty.COMPACTION_STRATEGY_CLASS;
import static sleeper.configuration.properties.table.TableProperty.PARTITION_SPLIT_THRESHOLD;
import static sleeper.systemtest.datageneration.GenerateNumberedValue.stringFromPrefixAndPadToSize;
import static sleeper.systemtest.datageneration.GenerateNumberedValueOverrides.overrideField;
import static sleeper.systemtest.suite.fixtures.SystemTestInstance.MAIN;
import static sleeper.systemtest.suite.testutil.FileInfoSystemTestHelper.fileInfoHelper;
import static sleeper.systemtest.suite.testutil.PartitionsTestHelper.partitionsBuilder;

@Tag("SystemTest")
public class MultipleTablesIT {
    private final SleeperSystemTest sleeper = SleeperSystemTest.getInstance();
    private final Schema schema = SystemTestSchema.DEFAULT_SCHEMA;

    @BeforeEach
    void setUp() {
        sleeper.connectToInstanceNoTables(MAIN);
    }

    @Test
    void shouldCreate200Tables() {
        sleeper.tables().createMany(200, schema);

        assertThat(sleeper.tables().loadIdentities())
                .hasSize(200);
    }

    @Test
    void shouldIngestOneFileTo200Tables() throws Exception {
        // Given we have 200 tables
        // And we have one source file to be ingested
        sleeper.tables().createMany(200, schema);
        sleeper.sourceFiles().createWithNumberedRecords(schema, "file.parquet", LongStream.range(0, 100));

        // When we send an ingest job with the source file to all 200 tables
        sleeper.ingest().byQueue().sendSourceFilesToAllTables("file.parquet")
                .invokeTask().waitForJobs();

        // Then all 200 tables should contain the source file records
        // And all 200 tables should have one active file
        assertThat(sleeper.query().byQueue().allRecordsByTable())
                .hasSize(200)
                .allSatisfy(((table, records) ->
                        assertThat(records).containsExactlyElementsOf(
                                sleeper.generateNumberedRecords(schema, LongStream.range(0, 100)))));
        assertThat(sleeper.tableFiles().activeByTable())
                .hasSize(200)
                .allSatisfy((table, files) ->
                        assertThat(files).hasSize(1));
    }

    @Test
    void shouldSplitPartitionsOf200TablesWith100RecordsAndThresholdOf20() throws InterruptedException {
        // Given we have 200 tables with a split threshold of 20
        // And we ingest a file of 100 records to each table
        sleeper.tables().createManyWithProperties(200, schema, Map.of(
                PARTITION_SPLIT_THRESHOLD, "20",
                COMPACTION_STRATEGY_CLASS, BasicCompactionStrategy.class.getName(),
                COMPACTION_FILES_BATCH_SIZE, "1"));
        sleeper.setGeneratorOverrides(
                overrideField(SystemTestSchema.ROW_KEY_FIELD_NAME,
                        stringFromPrefixAndPadToSize("row-", 2)));
        sleeper.sourceFiles().createWithNumberedRecords(schema, "file.parquet", LongStream.range(0, 100));
        sleeper.ingest().byQueue().sendSourceFilesToAllTables("file.parquet")
                .invokeTask().waitForJobs();

        // When we run 3 partition splits with compactions
        sleeper.partitioning().split();
        sleeper.compaction().createJobs().invokeSplittingTasks(1).waitForJobs()
                .createJobs().invokeStandardTasks(1).waitForJobs();
        sleeper.partitioning().split();
        sleeper.compaction().createJobs().invokeSplittingTasks(1).waitForJobs()
                .createJobs().invokeStandardTasks(1).waitForJobs();
        sleeper.partitioning().split();
        sleeper.compaction().createJobs().invokeSplittingTasks(1).waitForJobs()
                .createJobs().invokeStandardTasks(1).waitForJobs();

        // Then all 200 tables have their records split over 8 leaf partitions
        assertThat(sleeper.directQuery().byQueue().allRecordsByTable())
                .hasSize(200)
                .allSatisfy((table, records) -> assertThat(records)
                        .containsExactlyInAnyOrderElementsOf(
                                sleeper.generateNumberedRecords(schema, LongStream.range(0, 100))));
        var partitionsByTable = sleeper.partitioning().treeByTable();
        assertThat(partitionsByTable)
                .hasSize(200)
                .allSatisfy((table, tree) -> assertThat(tree.getAllPartitions())
                        .usingRecursiveFieldByFieldElementComparatorIgnoringFields("id", "parentPartitionId", "childPartitionIds")
                        .containsExactlyInAnyOrderElementsOf(
                                partitionsBuilder(schema)
                                        .rootFirst("root")
                                        .splitToNewChildren("root", "L", "R", "row-50")
                                        .splitToNewChildren("L", "LL", "LR", "row-25")
                                        .splitToNewChildren("R", "RL", "RR", "row-75")
                                        .splitToNewChildren("LL", "LLL", "LLR", "row-12")
                                        .splitToNewChildren("LR", "LRL", "LRR", "row-37")
                                        .splitToNewChildren("RL", "RLL", "RLR", "row-62")
                                        .splitToNewChildren("RR", "RRL", "RRR", "row-87")
                                        .buildList()));
        assertThat(sleeper.tableFiles().activeByTable())
                .hasSize(200)
                .allSatisfy((table, files) -> {
                    FileInfoSystemTestHelper fileHelper = fileInfoHelper(schema, table, partitionsByTable);
                    assertThat(files)
                            .usingRecursiveFieldByFieldElementComparatorIgnoringFields("filename", "lastStateStoreUpdateTime")
                            .containsExactlyInAnyOrder(
                                    fileHelper.leafFile(12, "row-00", "row-11"),
                                    fileHelper.leafFile(13, "row-12", "row-24"),
                                    fileHelper.leafFile(12, "row-25", "row-36"),
                                    fileHelper.leafFile(13, "row-37", "row-49"),
                                    fileHelper.leafFile(12, "row-50", "row-61"),
                                    fileHelper.leafFile(13, "row-62", "row-74"),
                                    fileHelper.leafFile(12, "row-75", "row-86"),
                                    fileHelper.leafFile(13, "row-87", "row-99"));
                });
    }
}
