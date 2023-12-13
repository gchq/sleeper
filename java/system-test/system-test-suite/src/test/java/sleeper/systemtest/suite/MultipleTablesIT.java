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

import sleeper.core.schema.Schema;
import sleeper.systemtest.suite.dsl.SleeperSystemTest;
import sleeper.systemtest.suite.fixtures.SystemTestSchema;
import sleeper.systemtest.suite.testutil.FileInfoSystemTestHelper;
import sleeper.systemtest.suite.testutil.PurgeQueueExtension;

import java.util.Map;
import java.util.stream.LongStream;

import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.configuration.properties.instance.CdkDefinedInstanceProperty.COMPACTION_JOB_QUEUE_URL;
import static sleeper.configuration.properties.instance.CdkDefinedInstanceProperty.INGEST_JOB_QUEUE_URL;
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
    @RegisterExtension
    public final PurgeQueueExtension purgeQueue = PurgeQueueExtension
            .purgeIfTestFailed(sleeper, INGEST_JOB_QUEUE_URL, COMPACTION_JOB_QUEUE_URL);

    @BeforeEach
    void setUp() {
        sleeper.connectToInstanceNoTables(MAIN);
    }

    @Test
    void shouldCreate20Tables() {
        sleeper.tables().createMany(20, schema);

        assertThat(sleeper.tables().loadIdentities())
                .hasSize(20);
    }

    @Test
    void shouldIngestOneFileTo20Tables() throws Exception {
        // Given we have 20 tables
        // And we have one source file to be ingested
        sleeper.tables().createMany(20, schema);
        sleeper.sourceFiles().createWithNumberedRecords(schema, "file.parquet", LongStream.range(0, 100));

        // When we send an ingest job with the source file to all 20 tables
        sleeper.ingest().byQueue().sendSourceFilesToAllTables("file.parquet")
                .invokeTask().waitForJobs();

        // Then all 20 tables should contain the source file records
        // And all 20 tables should have one active file
        assertThat(sleeper.query().byQueue().allRecordsByTable())
                .hasSize(20)
                .allSatisfy(((table, records) ->
                        assertThat(records).containsExactlyElementsOf(
                                sleeper.generateNumberedRecords(schema, LongStream.range(0, 100)))));
        assertThat(sleeper.tableFiles().activeByTable())
                .hasSize(20)
                .allSatisfy((table, files) ->
                        assertThat(files).hasSize(1));
    }

    @Test
    void shouldSplitPartitionsOf20TablesWith100RecordsAndThresholdOf20() throws InterruptedException {
        // Given we have 20 tables with a split threshold of 20
        // And we ingest a file of 100 records to each table
        sleeper.tables().createManyWithProperties(20, schema,
                Map.of(PARTITION_SPLIT_THRESHOLD, "20"));
        sleeper.setGeneratorOverrides(
                overrideField(SystemTestSchema.ROW_KEY_FIELD_NAME,
                        stringFromPrefixAndPadToSize("row-", 2)));
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

        // Then all 20 tables have their records split over 8 leaf partitions
        assertThat(sleeper.directQuery().byQueue().allRecordsByTable())
                .hasSize(20)
                .allSatisfy((table, records) -> assertThat(records)
                        .containsExactlyInAnyOrderElementsOf(
                                sleeper.generateNumberedRecords(schema, LongStream.range(0, 100))));
        var partitionsByTable = sleeper.partitioning().treeByTable();
        assertThat(partitionsByTable)
                .hasSize(20)
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
                .hasSize(20)
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
