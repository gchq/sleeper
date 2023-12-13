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
import org.junit.jupiter.api.io.TempDir;

import sleeper.core.partition.PartitionTree;
import sleeper.core.schema.Schema;
import sleeper.core.statestore.FileInfo;
import sleeper.core.statestore.FileInfoFactory;
import sleeper.systemtest.suite.dsl.SleeperSystemTest;
import sleeper.systemtest.suite.fixtures.SystemTestSchema;
import sleeper.systemtest.suite.testutil.PurgeQueueExtension;
import sleeper.systemtest.suite.testutil.ReportingExtension;

import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.stream.LongStream;

import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.configuration.properties.instance.CdkDefinedInstanceProperty.COMPACTION_JOB_QUEUE_URL;
import static sleeper.configuration.properties.table.TableProperty.PARTITION_SPLIT_THRESHOLD;
import static sleeper.systemtest.datageneration.GenerateNumberedValue.stringFromPrefixAndPadToSize;
import static sleeper.systemtest.datageneration.GenerateNumberedValueOverrides.overrideField;
import static sleeper.systemtest.suite.fixtures.SystemTestInstance.MAIN;
import static sleeper.systemtest.suite.testutil.PartitionsTestHelper.partitionsBuilder;
import static sleeper.systemtest.suite.testutil.TableFileInfoPrinter.printFiles;
import static sleeper.systemtest.suite.testutil.TablePartitionsPrinter.printPartitions;

@Tag("SystemTest")
public class PartitionSplittingIT {
    @TempDir
    private Path tempDir;
    private final SleeperSystemTest sleeper = SleeperSystemTest.getInstance();

    @RegisterExtension
    public final ReportingExtension reporting = ReportingExtension.reportIfTestFailed(
            sleeper.reportsForExtension().partitionStatus());
    @RegisterExtension
    public final PurgeQueueExtension purgeQueue = PurgeQueueExtension
            .purgeIfTestFailed(sleeper, COMPACTION_JOB_QUEUE_URL);

    @BeforeEach
    void setUp() {
        sleeper.connectToInstance(MAIN);
    }

    @Test
    void shouldSplitPartitionsWith100RecordsAndThresholdOf20() throws InterruptedException {
        // Given
        sleeper.setGeneratorOverrides(
                overrideField(SystemTestSchema.ROW_KEY_FIELD_NAME,
                        stringFromPrefixAndPadToSize("row-", 2)));
        sleeper.updateTableProperties(Map.of(PARTITION_SPLIT_THRESHOLD, "20"));
        sleeper.ingest().direct(tempDir).numberedRecords(LongStream.range(0, 100));

        // When
        sleeper.partitioning().split();
        sleeper.compaction().splitAndCompactFiles();
        sleeper.partitioning().split();
        sleeper.compaction().splitAndCompactFiles();
        sleeper.partitioning().split();
        sleeper.compaction().splitAndCompactFiles();

        // Then
        assertThat(sleeper.directQuery().allRecordsInTable())
                .containsExactlyInAnyOrderElementsOf(sleeper.generateNumberedRecords(LongStream.range(0, 100)));
        Schema schema = sleeper.tableProperties().getSchema();
        PartitionTree partitions = sleeper.partitioning().tree();
        List<FileInfo> activeFiles = sleeper.tableFiles().active();
        PartitionTree expectedPartitions = partitionsBuilder(schema).rootFirst("root")
                .splitToNewChildren("root", "L", "R", "row-50")
                .splitToNewChildren("L", "LL", "LR", "row-25")
                .splitToNewChildren("R", "RL", "RR", "row-75")
                .splitToNewChildren("LL", "LLL", "LLR", "row-12")
                .splitToNewChildren("LR", "LRL", "LRR", "row-37")
                .splitToNewChildren("RL", "RLL", "RLR", "row-62")
                .splitToNewChildren("RR", "RRL", "RRR", "row-87")
                .buildTree();
        assertThat(printPartitions(schema, partitions))
                .isEqualTo(printPartitions(schema, expectedPartitions));
        FileInfoFactory fileInfoFactory = FileInfoFactory.from(expectedPartitions);
        assertThat(printFiles(partitions, activeFiles))
                .isEqualTo(printFiles(expectedPartitions, List.of(
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
