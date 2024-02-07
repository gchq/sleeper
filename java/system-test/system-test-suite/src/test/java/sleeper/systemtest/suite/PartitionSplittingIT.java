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
import sleeper.systemtest.dsl.reporting.SystemTestReports;
import sleeper.systemtest.suite.dsl.SleeperSystemTest;
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
import static sleeper.configuration.properties.instance.CdkDefinedInstanceProperty.PARTITION_SPLITTING_QUEUE_URL;
import static sleeper.configuration.properties.table.TableProperty.PARTITION_SPLIT_THRESHOLD;
import static sleeper.core.testutils.printers.FileReferencePrinter.printFiles;
import static sleeper.core.testutils.printers.PartitionsPrinter.printPartitions;
import static sleeper.systemtest.datageneration.GenerateNumberedValue.addPrefix;
import static sleeper.systemtest.datageneration.GenerateNumberedValue.numberStringAndZeroPadTo;
import static sleeper.systemtest.datageneration.GenerateNumberedValueOverrides.overrideField;
import static sleeper.systemtest.suite.fixtures.SystemTestInstance.MAIN;
import static sleeper.systemtest.suite.testutil.PartitionsTestHelper.partitionsBuilder;

@SystemTest
public class PartitionSplittingIT {
    @TempDir
    private Path tempDir;

    @BeforeEach
    void setUp(SleeperSystemTest sleeper, AfterTestReports reporting, AfterTestPurgeQueues purgeQueues) {
        sleeper.connectToInstance(MAIN);
        reporting.reportIfTestFailed(SystemTestReports.SystemTestBuilder::partitionStatus);
        purgeQueues.purgeIfTestFailed(PARTITION_SPLITTING_QUEUE_URL, COMPACTION_JOB_QUEUE_URL);
    }

    @Test
    void shouldSplitPartitionsWith100RecordsAndThresholdOf20(SleeperSystemTest sleeper) throws InterruptedException {
        // Given
        sleeper.setGeneratorOverrides(
                overrideField(SystemTestSchema.ROW_KEY_FIELD_NAME,
                        numberStringAndZeroPadTo(2).then(addPrefix("row-"))));
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
        List<FileReference> activeFiles = sleeper.tableFiles().references();
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
        FileReferenceFactory fileReferenceFactory = FileReferenceFactory.from(expectedPartitions);
        assertThat(printFiles(partitions, activeFiles))
                .isEqualTo(printFiles(expectedPartitions, List.of(
                        fileReferenceFactory.partitionFile("LLL", 12),
                        fileReferenceFactory.partitionFile("LLR", 13),
                        fileReferenceFactory.partitionFile("LRL", 12),
                        fileReferenceFactory.partitionFile("LRR", 13),
                        fileReferenceFactory.partitionFile("RLL", 12),
                        fileReferenceFactory.partitionFile("RLR", 13),
                        fileReferenceFactory.partitionFile("RRL", 12),
                        fileReferenceFactory.partitionFile("RRR", 13)
                )));
    }
}
