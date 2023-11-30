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

import sleeper.compaction.strategy.impl.BasicCompactionStrategy;
import sleeper.systemtest.suite.dsl.SleeperSystemTest;
import sleeper.systemtest.suite.fixtures.SystemTestSchema;
import sleeper.systemtest.suite.testutil.FileInfoSystemTestHelper;
import sleeper.systemtest.suite.testutil.ReportingExtension;

import java.nio.file.Path;
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
public class PartitionSplittingIT {
    @TempDir
    private Path tempDir;
    private final SleeperSystemTest sleeper = SleeperSystemTest.getInstance();

    @RegisterExtension
    public final ReportingExtension reporting = ReportingExtension.reportIfTestFailed(
            sleeper.reportsForExtension().partitionStatus());

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
        sleeper.updateTableProperties(Map.of(
                PARTITION_SPLIT_THRESHOLD, "20",
                COMPACTION_STRATEGY_CLASS, BasicCompactionStrategy.class.getName(),
                COMPACTION_FILES_BATCH_SIZE, "1"));
        sleeper.ingest().direct(tempDir).numberedRecords(LongStream.range(0, 100));

        // When
        sleeper.partitioning().split();
        sleeper.compaction().createJobs().invokeSplittingTasks(1).waitForJobs()
                .createJobs().invokeStandardTasks(1).waitForJobs();
        sleeper.partitioning().split();
        sleeper.compaction().createJobs().invokeSplittingTasks(1).waitForJobs()
                .createJobs().invokeStandardTasks(1).waitForJobs();
        sleeper.partitioning().split();
        sleeper.compaction().createJobs().invokeSplittingTasks(1).waitForJobs()
                .createJobs().invokeStandardTasks(1).waitForJobs();

        // Then
        FileInfoSystemTestHelper fileInfoHelper = fileInfoHelper(sleeper);
        assertThat(sleeper.directQuery().allRecordsInTable())
                .containsExactlyInAnyOrderElementsOf(sleeper.generateNumberedRecords(LongStream.range(0, 100)));
        assertThat(sleeper.tableFiles().active())
                .usingRecursiveFieldByFieldElementComparatorIgnoringFields("filename", "lastStateStoreUpdateTime")
                .containsExactlyInAnyOrder(
                        fileInfoHelper.leafFile(12, "row-00", "row-11"),
                        fileInfoHelper.leafFile(13, "row-12", "row-24"),
                        fileInfoHelper.leafFile(12, "row-25", "row-36"),
                        fileInfoHelper.leafFile(13, "row-37", "row-49"),
                        fileInfoHelper.leafFile(12, "row-50", "row-61"),
                        fileInfoHelper.leafFile(13, "row-62", "row-74"),
                        fileInfoHelper.leafFile(12, "row-75", "row-86"),
                        fileInfoHelper.leafFile(13, "row-87", "row-99"));
        assertThat(sleeper.partitioning().allPartitions())
                .usingRecursiveFieldByFieldElementComparatorIgnoringFields("id", "parentPartitionId", "childPartitionIds")
                .containsExactlyInAnyOrderElementsOf(
                        partitionsBuilder(sleeper)
                                .rootFirst("root")
                                .splitToNewChildren("root", "L", "R", "row-50")
                                .splitToNewChildren("L", "LL", "LR", "row-25")
                                .splitToNewChildren("R", "RL", "RR", "row-75")
                                .splitToNewChildren("LL", "LLL", "LLR", "row-12")
                                .splitToNewChildren("LR", "LRL", "LRR", "row-37")
                                .splitToNewChildren("RL", "RLL", "RLR", "row-62")
                                .splitToNewChildren("RR", "RRL", "RRR", "row-87")
                                .buildList());
    }
}
