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

import sleeper.core.statestore.FileInfoFactory;
import sleeper.systemtest.suite.dsl.SleeperSystemTest;
import sleeper.systemtest.suite.testutil.ReportingExtension;

import java.nio.file.Path;
import java.util.stream.LongStream;

import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.configuration.properties.table.TableProperty.PARTITION_SPLIT_THRESHOLD;
import static sleeper.systemtest.suite.fixtures.SystemTestInstance.MAIN;
import static sleeper.systemtest.suite.testutil.FileInfoSystemTestHelper.fileInfoFactory;

@Tag("SystemTest")
public class PartitionSplittingIT {
    @TempDir
    private Path tempDir;
    private final SleeperSystemTest sleeper = SleeperSystemTest.getInstance();

    @RegisterExtension
    public final ReportingExtension reporting = ReportingExtension.reportIfFailed(
            sleeper.reportsForExtension().partitionStatus());

    @BeforeEach
    void setUp() {
        sleeper.connectToInstance(MAIN);
    }

    @Test
    void shouldSplitPartitionsWith100RecordsAndThresholdOf20() throws InterruptedException {
        // Given
        sleeper.updateTableProperties(properties -> properties.set(PARTITION_SPLIT_THRESHOLD, "20"));
        sleeper.ingest().direct(tempDir).numberedRecords(LongStream.range(0, 100));

        // When
        sleeper.partitioning().split();
        sleeper.compaction().createJobs().invokeSplittingTasks(1).waitForJobs();
        sleeper.partitioning().split();
        sleeper.compaction().createJobs().invokeSplittingTasks(1).waitForJobs();
        sleeper.partitioning().split();
        sleeper.compaction().createJobs().invokeSplittingTasks(1).waitForJobs();

        // Then
        FileInfoFactory fileFactory = fileInfoFactory(sleeper);
        assertThat(sleeper.directQuery().allRecordsInTable())
                .containsExactlyInAnyOrderElementsOf(sleeper.generateNumberedRecords(LongStream.range(0, 100)));
        assertThat(sleeper.tableFiles().active())
                .usingRecursiveFieldByFieldElementComparatorIgnoringFields("filename", "lastStateStoreUpdateTime")
                .containsExactlyInAnyOrder(
                        fileFactory.leafFile(12, "row-0000000000000000000", "row-0000000000000000011"),
                        fileFactory.leafFile(13, "row-0000000000000000012", "row-0000000000000000024"),
                        fileFactory.leafFile(12, "row-0000000000000000025", "row-0000000000000000036"),
                        fileFactory.leafFile(13, "row-0000000000000000037", "row-0000000000000000049"),
                        fileFactory.leafFile(12, "row-0000000000000000050", "row-0000000000000000061"),
                        fileFactory.leafFile(13, "row-0000000000000000062", "row-0000000000000000074"),
                        fileFactory.leafFile(12, "row-0000000000000000075", "row-0000000000000000086"),
                        fileFactory.leafFile(13, "row-0000000000000000087", "row-0000000000000000099"));
    }
}
