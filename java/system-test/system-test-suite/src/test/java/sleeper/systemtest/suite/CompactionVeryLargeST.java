/*
 * Copyright 2022-2025 Crown Copyright
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

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import sleeper.core.properties.model.DataEngine;
import sleeper.core.row.testutils.SortedRowsCheck;
import sleeper.core.statestore.AllReferencesToAllFiles;
import sleeper.core.statestore.FileReference;
import sleeper.core.util.PollWithRetries;
import sleeper.systemtest.dsl.SleeperSystemTest;
import sleeper.systemtest.dsl.extension.AfterTestReports;
import sleeper.systemtest.dsl.reporting.SystemTestReports;
import sleeper.systemtest.suite.testutil.Expensive;
import sleeper.systemtest.suite.testutil.SystemTest;

import java.time.Duration;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.core.properties.table.TableProperty.COMPACTION_FILES_BATCH_SIZE;
import static sleeper.core.properties.table.TableProperty.DATA_ENGINE;
import static sleeper.core.properties.table.TableProperty.TABLE_ONLINE;
import static sleeper.systemtest.configuration.SystemTestIngestMode.DIRECT;
import static sleeper.systemtest.dsl.util.SystemTestSchema.DEFAULT_SCHEMA;
import static sleeper.systemtest.suite.fixtures.SystemTestInstance.COMPACTION_PERFORMANCE_DATAFUSION;

@SystemTest
@Expensive // Expensive because it takes a long time to compact this many rows on fairly large ECS instances.
public class CompactionVeryLargeST {

    @BeforeEach
    void setUp(SleeperSystemTest sleeper, AfterTestReports reporting) {
        sleeper.connectToInstanceNoTables(COMPACTION_PERFORMANCE_DATAFUSION);
        reporting.reportAlways(SystemTestReports.SystemTestBuilder::compactionTasksAndJobs);
    }

    @AfterEach
    void tearDown(SleeperSystemTest sleeper) {
        sleeper.compaction().scaleToZero();
    }

    @Test
    void shouldMeetCompactionPerformanceStandards(SleeperSystemTest sleeper) {
        // Given
        sleeper.tables().createWithProperties("test", DEFAULT_SCHEMA, Map.of(
                TABLE_ONLINE, "false",
                DATA_ENGINE, DataEngine.DATAFUSION.toString(),
                COMPACTION_FILES_BATCH_SIZE, "40"));
        sleeper.systemTestCluster().runDataGenerationJobs(10,
                builder -> builder.ingestMode(DIRECT).rowsPerIngest(200_000_000),
                PollWithRetries.intervalAndPollingTimeout(Duration.ofSeconds(30), Duration.ofHours(1)))
                // 200 million rows don't fit in the local file system store of a data generation task,
                // so we end up with 4 files from each task.
                .waitForTotalFileReferences(40);

        // When
        sleeper.compaction().putTableOnlineUntilJobsAreCreated(1).waitForTasks(1)
                .waitForJobs(PollWithRetries.intervalAndPollingTimeout(Duration.ofSeconds(30), Duration.ofHours(5)));

        // Then
        AllReferencesToAllFiles files = sleeper.tableFiles().all();
        assertThat(files.getFilesWithReferences())
                .singleElement().satisfies(file -> {
                    assertThat(file.getReferences())
                            .singleElement()
                            .extracting(FileReference::getNumberOfRows)
                            .isEqualTo(2_000_000_000L);
                    assertThat(SortedRowsCheck.check(DEFAULT_SCHEMA, sleeper.getRows(file)))
                            .isEqualTo(SortedRowsCheck.sorted(2_000_000_000L));
                });
        assertThat(sleeper.reporting().compactionJobs().finishedStatistics())
                .matches(stats -> stats.isAverageRunRowsPerSecondInRange(3_000_000, 4_000_000),
                        "meets expected performance");
    }
}
