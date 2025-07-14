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

import sleeper.core.row.testutils.SortedRowsCheck;
import sleeper.core.statestore.AllReferencesToAllFiles;
import sleeper.core.util.PollWithRetries;
import sleeper.systemtest.dsl.SleeperSystemTest;
import sleeper.systemtest.dsl.extension.AfterTestReports;
import sleeper.systemtest.dsl.reporting.SystemTestReports;
import sleeper.systemtest.suite.testutil.Expensive;
import sleeper.systemtest.suite.testutil.SystemTest;

import java.time.Duration;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.core.properties.table.TableProperty.TABLE_ONLINE;
import static sleeper.core.statestore.AllReferencesToAFileTestHelper.sumFileReferenceRecordCounts;
import static sleeper.systemtest.configuration.SystemTestIngestMode.DIRECT;
import static sleeper.systemtest.dsl.util.SystemTestSchema.DEFAULT_SCHEMA;
import static sleeper.systemtest.suite.fixtures.SystemTestInstance.COMPACTION_PERFORMANCE;

@SystemTest
@Expensive // Expensive because it takes a long time to compact this many records on fairly large ECS instances.
public class CompactionPerformanceST {

    @BeforeEach
    void setUp(SleeperSystemTest sleeper, AfterTestReports reporting) {
        sleeper.connectToInstanceNoTables(COMPACTION_PERFORMANCE);
        reporting.reportAlways(SystemTestReports.SystemTestBuilder::compactionTasksAndJobs);
    }

    @AfterEach
    void tearDown(SleeperSystemTest sleeper) {
        sleeper.compaction().scaleToZero();
    }

    @Test
    void shouldMeetCompactionPerformanceStandards(SleeperSystemTest sleeper) {
        // Given
        sleeper.tables().createWithProperties("test", DEFAULT_SCHEMA, Map.of(TABLE_ONLINE, "false"));
        sleeper.systemTestCluster().runDataGenerationJobs(110,
                builder -> builder.ingestMode(DIRECT).recordsPerIngest(40_000_000),
                PollWithRetries.intervalAndPollingTimeout(Duration.ofSeconds(30), Duration.ofMinutes(20)))
                .waitForTotalFileReferences(110);

        // When
        sleeper.compaction().putTableOnlineUntilJobsAreCreated(10).waitForTasks(10)
                .waitForJobs(PollWithRetries.intervalAndPollingTimeout(Duration.ofSeconds(30), Duration.ofHours(1)));

        // Then
        AllReferencesToAllFiles files = sleeper.tableFiles().all();
        assertThat(sumFileReferenceRecordCounts(files)).isEqualTo(4_400_000_000L);
        assertThat(files.streamFileReferences()).hasSize(10);
        assertThat(files.getFilesWithReferences()).hasSize(10)
                .first() // Only check one file because it's time consuming to read all records
                .satisfies(file -> assertThat(SortedRowsCheck.check(DEFAULT_SCHEMA, sleeper.getRecords(file)))
                        .isEqualTo(SortedRowsCheck.sorted(sumFileReferenceRecordCounts(file))));
        assertThat(sleeper.reporting().compactionJobs().finishedStatistics())
                .matches(stats -> stats.isAllFinishedOneRunEach(10)
                        && stats.isAverageRunRecordsPerSecondInRange(180_000, 400_000),
                        "meets expected performance");
    }
}
