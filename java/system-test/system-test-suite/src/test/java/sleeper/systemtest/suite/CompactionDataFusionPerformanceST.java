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

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import sleeper.core.properties.validation.CompactionMethod;
import sleeper.core.util.PollWithRetries;
import sleeper.systemtest.dsl.SleeperSystemTest;
import sleeper.systemtest.dsl.extension.AfterTestReports;
import sleeper.systemtest.dsl.reporting.SystemTestReports;
import sleeper.systemtest.suite.testutil.Expensive;
import sleeper.systemtest.suite.testutil.SystemTest;

import java.time.Duration;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.core.properties.table.TableProperty.COMPACTION_METHOD;
import static sleeper.systemtest.configuration.SystemTestIngestMode.DIRECT;
import static sleeper.systemtest.configuration.SystemTestProperty.INGEST_MODE;
import static sleeper.systemtest.configuration.SystemTestProperty.NUMBER_OF_RECORDS_PER_INGEST;
import static sleeper.systemtest.configuration.SystemTestProperty.NUMBER_OF_WRITERS;
import static sleeper.systemtest.suite.fixtures.SystemTestInstance.COMPACTION_ON_DATAFUSION;
import static sleeper.systemtest.suite.testutil.FileReferenceSystemTestHelper.numberOfRecordsIn;

@SystemTest
@Expensive // Expensive because it takes a long time to compact this many records on fairly large ECS instances.
public class CompactionDataFusionPerformanceST {

    @BeforeEach
    void setUp(SleeperSystemTest sleeper, AfterTestReports reporting) {
        sleeper.connectToInstance(COMPACTION_ON_DATAFUSION);
        reporting.reportAlways(SystemTestReports.SystemTestBuilder::compactionTasksAndJobs);
    }

    @AfterEach
    void tearDown(SleeperSystemTest sleeper) {
        sleeper.compaction().scaleToZero();
    }

    @Test
    void shouldMeetCompactionPerformanceStandardsWithDataFusion(SleeperSystemTest sleeper) {
        // Given
        sleeper.updateTableProperties(Map.of(COMPACTION_METHOD, CompactionMethod.DATAFUSION.toString()));
        sleeper.systemTestCluster().updateProperties(properties -> {
            properties.setEnum(INGEST_MODE, DIRECT);
            properties.setNumber(NUMBER_OF_WRITERS, 110);
            properties.setNumber(NUMBER_OF_RECORDS_PER_INGEST, 40_000_000);
        }).runDataGenerationTasks(PollWithRetries.intervalAndPollingTimeout(Duration.ofSeconds(30), Duration.ofMinutes(20)))
                .waitForTotalFileReferences(110);

        // When
        sleeper.compaction().createJobs(10).invokeTasks(10)
                .waitForJobs(PollWithRetries.intervalAndPollingTimeout(Duration.ofSeconds(30), Duration.ofHours(1)));

        // Then
        assertThat(sleeper.tableFiles().references())
                .hasSize(10)
                .matches(files -> numberOfRecordsIn(files) == 4_400_000_000L,
                        "contain 4.4 billion records");
        assertThat(sleeper.reporting().compactionJobs().finishedStatistics())
                .matches(stats -> stats.isAllFinishedOneRunEach(10)
                        && stats.isAverageRunRecordsPerSecondInRange(1_200_000, 2_200_000),
                        "meets expected performance");
    }
}
