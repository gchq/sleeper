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

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import sleeper.core.util.PollWithRetries;
import sleeper.systemtest.dsl.SleeperSystemTest;
import sleeper.systemtest.dsl.extension.AfterTestReports;
import sleeper.systemtest.dsl.reporting.SystemTestReports;
import sleeper.systemtest.suite.testutil.SystemTest;
import sleeper.systemtest.suite.testutil.parallel.Expensive3;

import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.core.properties.model.IngestQueue.STANDARD_INGEST;
import static sleeper.systemtest.configuration.SystemTestIngestMode.QUEUE;
import static sleeper.systemtest.dsl.testutil.SystemTestPartitionsTestHelper.create128StringPartitions;
import static sleeper.systemtest.suite.fixtures.SystemTestInstance.INGEST_PERFORMANCE;
import static sleeper.systemtest.suite.testutil.FileReferenceSystemTestHelper.numberOfRowsIn;

@SystemTest
// Expensive because it takes a long time to ingest this many rows on fairly large ECS instances.
@Expensive3
public class IngestPerformanceST {

    @BeforeEach
    void setUp(SleeperSystemTest sleeper, AfterTestReports reporting) {
        sleeper.connectToInstanceAddOnlineTable(INGEST_PERFORMANCE);
        reporting.reportAlways(SystemTestReports.SystemTestBuilder::ingestTasksAndJobs);
    }

    @Test
    void shouldMeetIngestPerformanceStandardsAcrossManyPartitions(SleeperSystemTest sleeper) {
        sleeper.partitioning().setPartitions(create128StringPartitions(sleeper));
        sleeper.systemTestCluster()
                .runDataGenerationJobs(11,
                        builder -> builder.ingestMode(QUEUE)
                                .ingestQueue(STANDARD_INGEST)
                                .rowsPerIngest(40_000_000),
                        PollWithRetries.intervalAndPollingTimeout(Duration.ofSeconds(30), Duration.ofMinutes(20)))
                .waitForStandardIngestTasks(11,
                        PollWithRetries.intervalAndPollingTimeout(Duration.ofSeconds(30), Duration.ofMinutes(10)))
                .waitForIngestJobs(PollWithRetries.intervalAndPollingTimeout(Duration.ofSeconds(30), Duration.ofMinutes(40)));

        assertThat(sleeper.tableFiles().references())
                .hasSize(1408)
                .matches(files -> numberOfRowsIn(files) == 440_000_000,
                        "contain 440 million rows");
        assertThat(sleeper.reporting().ingestJobs().finishedStatistics())
                .matches(stats -> stats.isAllFinishedOneRunEach(11)
                        && stats.isAverageRunRowsPerSecondInRange(150_000, 250_000),
                        "meets expected performance");
    }
}
