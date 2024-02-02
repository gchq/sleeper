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

import sleeper.core.util.PollWithRetries;
import sleeper.systemtest.suite.dsl.SleeperSystemTest;
import sleeper.systemtest.suite.dsl.reports.SystemTestReports;
import sleeper.systemtest.suite.testutil.AfterTestReports;
import sleeper.systemtest.suite.testutil.Expensive;
import sleeper.systemtest.suite.testutil.SystemTest;

import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.systemtest.configuration.IngestMode.QUEUE;
import static sleeper.systemtest.configuration.SystemTestProperty.INGEST_MODE;
import static sleeper.systemtest.configuration.SystemTestProperty.NUMBER_OF_RECORDS_PER_WRITER;
import static sleeper.systemtest.configuration.SystemTestProperty.NUMBER_OF_WRITERS;
import static sleeper.systemtest.suite.fixtures.SystemTestInstance.INGEST_PERFORMANCE;
import static sleeper.systemtest.suite.testutil.FileReferenceSystemTestHelper.numberOfRecordsIn;
import static sleeper.systemtest.suite.testutil.PartitionsTestHelper.create128StringPartitions;

@SystemTest
@Expensive // Expensive because it takes a long time to ingest this many records on fairly large ECS instances.
public class IngestPerformanceIT {

    @BeforeEach
    void setUp(SleeperSystemTest sleeper, AfterTestReports reporting) {
        sleeper.connectToInstance(INGEST_PERFORMANCE);
        reporting.reportAlways(SystemTestReports.SystemTestBuilder::ingestTasksAndJobs);
    }

    @Test
    void shouldMeetIngestPerformanceStandardsAcrossManyPartitions(SleeperSystemTest sleeper) throws InterruptedException {
        sleeper.partitioning().setPartitions(create128StringPartitions(sleeper));
        sleeper.systemTestCluster().updateProperties(properties -> {
                    properties.setEnum(INGEST_MODE, QUEUE);
                    properties.setNumber(NUMBER_OF_WRITERS, 11);
                    properties.setNumber(NUMBER_OF_RECORDS_PER_WRITER, 40_000_000);
                })
                .generateData(PollWithRetries.intervalAndPollingTimeout(Duration.ofSeconds(30), Duration.ofMinutes(20)))
                .invokeStandardIngestTasks(11,
                        PollWithRetries.intervalAndPollingTimeout(Duration.ofSeconds(30), Duration.ofMinutes(10)))
                .waitForIngestJobs(PollWithRetries.intervalAndPollingTimeout(Duration.ofSeconds(30), Duration.ofMinutes(40)));

        assertThat(sleeper.tableFiles().references())
                .hasSize(1408)
                .matches(files -> numberOfRecordsIn(files) == 440_000_000,
                        "contain 440 million records");
        assertThat(sleeper.reporting().ingestJobs().finishedStatistics())
                .matches(stats -> stats.isAllFinishedOneRunEach(11)
                                && stats.isMinAverageRunRecordsPerSecond(130_000),
                        "meets minimum performance");
    }
}
