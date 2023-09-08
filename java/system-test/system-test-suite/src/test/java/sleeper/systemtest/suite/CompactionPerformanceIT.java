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
import org.junit.jupiter.api.condition.DisabledIf;
import org.junit.jupiter.api.extension.RegisterExtension;

import sleeper.core.util.PollWithRetries;
import sleeper.systemtest.configuration.IngestMode;
import sleeper.systemtest.suite.dsl.SleeperSystemTest;
import sleeper.systemtest.suite.testutil.ReportingExtension;

import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.systemtest.configuration.SystemTestProperty.INGEST_MODE;
import static sleeper.systemtest.configuration.SystemTestProperty.NUMBER_OF_RECORDS_PER_WRITER;
import static sleeper.systemtest.configuration.SystemTestProperty.NUMBER_OF_WRITERS;
import static sleeper.systemtest.suite.fixtures.SystemTestInstance.COMPACTION_PERFORMANCE;
import static sleeper.systemtest.suite.testutil.FileInfoSystemTestHelper.numberOfRecordsIn;

@Tag("SystemTest")
public class CompactionPerformanceIT {
    private final SleeperSystemTest sleeper = SleeperSystemTest.getInstance();

    @RegisterExtension
    public final ReportingExtension reporting = ReportingExtension.reportAlways(
            sleeper.reportsForExtension().compactionTasksAndJobs());

    @BeforeEach
    void setUp() {
        sleeper.connectToInstance(COMPACTION_PERFORMANCE);
    }

    @Test
    @DisabledIf("systemTestClusterDisabled")
    void shouldMeetCompactionPerformanceStandards() throws InterruptedException {
        sleeper.systemTestCluster().updateProperties(properties -> {
            properties.set(INGEST_MODE, IngestMode.DIRECT.toString());
            properties.set(NUMBER_OF_WRITERS, "110");
            properties.set(NUMBER_OF_RECORDS_PER_WRITER, "40000000");
        }).generateData(PollWithRetries.intervalAndPollingTimeout(Duration.ofSeconds(30), Duration.ofMinutes(20)));

        sleeper.compaction().createJobs().invokeStandardTasks(10)
                .waitForJobs(PollWithRetries.intervalAndPollingTimeout(Duration.ofSeconds(30), Duration.ofMinutes(40)));

        assertThat(sleeper.tableFiles().active())
                .hasSize(10)
                .matches(files -> numberOfRecordsIn(files) == 4_400_000_000L,
                        "contain 4.4 billion records");
        assertThat(sleeper.reporting().compactionJobs().finishedStatistics())
                .matches(stats -> stats.isAllFinishedOneRunEach(10)
                                && stats.isMinAverageRunRecordsPerSecond(300000),
                        "meets minimum performance");
    }

    boolean systemTestClusterDisabled() {
        return sleeper.systemTestCluster().isDisabled();
    }
}
