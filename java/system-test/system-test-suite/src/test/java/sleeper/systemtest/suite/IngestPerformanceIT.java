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

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.condition.DisabledIf;

import sleeper.core.util.PollWithRetries;
import sleeper.systemtest.configuration.IngestMode;
import sleeper.systemtest.suite.dsl.SleeperSystemTest;

import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.systemtest.configuration.SystemTestProperty.INGEST_MODE;
import static sleeper.systemtest.configuration.SystemTestProperty.MAX_ENTRIES_RANDOM_LIST;
import static sleeper.systemtest.configuration.SystemTestProperty.MAX_ENTRIES_RANDOM_MAP;
import static sleeper.systemtest.configuration.SystemTestProperty.MAX_RANDOM_INT;
import static sleeper.systemtest.configuration.SystemTestProperty.MAX_RANDOM_LONG;
import static sleeper.systemtest.configuration.SystemTestProperty.MIN_RANDOM_INT;
import static sleeper.systemtest.configuration.SystemTestProperty.MIN_RANDOM_LONG;
import static sleeper.systemtest.configuration.SystemTestProperty.NUMBER_OF_RECORDS_PER_WRITER;
import static sleeper.systemtest.configuration.SystemTestProperty.NUMBER_OF_WRITERS;
import static sleeper.systemtest.configuration.SystemTestProperty.RANDOM_BYTE_ARRAY_LENGTH;
import static sleeper.systemtest.configuration.SystemTestProperty.RANDOM_STRING_LENGTH;
import static sleeper.systemtest.suite.fixtures.SystemTestInstance.INGEST_PERFORMANCE;
import static sleeper.systemtest.suite.testutil.PartitionsTestHelper.create128Partitions;
import static sleeper.systemtest.suite.testutil.TestContextFactory.testContext;

@Tag("SystemTest")
public class IngestPerformanceIT {
    private final SleeperSystemTest sleeper = SleeperSystemTest.getInstance();

    @BeforeEach
    void setUp() {
        sleeper.connectToInstance(INGEST_PERFORMANCE);
        sleeper.reporting().startRecording();
    }

    @AfterEach
    void tearDown(TestInfo testInfo) {
        sleeper.reporting().printIngestTasksAndJobs(testContext(testInfo));
    }

    @Test
    @DisabledIf("systemTestClusterDisabled")
    void shouldMeetIngestPerformanceStandardsAcrossManyPartitions() throws InterruptedException {
        sleeper.stateStore().setPartitions(create128Partitions(sleeper));
        sleeper.systemTestCluster().updateProperties(properties -> {
                    properties.set(INGEST_MODE, IngestMode.QUEUE.toString());
                    properties.set(NUMBER_OF_WRITERS, "11");
                    properties.set(NUMBER_OF_RECORDS_PER_WRITER, "40000000");
                    properties.set(MIN_RANDOM_INT, "0");
                    properties.set(MAX_RANDOM_INT, "100000000");
                    properties.set(MIN_RANDOM_LONG, "0");
                    properties.set(MAX_RANDOM_LONG, "10000000000");
                    properties.set(RANDOM_STRING_LENGTH, "10");
                    properties.set(RANDOM_BYTE_ARRAY_LENGTH, "10");
                    properties.set(MAX_ENTRIES_RANDOM_MAP, "10");
                    properties.set(MAX_ENTRIES_RANDOM_LIST, "10");
                })
                .generateData(PollWithRetries.intervalAndPollingTimeout(Duration.ofSeconds(30), Duration.ofMinutes(20)))
                .invokeStandardIngestTasks(11,
                        PollWithRetries.intervalAndPollingTimeout(Duration.ofSeconds(30), Duration.ofMinutes(10)))
                .waitForJobs(PollWithRetries.intervalAndPollingTimeout(Duration.ofSeconds(30), Duration.ofMinutes(40)));

        assertThat(sleeper.stateStore().activeFiles()).hasSize(1408);
        assertThat(sleeper.reporting().ingestJobs().finishedStatistics())
                .matches(stats -> stats.isAllFinishedOneRunEach(11)
                                && stats.isMinAverageRunRecordsPerSecond(135000),
                        "meets minimum performance");
    }

    boolean systemTestClusterDisabled() {
        return sleeper.systemTestCluster().isDisabled();
    }
}
