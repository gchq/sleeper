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
import static sleeper.systemtest.configuration.SystemTestProperty.BULK_IMPORT_QUEUE_PROPERTY;
import static sleeper.systemtest.configuration.SystemTestProperty.INGEST_MODE;
import static sleeper.systemtest.configuration.SystemTestProperty.NUMBER_OF_RECORDS_PER_WRITER;
import static sleeper.systemtest.configuration.SystemTestProperty.NUMBER_OF_WRITERS;
import static sleeper.systemtest.suite.fixtures.SystemTestInstance.BULK_IMPORT_PERFORMANCE;
import static sleeper.systemtest.suite.testutil.FileInfoSystemTestHelper.numberOfRecordsIn;
import static sleeper.systemtest.suite.testutil.PartitionsTestHelper.create512StringPartitions;
import static sleeper.systemtest.suite.testutil.TestContextFactory.testContext;

@Tag("SystemTest")
public class EmrBulkImportPerformanceIT {
    private final SleeperSystemTest sleeper = SleeperSystemTest.getInstance();

    @BeforeEach
    void setUp() {
        sleeper.connectToInstance(BULK_IMPORT_PERFORMANCE);
        sleeper.reporting().startRecording();
    }

    @AfterEach
    void tearDown(TestInfo testInfo) {
        sleeper.reporting().printIngestJobs(testContext(testInfo));
    }

    @Test
    @DisabledIf("systemTestClusterDisabled")
    void shouldMeetBulkImportPerformanceStandardsAcrossManyPartitions() throws InterruptedException {
        sleeper.partitioning().setPartitions(create512StringPartitions(sleeper));
        sleeper.systemTestCluster().updateProperties(properties -> {
                    properties.set(INGEST_MODE, IngestMode.GENERATE_ONLY.toString());
                    properties.set(NUMBER_OF_WRITERS, "100");
                    properties.set(NUMBER_OF_RECORDS_PER_WRITER, "10000000");
                })
                .generateData(PollWithRetries.intervalAndPollingTimeout(Duration.ofSeconds(30), Duration.ofMinutes(20)))
                .sendAllGeneratedFilesAsOneJob(BULK_IMPORT_QUEUE_PROPERTY)
                .sendAllGeneratedFilesAsOneJob(BULK_IMPORT_QUEUE_PROPERTY)
                .sendAllGeneratedFilesAsOneJob(BULK_IMPORT_QUEUE_PROPERTY)
                .sendAllGeneratedFilesAsOneJob(BULK_IMPORT_QUEUE_PROPERTY)
                .sendAllGeneratedFilesAsOneJob(BULK_IMPORT_QUEUE_PROPERTY)
                .waitForJobs(PollWithRetries.intervalAndPollingTimeout(Duration.ofSeconds(30), Duration.ofMinutes(40)));

        assertThat(sleeper.tableFiles().active())
                .hasSize(640)
                .matches(files -> numberOfRecordsIn(files) == 5_000_000_000L,
                        "contain 5 billion records");
        assertThat(sleeper.reporting().ingestJobs().finishedStatistics())
                .matches(stats -> stats.isAllFinishedOneRunEach(5)
                                && stats.isMinAverageRunRecordsPerSecond(3_500_000),
                        "meets minimum performance");
    }

    boolean systemTestClusterDisabled() {
        return sleeper.systemTestCluster().isDisabled();
    }
}
