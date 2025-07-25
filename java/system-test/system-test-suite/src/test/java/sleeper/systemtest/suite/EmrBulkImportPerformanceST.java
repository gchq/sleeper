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
import sleeper.systemtest.suite.testutil.Expensive;
import sleeper.systemtest.suite.testutil.SystemTest;

import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.BULK_IMPORT_EMR_JOB_QUEUE_URL;
import static sleeper.systemtest.configuration.SystemTestIngestMode.GENERATE_ONLY;
import static sleeper.systemtest.dsl.testutil.SystemTestPartitionsTestHelper.create512StringPartitions;
import static sleeper.systemtest.suite.fixtures.SystemTestInstance.BULK_IMPORT_PERFORMANCE;
import static sleeper.systemtest.suite.testutil.FileReferenceSystemTestHelper.numberOfRowsIn;

@SystemTest
@Expensive // Expensive because it takes a lot of very costly EMR instances to import this many rows.
public class EmrBulkImportPerformanceST {

    @BeforeEach
    void setUp(SleeperSystemTest sleeper, AfterTestReports reporting) {
        sleeper.connectToInstanceAddOnlineTable(BULK_IMPORT_PERFORMANCE);
        reporting.reportAlways(SystemTestReports.SystemTestBuilder::ingestJobs);
    }

    @Test
    void shouldMeetBulkImportPerformanceStandardsAcrossManyPartitions(SleeperSystemTest sleeper) {
        sleeper.partitioning().setPartitions(create512StringPartitions(sleeper));
        sleeper.systemTestCluster()
                .runDataGenerationJobs(100,
                        builder -> builder.ingestMode(GENERATE_ONLY).rowsPerIngest(10_000_000),
                        PollWithRetries.intervalAndPollingTimeout(Duration.ofSeconds(30), Duration.ofMinutes(5)))
                .sendAllGeneratedFilesAsOneJob(BULK_IMPORT_EMR_JOB_QUEUE_URL)
                .sendAllGeneratedFilesAsOneJob(BULK_IMPORT_EMR_JOB_QUEUE_URL)
                .sendAllGeneratedFilesAsOneJob(BULK_IMPORT_EMR_JOB_QUEUE_URL)
                .sendAllGeneratedFilesAsOneJob(BULK_IMPORT_EMR_JOB_QUEUE_URL)
                .sendAllGeneratedFilesAsOneJob(BULK_IMPORT_EMR_JOB_QUEUE_URL)
                .waitForBulkImportJobs(PollWithRetries.intervalAndPollingTimeout(Duration.ofSeconds(30), Duration.ofMinutes(30)));

        assertThat(sleeper.tableFiles().references())
                .hasSize(2560)
                .matches(files -> numberOfRowsIn(files) == 5_000_000_000L,
                        "contain 5 billion rows");
        assertThat(sleeper.reporting().ingestJobs().finishedStatistics())
                .matches(stats -> stats.isAllFinishedOneRunEach(5)
                        && stats.isAverageRunRowsPerSecondInRange(3_500_000, 5_000_000),
                        "meets expected performance");
    }
}
