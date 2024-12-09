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

import sleeper.compaction.core.job.creation.strategy.impl.BasicCompactionStrategy;
import sleeper.core.properties.validation.IngestFileWritingStrategy;
import sleeper.core.statestore.FileReference;
import sleeper.core.util.PollWithRetries;
import sleeper.systemtest.dsl.SleeperSystemTest;
import sleeper.systemtest.dsl.extension.AfterTestPurgeQueues;
import sleeper.systemtest.dsl.extension.AfterTestReports;
import sleeper.systemtest.dsl.reporting.SystemTestReports;
import sleeper.systemtest.suite.testutil.Expensive;
import sleeper.systemtest.suite.testutil.SystemTest;

import java.time.Duration;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.COMPACTION_JOB_CREATION_QUEUE_URL;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.COMPACTION_JOB_QUEUE_URL;
import static sleeper.core.properties.table.TableProperty.COMPACTION_FILES_BATCH_SIZE;
import static sleeper.core.properties.table.TableProperty.COMPACTION_STRATEGY_CLASS;
import static sleeper.core.properties.table.TableProperty.INGEST_FILE_WRITING_STRATEGY;
import static sleeper.systemtest.configuration.SystemTestIngestMode.DIRECT;
import static sleeper.systemtest.configuration.SystemTestProperty.INGEST_MODE;
import static sleeper.systemtest.configuration.SystemTestProperty.NUMBER_OF_RECORDS_PER_INGEST;
import static sleeper.systemtest.configuration.SystemTestProperty.NUMBER_OF_WRITERS;
import static sleeper.systemtest.dsl.testutil.SystemTestPartitionsTestHelper.create8192StringPartitions;
import static sleeper.systemtest.suite.fixtures.SystemTestInstance.PARALLEL_COMPACTIONS;

@SystemTest
@Expensive
public class ParallelCompactionsST {

    @BeforeEach
    void setUp(SleeperSystemTest sleeper, AfterTestReports reporting, AfterTestPurgeQueues purgeQueues) throws Exception {
        sleeper.connectToInstance(PARALLEL_COMPACTIONS);
        reporting.reportIfTestFailed(SystemTestReports.SystemTestBuilder::compactionTasksAndJobs);
        purgeQueues.purgeIfTestFailed(COMPACTION_JOB_CREATION_QUEUE_URL, COMPACTION_JOB_QUEUE_URL);
    }

    @Test
    void shouldApplyOneCompactionPerPartition(SleeperSystemTest sleeper) {
        // Given we configure to compact many partitions
        sleeper.partitioning().setPartitions(create8192StringPartitions(sleeper));
        sleeper.updateTableProperties(Map.of(
                COMPACTION_STRATEGY_CLASS, BasicCompactionStrategy.class.getName(),
                COMPACTION_FILES_BATCH_SIZE, "10",
                INGEST_FILE_WRITING_STRATEGY, IngestFileWritingStrategy.ONE_FILE_PER_LEAF.toString()));
        // And we have records spread across all partitions in many files per partition
        sleeper.systemTestCluster()
                .updateProperties(properties -> {
                    properties.setEnum(INGEST_MODE, DIRECT);
                    properties.setNumber(NUMBER_OF_WRITERS, 10);
                    properties.setNumber(NUMBER_OF_RECORDS_PER_INGEST, 1_000_000);
                }).runDataGenerationTasks(
                        PollWithRetries.intervalAndPollingTimeout(
                                Duration.ofSeconds(10), Duration.ofMinutes(10)))
                .waitForTotalFileReferences(81920);

        // When we run compaction
        sleeper.compaction()
                .createJobs(8192,
                        PollWithRetries.intervalAndPollingTimeout(
                                Duration.ofSeconds(10), Duration.ofMinutes(10)))
                .invokeTasks(300)
                .waitForJobsToFinishThenCommit(
                        PollWithRetries.intervalAndPollingTimeout(
                                Duration.ofSeconds(10), Duration.ofMinutes(5)),
                        PollWithRetries.intervalAndPollingTimeout(
                                Duration.ofSeconds(10), Duration.ofMinutes(5)));

        // Then we have one file per partition
        assertThat(sleeper.tableFiles().references())
                .hasSize(8192)
                .satisfies(files -> assertThat(files.stream().mapToLong(FileReference::getNumberOfRecords).sum())
                        .isEqualTo(10_000_000))
                .allMatch(file -> file.onlyContainsDataForThisPartition() && !file.isCountApproximate(),
                        "only contains data for one partition")
                .allMatch(file -> file.getJobId() == null,
                        "not assigned to any job")
                .allSatisfy(file -> assertThat(file.getNumberOfRecords())
                        .describedAs("contains an even distribution of records for the partition")
                        .isBetween(800L, 1600L));
        // And all jobs have finished and only ran once
        assertThat(sleeper.reporting().compactionJobs().finishedStatistics())
                .matches(statistics -> statistics.isAllFinishedOneRunEach(8192),
                        "all jobs finished and ran once");
        assertThat(sleeper.reporting().finishedCompactionTasks())
                .allSatisfy(task -> assertThat(task.getJobRuns())
                        .describedAs("ran the expected distribution of jobs")
                        .isBetween(0, 100));
    }

}
