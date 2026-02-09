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

import sleeper.compaction.core.job.creation.strategy.impl.BasicCompactionStrategy;
import sleeper.core.properties.model.IngestFileWritingStrategy;
import sleeper.core.statestore.FileReference;
import sleeper.core.util.PollWithRetries;
import sleeper.systemtest.dsl.SleeperDsl;
import sleeper.systemtest.dsl.extension.AfterTestReports;
import sleeper.systemtest.dsl.reporting.SystemTestReports;
import sleeper.systemtest.suite.testutil.SystemTest;
import sleeper.systemtest.suite.testutil.parallel.Expensive3;

import java.time.Duration;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.core.properties.table.TableProperty.COMPACTION_FILES_BATCH_SIZE;
import static sleeper.core.properties.table.TableProperty.COMPACTION_STRATEGY_CLASS;
import static sleeper.core.properties.table.TableProperty.INGEST_FILE_WRITING_STRATEGY;
import static sleeper.core.properties.table.TableProperty.TABLE_ONLINE;
import static sleeper.systemtest.configuration.SystemTestIngestMode.DIRECT;
import static sleeper.systemtest.dsl.testutil.SystemTestPartitionsTestHelper.create8192StringPartitions;
import static sleeper.systemtest.dsl.util.SystemTestSchema.DEFAULT_SCHEMA;
import static sleeper.systemtest.suite.fixtures.SystemTestInstance.PARALLEL_COMPACTIONS;

@SystemTest
@Expensive3
public class ParallelCompactionsST {

    @BeforeEach
    void setUp(SleeperDsl sleeper, AfterTestReports reporting) throws Exception {
        sleeper.connectToInstanceNoTables(PARALLEL_COMPACTIONS);
        reporting.reportIfTestFailed(SystemTestReports.SystemTestBuilder::compactionTasksAndJobs);
    }

    @Test
    void shouldApplyOneCompactionPerPartition(SleeperDsl sleeper) {
        // Given we configure to compact many partitions
        sleeper.tables().createWithProperties("test", DEFAULT_SCHEMA, Map.of(
                TABLE_ONLINE, "false",
                COMPACTION_STRATEGY_CLASS, BasicCompactionStrategy.class.getName(),
                COMPACTION_FILES_BATCH_SIZE, "2",
                INGEST_FILE_WRITING_STRATEGY, IngestFileWritingStrategy.ONE_FILE_PER_LEAF.toString()));
        sleeper.partitioning().setPartitions(create8192StringPartitions(sleeper));
        // And we have rows spread across all partitions in many files per partition
        sleeper.systemTestCluster()
                .runDataGenerationJobs(10,
                        builder -> builder.ingestMode(DIRECT).rowsPerIngest(1_000_000),
                        PollWithRetries.intervalAndPollingTimeout(
                                Duration.ofSeconds(10), Duration.ofMinutes(10)))
                .waitForTotalFileReferences(81920);

        // When we run compaction
        sleeper.compaction()
                .putTableOnlineWaitForJobCreation(40960,
                        PollWithRetries.intervalAndPollingTimeout(
                                Duration.ofSeconds(10), Duration.ofMinutes(5)))
                .waitForTasks(200,
                        PollWithRetries.intervalAndPollingTimeout(
                                Duration.ofSeconds(10), Duration.ofMinutes(10)))
                .waitForJobsToFinishThenCommit(
                        PollWithRetries.intervalAndPollingTimeout(
                                Duration.ofSeconds(10), Duration.ofMinutes(5)),
                        PollWithRetries.intervalAndPollingTimeout(
                                Duration.ofSeconds(10), Duration.ofMinutes(3)))
                // The table is still online, so it will continue to compact until each partition has 1 file.
                // Because the table is online, we can't control the order in which the files are compacted, so we
                // don't know how many jobs will run. We do know it will eventually get to one file per partition.
                .waitForTotalFileReferences(8192,
                        PollWithRetries.intervalAndPollingTimeout(
                                Duration.ofSeconds(10), Duration.ofMinutes(5)))
                .waitForAllJobsToCommit(
                        PollWithRetries.intervalAndPollingTimeout(
                                Duration.ofSeconds(10), Duration.ofMinutes(5)));

        // Then we have one file per partition
        assertThat(sleeper.tableFiles().references())
                .hasSize(8192)
                .satisfies(files -> assertThat(files.stream().mapToLong(FileReference::getNumberOfRows).sum())
                        .isEqualTo(10_000_000))
                .allMatch(file -> file.onlyContainsDataForThisPartition() && !file.isCountApproximate(),
                        "only contains data for one partition")
                .allMatch(file -> file.getJobId() == null,
                        "not assigned to any job")
                .allSatisfy(file -> assertThat(file.getNumberOfRows())
                        .describedAs("contains an even distribution of rows for the partition")
                        .isBetween(800L, 1600L));
        // And all jobs have finished and only ran once
        assertThat(sleeper.reporting().compactionJobs().finishedStatistics())
                .matches(statistics -> statistics.isAllFinishedOneRunEachWithMinimum(40960),
                        "all jobs finished and ran once");
        assertThat(sleeper.reporting().finishedCompactionTasks())
                .allSatisfy(task -> assertThat(task.getJobRuns())
                        .describedAs("ran the expected distribution of jobs")
                        .isBetween(0, 600));
    }

}
