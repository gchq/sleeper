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

import java.time.Duration;
import java.util.Map;
import java.util.stream.LongStream;

import static org.assertj.core.api.Assertions.assertThat;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import sleeper.compaction.strategy.impl.BasicCompactionStrategy;
import static sleeper.configuration.properties.instance.CdkDefinedInstanceProperty.COMPACTION_JOB_CREATION_QUEUE_URL;
import static sleeper.configuration.properties.instance.CdkDefinedInstanceProperty.COMPACTION_JOB_QUEUE_URL;
import static sleeper.configuration.properties.instance.CdkDefinedInstanceProperty.INGEST_JOB_QUEUE_URL;
import static sleeper.configuration.properties.table.TableProperty.COMPACTION_FILES_BATCH_SIZE;
import static sleeper.configuration.properties.table.TableProperty.COMPACTION_STRATEGY_CLASS;
import static sleeper.configuration.properties.table.TableProperty.INGEST_FILE_WRITING_STRATEGY;
import sleeper.configuration.properties.validation.IngestFileWritingStrategy;
import sleeper.core.statestore.FileReference;
import sleeper.core.util.PollWithRetries;
import static sleeper.systemtest.configuration.SystemTestIngestMode.DIRECT;
import static sleeper.systemtest.configuration.SystemTestProperty.INGEST_MODE;
import static sleeper.systemtest.configuration.SystemTestProperty.NUMBER_OF_RECORDS_PER_INGEST;
import static sleeper.systemtest.configuration.SystemTestProperty.NUMBER_OF_WRITERS;
import sleeper.systemtest.dsl.SleeperSystemTest;
import sleeper.systemtest.dsl.compaction.SystemTestCompaction;
import sleeper.systemtest.dsl.extension.AfterTestPurgeQueues;
import sleeper.systemtest.dsl.extension.AfterTestReports;
import sleeper.systemtest.dsl.reporting.SystemTestReports;
import static sleeper.systemtest.suite.fixtures.SystemTestInstance.CONTENTION_PERFORMANCE;
import sleeper.systemtest.suite.testutil.Expensive;
import static sleeper.systemtest.suite.testutil.PartitionsTestHelper.create512StringPartitions;
import sleeper.systemtest.suite.testutil.SystemTest;

@SystemTest
@Expensive
public class StateStoreContentionIT {

    @BeforeEach
    void setUp(SleeperSystemTest sleeper, AfterTestReports reporting, AfterTestPurgeQueues purgeQueues) throws Exception {
        sleeper.connectToInstance(CONTENTION_PERFORMANCE);
        reporting.reportIfTestFailed(SystemTestReports.SystemTestBuilder::compactionTasksAndJobs);
        // reporting.reportIfTestFailed(SystemTestReports.SystemTestBuilder::ingestTasksAndJobs);
        purgeQueues.purgeIfTestFailed(COMPACTION_JOB_CREATION_QUEUE_URL, COMPACTION_JOB_QUEUE_URL,
                INGEST_JOB_QUEUE_URL);
    }

    @Test
    void shouldApplyOneCompactionPerPartition(SleeperSystemTest sleeper) {
        // Given we configure to compact many partitions
        // sleeper.setGeneratorOverrides();
        sleeper.partitioning().setPartitions(create512StringPartitions(sleeper));

        sleeper.updateTableProperties(Map.of(
                COMPACTION_STRATEGY_CLASS, BasicCompactionStrategy.class.getName(),
                COMPACTION_FILES_BATCH_SIZE, "10",
                INGEST_FILE_WRITING_STRATEGY, IngestFileWritingStrategy.ONE_FILE_PER_LEAF.toString()));

        // And we have records spread across all partitions in many files per partition
        sleeper.systemTestCluster()
                .updateProperties(properties -> {
                    properties.setEnum(INGEST_MODE, DIRECT);
                    properties.setNumber(NUMBER_OF_WRITERS, 10);
                    properties.setNumber(NUMBER_OF_RECORDS_PER_INGEST, 10_000_000);
                })
                .generateData(
                        PollWithRetries.intervalAndPollingTimeout(
                                Duration.ofSeconds(10), Duration.ofMinutes(5)));

        // When we run compaction
        SystemTestCompaction compaction = sleeper.compaction()
                .createJobs(
                        512,
                        PollWithRetries.intervalAndPollingTimeout(
                                Duration.ofSeconds(10), Duration.ofMinutes(10)))
                .invokeTasks(100);

        sleeper.systemTestCluster()
                .updateProperties(properties -> {
                    properties.setEnum(INGEST_MODE, DIRECT);
                    properties.setNumber(NUMBER_OF_WRITERS, 10);
                    properties.setNumber(NUMBER_OF_RECORDS_PER_INGEST, 10_000_000);
                })
                .generateData(
                        PollWithRetries.intervalAndPollingTimeout(
                                Duration.ofSeconds(10), Duration.ofMinutes(5)));

        compaction.waitForJobsToFinishThenCommit(
                PollWithRetries.intervalAndPollingTimeout(
                        Duration.ofSeconds(10), Duration.ofMinutes(5)),
                PollWithRetries.intervalAndPollingTimeout(
                        Duration.ofSeconds(10), Duration.ofMinutes(5)));
        // Ingest setup
        /*
         * sleeper.systemTestCluster()
         * .updateProperties(properties -> {
         * properties.setEnum(INGEST_MODE, QUEUE);
         * });
         * sleeper.sourceFiles()
         * .createWithNumberedRecords("file1.parquet", LongStream.range(0, 1000))
         * .createWithNumberedRecords("file2.parquet", LongStream.range(1000, 2000))
         * .createWithNumberedRecords("file3.parquet", LongStream.range(2000, 3000))
         * .createWithNumberedRecords("file4.parquet", LongStream.range(3000, 4000))
         * .createWithNumberedRecords("file5.parquet", LongStream.range(4000, 5000));
         * 
         * compaction.waitForJobsToFinishThenCommit(
         * PollWithRetries.intervalAndPollingTimeout(
         * Duration.ofSeconds(10), Duration.ofMinutes(20)),
         * PollWithRetries.intervalAndPollingTimeout(
         * Duration.ofSeconds(10), Duration.ofMinutes(20)));
         * 
         * SystemTestIngestByQueue ingestByQueue = sleeper.ingest().byQueue()
         * .sendSourceFiles("file1.parquet", "file2.parquet", "file3.parquet",
         * "file4.parquet", "file5.parquet")
         * .invokeTask();
         * 
         * ingestByQueue.waitForJobs();
         */

        /*
         * sleeper.systemTestCluster()
         * .updateProperties(properties -> {
         * properties.setEnum(INGEST_MODE, QUEUE);
         * properties.setEnum(INGEST_QUEUE, STANDARD_INGEST);
         * properties.setNumber(NUMBER_OF_WRITERS, 10);
         * properties.setNumber(NUMBER_OF_RECORDS_PER_INGEST, 1000);
         * })
         * .generateData(
         * PollWithRetries.intervalAndPollingTimeout(
         * Duration.ofSeconds(30), Duration.ofMinutes(30)))
         * .invokeStandardIngestTasks(11,
         * PollWithRetries.intervalAndPollingTimeout(Duration.ofSeconds(30),
         * Duration.ofMinutes(10)))
         * .waitForIngestJobs(
         * PollWithRetries.intervalAndPollingTimeout(Duration.ofSeconds(30),
         * Duration.ofMinutes(30)));
         */
        // Then we have one file per partition
        assertThat(sleeper.tableFiles().references())
                .hasSize(512)
                .satisfies(files -> assertThat(files.stream().mapToLong(FileReference::getNumberOfRecords).sum())
                        .isEqualTo(10_000_000))
                .allMatch(file -> file.onlyContainsDataForThisPartition() && !file.isCountApproximate(),
                        "only contains data for one partition")
                .allMatch(file -> file.getJobId() == null,
                        "not assigned to any job")
                .allSatisfy(file -> assertThat(file.getNumberOfRecords())
                        .describedAs("contains an even distribution of records for the partition")
                        .isBetween(800L, 2000L));
        // And all jobs have finished and only ran once
        assertThat(sleeper.reporting().compactionJobs().finishedStatistics())
                .matches(statistics -> statistics.isAllFinishedOneRunEach(512),
                        "all jobs finished and ran once");
        assertThat(sleeper.reporting().finishedCompactionTasks())
                .allSatisfy(task -> assertThat(task.getJobRuns())
                        .describedAs("ran the expected distribution of jobs")
                        .isBetween(1, 60));
        // Ingest Then
        assertThat(sleeper.directQuery().allRecordsInTable())
                .containsExactlyElementsOf(sleeper.generateNumberedRecords(LongStream.range(0, 5000)));
        assertThat(sleeper.tableFiles().references()).hasSize(1);
    }

}
