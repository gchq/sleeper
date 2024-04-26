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

import sleeper.compaction.strategy.impl.BasicCompactionStrategy;
import sleeper.configuration.properties.validation.IngestFileWritingStrategy;
import sleeper.core.statestore.FileReference;
import sleeper.core.util.PollWithRetries;
import sleeper.systemtest.dsl.SleeperSystemTest;
import sleeper.systemtest.dsl.extension.AfterTestReports;
import sleeper.systemtest.dsl.reporting.SystemTestReports;
import sleeper.systemtest.suite.testutil.Expensive;
import sleeper.systemtest.suite.testutil.SystemTest;

import java.time.Duration;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.configuration.properties.table.TableProperty.COMPACTION_FILES_BATCH_SIZE;
import static sleeper.configuration.properties.table.TableProperty.COMPACTION_JOB_SEND_BATCH_SIZE;
import static sleeper.configuration.properties.table.TableProperty.COMPACTION_STRATEGY_CLASS;
import static sleeper.configuration.properties.table.TableProperty.INGEST_FILE_WRITING_STRATEGY;
import static sleeper.systemtest.configuration.SystemTestIngestMode.DIRECT;
import static sleeper.systemtest.configuration.SystemTestProperty.INGEST_MODE;
import static sleeper.systemtest.configuration.SystemTestProperty.NUMBER_OF_RECORDS_PER_INGEST;
import static sleeper.systemtest.configuration.SystemTestProperty.NUMBER_OF_WRITERS;
import static sleeper.systemtest.suite.fixtures.SystemTestInstance.PARALLEL_COMPACTIONS;
import static sleeper.systemtest.suite.testutil.PartitionsTestHelper.create2048StringPartitions;

@SystemTest
@Expensive
public class ParallelCompactionsIT {

    @BeforeEach
    void setUp(SleeperSystemTest sleeper, AfterTestReports reporting) throws Exception {
        sleeper.connectToInstance(PARALLEL_COMPACTIONS);
        reporting.reportIfTestFailed(SystemTestReports.SystemTestBuilder::compactionTasksAndJobs);
    }

    @Test
    void shouldApplyOneCompactionPerPartition(SleeperSystemTest sleeper) {
        // Given we configure to compact many partitions
        sleeper.partitioning().setPartitions(create2048StringPartitions(sleeper));
        sleeper.updateTableProperties(Map.of(
                COMPACTION_STRATEGY_CLASS, BasicCompactionStrategy.class.getName(),
                COMPACTION_FILES_BATCH_SIZE, "10",
                COMPACTION_JOB_SEND_BATCH_SIZE, "1000",
                INGEST_FILE_WRITING_STRATEGY, IngestFileWritingStrategy.ONE_FILE_PER_LEAF.toString()));
        // And we have records spread across all partitions in many files per partition
        sleeper.systemTestCluster()
                .updateProperties(properties -> {
                    properties.setEnum(INGEST_MODE, DIRECT);
                    properties.setNumber(NUMBER_OF_WRITERS, 10);
                    properties.setNumber(NUMBER_OF_RECORDS_PER_INGEST, 1_000_000);
                }).generateData(
                        PollWithRetries.intervalAndPollingTimeout(
                                Duration.ofSeconds(30), Duration.ofMinutes(20)));

        // When we run compaction
        sleeper.compaction()
                .forceStartTasks(300)
                .createJobs(2048,
                        PollWithRetries.intervalAndPollingTimeout(
                                Duration.ofSeconds(10), Duration.ofMinutes(5)))
                .waitForJobs(
                        PollWithRetries.intervalAndPollingTimeout(
                                Duration.ofSeconds(30), Duration.ofMinutes(40)));

        // Then we have one file per partition
        assertThat(sleeper.tableFiles().references())
                .hasSize(2048)
                .satisfies(files -> assertThat(files.stream().mapToLong(FileReference::getNumberOfRecords).sum())
                        .isEqualTo(10_000_000))
                .allMatch(file -> file.onlyContainsDataForThisPartition() && !file.isCountApproximate(),
                        "only contains data for one partition")
                .allMatch(file -> file.getJobId() != null,
                        "not assigned to any job")
                .allMatch(file -> file.getNumberOfRecords() > 400 && file.getNumberOfRecords() < 550,
                        "contains an even distribution of records for the partition");
    }

}
