/*
 * Copyright 2022-2026 Crown Copyright
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
import sleeper.systemtest.dsl.SleeperDsl;
import sleeper.systemtest.dsl.extension.AfterTestReports;
import sleeper.systemtest.suite.testutil.SystemTest;
import sleeper.systemtest.suite.testutil.parallel.Slow2;

import java.util.List;
import java.util.Map;
import java.util.stream.LongStream;

import static sleeper.core.properties.table.TableProperty.COMPACTION_FILES_BATCH_SIZE;
import static sleeper.core.properties.table.TableProperty.COMPACTION_STRATEGY_CLASS;
import static sleeper.core.properties.table.TableProperty.TABLE_ID;
import static sleeper.core.properties.table.TableProperty.TABLE_ONLINE;
import static sleeper.systemtest.dsl.util.SystemTestSchema.DEFAULT_SCHEMA;
import static sleeper.systemtest.suite.fixtures.SystemTestInstance.BATCHING;

@SystemTest
// Slow because we need to deploy a separate instance to configure batching sizes over the default of 1.
@Slow2
public class LambdaBatchFailuresST {

    @BeforeEach
    void setUp(SleeperDsl sleeper, AfterTestReports reporting) {
        // Given an instance configured to process 10 messages in a single lambda invocation
        sleeper.connectToInstanceNoTables(BATCHING);
    }

    @Test
    void shouldReportFailureCreatingCompactionJobsForSomeTables(SleeperDsl sleeper) {
        // Given 5 Sleeper tables with files to be compacted
        sleeper.tables().createManyWithProperties(5, DEFAULT_SCHEMA, Map.of(
                TABLE_ONLINE, "false",
                COMPACTION_STRATEGY_CLASS, BasicCompactionStrategy.class.getName(),
                COMPACTION_FILES_BATCH_SIZE, "2"));
        sleeper.tables().forEach(() -> {
            String tableId = sleeper.tableProperties().get(TABLE_ID);
            sleeper.sourceFiles().inDataBucket().writeSketches()
                    .createWithNumberedRows(tableId + "-1.parquet", LongStream.range(0, 10))
                    .createWithNumberedRows(tableId + "-2.parquet", LongStream.range(0, 10));
            sleeper.ingest().toStateStore()
                    .addFileOnEveryPartition(tableId + "-1.parquet", 10)
                    .addFileOnEveryPartition(tableId + "-2.parquet", 10);
        });

        // When we trigger compaction creation for the tables, interleaved with non-existent tables
        sleeper.compaction().createJobsForAllInterleavedWithFakeTables(5,
                List.of("missing-1", "missing-2", "missing-3", "missing-4", "missing-5"));

        // Then the compactions run
        // And the messages for the non-existent tables appear on the dead letter queue
    }

    @Test
    void shouldReportFailureCommittingSomeCompactionJobs() {
        // Given a Sleeper table with 5 compaction jobs ready to be committed to the state store

        // When we submit commits for those jobs, interleaved with invalid jobs that do not parse

        // Then the compactions are committed
        // And the messages for the invalid jobs appear on the dead letter queue
    }

    @Test
    void shouldReportFailureRunningGCForSomeTables() {
        // Given 5 Sleeper tables with files ready for GC

        // When we trigger GC for the tables, interleaved with non-existent tables

        // Then the files are garbage collected
        // And the messages for the non-existent tables appear on the dead letter queue
    }

    @Test
    void shouldReportFailureCreatingSnapshotsForSomeTables() {
        // Given 5 Sleeper tables with state store transactions

        // When we trigger snapshot creation for the tables, interleaved with non-existent tables

        // Then the snapshots are created
        // And the messages for the non-existent tables appear on the dead letter queue
    }

    @Test
    void shouldReportFailureDeletingSnapshotsForSomeTables() {
        // Given 5 Sleeper tables with state store snapshots ready to be deleted

        // When we trigger snapshot deletion for the tables, interleaved with non-existent tables

        // Then the snapshots are deleted
        // And the messages for the non-existent tables appear on the dead letter queue
    }

    @Test
    void shouldReportFailureDeletingTransactionsForSomeTables() {
        // Given 5 Sleeper tables with state store transactions ready to be deleted

        // When we trigger transaction deletion for the tables, interleaved with non-existent tables

        // Then the transactions are deleted
        // And the messages for the non-existent tables appear on the dead letter queue
    }

    @Test
    void shouldReportFailureWritingMetricsForSomeTables() {
        // Given 5 Sleeper tables with file references

        // When we trigger table metrics for the tables, interleaved with non-existent tables

        // Then the metrics are published
        // And the messages for the non-existent tables appear on the dead letter queue
    }

}
