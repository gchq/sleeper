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

import sleeper.systemtest.dsl.SleeperDsl;
import sleeper.systemtest.dsl.extension.AfterTestReports;
import sleeper.systemtest.suite.testutil.SystemTest;
import sleeper.systemtest.suite.testutil.parallel.Slow2;

import static sleeper.systemtest.suite.fixtures.SystemTestInstance.BATCHING;

@SystemTest
// Slow because we need to deploy a separate instance to configure batching sizes over the default of 1.
@Slow2
public class LambdaBatchFailuresST {

    @BeforeEach
    void setUp(SleeperDsl sleeper, AfterTestReports reporting) {
        // Given an instance configured to process multiple tables in a single lambda invocation
        sleeper.connectToInstanceNoTables(BATCHING);
    }

    @Test
    void shouldReportFailureCreatingCompactionJobsForSomeTables() {
        // Given 5 Sleeper tables with files to be compacted

        // When we trigger compaction creation for the tables, interleaved with non-existent tables

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
