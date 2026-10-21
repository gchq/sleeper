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

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import sleeper.core.properties.model.OptionalStack;
import sleeper.core.row.Row;
import sleeper.core.util.PollWithRetries;
import sleeper.systemtest.dsl.SleeperDsl;
import sleeper.systemtest.dsl.extension.AfterTestReports;
import sleeper.systemtest.dsl.instance.SystemTestParameters;
import sleeper.systemtest.dsl.reporting.SystemTestReports;
import sleeper.systemtest.dsl.util.SystemTestSchema;
import sleeper.systemtest.suite.testutil.SystemTest;
import sleeper.systemtest.suite.testutil.parallel.Slow2;

import java.time.Duration;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.BULK_IMPORT_EKS_JOB_QUEUE_URL;
import static sleeper.core.properties.table.TableProperty.BULK_IMPORT_MIN_LEAF_PARTITION_COUNT;
import static sleeper.core.properties.table.TableProperty.PARTITION_SPLIT_MIN_ROWS;
import static sleeper.systemtest.suite.fixtures.SystemTestInstance.BULK_IMPORT_EKS_FARGATE;

@SystemTest
@Slow2
// Slow because it needs to do two CDK deployments, one to add the EKS cluster and one to remove it.
// Each CDK deployment takes around 20 minutes.
// If we left the EKS cluster around, there would be extra costs as the control pane is persistent.
public class EksFargateBulkImportST {

    @BeforeEach
    void setUp(SleeperDsl sleeper, AfterTestReports reporting, SystemTestParameters parameters) {
        sleeper.connectToInstanceAddOnlineTable(BULK_IMPORT_EKS_FARGATE);
        sleeper.enableOptionalStack(OptionalStack.EksBulkImportStack);
        reporting.reportIfTestFailed(SystemTestReports.SystemTestBuilder::ingestJobs);
    }

    @AfterEach
    void tearDown(SleeperDsl sleeper, SystemTestParameters parameters) {
        sleeper.disableOptionalStack(OptionalStack.EksBulkImportStack);
    }

    @Test
    void shouldPreSplitPartitionTreeAndBulkImport(SleeperDsl sleeper, SystemTestParameters parameters) {
        // Given a source file and a table configured for pre-splitting
        sleeper.updateTableProperties(Map.of(
                BULK_IMPORT_MIN_LEAF_PARTITION_COUNT, "8",
                PARTITION_SPLIT_MIN_ROWS, "100"));
        Iterable<Row> rows = sleeper.generateNumberedRows().iterableOverRange(0, 10_000);
        sleeper.sourceFiles().create("test.parquet", rows);

        // When the source file is bulk imported
        sleeper.ingest().bulkImportByQueue()
                .sendSourceFiles(BULK_IMPORT_EKS_JOB_QUEUE_URL, "test.parquet")
                .waitForJobs();

        // Then all rows are imported successfully
        assertThat(SystemTestSchema.sorted(sleeper.directQuery().allRowsInTable()))
                .containsExactlyElementsOf(rows);
        // And the partition tree is pre-split into the expected leaf partitions
        assertThat(sleeper.partitioning().tree().getLeafPartitions())
                .hasSize(8);
        // And the imported data is distributed across those partitions
        assertThat(sleeper.tableFiles().references())
                .hasSize(8);
        // And all bulk import jobs complete successfully
        assertThat(sleeper.eksBulkImportCheck().waitUntilExecutionsFinishedGetStatuses(
                PollWithRetries.intervalAndPollingTimeout(Duration.ofSeconds(10), Duration.ofMinutes(5))))
                .containsOnly("SUCCEEDED");
        // And no bulk import resources remain running
        assertThat(sleeper.eksBulkImportCheck().getDriverPods())
                .isEmpty();
        assertThat(sleeper.eksBulkImportCheck().getJobs())
                .isEmpty();
    }
}
