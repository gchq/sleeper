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

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import sleeper.core.properties.model.OptionalStack;
import sleeper.core.row.Row;
import sleeper.systemtest.dsl.SleeperDsl;
import sleeper.systemtest.dsl.extension.AfterTestReports;
import sleeper.systemtest.dsl.instance.SystemTestParameters;
import sleeper.systemtest.dsl.reporting.SystemTestReports;
import sleeper.systemtest.suite.testutil.SystemTest;
import sleeper.systemtest.suite.testutil.parallel.Slow2;

import java.util.Map;
import java.util.stream.LongStream;

import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.BULK_IMPORT_EKS_JOB_QUEUE_URL;
import static sleeper.core.properties.instance.CommonProperty.LOG_RETENTION_IN_DAYS;
import static sleeper.core.properties.table.TableProperty.BULK_IMPORT_MIN_LEAF_PARTITION_COUNT;
import static sleeper.core.properties.table.TableProperty.PARTITION_SPLIT_MIN_ROWS;
import static sleeper.systemtest.suite.fixtures.SystemTestInstance.BULK_IMPORT_EKS;

@SystemTest
@Slow2
// Slow because it needs to do two CDK deployments, one to add the EKS cluster and one to remove it.
// Each CDK deployment takes around 20 minutes.
// If we left the EKS cluster around, there would be extra costs as the control pane is persistent.
public class EksBulkImportST {

    @BeforeEach
    void setUp(SleeperDsl sleeper, AfterTestReports reporting, SystemTestParameters parameters) {
        if (parameters.isInstancePropertyOverridden(LOG_RETENTION_IN_DAYS)) {
            return;
        }
        sleeper.connectToInstanceAddOnlineTable(BULK_IMPORT_EKS);
        sleeper.enableOptionalStack(OptionalStack.EksBulkImportStack);
        reporting.reportIfTestFailed(SystemTestReports.SystemTestBuilder::ingestJobs);
    }

    @AfterEach
    void tearDown(SleeperDsl sleeper, SystemTestParameters parameters) {
        if (parameters.isInstancePropertyOverridden(LOG_RETENTION_IN_DAYS)) {
            return;
        }
        sleeper.disableOptionalStack(OptionalStack.EksBulkImportStack);
    }

    @Test
    void shouldPreSplitPartitionTreeAndBulkImport(SleeperDsl sleeper, SystemTestParameters parameters) {
        // This is intended to ignore this test when running in an environment where log retention must not be set.
        // This is because we're currently unable to prevent an EKS cluster deployment from creating log groups with log
        // retention set. See the following issue:
        // https://github.com/gchq/sleeper/issues/3451 (Logs retention policy is not applied to all EKS cluster resources)
        if (parameters.isInstancePropertyOverridden(LOG_RETENTION_IN_DAYS)) {
            return;
        }
        // Given
        sleeper.updateTableProperties(Map.of(
                BULK_IMPORT_MIN_LEAF_PARTITION_COUNT, "8",
                PARTITION_SPLIT_MIN_ROWS, "100"));
        Iterable<Row> rows = sleeper.generateNumberedRows(LongStream.range(0, 10_000));
        sleeper.sourceFiles().create("file.parquet", rows);

        // When
        sleeper.ingest().bulkImportByQueue()
                .sendSourceFiles(BULK_IMPORT_EKS_JOB_QUEUE_URL, "test.parquet")
                .waitForJobs();

        // Then
        assertThat(sleeper.directQuery().allRowsInTable())
                .containsExactlyElementsOf(rows);
        assertThat(sleeper.partitioning().tree().getLeafPartitions())
                .hasSize(8);
        assertThat(sleeper.tableFiles().references())
                .hasSize(8);
    }
}
