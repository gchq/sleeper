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

import sleeper.core.row.Row;
import sleeper.core.util.PollWithRetries;
import sleeper.systemtest.dsl.SleeperDsl;
import sleeper.systemtest.dsl.util.SystemTestSchema;
import sleeper.systemtest.suite.testutil.SystemTest;

import java.time.Duration;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.BULK_IMPORT_EMR_JOB_QUEUE_URL;
import static sleeper.core.properties.table.TableProperty.BULK_IMPORT_MIN_LEAF_PARTITION_COUNT;
import static sleeper.core.properties.table.TableProperty.PARTITION_SPLIT_MIN_ROWS;
import static sleeper.systemtest.suite.fixtures.SystemTestInstance.MAIN;

@SystemTest
public class EmrBulkImportST {

    @BeforeEach
    void setUp(SleeperDsl sleeper) {
        sleeper.connectToInstanceAddOnlineTable(MAIN);
    }

    @Test
    void shouldPreSplitPartitionTreeAndBulkImport(SleeperDsl sleeper) {
        // Given
        sleeper.updateTableProperties(Map.of(
                BULK_IMPORT_MIN_LEAF_PARTITION_COUNT, "8",
                PARTITION_SPLIT_MIN_ROWS, "100"));
        Iterable<Row> rows = sleeper.generateNumberedRows().iterableOverRange(0, 10_000);
        sleeper.sourceFiles().create("file.parquet", rows);

        // When
        sleeper.ingest().bulkImportByQueue().sendSourceFiles(BULK_IMPORT_EMR_JOB_QUEUE_URL, "file.parquet")
                .waitForJobs(PollWithRetries.intervalAndPollingTimeout(Duration.ofSeconds(30), Duration.ofMinutes(30)));

        // Then
        assertThat(SystemTestSchema.sorted(sleeper.directQuery().allRowsInTable()))
                .containsExactlyElementsOf(rows);
        assertThat(sleeper.partitioning().tree().getLeafPartitions())
                .hasSize(8);
        assertThat(sleeper.tableFiles().references())
                .hasSize(8);
    }
}
