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

import sleeper.core.row.Row;
import sleeper.core.util.PollWithRetries;
import sleeper.systemtest.dsl.SleeperDsl;
import sleeper.systemtest.dsl.util.SystemTestSchema;
import sleeper.systemtest.suite.testutil.SystemTest;

import java.time.Duration;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.BULK_IMPORT_EMR_SERVERLESS_JOB_QUEUE_URL;
import static sleeper.core.properties.table.TableProperty.BULK_IMPORT_MIN_LEAF_PARTITION_COUNT;
import static sleeper.core.properties.table.TableProperty.PARTITION_SPLIT_MIN_ROWS;
import static sleeper.systemtest.suite.fixtures.SystemTestInstance.MAIN;

@SystemTest
public class EmrServerlessBulkImportST {

    @BeforeEach
    void setUp(SleeperDsl sleeper) {
        sleeper.connectToInstanceAddOnlineTable(MAIN);
    }

    @Test
    void shouldPreSplitPartitionTreeAndBulkImportWithEmrServerlessByQueue(SleeperDsl sleeper) {
        // Given a source file and a table configured for pre-splitting
        sleeper.updateTableProperties(Map.of(
                BULK_IMPORT_MIN_LEAF_PARTITION_COUNT, "8",
                PARTITION_SPLIT_MIN_ROWS, "100"));
        Iterable<Row> rows = sleeper.generateNumberedRows().iterableOverRange(0, 10_000);
        sleeper.sourceFiles().create("file.parquet", rows);

        // When the source file is bulk imported
        sleeper.ingest().bulkImportByQueue().sendSourceFiles(BULK_IMPORT_EMR_SERVERLESS_JOB_QUEUE_URL, "file.parquet")
                .waitForJobs(PollWithRetries.intervalAndPollingTimeout(Duration.ofSeconds(30), Duration.ofMinutes(30)));

        // Then all rows are imported successfully
        assertThat(SystemTestSchema.sorted(sleeper.directQuery().allRowsInTable()))
                .containsExactlyElementsOf(rows);
        // And the partition tree is pre-split into the expected leaf partitions
        assertThat(sleeper.partitioning().tree().getLeafPartitions())
                .hasSize(8);
        // And the imported data is distributed across those partitions
        assertThat(sleeper.tableFiles().references())
                .hasSize(8);
    }

    @Test
    void shouldBulkImportOneRowWithEmrServerlessDirectly(SleeperDsl sleeper) {
        // Given a source file containing a single row
        sleeper.updateTableProperties(Map.of(BULK_IMPORT_MIN_LEAF_PARTITION_COUNT, "1"));
        Row row = new Row();
        row.put("key", "some-id");
        row.put("timestamp", 1234L);
        row.put("value", "Some value");
        row.put("nullable_value", null);

        // When the source file is bulk imported directly
        sleeper.sourceFiles().create("file.parquet", row);
        sleeper.ingest().directEmrServerless().sendSourceFiles("file.parquet")
                .waitForJobs(PollWithRetries.intervalAndPollingTimeout(Duration.ofSeconds(30), Duration.ofMinutes(30)));

        // Then the row is imported successfully
        assertThat(sleeper.directQuery().allRowsInTable())
                .containsExactly(row);
    }
}
