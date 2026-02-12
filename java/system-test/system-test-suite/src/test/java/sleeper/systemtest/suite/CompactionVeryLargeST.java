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

import sleeper.core.properties.model.DataEngine;
import sleeper.core.row.testutils.SortedRowsCheck;
import sleeper.core.statestore.AllReferencesToAllFiles;
import sleeper.core.statestore.FileReference;
import sleeper.core.util.PollWithRetries;
import sleeper.systemtest.dsl.SleeperDsl;
import sleeper.systemtest.dsl.extension.AfterTestReports;
import sleeper.systemtest.dsl.reporting.SystemTestReports;
import sleeper.systemtest.dsl.util.DataFileDuplications;
import sleeper.systemtest.dsl.util.SystemTestSchema;
import sleeper.systemtest.suite.testutil.SystemTest;
import sleeper.systemtest.suite.testutil.parallel.Expensive1;

import java.time.Duration;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.core.properties.table.TableProperty.COMPACTION_FILES_BATCH_SIZE;
import static sleeper.core.properties.table.TableProperty.DATAFUSION_S3_READAHEAD_ENABLED;
import static sleeper.core.properties.table.TableProperty.DATA_ENGINE;
import static sleeper.core.properties.table.TableProperty.TABLE_ONLINE;
import static sleeper.systemtest.configuration.SystemTestIngestMode.DIRECT;
import static sleeper.systemtest.dsl.query.QueryRange.range;
import static sleeper.systemtest.dsl.util.SystemTestSchema.DEFAULT_SCHEMA;
import static sleeper.systemtest.suite.fixtures.SystemTestInstance.COMPACTION_PERFORMANCE_DATAFUSION;

@SystemTest
// Expensive because it takes a long time to compact this many rows on fairly large ECS instances.
@Expensive1
public class CompactionVeryLargeST {

    @BeforeEach
    void setUp(SleeperDsl sleeper, AfterTestReports reporting) {
        sleeper.connectToInstanceNoTables(COMPACTION_PERFORMANCE_DATAFUSION);
        reporting.reportAlways(SystemTestReports.SystemTestBuilder::compactionTasksAndJobs);
    }

    @AfterEach
    void tearDown(SleeperDsl sleeper) {
        sleeper.compaction().scaleToZero();
    }

    @Test
    // This test takes about 2 hours to run, or an hour and a half if the Sleeper instance is already deployed.
    // We want to know compactions can deal with a very large amount of data.
    // This test is based on how much data we expect to be compacted at once.
    // The most data that can be compacted at once is the contents of a whole partition.
    // At time of writing, by default a partition will be split if it contains 1 billion rows.
    // To allow for the fact that partition splitting takes time, we test with 2 billion rows.
    void shouldRunVeryLargeCompaction(SleeperDsl sleeper) {
        // Given a table set to compact in batches of 40 files
        sleeper.tables().createWithProperties("test", DEFAULT_SCHEMA, Map.of(
                TABLE_ONLINE, "false",
                DATA_ENGINE, DataEngine.DATAFUSION_EXPERIMENTAL.toString(),
                COMPACTION_FILES_BATCH_SIZE, "40",
                // We've disabled readahead temporarily until the following bug is resolved:
                // https://github.com/gchq/sleeper/issues/5777
                DATAFUSION_S3_READAHEAD_ENABLED, "false"));
        // And 40 input files
        sleeper.systemTestCluster().runDataGenerationJobs(40,
                builder -> builder.ingestMode(DIRECT).rowsPerIngest(50_000_000),
                PollWithRetries.intervalAndPollingTimeout(Duration.ofSeconds(30), Duration.ofMinutes(30)))
                .waitForTotalFileReferences(40);
        // And we duplicate those 40 files so that we have that 10 times in total
        DataFileDuplications duplications = sleeper.ingest().toStateStore()
                .duplicateFilesOnSamePartitions(9);

        // When we run 10 compactions each with the same files
        sleeper.compaction()
                .createSeparateCompactionsForOriginalAndDuplicates(duplications)
                .waitForTasks(10)
                .waitForJobs(PollWithRetries.intervalAndPollingTimeout(Duration.ofSeconds(30), Duration.ofMinutes(30)));

        // Then
        AllReferencesToAllFiles files = sleeper.tableFiles().all();
        assertThat(files.streamFileReferences())
                .hasSize(10)
                .extracting(FileReference::getNumberOfRows)
                .allMatch(rows -> rows == 2_000_000_000L, "each file has 2 billion rows");
        assertThat(sleeper.reporting().compactionJobs().finishedStatistics())
                .matches(stats -> stats.isAllFinishedOneRunEach(10),
                        "compactions finished with one run each")
                .matches(stats -> stats.isAverageRunRowsPerSecondInRange(2_000_000, 4_000_000),
                        "meets expected performance");
        assertThat(files.getFilesWithReferences())
                .first().satisfies(file -> assertThat(
                        SortedRowsCheck.check(DEFAULT_SCHEMA, sleeper.getRows(file)))
                        .isEqualTo(SortedRowsCheck.sorted(2_000_000_000L)));
        // And the first query can be slower during warm-up
        assertThat(sleeper.query().webSocket()
                .timedByRowKey(SystemTestSchema.ROW_KEY_FIELD_NAME, range("aaaaaa", "aaaazz")))
                .satisfies(results -> {
                    assertThat(results.rows()).hasSizeBetween(30000, 50000);
                    assertThat(results.duration()).isLessThan(Duration.ofMinutes(1));
                });
        // And the second query should be faster
        assertThat(sleeper.query().webSocket()
                .timedByRowKey(SystemTestSchema.ROW_KEY_FIELD_NAME, range("aaaaaa", "aaaazz")))
                .satisfies(results -> {
                    assertThat(results.rows()).hasSizeBetween(30000, 50000);
                    assertThat(results.duration()).isLessThan(Duration.ofSeconds(2));
                });
    }
}
